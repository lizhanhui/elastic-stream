use std::{
    cell::RefCell,
    os::fd::{AsRawFd, RawFd},
    sync::Arc,
    thread::{Builder, JoinHandle},
};

use crate::{
    error::{AppendError, FetchError, StoreError},
    index::{driver::IndexDriver, MinOffset},
    io::{
        self,
        task::{
            IoTask::{self, Read, Write},
            WriteTask,
        },
        ReadTask,
    },
    offset_manager::WalOffsetManager,
    option::{ReadOptions, StoreOptions, WriteOptions},
    AppendRecordRequest, Store,
};
use core_affinity::CoreId;
use crossbeam::channel::Sender;
use futures::future::join_all;
use model::range::StreamRange;
use slog::{error, trace, Logger};
use tokio::sync::{mpsc, oneshot};

use super::{append_result::AppendResult, fetch_result::FetchResult};

#[derive(Clone)]
pub struct ElasticStore {
    /// The channel for server layer to communicate with io module.
    ///
    /// For sending a message from async to sync, you should use the standard library unbounded channel or crossbeam.
    /// https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html#communicating-between-sync-and-async-code
    io_tx: Sender<IoTask>,

    /// The reference to index driver, which is used to communicate with index module.
    indexer: Arc<IndexDriver>,

    wal_offset_manager: Arc<WalOffsetManager>,

    /// Expose underlying I/O Uring FD so that its worker pool may be shared with
    /// server layer I/O Uring instances.
    sharing_uring: RawFd,

    log: Logger,
}

impl ElasticStore {
    pub fn new(log: Logger, options: StoreOptions) -> Result<Self, StoreError> {
        let logger = log.clone();
        let mut opt = io::Options::default();

        // Customize IO options from store options.
        opt.add_wal_path(options.store_path);
        opt.metadata_path = options.metadata_path;

        // Build wal offset manager
        let wal_offset_manager = Arc::new(WalOffsetManager::new());

        // Build index driver
        let indexer = Arc::new(IndexDriver::new(
            log.clone(),
            &opt.metadata_path,
            Arc::clone(&wal_offset_manager) as Arc<dyn MinOffset>,
        )?);

        let (sender, receiver) = oneshot::channel();

        // IO thread will be left in detached state.
        // Copy a indexer
        let indexer_cp = Arc::clone(&indexer);
        let _io_thread_handle = Self::with_thread(
            "IO",
            move || {
                let log = log.clone();
                let mut io = io::IO::new(&mut opt, indexer_cp, log.clone())?;
                let sharing_uring = io.as_raw_fd();
                let tx = io
                    .sender
                    .take()
                    .ok_or(StoreError::Configuration("IO channel".to_owned()))?;

                let io = RefCell::new(io);
                if let Err(e) = sender.send((tx, sharing_uring)) {
                    error!(
                        log,
                        "Failed to expose sharing_uring and task channel sender"
                    );
                }
                io::IO::run(io)
            },
            None,
        )?;
        let (tx, sharing_uring) = receiver
            .blocking_recv()
            .map_err(|e| StoreError::Internal("Start".to_owned()))?;

        let store = Self {
            io_tx: tx,
            indexer,
            wal_offset_manager,
            sharing_uring,
            log: logger,
        };
        trace!(store.log, "ElasticStore launched");
        Ok(store)
    }

    fn with_thread<F>(
        name: &str,
        task: F,
        affinity: Option<CoreId>,
    ) -> Result<JoinHandle<()>, StoreError>
    where
        F: FnOnce() -> Result<(), StoreError> + Send + 'static,
    {
        if let Some(core) = affinity {
            let closure = move || {
                if !core_affinity::set_for_current(core) {
                    todo!("Log error when setting core affinity");
                }
                if let Err(_e) = task() {
                    todo!("Log internal store error");
                }
            };
            let handle = Builder::new()
                .name(name.to_owned())
                .spawn(closure)
                .map_err(|_e| StoreError::IoUring)?;
            Ok(handle)
        } else {
            let closure = move || {
                if let Err(_e) = task() {
                    todo!("Log internal store error");
                }
            };
            let handle = Builder::new()
                .name(name.to_owned())
                .spawn(closure)
                .map_err(|_e| StoreError::IoUring)?;
            Ok(handle)
        }
    }

    /// Send append request to IO module.
    ///
    /// * `request` - Append record request, which includes target stream_id, logical offset and serialized `Record` data.
    /// * `observer` - Oneshot sender, used to return `AppendResult` or propagate error.
    fn do_append(
        &self,
        request: AppendRecordRequest,
        observer: oneshot::Sender<Result<AppendResult, AppendError>>,
    ) {
        let task = WriteTask {
            stream_id: request.stream_id,
            offset: request.offset,
            buffer: request.buffer,
            observer,
            written_len: None,
        };
        let io_task = Write(task);
        if let Err(e) = self.io_tx.send(io_task) {
            if let Write(task) = e.0 {
                if let Err(e) = task.observer.send(Err(AppendError::SubmissionQueue)) {
                    error!(self.log, "Failed to propagate error: {:?}", e);
                }
            }
        }
    }
}

impl Store for ElasticStore {
    async fn append(
        &self,
        options: WriteOptions,
        request: AppendRecordRequest,
    ) -> Result<AppendResult, AppendError> {
        let (sender, receiver) = oneshot::channel();

        self.do_append(request, sender);

        match receiver.await.map_err(|_e| AppendError::ChannelRecv) {
            Ok(res) => res,
            Err(e) => Err(e),
        }
    }

    async fn fetch(&self, options: ReadOptions) -> Result<FetchResult, FetchError> {
        let (index_tx, index_rx) = oneshot::channel();
        self.indexer.scan_record_handles(
            options.stream_id,
            options.offset as u64,
            options.max_bytes as u32,
            index_tx,
        );

        let io_tx_cp = self.io_tx.clone();
        let logger = self.log.clone();
        let scan_res = match index_rx.await.map_err(|_e| FetchError::TranslateIndex) {
            Ok(res) => res.map_err(|_e| FetchError::TranslateIndex),
            Err(e) => Err(e),
        }?;

        if let Some(handles) = scan_res {
            let mut io_receiver = Vec::with_capacity(handles.len());
            for handle in handles {
                let (sender, receiver) = oneshot::channel();
                let io_task = ReadTask {
                    stream_id: options.stream_id,
                    wal_offset: handle.wal_offset,
                    len: handle.len,
                    observer: sender,
                };

                if let Err(e) = io_tx_cp.send(Read(io_task)) {
                    if let Read(io_task) = e.0 {
                        if let Err(e) = io_task.observer.send(Err(FetchError::SubmissionQueue)) {
                            error!(logger, "Failed to propagate error: {:?}", e);
                        }
                    }
                }

                io_receiver.push(receiver);
            }

            // Join all IO tasks.
            let io_result = join_all(io_receiver).await;

            let flattened_result: Vec<_> = io_result
                .into_iter()
                .map(|res| match res {
                    Ok(Ok(res)) => Ok(res),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(FetchError::ChannelRecv), // Channel receive error branch
                })
                .collect();

            // Take the first error from the flattened result, and return it.
            let first_error = flattened_result.iter().find(|res| res.is_err());
            if let Some(Err(e)) = first_error {
                return Err(e.clone());
            }

            // Collect all successful IO results, and sort it by the wal offset
            let mut result: Vec<_> = flattened_result.into_iter().flatten().collect();

            // Sort the result
            result.sort_by(|a, b| a.wal_offset.cmp(&b.wal_offset));

            // Extract the payload from the result, and assemble the final result.
            let final_result: Vec<_> = result.into_iter().flat_map(|res| res.payload).collect();

            return Ok(FetchResult {
                stream_id: options.stream_id,
                offset: options.offset,
                payload: final_result,
            });
        }

        Err(FetchError::NoRecord)
    }

    /// List ranges of all streams that are served by this data node.
    ///
    /// Note this job is delegated to RocksDB threads.
    ///
    /// True that RocksDB may internally use asynchronous IO, but its public API is blocking and synchronous.
    /// Given that we do NOT accept any blocking code in data-node and store crates, we have to delegate these
    /// tasks to RocksDB threads and asynchronously await in tokio::sync::mpsc::unbounded channel.
    ///
    /// This method involves communication between sync and async code, remember to read
    /// https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html#communicating-between-sync-and-async-code
    async fn list<F>(&self, filter: F) -> Result<Vec<StreamRange>, StoreError>
    where
        F: Fn(&StreamRange) -> bool,
    {
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.indexer.list_ranges(tx);

        let mut ranges = vec![];
        loop {
            match rx.recv().await {
                Some(range) => {
                    if filter(&range) {
                        ranges.push(range);
                    }
                }
                None => {
                    break;
                }
            }
        }
        Ok(ranges)
    }

    /// List ranges of the specified stream.
    ///
    /// Rationale of delegating this job to RocksDB threads is exactly same to `list` ranges of all streams served by
    /// this data node.
    async fn list_by_stream<F>(
        &self,
        stream_id: i64,
        filter: F,
    ) -> Result<Vec<StreamRange>, StoreError>
    where
        F: Fn(&StreamRange) -> bool,
    {
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.indexer.list_ranges_by_stream(stream_id, tx);

        let mut ranges = vec![];
        loop {
            match rx.recv().await {
                Some(range) => {
                    debug_assert_eq!(stream_id, range.stream_id());
                    if filter(&range) {
                        ranges.push(range);
                    }
                }
                None => {
                    break;
                }
            }
        }
        Ok(ranges)
    }

    async fn seal(&self, range: StreamRange) -> Result<(), StoreError> {
        todo!()
    }

    async fn create(&self, range: StreamRange) -> Result<(), StoreError> {
        todo!()
    }
}

impl AsRawFd for ElasticStore {
    /// FD of the underlying I/O Uring instance, for the purpose of sharing worker pool with other I/O Uring instances.
    fn as_raw_fd(&self) -> RawFd {
        self.sharing_uring
    }
}

/// Some tests for ElasticStore.
#[cfg(test)]
mod tests {
    use std::path::Path;
    use tokio::time::{sleep, Duration};

    use bytes::{Bytes, BytesMut};
    use futures::future::join_all;
    use slog::trace;
    use tokio::sync::oneshot;

    use crate::{
        error::{AppendError, FetchError},
        option::{ReadOptions, StoreOptions, WalPath, WriteOptions},
        store::{append_result::AppendResult, fetch_result::FetchResult},
        AppendRecordRequest, ElasticStore, Store,
    };

    fn build_store(store_path: &str, index_path: &str) -> ElasticStore {
        let log = test_util::terminal_logger();

        let size_10g = 10u64 * (1 << 30);

        let wal_path = WalPath::new(store_path, size_10g).unwrap();

        let options = StoreOptions::new(&wal_path, index_path.to_string());
        let store = match ElasticStore::new(log.clone(), options) {
            Ok(store) => store,
            Err(e) => {
                panic!("Failed to launch ElasticStore: {:?}", e);
            }
        };
        store
    }
    /// Test the basic append and fetch operations.
    #[tokio::test]
    async fn test_run_store() {
        let log = test_util::terminal_logger();

        let store_dir = test_util::create_random_path().unwrap();
        let index_dir = test_util::create_random_path().unwrap();
        let store_path = String::from(store_dir.as_path().to_str().unwrap());
        let index_path = String::from(index_dir.as_path().to_str().unwrap());

        let store_path_g = store_path.clone();
        let index_path_g = index_path.clone();
        let _store_dir_guard =
            test_util::DirectoryRemovalGuard::new(log.clone(), &Path::new(store_path_g.as_str()));
        let _index_dir_guard =
            test_util::DirectoryRemovalGuard::new(log.clone(), &Path::new(index_path_g.as_str()));

        let (tx, rx) = oneshot::channel();

        let _ = std::thread::spawn(move || {
            let store = build_store(store_path.as_str(), index_path.as_str());
            let send_r = tx.send(store);
            if let Err(_) = send_r {
                panic!("Failed to send store");
            }
        });

        let store = rx.await.unwrap();

        let mut append_fs = vec![];
        (0..2)
            .into_iter()
            .map(|i| AppendRecordRequest {
                stream_id: 1,
                offset: i,
                buffer: Bytes::from(format!("{}-{}", "hello, world", i)),
            })
            .for_each(|req| {
                let options = WriteOptions::default();
                let append_f = store.append(options, req);
                append_fs.push(append_f)
            });

        let append_rs: Vec<Result<AppendResult, AppendError>> = join_all(append_fs).await;

        let mut fetch_fs = vec![];

        append_rs.iter().for_each(|res| {
            // Log the append result
            match res {
                Ok(res) => {
                    trace!(store.log, "Append result: {:?}", res);
                    let options = ReadOptions {
                        stream_id: 1,
                        offset: res.offset,
                        // TODO: Currently, indexer explain the max_bytes as number of records, not bytes.
                        max_bytes: 1,
                        max_wait_ms: 1000,
                    };
                    let fetch_f = store.fetch(options);
                    fetch_fs.push(fetch_f);
                }
                Err(e) => {
                    panic!("Append error: {:?}", e);
                }
            }
        });

        let fetch_rs: Vec<Result<FetchResult, FetchError>> = join_all(fetch_fs).await;
        fetch_rs.iter().for_each(|res| {
            // Assert the fetch result with the append result.
            match res {
                Ok(res) => {
                    trace!(store.log, "Fetch result: {:?}", res);

                    let mut res_payload = BytesMut::new();
                    res.payload.iter().for_each(|r| {
                        res_payload.extend_from_slice(&r[..]);
                    });

                    assert_eq!(
                        Bytes::copy_from_slice(&res_payload[8..]),
                        Bytes::from(format!("{}-{}", "hello, world", res.offset))
                    );
                }
                Err(e) => {
                    panic!("Fetch error: {:?}", e);
                }
            }
        });

        drop(store);
        sleep(Duration::from_millis(1000)).await;
    }
}
