use super::lock::Lock;
use crate::{
    error::{AppendError, FetchError, StoreError},
    index::{driver::IndexDriver, Indexer, MinOffset},
    io::{
        self,
        record::RecordType,
        segment::LogSegment,
        task::{
            IoTask::{self, Read, Write},
            WriteTask,
        },
        ReadTask,
    },
    offset_manager::WalOffsetManager,
    option::{ReadOptions, WriteOptions},
    AppendRecordRequest, AppendResult, FetchResult, Store,
};
use bytes::Buf;
use client::PlacementDriverIdGenerator;
use crossbeam::channel::{Receiver, Sender, TryRecvError};
use futures::future::join_all;
use log::{error, trace, warn};
use model::range::{RangeLifecycleEvent, RangeMetadata};
use observation::metrics::store_metrics::{
    RangeServerStatistics, STORE_APPEND_BYTES_COUNT, STORE_APPEND_COUNT,
    STORE_APPEND_LATENCY_HISTOGRAM, STORE_FAILED_APPEND_COUNT, STORE_FAILED_FETCH_COUNT,
    STORE_FETCH_BYTES_COUNT, STORE_FETCH_COUNT, STORE_FETCH_LATENCY_HISTOGRAM,
};
use std::{
    cell::{RefCell, UnsafeCell},
    collections::VecDeque,
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
    sync::Arc,
    thread::{Builder, JoinHandle},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

struct Shared {
    lock: Lock,

    /// The channel for server layer to communicate with io module.
    ///
    /// For sending a message from async to sync, you should use the standard library unbounded channel or crossbeam.
    /// https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html#communicating-between-sync-and-async-code
    sq_tx: Sender<IoTask>,

    /// The reference to index driver, which is used to communicate with index module.
    indexer: Arc<IndexDriver>,

    #[allow(dead_code)]
    wal_offset_manager: Arc<WalOffsetManager>,

    /// Expose underlying I/O Uring FD so that its worker pool may be shared with
    /// server layer I/O Uring instances.
    sharing_uring: RawFd,

    #[allow(dead_code)]
    join_handles: util::HandleJoiner,
}

type InflightAppends = VecDeque<local_sync::oneshot::Sender<Result<AppendResult, AppendError>>>;
type Rx = Receiver<Result<AppendResult, AppendError>>;

pub struct ElasticStore {
    config: Arc<config::Configuration>,
    shared: Arc<Shared>,
    cq_tx: Sender<Result<AppendResult, AppendError>>,
    cq_rx: Rc<Rx>,
    inflight_appends: Rc<UnsafeCell<InflightAppends>>,
}

impl ElasticStore {
    pub fn new(
        mut config: config::Configuration,
        recovery_completion_tx: oneshot::Sender<()>,
    ) -> Result<Self, StoreError> {
        let id_generator = Box::new(PlacementDriverIdGenerator::new(&config));

        let lock = Lock::new(&config, id_generator)?;

        // Fill server_id
        config.server.server_id = lock.id();
        let config = Arc::new(config);

        // Build wal offset manager
        let wal_offset_manager = Arc::new(WalOffsetManager::new());
        // Build index driver
        let indexer = Arc::new(IndexDriver::new(
            &config,
            Arc::clone(&wal_offset_manager) as Arc<dyn MinOffset>,
            config.store.rocksdb.flush_threshold,
        )?);

        // Clone indexer for IO thread
        let indexer_ = Arc::clone(&indexer);
        let cfg = Arc::clone(&config);
        let (sq_tx, sq_rx) = crossbeam::channel::unbounded();
        let (sender, receiver) = oneshot::channel();
        let io_thread_handle = Self::with_thread("IO", move || {
            if !core_affinity::set_for_current(core_affinity::CoreId {
                id: cfg.store.io_cpu,
            }) {
                error!("Failed to set affinity for IO thread");
            }
            let io = io::IO::new(&cfg, indexer_, sq_rx)?;
            let sharing_uring = io.as_raw_fd();

            let io = RefCell::new(io);
            if let Err(_e) = sender.send(sharing_uring) {
                error!("Failed to expose sharing_uring and task channel sender");
            }
            io::IO::run(io, recovery_completion_tx)
        })?;
        let sharing_uring = receiver
            .blocking_recv()
            .map_err(|_e| StoreError::Internal("Start".to_owned()))?;

        let mut handle_joiner = util::HandleJoiner::new();
        handle_joiner.push(io_thread_handle);

        let shared = Arc::new(Shared {
            lock,
            sq_tx,
            indexer,
            wal_offset_manager,
            sharing_uring,
            join_handles: handle_joiner,
        });

        let (cq_tx, cq_rx) = crossbeam::channel::unbounded();

        let store = Self {
            config,
            shared,
            cq_tx,
            cq_rx: Rc::new(cq_rx),
            inflight_appends: Rc::new(UnsafeCell::new(VecDeque::new())),
        };
        trace!("ElasticStore launched");
        Ok(store)
    }

    fn with_thread<F>(name: &str, task: F) -> Result<JoinHandle<()>, StoreError>
    where
        F: FnOnce() -> Result<(), StoreError> + Send + 'static,
    {
        let closure = move || {
            if let Err(e) = task() {
                eprintln!("{}", e);
                todo!("Log internal store error");
            }
        };
        let handle = Builder::new()
            .name(name.to_owned())
            .spawn(closure)
            .map_err(|_e| StoreError::IoUring)?;
        Ok(handle)
    }

    /// Send append request to IO module.
    ///
    /// * `request` - Append record request, which includes target stream_id, logical offset and serialized `Record` data.
    /// * `observer` - Oneshot sender, used to return `AppendResult` or propagate error.
    fn do_append(&self, request: AppendRecordRequest) {
        let task = WriteTask {
            stream_id: request.stream_id,
            range: request.range_index as u32,
            offset: request.offset,
            len: request.len,
            buffer: request.buffer,
            observer: self.cq_tx.clone(),
            written_len: None,
        };
        let io_task = Write(task);
        if let Err(e) = self.shared.sq_tx.send(io_task) {
            if let Write(task) = e.0 {
                if let Err(e) = task.observer.send(Err(AppendError::SubmissionQueue)) {
                    error!("Failed to propagate error: {:?}", e);
                }
            }
        }
    }
}

impl Store for ElasticStore {
    fn start(&self) {
        let cq = Rc::clone(&self.cq_rx);
        let inflight = Rc::clone(&self.inflight_appends);
        monoio::spawn(async move {
            let appends = unsafe { &mut *inflight.get() };
            'main: loop {
                // Try to reap append result as many as possible.
                loop {
                    match cq.try_recv() {
                        Ok(res) => {
                            trace!("Received append result {:?}", res);
                            if let Some(tx) = appends.pop_front() {
                                match tx.send(res) {
                                    Ok(_) => {
                                        trace!("Notify append result OK");
                                    }
                                    Err(e) => {
                                        warn!("Failed to notify append result {:?}", e);
                                    }
                                }
                            }
                        }
                        Err(TryRecvError::Empty) => {
                            trace!("AppendResultCQ is empty");
                            break;
                        }

                        Err(TryRecvError::Disconnected) => {
                            break 'main;
                        }
                    }
                }

                // This spinning task shall not take up too much run time of the executor.
                monoio::time::sleep(Duration::from_millis(1)).await;
            }
        });
    }

    async fn append(
        &self,
        _options: &WriteOptions,
        request: AppendRecordRequest,
    ) -> Result<AppendResult, AppendError> {
        let now = std::time::Instant::now();
        let len = request.buffer.len();
        let (tx, rx) = local_sync::oneshot::channel();
        let inflight = unsafe { &mut *self.inflight_appends.get() };
        inflight.push_back(tx);
        self.do_append(request);
        match rx.await.map_err(|_e| AppendError::ChannelRecv) {
            Ok(res) => {
                let latency = now.elapsed();
                STORE_APPEND_LATENCY_HISTOGRAM.observe(latency.as_micros() as f64);
                RangeServerStatistics::observe_append_latency(latency.as_millis() as i16);
                STORE_APPEND_COUNT.inc();
                STORE_APPEND_BYTES_COUNT.inc_by(len as u64);
                res
            }
            Err(e) => {
                STORE_FAILED_APPEND_COUNT.inc();
                Err(e)
            }
        }
    }

    async fn fetch(&self, options: ReadOptions) -> Result<FetchResult, FetchError> {
        let now = std::time::Instant::now();
        let (index_tx, index_rx) = oneshot::channel();
        self.shared.indexer.scan_record_handles(
            options.stream_id,
            options.range,
            options.offset as u64,
            options.max_offset,
            options.max_bytes as u32,
            index_tx,
        );

        let io_tx_cp = self.shared.sq_tx.clone();
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
                            error!("Failed to propagate error: {:?}", e);
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
                    Ok(Ok(mut res)) => {
                        // Strip storage record header
                        let mut record_prefix = util::bytes::advance_bytes(
                            &mut res.payload,
                            crate::RECORD_PREFIX_LENGTH as usize,
                        );

                        // Verify data integrity
                        if record_prefix.len() != crate::RECORD_PREFIX_LENGTH as usize {
                            error!("Data corrupted: Record does not even have a prefix");
                            return Err(FetchError::DataCorrupted);
                        }
                        let expected_crc32 = record_prefix.get_u32();

                        let length_type = record_prefix.get_u32();
                        let _type = length_type & 0xFF;
                        debug_assert_eq!(_type as u8, RecordType::Full.into());
                        let expected_length = length_type >> 8;
                        let actual_length = res.payload.iter().map(|buf| buf.len()).sum::<usize>();
                        if expected_length != actual_length as u32 {
                            error!(
                                "Data corrupted: Record length mismatch. Expected: {}, Actual: {}",
                                expected_length, actual_length
                            );
                            return Err(FetchError::DataCorrupted);
                        }

                        let segment_size = self.config.store.segment_size;
                        let base_file_offset = res.wal_offset as u64 / segment_size * segment_size;
                        let actual_crc =
                            LogSegment::checksum_record(&res.payload, base_file_offset);
                        if actual_crc != expected_crc32 {
                            error!(
                                "Data corrupted: CRC32 checksum failed. Expected: {}, Actual: {}",
                                expected_crc32, actual_crc
                            );
                            return Err(FetchError::DataCorrupted);
                        }
                        Ok(res)
                    }
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
            let mut final_result: Vec<_> = flattened_result.into_iter().flatten().collect();

            // Sort the result
            final_result.sort_by(|a, b| a.wal_offset.cmp(&b.wal_offset));

            STORE_FETCH_COUNT.inc();
            let latency = now.elapsed();
            STORE_FETCH_LATENCY_HISTOGRAM.observe(latency.as_micros() as f64);
            RangeServerStatistics::observe_fetch_latency(latency.as_millis() as i16);
            STORE_FETCH_BYTES_COUNT
                .inc_by(final_result.iter().map(|re| re.total_len()).sum::<usize>() as u64);
            return Ok(FetchResult {
                stream_id: options.stream_id,
                offset: options.offset,
                results: final_result,
            });
        }
        STORE_FAILED_FETCH_COUNT.inc();
        Err(FetchError::NoRecord)
    }

    /// List ranges of all streams that are served by this range server.
    ///
    /// Note this job is delegated to RocksDB threads.
    ///
    /// True that RocksDB may internally use asynchronous IO, but its public API is blocking and synchronous.
    /// Given that we do NOT accept any blocking code in range-server and store crates, we have to delegate these
    /// tasks to RocksDB threads and asynchronously await in tokio::sync::mpsc::unbounded channel.
    ///
    /// This method involves communication between sync and async code, remember to read
    /// https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html#communicating-between-sync-and-async-code
    async fn list<F>(&self, filter: F) -> Result<Vec<RangeMetadata>, StoreError>
    where
        F: Fn(&RangeMetadata) -> bool,
    {
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.shared.indexer.list_ranges(tx);

        let mut ranges = vec![];
        while let Some(range) = rx.recv().await {
            if filter(&range) {
                ranges.push(range);
            }
        }
        Ok(ranges)
    }

    /// List ranges of the specified stream.
    ///
    /// Rationale of delegating this job to RocksDB threads is exactly same to `list` ranges of all streams served by
    /// this range server.
    async fn list_by_stream<F>(
        &self,
        stream_id: i64,
        filter: F,
    ) -> Result<Vec<RangeMetadata>, StoreError>
    where
        F: Fn(&RangeMetadata) -> bool,
    {
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.shared.indexer.list_ranges_by_stream(stream_id, tx);
        let mut ranges = vec![];
        while let Some(range) = rx.recv().await {
            debug_assert_eq!(stream_id, range.stream_id());
            if filter(&range) {
                ranges.push(range);
            }
        }
        Ok(ranges)
    }

    /// The single writer has determined to seal the specified range.
    ///
    /// The `end` offset in metadata may be current range server specific and thus non-final.
    /// Additionally, even if the `end` offset is final, range replica may have incomplete data.
    ///
    /// Either case, we need to run the range integrity procedure to ensure we are having complete
    /// and integral replica.
    async fn seal(&self, range: RangeMetadata) -> Result<(), StoreError> {
        let (tx, rx) = oneshot::channel();
        self.shared.indexer.seal_range(range, tx);
        rx.await
            .map_err(|_e| StoreError::Internal("Channel error".to_owned()))
            .flatten()
    }

    async fn create(&self, range: RangeMetadata) -> Result<(), StoreError> {
        let (tx, rx) = oneshot::channel();
        self.shared.indexer.create_range(range, tx);
        rx.await
            .map_err(|_e| StoreError::Internal("Channel error".to_owned()))
            .flatten()
    }

    /// Find max record offset of the specified stream and range.
    ///
    /// # Returns
    /// `StoreError` - If something is wrong when accessing RocksDB;
    /// `Some(u64)` - If the max record offset is found;
    /// `None` - If there is no record of the given stream;
    fn get_range_end_offset(&self, stream_id: i64, range: u32) -> Result<Option<u64>, StoreError> {
        self.shared
            .indexer
            .retrieve_max_key(stream_id, range)
            .map(|buf| buf.map(|entry| entry.end_offset()))
    }

    fn id(&self) -> i32 {
        self.shared.lock.id()
    }

    fn config(&self) -> Arc<config::Configuration> {
        Arc::clone(&self.config)
    }

    async fn handle_range_event(&self, events: Vec<RangeLifecycleEvent>) {
        match self.shared.indexer.handle_range_event(events).await {
            Ok(deletable_offset) => {
                self.shared
                    .wal_offset_manager
                    .set_deletable_offset(deletable_offset);
            }
            Err(e) => {
                error!("Failed to handle range event: {}", e);
            }
        }
    }
}

impl AsRawFd for ElasticStore {
    /// FD of the underlying I/O Uring instance, for the purpose of sharing worker pool with other I/O Uring instances.
    fn as_raw_fd(&self) -> RawFd {
        self.shared.sharing_uring
    }
}

impl Clone for ElasticStore {
    fn clone(&self) -> Self {
        let (cq_tx, cq_rx) = crossbeam::channel::unbounded();
        Self {
            config: Arc::clone(&self.config),
            shared: Arc::clone(&self.shared),
            cq_tx,
            cq_rx: Rc::new(cq_rx),
            inflight_appends: Rc::new(UnsafeCell::new(VecDeque::new())),
        }
    }
}

/// Safety: Given that we implement a custom `Clone` and shared parts are immutable, `ElasticStore` is safe
/// to be marked as `Send`.
///
/// Warning `ElasticStore` is not thread-safe because `append` and `run` may concurrently modify `inflight_appends` otherwise.
///
/// `ElasticStore` is supposed to be used in a single thread runtime like `tokio-uring` or `monoio`, following thread-per-core design pattern.
unsafe impl Send for ElasticStore {}

/// Some tests for ElasticStore.
#[cfg(test)]
mod tests {
    use crate::{
        error::{AppendError, FetchError},
        io::task::SingleFetchResult,
        option::{ReadOptions, WriteOptions},
        store::{append_result::AppendResult, fetch_result::FetchResult},
        AppendRecordRequest, ElasticStore, Store,
    };
    use bytes::{Bytes, BytesMut};
    use futures::future::join_all;
    use log::trace;
    use mock_server::run_listener;
    use std::error::Error;
    use tokio::sync::oneshot;

    fn build_store(pd_address: &str, store_path: &str) -> ElasticStore {
        let mut config = config::Configuration {
            placement_driver: pd_address.to_owned(),
            ..Default::default()
        };
        config.store.path.set_base(store_path);
        config.check_and_apply().expect("Configuration is invalid");
        let (tx, rx) = oneshot::channel();
        let store = match ElasticStore::new(config, tx) {
            Ok(store) => store,
            Err(e) => {
                panic!("Failed to launch ElasticStore: {:?}", e);
            }
        };
        rx.blocking_recv().expect("Await recovery completion");
        store
    }

    /// Test the basic append and fetch operations.
    #[test]
    fn test_run_store() -> Result<(), Box<dyn Error>> {
        crate::log::try_init_log();
        let store_dir = tempfile::tempdir()?;
        let store_path = store_dir.path().to_str().unwrap().to_owned();

        let (stop_tx, stop_rx) = oneshot::channel();
        let (port_tx, port_rx) = oneshot::channel();

        let handle = std::thread::spawn(move || {
            tokio_uring::start(async {
                let port = run_listener().await;
                let _ = port_tx.send(port);
                let _ = stop_rx.await;
            });
        });

        let port = port_rx.blocking_recv()?;
        let pd_address = format!("localhost:{}", port);
        let store = build_store(&pd_address, store_path.as_str());

        tokio_uring::start(async move {
            store.start();
            let options = WriteOptions::default();
            let mut append_fs = vec![];
            (0..1024)
                .map(|i| AppendRecordRequest {
                    stream_id: 1,
                    range_index: 0,
                    offset: i,
                    len: 1,
                    buffer: Bytes::from(format!("{}-{}", "hello, world", i)),
                })
                .for_each(|req| {
                    let append_f = store.append(&options, req);
                    append_fs.push(append_f)
                });

            let append_rs: Vec<Result<AppendResult, AppendError>> = join_all(append_fs).await;

            // Case One: Fetch all records one by one.
            {
                let mut fetch_futures = vec![];
                append_rs.iter().for_each(|res| {
                    // Log the append result
                    match res {
                        Ok(res) => {
                            trace!("Append result: {:?}", res);
                            let options = ReadOptions {
                                stream_id: 1,
                                range: 0,
                                offset: res.offset,
                                max_offset: 1024,
                                max_bytes: 1,
                                max_wait_ms: 1000,
                            };
                            let fetch_future = store.fetch(options);
                            fetch_futures.push(fetch_future);
                        }
                        Err(e) => {
                            panic!("Append error: {:?}", e);
                        }
                    }
                });

                let fetch_rs: Vec<Result<FetchResult, FetchError>> = join_all(fetch_futures).await;
                assert!(fetch_rs.len() == append_rs.len());
                fetch_rs.iter().for_each(|res| {
                    let res = res.as_ref().expect("Fetch should not fail");
                    let mut res_payload = BytesMut::new();
                    res.results.iter().for_each(|r| {
                        res_payload.extend_from_slice(&copy_single_fetch_result(r));
                    });

                    assert_eq!(
                        Bytes::copy_from_slice(&res_payload[..]),
                        Bytes::from(format!("{}-{}", "hello, world", res.offset))
                    );
                });
            }

            // Case Two: Fetch all records through a single call
            {
                let mut fetch_futures = vec![];
                let options = ReadOptions {
                    stream_id: 1,
                    range: 0,
                    offset: 0,
                    max_offset: 1024,
                    max_bytes: 1024 * 1024 * 1024,
                    max_wait_ms: 1000,
                };
                let fetch_future = store.fetch(options);
                fetch_futures.push(fetch_future);
                let fetch_results: Vec<Result<FetchResult, FetchError>> =
                    join_all(fetch_futures).await;
                assert_eq!(1, fetch_results.len());
                let fetch_result = fetch_results[0].as_ref().unwrap();
                fetch_result.results.iter().enumerate().for_each(|(i, r)| {
                    let mut res_payload = BytesMut::new();
                    res_payload.extend_from_slice(&copy_single_fetch_result(r));
                    assert_eq!(
                        Bytes::copy_from_slice(&res_payload[..]),
                        Bytes::from(format!("{}-{}", "hello, world", i))
                    );
                });
            }

            drop(store);
        });
        let _ = stop_tx.send(());
        let _ = handle.join();
        Ok(())
    }

    /// Copy all buffers from a SingleFetchResult.
    fn copy_single_fetch_result(res: &SingleFetchResult) -> Bytes {
        let mut res_payload = BytesMut::new();
        res.iter().for_each(|r| {
            res_payload.extend_from_slice(&r[..]);
        });
        res_payload.freeze()
    }
}
