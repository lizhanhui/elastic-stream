use std::{
    cell::RefCell,
    os::fd::{AsRawFd, RawFd},
    thread::{Builder, JoinHandle},
};

use crate::{
    error::{AppendError, FetchError, StoreError},
    io::{
        self,
        task::{
            IoTask::{self, Read, Write},
            WriteTask,
        },
        ReadTask,
    },
    ops::{append::AppendResult, fetch::FetchResult, Append, Fetch, Scan},
    option::{ReadOptions, WalPath, WriteOptions},
    AppendRecordRequest, Store,
};
use core_affinity::CoreId;
use crossbeam::channel::Sender;
use futures::Future;
use slog::{error, trace, Logger};
use tokio::sync::oneshot;

#[derive(Clone)]
pub struct ElasticStore {
    /// The channel for server layer to communicate with storage.
    tx: Sender<IoTask>,

    /// Expose underlying I/O Uring FD so that its worker pool may be shared with
    /// server layer I/O Uring instances.
    sharing_uring: RawFd,

    log: Logger,
}

impl ElasticStore {
    pub fn new(log: Logger) -> Result<Self, StoreError> {
        let logger = log.clone();
        let mut opt = io::Options::default();

        // Customize IO options from store options.
        let size_10g = 10u64 * (1 << 30);
        opt.add_wal_path(WalPath::new("/data/store", size_10g)?);

        let (sender, receiver) = oneshot::channel();

        // IO thread will be left in detached state.
        let _io_thread_handle = Self::with_thread(
            "IO",
            move || {
                let log = log.clone();
                let mut io = io::IO::new(&mut opt, log.clone())?;
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
            tx,
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
        };
        let io_task = Write(task);
        if let Err(e) = self.tx.send(io_task) {
            if let Write(task) = e.0 {
                if let Err(e) = task.observer.send(Err(AppendError::SubmissionQueue)) {
                    error!(self.log, "Failed to propagate error: {:?}", e);
                }
            }
        }
    }

    /// Send fetch request to IO module.
    ///
    /// * `options` - Fetch options, which includes target stream_id, logical offset and max_bytes to read.
    /// * `observer` - Oneshot sender, used to return `FetchResult` or propagate error.
    fn do_fecth(
        &self,
        options: ReadOptions,
        observer: oneshot::Sender<Result<FetchResult, FetchError>>,
    ) {
        let task = ReadTask {
            stream_id: options.stream_id,
            offset: options.offset as u64,
            len: options.max_bytes as u32,
            observer,
        };

        let io_task = Read(task);
        if let Err(e) = self.tx.send(io_task) {
            if let Read(task) = e.0 {
                if let Err(e) = task.observer.send(Err(FetchError::SubmissionQueue)) {
                    error!(self.log, "Failed to propagate error: {:?}", e);
                }
            }
        }
    }
}

impl Store for ElasticStore {
    type AppendOp = impl Future<Output = Result<AppendResult, AppendError>>;
    type FetchOp = impl Future<Output = Result<FetchResult, FetchError>>;

    fn append(&self, options: WriteOptions, request: AppendRecordRequest) -> Append<Self::AppendOp>
    where
        <Self as Store>::AppendOp: Future<Output = Result<AppendResult, AppendError>>,
    {
        let (sender, receiver) = oneshot::channel();

        self.do_append(request, sender);

        let inner = async {
            match receiver.await.map_err(|_e| AppendError::ChannelRecv) {
                Ok(res) => res,
                Err(e) => Err(e),
            }
        };

        Append { inner }
    }

    fn fetch(&self, options: ReadOptions) -> Fetch<<Self as Store>::FetchOp>
    where
        <Self as Store>::FetchOp: Future<Output = Result<FetchResult, FetchError>>,
    {
        let (sender, receiver) = oneshot::channel();
        self.do_fecth(options, sender);

        let inner = async {
            match receiver.await.map_err(|_e| FetchError::ChannelRecv) {
                Ok(res) => res,
                Err(e) => Err(e),
            }
        };

        Fetch { inner }
    }

    fn scan(&self, options: ReadOptions) -> Scan {
        todo!()
    }
}

impl AsRawFd for ElasticStore {
    /// FD of the underlying I/O Uring instance, for the purpose of sharing worker pool with other I/O Uring instances.
    fn as_raw_fd(&self) -> RawFd {
        self.sharing_uring
    }
}
