use std::{
    cell::RefCell,
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
    thread::{Builder, JoinHandle},
};

use crate::{
    error::{PutError, StoreError},
    io::{self, task::IoTask},
    ops::{put::PutResult, Get, Put, Scan},
    option::{ReadOptions, WalPath, WriteOptions},
    Record, Store,
};
use core_affinity::CoreId;
use crossbeam::channel::Sender;
use futures::Future;
use slog::{trace, Logger};
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
        let mut opt = io::Options::default();

        // Customize IO options from store options.
        let size_10g = 10u64 * (1 << 30);
        opt.add_wal_path(WalPath::new("/data/store", size_10g)?);

        let io = io::IO::new(&mut opt, log.clone())?;
        let sharing_uring = io.as_raw_fd();
        let tx = io.sender.clone();

        let io = RefCell::new(io);

        // IO thread will be left in detached state.
        let _io_thread_handle = Self::with_thread("IO", move || io::IO::run(io), None)?;
        let store = Self {
            tx,
            sharing_uring,
            log,
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

    fn append(
        &self,
        record: Record,
        response_observer: oneshot::Sender<Result<PutResult, PutError>>,
    ) {
    }
}

impl Store for ElasticStore {
    // type PutOp = impl Future<Output = Result<PutResult, PutError>>;
    type PutOp = impl Future<Output = Result<PutResult, PutError>>;

    fn put(&self, opt: WriteOptions, record: Record) -> Put<Self::PutOp>
    where
        <Self as Store>::PutOp: Future<Output = Result<PutResult, PutError>>,
    {
        let (sender, receiver) = oneshot::channel();

        self.append(record, sender);

        let inner = async {
            match receiver.await.map_err(|_e| PutError::ChannelRecv) {
                Ok(res) => res,
                Err(e) => Err(e),
            }
        };

        Put { inner }
    }

    fn get(&self, options: ReadOptions) -> Get {
        todo!()
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
