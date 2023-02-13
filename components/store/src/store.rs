use std::thread::{Builder, JoinHandle};

use crate::{
    error::{PutError, StoreError},
    io,
    ops::{put::PutResult, Get, Put, Scan},
    option::{ReadOptions, WriteOptions},
    Record, Store,
};
use core_affinity::CoreId;
use crossbeam::channel::Sender;
use futures::Future;
use tokio::sync::oneshot::{self, error::RecvError};

#[derive(Clone)]
pub struct ElasticStore {
    tx: Sender<()>,
}

impl ElasticStore {
    pub fn new() -> Result<Self, StoreError> {
        let mut opt = io::Options::default();
        let mut io = io::IO::new(&mut opt)?;
        let tx = io.sender.clone();
        let _io_thread_handle = Self::with_thread("IO", move || io.run(), None)?;
        Ok(Self { tx })
    }

    fn with_thread<F>(
        name: &str,
        task: F,
        affinity: Option<CoreId>,
    ) -> Result<JoinHandle<()>, StoreError>
    where
        F: FnOnce() + Send + 'static,
    {
        if let Some(core) = affinity {
            let closure = move || {
                if !core_affinity::set_for_current(core) {
                    todo!("Log error when setting core affinity");
                }
                task();
            };
            let handle = Builder::new()
                .name(name.to_owned())
                .spawn(closure)
                .map_err(|_e| StoreError::IoUring)?;
            Ok(handle)
        } else {
            let closure = move || {
                task();
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

        let put = Put { inner };
        put
    }

    fn get(&self, options: ReadOptions) -> Get {
        todo!()
    }

    fn scan(&self, options: ReadOptions) -> Scan {
        todo!()
    }
}
