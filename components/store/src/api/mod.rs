use std::future::Future;

use crate::error::StoreError;

pub struct PutResult {}

pub trait AsyncStore {
    type PutFuture<'a>: Future<Output = Result<PutResult, StoreError>>
    where
        Self: 'a;

    fn put(&mut self, buf: &[u8]) -> Self::PutFuture<'_>;
}

pub trait Segment {}

pub struct WriteCursor {
    pub(crate) write: u64,
    pub(crate) commit: u64,
}

impl WriteCursor {
    pub fn new() -> Self {
        Self {
            write: 0,
            commit: 0,
        }
    }
}
