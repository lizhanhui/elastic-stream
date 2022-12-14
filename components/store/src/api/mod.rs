use std::future::Future;

use crate::error::StoreError;

pub struct PutResult {}

pub trait AsyncStore {
    type PutFuture<'a>: Future<Output = Result<PutResult, StoreError>>
    where
        Self: 'a;

    fn put(&mut self, buf: &[u8]) -> Self::PutFuture<'_>;
}
