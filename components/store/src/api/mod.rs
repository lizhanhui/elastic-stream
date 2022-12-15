use async_trait::async_trait;
use monoio::buf::IoBuf;

use crate::error::StoreError;

pub struct PutResult {}

#[async_trait(?Send)]
pub trait AsyncStore {
    async fn put<T>(&mut self, buf: T) -> Result<PutResult, StoreError>
    where
        T: IoBuf;
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
