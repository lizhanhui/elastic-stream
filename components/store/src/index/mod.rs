use crate::error::StoreError;
use model::range::RangeMetadata;
use tokio::sync::mpsc;

use self::{entry::IndexEntry, record_handle::RecordHandle};

#[cfg(any(test, feature = "mock"))]
use mockall::automock;

pub(crate) mod compaction;
pub(crate) mod driver;
pub(crate) mod entry;
pub(crate) mod indexer;
pub(crate) mod record_handle;

/// Trait of local range manger.
pub trait LocalRangeManager {
    // TODO: error propagation
    fn list_by_stream(&self, stream_id: u64, tx: mpsc::UnboundedSender<RangeMetadata>);

    // TODO: error propagation
    fn list(&self, tx: mpsc::UnboundedSender<RangeMetadata>);

    fn seal(&self, stream_id: u64, range: &RangeMetadata) -> Result<(), StoreError>;

    fn add(&self, stream_id: u64, range: &RangeMetadata) -> Result<(), StoreError>;
}

/// Definition of core storage trait.
#[cfg_attr(any(test, feature = "mock"), automock)]
pub(crate) trait Indexer {
    fn index(
        &self,
        stream_id: u64,
        range: u32,
        offset: u64,
        handle: &RecordHandle,
    ) -> Result<(), StoreError>;

    fn scan_wal_offset(
        &self,
        stream_id: u64,
        range: u32,
        offset: u64,
        end: Option<u64>,
    ) -> Option<u64>;

    fn scan_record_handles_left_shift(
        &self,
        stream_id: u64,
        range: u32,
        offset: u64,
        max_offset: u64,
        max_bytes: u32,
    ) -> Result<Option<Vec<RecordHandle>>, StoreError>;

    fn get_wal_checkpoint(&self) -> Result<u64, StoreError>;

    fn advance_wal_checkpoint(&self, offset: u64) -> Result<(), StoreError>;

    fn flush(&self, wait: bool) -> Result<(), StoreError>;

    fn retrieve_max_key(
        &self,
        stream_id: u64,
        range: u32,
    ) -> Result<Option<IndexEntry>, StoreError>;
}
