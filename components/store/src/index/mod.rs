#[cfg(any(test, feature = "mock"))]
use mockall::automock;
use tokio::sync::mpsc;

use model::range::RangeMetadata;

use crate::error::StoreError;
use crate::index::record::Record;

pub(crate) mod compaction;
pub(crate) mod driver;
pub(crate) mod indexer;
pub(crate) mod record;

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
    fn index(&self, record: &Record) -> Result<(), StoreError>;

    fn scan_wal_offset(
        &self,
        stream_id: u64,
        range: u32,
        offset: u64,
        end: Option<u64>,
    ) -> Option<u64>;

    fn scan_record_left_shift(
        &self,
        stream_id: u64,
        range: u32,
        offset: u64,
        max_offset: u64,
        max_bytes: u32,
    ) -> Result<Option<Vec<Record>>, StoreError>;

    fn get_wal_checkpoint(&self) -> Result<u64, StoreError>;

    fn advance_wal_checkpoint(&self, offset: u64) -> Result<(), StoreError>;

    fn flush(&self, wait: bool) -> Result<(), StoreError>;

    fn retrieve_max_key(&self, stream_id: u64, range: u32) -> Result<Option<Record>, StoreError>;
}
