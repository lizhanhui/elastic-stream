pub(crate) mod fetcher;
pub(crate) mod manager;
pub(crate) mod range;
pub(crate) mod stream;
pub(crate) mod window;

use crate::error::ServiceError;
#[cfg(test)]
use mockall::automock;
use model::{
    error::EsError, object::ObjectMetadata, range::RangeMetadata, replica::RangeProgress, Batch,
};
use store::error::AppendError;

#[cfg_attr(test, automock)]
pub(crate) trait RangeManager {
    async fn start(&mut self) -> Result<(), EsError>;

    /// Create a new range for the specified stream.
    fn create_range(&mut self, range: RangeMetadata) -> Result<(), ServiceError>;

    /// Commit work-in-progress append requests
    fn commit(
        &mut self,
        stream_id: i64,
        range_index: i32,
        offset: u64,
        last_offset_delta: u32,
        bytes_len: u32,
    ) -> Result<(), ServiceError>;

    /// Seal the given range.
    ///
    /// Two cases are involved:
    /// - Active seal operation where range metadata has end offset filled;
    /// - Passive seal operation where end of range metadata is `None`;
    fn seal(&mut self, range: &mut RangeMetadata) -> Result<(), ServiceError>;

    /// Check if current server is prepared to process the given append request.
    ///
    /// It is true that the underlying `BufferedStore` is capable of handling out-of-order
    /// append requests, we still prefer to accept append request orderly at the moment.
    fn check_barrier<R>(
        &mut self,
        stream_id: i64,
        range_index: i32,
        req: &R,
    ) -> Result<(), AppendError>
    where
        R: Batch + Ord + 'static;

    /// Check if the specified range is being served.
    fn has_range(&self, stream_id: u64, index: u32) -> bool;

    /// Get objects that in the specified range.
    /// return (objects, cover_all)
    async fn get_objects(
        &self,
        stream_id: u64,
        range_index: u32,
        start_offset: u64,
        end_offset: u64,
        size_hint: u32,
    ) -> (Vec<ObjectMetadata>, bool);

    async fn get_range_progress(&self) -> Vec<RangeProgress>;
}
