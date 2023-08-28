use log::{error, info, trace, warn};
use model::{range::RangeMetadata, stream::StreamMetadata};

use crate::error::ServiceError;

use super::range::Range;

pub(crate) struct Stream {
    metadata: StreamMetadata,

    pub(crate) ranges: Vec<Range>,
}

impl Stream {
    pub(crate) fn new(metadata: StreamMetadata) -> Self {
        Self {
            metadata,
            ranges: Vec::new(),
        }
    }

    /// Upstream layers may prefer to update stream metadata: changing replica, trimming minimum stream offset, etc.
    ///
    /// As a result, `Stream` shall delete or trim ranges under its administration.
    ///
    /// # Argument
    /// * `metadata` - Latest stream specification;
    pub(crate) fn update_metadata(&mut self, metadata: StreamMetadata) {
        debug_assert_eq!(self.metadata.stream_id, metadata.stream_id);
        self.metadata = metadata;
        self.trim_ranges(self.metadata.start_offset);
    }

    /// Trim ranges that falls behind minimum `offset`.
    ///
    /// # Argument
    /// * `offset` - Minimum stream offset;
    fn trim_ranges(&mut self, offset: u64) {
        self.ranges.retain_mut(|range| {
            if let Some(end) = range.metadata.end() {
                if end <= offset {
                    info!("Delete range[StreamId={}, index={}] as its end-offset is less than minimum stream min offset", self.metadata.stream_id, range.metadata.index());
                    return false;
                }
            }
            let metadata = &mut range.metadata;
            if metadata.start() < offset {
                metadata.trim_start(offset);
            }
            true
        });
    }

    fn verify_stream_id(&self, metadata: &RangeMetadata) -> Result<(), ServiceError> {
        if self.metadata.stream_id != metadata.stream_id() {
            error!(
                "Stream-id mismatch, stream_id={:?}, metadata={}",
                self.metadata.stream_id, metadata
            );
            return Err(ServiceError::Internal("Stream-id mismatch".to_owned()));
        }
        Ok(())
    }

    pub(crate) fn create_range(&mut self, metadata: RangeMetadata) {
        if self.verify_stream_id(&metadata).is_err() {
            return;
        }

        let res = self
            .ranges
            .iter()
            .find(|range| range.metadata.index() == metadata.index());

        if let Some(range) = res {
            if range.metadata == metadata {
                trace!("No-op, when creating range with the same metadata");
                return;
            } else {
                error!(
                    "Attempting to create inconsistent range. Prior range-metadata={}, attempted range-metadata={:?}",
                    range.metadata, metadata
                );
                return;
            }
        }

        if let Some(range) = self.ranges.iter().next_back() {
            if range.metadata.index() > metadata.index() {
                warn!(
                    "Attempting to create a range which should have been sealed: {:?}. Last range on current server: {:?}",
                    metadata, range.metadata
                );
            }
        }

        self.ranges.push(Range::new(metadata));
        self.sort();
    }

    pub(crate) fn remove_range(&mut self, metadata: &RangeMetadata) {
        info!(
            "Try to remove range[StreamId={}, index={}]",
            metadata.stream_id(),
            metadata.index()
        );
        self.ranges
            .extract_if(|range| range.metadata.index() == metadata.index())
            .count();
    }

    // Sort ranges
    fn sort(&mut self) {
        self.ranges
            .sort_by(|a, b| a.metadata.index().cmp(&b.metadata.index()));
    }

    pub(crate) fn seal(&mut self, metadata: &mut RangeMetadata) -> Result<(), ServiceError> {
        self.verify_stream_id(metadata)?;
        if let Some(range) = self.get_range_mut(metadata.index()) {
            range.seal(metadata);
            Ok(())
        } else {
            info!("Range does not exist, metadata={:?}. Create the sealed range on range-server directly", metadata);
            Err(ServiceError::NotFound(format!(
                "Range[{}#{}] is not found",
                metadata.stream_id(),
                metadata.index()
            )))
        }
    }

    pub(crate) fn get_range_mut(&mut self, index: i32) -> Option<&mut Range> {
        self.ranges
            .iter_mut()
            .find(|range| range.metadata.index() == index)
    }

    pub(crate) fn get_range(&self, index: i32) -> Option<&Range> {
        self.ranges
            .iter()
            .find(|range| range.metadata.index() == index)
    }

    pub(crate) fn has_range(&self, index: i32) -> bool {
        self.ranges
            .iter()
            .any(|range| range.metadata.index() == index)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use model::{range::RangeMetadata, stream::StreamMetadata, Batch};
    use protocol::rpc::header::StreamT;

    #[test]
    fn test_new() -> Result<(), Box<dyn Error>> {
        let mut stream = StreamT::default();
        stream.stream_id = 1;
        stream.replica = 1;
        stream.retention_period_ms = 1000;
        let stream = StreamMetadata::from(&stream);
        assert_eq!(stream.stream_id, 1);
        assert_eq!(stream.replica, 1);
        assert_eq!(stream.retention_period.as_millis(), 1000);
        Ok(())
    }

    #[test]
    fn test_create_range() -> Result<(), Box<dyn Error>> {
        let mut stream = StreamT::default();
        stream.stream_id = 1;
        stream.replica = 1;
        stream.retention_period_ms = 1000;
        let stream_metadata = StreamMetadata::from(&stream);
        let mut stream = super::Stream::new(stream_metadata);

        let range = RangeMetadata::new(1, 0, 0, 0, None);
        stream.create_range(range);

        assert_eq!(stream.ranges.len(), 1);

        Ok(())
    }

    #[test]
    fn test_update_metadata_trim_range() -> Result<(), Box<dyn Error>> {
        let mut stream_t = StreamT::default();
        stream_t.stream_id = 1;
        stream_t.replica = 1;
        stream_t.retention_period_ms = 1000;
        let stream_metadata = StreamMetadata::from(&stream_t);
        let mut stream = super::Stream::new(stream_metadata);

        let range = RangeMetadata::new(1, 0, 0, 0, Some(50));
        stream.create_range(range);

        let range = RangeMetadata::new(1, 1, 0, 50, Some(110));
        stream.create_range(range);

        let range = RangeMetadata::new(1, 2, 0, 110, None);
        stream.create_range(range);

        stream_t.start_offset = 100;
        let stream_metadata = StreamMetadata::from(&stream_t);
        stream.update_metadata(stream_metadata);

        // Deprecated ranges should have been removed
        assert!(stream.get_range(0).is_none());

        // Ranges, spanning across stream minimum offset, should be properly trimmed
        let range = stream.get_range(1).unwrap();
        assert_eq!(range.metadata.start(), 100);

        // Ranges, whose start offset is greater than stream minimum offset should not be affected.
        let range = stream.get_range(2).unwrap();
        assert_eq!(range.metadata.start(), 110);
        assert_eq!(range.metadata.end(), None);

        Ok(())
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct Foo {
        offset: u64,
        len: u32,
    }

    impl Batch for Foo {
        fn len(&self) -> u32 {
            self.len
        }

        fn offset(&self) -> u64 {
            self.offset
        }
    }

    #[tokio::test]
    async fn test_seal() -> Result<(), Box<dyn Error>> {
        let mut stream = StreamT::default();
        stream.stream_id = 1;
        stream.replica = 1;
        stream.retention_period_ms = 1000;
        let stream_metadata = StreamMetadata::from(&stream);
        let mut stream = super::Stream::new(stream_metadata);

        let range = RangeMetadata::new(1, 0, 0, 0, None);
        stream.create_range(range);

        let range = stream
            .get_range_mut(0)
            .expect("The first range should exist");
        range.window_mut().and_then(|window| {
            window
                .check_barrier(&Foo {
                    offset: 0,
                    len: 100,
                })
                .ok()
        });
        range.commit(100)?;
        let mut metadata = RangeMetadata::new(1, 0, 0, 0, Some(50));
        stream.seal(&mut metadata)?;

        let committed = stream
            .ranges
            .get(0)
            .expect("The first range should exist")
            .committed();
        assert_eq!(committed, Some(100));
        Ok(())
    }
}
