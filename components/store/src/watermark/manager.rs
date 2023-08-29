use super::{stream_descriptor::StreamDescriptor, Watermark, WatermarkManager};
use crate::index::Indexer;
use rustc_hash::FxHashMap;
use std::sync::Arc;

pub(crate) struct DefaultWatermarkManager<I> {
    indexer: Arc<I>,
    streams: FxHashMap<u64, StreamDescriptor<I>>,
    watermarks: Vec<Arc<dyn Watermark>>,
}

impl<I> DefaultWatermarkManager<I>
where
    I: Indexer,
{
    pub(crate) fn new(indexer: Arc<I>) -> Self {
        Self {
            indexer,
            streams: FxHashMap::default(),
            watermarks: vec![],
        }
    }

    fn stream_mut(&mut self, stream_id: u64) -> &mut StreamDescriptor<I> {
        self.streams
            .entry(stream_id)
            .or_insert(StreamDescriptor::new(stream_id, Arc::clone(&self.indexer)))
    }

    pub(crate) fn add_watermark(&mut self, watermark: Arc<dyn Watermark>) {
        self.watermarks.push(watermark);
    }
}

impl<I> WatermarkManager for DefaultWatermarkManager<I>
where
    I: Indexer,
{
    fn on_index(&mut self, stream_id: u64, range: u32, offset: u64) {
        let stream = self.stream_mut(stream_id);
        stream.index(range, offset);
    }

    fn trim_stream(&mut self, stream_id: u64, offset: u64) {
        let stream = self.stream_mut(stream_id);
        stream.trim(offset);

        let min = self.min_wal_offset();
        for watermark in self.watermarks.iter() {
            watermark.set_min(min);
        }
    }

    fn on_data_offload(&mut self, stream_id: u64, range: u32, offset: u64, delta: u32) {
        let stream = self.stream_mut(stream_id);
        stream.data_offload(range, offset, delta);

        let offload = self.offload_wal_offset();
        for watermark in self.watermarks.iter() {
            watermark.set_offload(offload);
        }
    }

    fn add_range(&mut self, stream_id: u64, range: u32, start: u64, end: Option<u64>) {
        let stream = self.stream_mut(stream_id);
        stream.add_range(range, start, end);
    }

    fn delete_range(&mut self, stream_id: u64, range: u32) {
        let stream = self.stream_mut(stream_id);
        stream.delete_range(range);
    }

    fn seal_range(&mut self, stream_id: u64, range: u32, end: u64) {
        let stream = self.stream_mut(stream_id);
        stream.seal(range, end);
    }

    fn trim_range(&mut self, stream_id: u64, range: u32, start: u64) {
        let stream = self.stream_mut(stream_id);
        stream.trim_range(range, start);
    }

    fn min_wal_offset(&mut self) -> u64 {
        self.streams
            .iter_mut()
            .filter_map(|entry| entry.1.min_wal())
            .reduce(|prev, current| prev.min(current))
            .unwrap_or_default()
    }

    fn offload_wal_offset(&mut self) -> u64 {
        self.streams
            .iter()
            .filter_map(|entry| entry.1.wal_high())
            .reduce(|prev, current| prev.min(current))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        index::MockIndexer,
        watermark::{MockWatermark, WalWatermark, Watermark, WatermarkManager},
    };

    use super::DefaultWatermarkManager;

    #[test]
    fn test_new() {
        let indexer = MockIndexer::new();
        let indexer = Arc::new(indexer);

        let manager = DefaultWatermarkManager::new(indexer);
        assert!(manager.streams.is_empty());
        assert!(manager.watermarks.is_empty());
    }

    #[test]
    fn test_add_watermark() {
        let indexer = MockIndexer::new();
        let indexer = Arc::new(indexer);

        let mut manager = DefaultWatermarkManager::new(indexer);
        let watermark = MockWatermark::new();
        let watermark = Arc::new(watermark);
        manager.add_watermark(watermark);

        assert_eq!(manager.watermarks.len(), 1);
    }

    #[test]
    fn test_works() {
        let mut indexer = MockIndexer::new();
        indexer
            .expect_scan_wal_offset()
            .returning_st(|_stream_id, _range, offset, _end| Some(offset * 10));
        let indexer = Arc::new(indexer);

        let mut manager = DefaultWatermarkManager::new(indexer);

        let watermark = Arc::new(WalWatermark::new());
        manager.add_watermark(Arc::clone(&watermark) as Arc<dyn Watermark>);

        manager.add_range(42, 0, 0, None);
        manager.on_index(42, 0, 100);
        manager.trim_stream(42, 50);
        assert_eq!(manager.min_wal_offset(), 500);

        manager.on_data_offload(42, 0, 50, 50);
        assert_eq!(manager.offload_wal_offset(), 1000);
    }
}
