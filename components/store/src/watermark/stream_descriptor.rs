use std::{collections::VecDeque, sync::Arc};

use super::{range_descriptor::RangeDescriptor, OffloadSlice};
use crate::index::Indexer;

pub(crate) struct StreamDescriptor<T> {
    tlb: Arc<T>,
    stream_id: u64,
    ranges: VecDeque<RangeDescriptor<T>>,
}

impl<T> StreamDescriptor<T>
where
    T: Indexer,
{
    pub(crate) fn new(stream_id: u64, tlb: Arc<T>) -> Self {
        Self {
            tlb,
            stream_id,
            ranges: VecDeque::new(),
        }
    }

    pub(crate) fn add_range(&mut self, range: u32, start: u64, end: Option<u64>) {
        let r = match end {
            Some(end) => {
                RangeDescriptor::with_end(Arc::clone(&self.tlb), self.stream_id, range, start, end)
            }
            None => RangeDescriptor::new(Arc::clone(&self.tlb), self.stream_id, range, start),
        };
        if let Some((idx, _)) = self
            .ranges
            .iter()
            .enumerate()
            .rev()
            .find(|(_, r)| r.range < range)
        {
            self.ranges.insert(idx + 1, r);
        } else {
            self.ranges.push_back(r);
        }
    }

    pub(crate) fn trim(&mut self, offset: u64) {
        loop {
            let mut drop = false;
            if let Some(range_descriptor) = self.ranges.front_mut() {
                if range_descriptor.start >= offset {
                    break;
                }

                if let Some(end) = range_descriptor.seal {
                    // The range has been sealed
                    debug_assert_eq!(end, range_descriptor.end);
                    if end <= offset {
                        drop = true;
                    }
                } else {
                    // In case of handling the last mutable range
                    debug_assert!(range_descriptor.end >= offset);

                    if range_descriptor.start < offset {
                        range_descriptor.trim(offset);
                    }

                    break;
                }
            } else {
                break;
            }
            if drop {
                self.ranges.pop_front();
            }
        }
    }

    pub(crate) fn index(&mut self, range: u32, offset: u64) {
        if let Some(r) = self.ranges.back_mut() {
            if r.range == range {
                r.index(offset);
            }
        }
    }

    pub(crate) fn seal(&mut self, range: u32, end: u64) {
        self.ranges
            .iter_mut()
            .filter(|r| r.range == range)
            .for_each(|range| {
                range.seal(end);
            });
    }

    pub(crate) fn trim_range(&mut self, range: u32, offset: u64) {
        self.ranges
            .iter_mut()
            .filter(|r| r.range == range)
            .for_each(|range| {
                range.trim(offset);
            });
    }

    /// Delete ranges whose range index is less than or equal to `range`.
    pub(crate) fn delete_range(&mut self, range: u32) {
        loop {
            let drop;
            if let Some(r) = self.ranges.front() {
                if r.range <= range {
                    drop = true;
                } else {
                    break;
                }
            } else {
                break;
            }

            if drop {
                self.ranges.pop_front();
            }
        }
    }

    pub(crate) fn min_wal(&mut self) -> Option<u64> {
        if let Some(range) = self.ranges.front_mut() {
            range.min_wal()
        } else {
            None
        }
    }

    pub(crate) fn data_offload(&mut self, range: u32, start: u64, delta: u32) {
        // Range server is only interested in the data offset event if it is serving the range.
        if let Some(r) = self.ranges.iter_mut().find(|r| r.range == range) {
            let end = start + delta as u64;
            r.offload_slice(OffloadSlice { start, end });
        }
    }

    pub(crate) fn wal_high(&self) -> Option<u64> {
        for range in self.ranges.iter() {
            let watermark = range.high();
            match range.seal {
                Some(end) => {
                    if watermark >= end {
                        continue;
                    } else {
                        return self.tlb.scan_wal_offset(
                            self.stream_id,
                            range.range,
                            watermark,
                            Some(end),
                        );
                    }
                }
                None => {
                    return self.tlb.scan_wal_offset(
                        self.stream_id,
                        range.range,
                        watermark,
                        Some(range.end),
                    );
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Arc};

    use crate::{index::MockIndexer, watermark::range_descriptor::RangeDescriptor};

    use super::StreamDescriptor;

    #[test]
    fn test_new() {
        let tlb = MockIndexer::default();
        let tlb = Arc::new(tlb);
        let descriptor = StreamDescriptor::new(42, tlb);
        assert_eq!(descriptor.stream_id, 42);
        assert_eq!(descriptor.ranges, vec![]);
    }

    /// Verify add-range supports out of order addition of range metadata.
    #[test]
    fn test_add_range() {
        let tlb = MockIndexer::default();
        let tlb = Arc::new(tlb);
        let mut descriptor = StreamDescriptor::new(42, Arc::clone(&tlb));
        descriptor.add_range(0, 0, Some(10));
        descriptor.add_range(2, 20, Some(30));
        descriptor.add_range(1, 10, Some(20));
        descriptor.add_range(3, 30, None);

        let mut expected = VecDeque::new();
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 0, 0, 10));
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 1, 10, 20));
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 2, 20, 30));
        expected.push_back(RangeDescriptor::new(Arc::clone(&tlb), 42, 3, 30));

        assert_eq!(descriptor.ranges, expected);
    }

    #[test]
    fn test_trim() {
        let mut tlb = MockIndexer::default();

        tlb.expect_scan_wal_offset()
            .returning_st(|_stream_id, _range, offset, _end| Some(offset * 10));

        let tlb = Arc::new(tlb);
        let mut descriptor = StreamDescriptor::new(42, Arc::clone(&tlb));
        descriptor.add_range(0, 0, Some(10));
        descriptor.add_range(2, 20, Some(30));
        descriptor.add_range(1, 10, Some(20));
        descriptor.add_range(3, 30, None);

        descriptor.trim(0);
        let mut expected = VecDeque::new();
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 0, 0, 10));
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 1, 10, 20));
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 2, 20, 30));
        expected.push_back(RangeDescriptor::new(Arc::clone(&tlb), 42, 3, 30));
        assert_eq!(descriptor.ranges, expected);

        descriptor.trim(10);
        let mut expected = VecDeque::new();
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 1, 10, 20));
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 2, 20, 30));
        expected.push_back(RangeDescriptor::new(Arc::clone(&tlb), 42, 3, 30));
        assert_eq!(descriptor.ranges, expected);

        descriptor.index(3, 35);
        descriptor.trim(35);
        let mut expected = VecDeque::new();
        expected.push_back(RangeDescriptor::new(Arc::clone(&tlb), 42, 3, 35));
        assert_eq!(descriptor.ranges, expected);
    }

    #[test]
    fn test_trim_all() {
        let mut tlb = MockIndexer::default();

        tlb.expect_scan_wal_offset()
            .returning_st(|_stream_id, _range, offset, _end| Some(offset * 10));

        let tlb = Arc::new(tlb);
        let mut descriptor = StreamDescriptor::new(42, Arc::clone(&tlb));
        descriptor.add_range(0, 0, Some(10));
        descriptor.add_range(2, 20, Some(30));
        descriptor.add_range(1, 10, Some(20));
        descriptor.trim(30);
        let expected = VecDeque::new();
        assert_eq!(descriptor.ranges, expected);
    }

    #[should_panic]
    #[test]
    fn test_over_trim() {
        let mut tlb = MockIndexer::default();

        tlb.expect_scan_wal_offset()
            .returning_st(|_stream_id, _range, offset, _end| Some(offset * 10));

        let tlb = Arc::new(tlb);
        let mut descriptor = StreamDescriptor::new(42, Arc::clone(&tlb));
        descriptor.add_range(0, 0, Some(10));
        descriptor.add_range(2, 20, Some(30));
        descriptor.add_range(1, 10, Some(20));
        descriptor.add_range(3, 30, None);

        // should panic here
        descriptor.trim(35);
    }

    #[test]
    fn test_index() {
        let mut tlb = MockIndexer::default();

        tlb.expect_scan_wal_offset()
            .returning_st(|_stream_id, _range, offset, _end| Some(offset * 10));

        let tlb = Arc::new(tlb);
        let mut descriptor = StreamDescriptor::new(42, Arc::clone(&tlb));
        descriptor.add_range(0, 0, Some(10));
        descriptor.add_range(2, 20, Some(30));
        descriptor.add_range(1, 10, Some(20));
        descriptor.add_range(3, 30, None);
        descriptor.index(3, 35);

        let mut expected = VecDeque::new();
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 0, 0, 10));
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 1, 10, 20));
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 2, 20, 30));
        let mut rd = RangeDescriptor::new(Arc::clone(&tlb), 42, 3, 30);
        rd.index(35);
        expected.push_back(rd);
        assert_eq!(descriptor.ranges, expected);
    }

    #[test]
    fn test_seal() {
        let mut tlb = MockIndexer::default();

        tlb.expect_scan_wal_offset()
            .returning_st(|_stream_id, _range, offset, _end| Some(offset * 10));

        let tlb = Arc::new(tlb);
        let mut descriptor = StreamDescriptor::new(42, Arc::clone(&tlb));
        descriptor.add_range(0, 0, Some(10));
        descriptor.add_range(2, 20, Some(30));
        descriptor.add_range(1, 10, Some(20));
        descriptor.add_range(3, 30, None);
        descriptor.seal(3, 35);

        let mut expected = VecDeque::new();
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 0, 0, 10));
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 1, 10, 20));
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 2, 20, 30));
        let mut rd = RangeDescriptor::new(Arc::clone(&tlb), 42, 3, 30);
        rd.seal(35);
        expected.push_back(rd);
        assert_eq!(descriptor.ranges, expected);
    }

    #[test]
    fn test_trim_range() {
        let mut tlb = MockIndexer::default();
        tlb.expect_scan_wal_offset()
            .returning_st(|_stream_id, _range, offset, _end| Some(offset * 10));
        let tlb = Arc::new(tlb);
        let mut descriptor = StreamDescriptor::new(42, Arc::clone(&tlb));
        descriptor.add_range(0, 0, Some(10));
        descriptor.add_range(2, 20, Some(30));
        descriptor.add_range(1, 10, Some(20));
        descriptor.add_range(3, 30, None);

        descriptor.trim_range(0, 5);
        let mut expected = VecDeque::new();
        let mut rd = RangeDescriptor::with_end(Arc::clone(&tlb), 42, 0, 0, 10);
        rd.trim(5);
        expected.push_back(rd);
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 1, 10, 20));
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 2, 20, 30));
        expected.push_back(RangeDescriptor::new(Arc::clone(&tlb), 42, 3, 30));
        assert_eq!(descriptor.ranges, expected);
    }

    #[test]
    fn test_delete_range() {
        let mut tlb = MockIndexer::default();
        tlb.expect_scan_wal_offset()
            .returning_st(|_stream_id, _range, offset, _end| Some(offset * 10));
        let tlb = Arc::new(tlb);
        let mut descriptor = StreamDescriptor::new(42, Arc::clone(&tlb));
        descriptor.add_range(0, 0, Some(10));
        descriptor.add_range(2, 20, Some(30));
        descriptor.add_range(1, 10, Some(20));
        descriptor.add_range(3, 30, None);

        descriptor.delete_range(0);

        let mut expected = VecDeque::new();
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 1, 10, 20));
        expected.push_back(RangeDescriptor::with_end(Arc::clone(&tlb), 42, 2, 20, 30));
        expected.push_back(RangeDescriptor::new(Arc::clone(&tlb), 42, 3, 30));
        assert_eq!(descriptor.ranges, expected);
    }

    #[test]
    fn test_min_wal() {
        let mut tlb = MockIndexer::default();
        tlb.expect_scan_wal_offset()
            .returning_st(|_stream_id, _range, offset, _end| Some(offset * 10));
        let tlb = Arc::new(tlb);
        let mut descriptor = StreamDescriptor::new(42, Arc::clone(&tlb));
        descriptor.add_range(0, 0, Some(10));
        descriptor.add_range(2, 20, Some(30));
        descriptor.add_range(1, 10, Some(20));
        descriptor.add_range(3, 30, None);

        assert_eq!(descriptor.min_wal(), Some(0));

        descriptor.trim(10);
        assert_eq!(descriptor.min_wal(), Some(100));
    }

    #[test]
    fn test_data_offload() {
        let mut tlb = MockIndexer::default();
        tlb.expect_scan_wal_offset()
            .returning_st(|_stream_id, _range, offset, _end| Some(offset * 10));
        let tlb = Arc::new(tlb);
        let mut descriptor = StreamDescriptor::new(42, Arc::clone(&tlb));
        descriptor.add_range(0, 0, Some(10));
        descriptor.add_range(2, 20, Some(30));
        descriptor.add_range(1, 10, Some(20));
        descriptor.add_range(3, 30, None);

        descriptor.data_offload(0, 0, 5);
        assert_eq!(descriptor.wal_high(), Some(50));
    }
}
