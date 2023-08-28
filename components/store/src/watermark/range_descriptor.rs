use std::sync::Arc;

use derivative::Derivative;

use super::OffloadSlice;
use crate::index::Indexer;

#[derive(Derivative, Debug)]
#[derivative(PartialEq)]
pub(crate) struct RangeDescriptor<T> {
    /// Translation Look-aside Buffer translates logical offset to WAL offset.
    #[derivative(PartialEq = "ignore")]
    tlb: Arc<T>,

    stream: u64,
    pub(crate) range: u32,
    pub(crate) start: u64,

    /// Offset that this range has indexed up to
    pub(crate) end: u64,

    /// End offset of this range if sealed.
    ///
    /// # Note:
    /// Due to the replication algorithm used, seal is likely inconsistent with end. Namely, seal may be '>', '<'
    /// or '=' the 'end' field.
    pub(crate) seal: Option<u64>,

    /// Offloaded slices
    offloaded: Vec<OffloadSlice>,

    /// Minimum WAL offset of current range.
    #[derivative(PartialEq = "ignore")]
    wal: Option<u64>,
}

impl<T> RangeDescriptor<T>
where
    T: Indexer,
{
    pub(crate) fn new(tlb: Arc<T>, stream: u64, range: u32, start: u64) -> Self {
        Self {
            tlb,
            stream,
            range,
            start,
            end: start,
            seal: None,
            offloaded: vec![],
            wal: None,
        }
    }

    pub(crate) fn with_end(tlb: Arc<T>, stream: u64, range: u32, start: u64, end: u64) -> Self {
        Self {
            tlb,
            stream,
            range,
            start,
            end,
            seal: Some(end),
            offloaded: vec![],
            wal: None,
        }
    }

    pub(crate) fn trim(&mut self, start: u64) {
        if self.start <= start {
            self.start = start;

            let end = match self.seal {
                Some(offset) => offset,
                None => self.end,
            };
            if let Some(wal_offset) =
                self.tlb
                    .scan_wal_offset(self.stream, self.range, start, Some(end))
            {
                // Update minimum WAL offset of this range.
                self.wal = Some(wal_offset);
            }
        }
    }

    /// Index the new record (group) once append completes
    pub(crate) fn index(&mut self, offset: u64) {
        if let Some(end) = self.seal {
            debug_assert!(end > offset);
        }

        if offset > self.end {
            self.end = offset;
        }
    }

    pub(crate) fn seal(&mut self, offset: u64) {
        debug_assert!(offset >= self.start);
        self.seal = Some(offset);
    }

    pub(crate) fn offload_slice(&mut self, slice: OffloadSlice) {
        let mut expanded = false;

        // Try to expand slice
        for s in self.offloaded.iter_mut().rev() {
            if slice.start == s.end {
                s.end = slice.end;
                expanded = true;
                break;
            }

            if slice.end == s.start {
                s.start = slice.start;
                expanded = true;
                break;
            }
        }

        if expanded {
            let res = self
                .offloaded
                .iter()
                .enumerate()
                .try_reduce(|prev, current| {
                    if prev.1.end == current.1.start {
                        // We have found a slice that can be merged with its follow-up; stop iterating.
                        Err(prev.0)
                    } else {
                        Ok(current)
                    }
                });
            if let Err(pos) = res {
                let s = self.offloaded.remove(pos);
                if let Some(slice) = self.offloaded.get_mut(pos) {
                    slice.start = s.start;
                }
            }
        } else {
            // Insert and keep slices in-order.
            match self.offloaded.iter().position(|s| s.end <= slice.start) {
                Some(idx) => {
                    self.offloaded.insert(idx + 1, slice);
                }
                None => {
                    self.offloaded.push(slice);
                }
            };
        }
    }

    pub(crate) fn min_wal(&mut self) -> Option<u64> {
        if self.end == self.start {
            None
        } else if self.wal.is_none() {
            if let Some(offset) =
                self.tlb
                    .scan_wal_offset(self.stream, self.range, self.start, Some(self.end))
            {
                self.wal = Some(offset);
            }
            self.wal
        } else {
            self.wal
        }
    }

    /// Try to calculate high watermark of this range, records with offset prior to it should have been offloaded to
    ///  object storage.
    pub(crate) fn high(&self) -> u64 {
        let mut offset = self.start;
        for s in self.offloaded.iter() {
            if offset == s.start {
                offset = s.end;
            } else {
                break;
            }
        }
        offset
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{index::MockIndexer, watermark::OffloadSlice};

    use super::RangeDescriptor;

    #[test]
    fn test_new() {
        let tlb = MockIndexer::default();
        let tlb = Arc::new(tlb);
        let mut descriptor = RangeDescriptor::new(tlb, 1, 1, 42);
        assert_eq!(descriptor.stream, 1);
        assert_eq!(descriptor.range, 1);
        assert_eq!(descriptor.start, 42);
        assert_eq!(descriptor.end, 42);
        assert_eq!(descriptor.seal, None);
        assert_eq!(descriptor.offloaded, vec![]);
        assert_eq!(descriptor.wal, None);
        assert_eq!(descriptor.min_wal(), None);
    }

    #[test]
    fn test_with_end() {
        let mut tlb = MockIndexer::default();
        tlb.expect_scan_wal_offset()
            .once()
            .returning_st(
                |_stream_id, _range, offset, _end| {
                    if 42 == offset {
                        Some(100)
                    } else {
                        Some(1000)
                    }
                },
            );
        let tlb = Arc::new(tlb);
        let mut descriptor = RangeDescriptor::with_end(tlb, 1, 1, 42, 100);
        assert_eq!(descriptor.stream, 1);
        assert_eq!(descriptor.range, 1);
        assert_eq!(descriptor.start, 42);
        assert_eq!(descriptor.end, 100);
        assert_eq!(descriptor.seal, Some(100));
        assert_eq!(descriptor.offloaded, vec![]);
        assert_eq!(descriptor.wal, None);
        assert_eq!(descriptor.min_wal(), Some(100));
        assert_eq!(descriptor.wal, Some(100));
    }

    #[test]
    fn test_trim_mutable() {
        let mut tlb = MockIndexer::default();
        tlb.expect_scan_wal_offset()
            .once()
            .returning_st(|_stream_id, _range, _offset, _end| Some(1000));
        let tlb = Arc::new(tlb);
        let mut descriptor = RangeDescriptor::new(tlb, 1, 1, 42);

        descriptor.index(65);

        descriptor.trim(50);
        assert_eq!(descriptor.stream, 1);
        assert_eq!(descriptor.range, 1);
        assert_eq!(descriptor.start, 50);
        assert_eq!(descriptor.end, 65);
        assert_eq!(descriptor.seal, None);
        assert_eq!(descriptor.offloaded, vec![]);
        assert_eq!(descriptor.wal, Some(1000));

        // Trimming to an offset less than current start should be no-op.
        descriptor.trim(30);
        assert_eq!(descriptor.stream, 1);
        assert_eq!(descriptor.range, 1);
        assert_eq!(descriptor.start, 50);
        assert_eq!(descriptor.end, 65);
        assert_eq!(descriptor.seal, None);
        assert_eq!(descriptor.offloaded, vec![]);
        assert_eq!(descriptor.wal, Some(1000));
    }

    #[test]
    fn test_trim_sealed() {
        let mut tlb = MockIndexer::default();
        tlb.expect_scan_wal_offset()
            .once()
            .returning_st(|_stream_id, _range, _offset, _end| Some(1000));
        let tlb = Arc::new(tlb);
        let mut descriptor = RangeDescriptor::with_end(tlb, 1, 1, 42, 100);
        descriptor.trim(50);
        assert_eq!(descriptor.stream, 1);
        assert_eq!(descriptor.range, 1);
        assert_eq!(descriptor.start, 50);
        assert_eq!(descriptor.end, 100);
        assert_eq!(descriptor.seal, Some(100));
        assert_eq!(descriptor.offloaded, vec![]);
        assert_eq!(descriptor.wal, Some(1000));

        // Trimming to an offset that is less than current start should be a no-op.
        descriptor.trim(30);
        assert_eq!(descriptor.stream, 1);
        assert_eq!(descriptor.range, 1);
        assert_eq!(descriptor.start, 50);
        assert_eq!(descriptor.end, 100);
        assert_eq!(descriptor.seal, Some(100));
        assert_eq!(descriptor.offloaded, vec![]);
        assert_eq!(descriptor.wal, Some(1000));
    }

    #[test]
    fn test_seal() {
        let tlb = MockIndexer::default();
        let tlb = Arc::new(tlb);
        let mut descriptor = RangeDescriptor::new(tlb, 1, 1, 42);
        descriptor.seal(100);
        assert_eq!(descriptor.stream, 1);
        assert_eq!(descriptor.range, 1);
        assert_eq!(descriptor.start, 42);
        assert_eq!(descriptor.end, 42);
        assert_eq!(descriptor.seal, Some(100));
        assert_eq!(descriptor.offloaded, vec![]);
        assert_eq!(descriptor.wal, None);
    }

    #[should_panic]
    #[test]
    fn test_seal_panic() {
        let tlb = MockIndexer::default();
        let tlb = Arc::new(tlb);
        let mut descriptor = RangeDescriptor::new(tlb, 1, 1, 42);
        descriptor.seal(10);
        assert_eq!(descriptor.stream, 1);
        assert_eq!(descriptor.range, 1);
        assert_eq!(descriptor.start, 42);
        assert_eq!(descriptor.end, 42);
        assert_eq!(descriptor.seal, Some(100));
        assert_eq!(descriptor.offloaded, vec![]);
        assert_eq!(descriptor.wal, None);
    }

    #[should_panic]
    #[test]
    fn test_index_panic() {
        let tlb = MockIndexer::default();
        let tlb = Arc::new(tlb);
        let mut descriptor = RangeDescriptor::new(tlb, 1, 1, 42);
        descriptor.index(50);
        descriptor.seal(51);
        descriptor.index(51);
    }

    #[test]
    fn test_offload_slice() {
        let tlb = MockIndexer::default();
        let tlb = Arc::new(tlb);
        let mut descriptor = RangeDescriptor::with_end(tlb, 1, 1, 42, 100);
        descriptor.offload_slice(OffloadSlice { start: 42, end: 50 });
        assert_eq!(descriptor.high(), 50);

        descriptor.offload_slice(OffloadSlice { start: 50, end: 60 });
        descriptor.offload_slice(OffloadSlice { start: 70, end: 80 });
        descriptor.offload_slice(OffloadSlice { start: 80, end: 90 });
        descriptor.offload_slice(OffloadSlice {
            start: 90,
            end: 100,
        });
        assert_eq!(descriptor.high(), 60);
    }
}
