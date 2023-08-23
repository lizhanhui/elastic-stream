use std::sync::Arc;

use super::OffloadSlice;
use crate::index::Indexer;

pub(crate) struct RangeDescriptor<T> {
    /// Translation Look-aside Buffer translates logical offset to WAL offset.
    tlb: Arc<T>,
    stream: u64,
    pub(crate) range: u32,
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) seal: Option<u64>,

    /// Offloaded slices
    offloaded: Vec<OffloadSlice>,

    /// Minimum WAL offset of current range.
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

    pub(crate) fn index(&mut self, offset: u64) {
        if offset > self.end {
            self.end = offset;
        }
    }

    pub(crate) fn seal(&mut self, offset: u64) {
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
