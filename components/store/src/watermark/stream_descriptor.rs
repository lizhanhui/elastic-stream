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
                if range_descriptor.end < offset {
                    drop = true;
                } else {
                    if range_descriptor.start < offset {
                        range_descriptor.trim(offset);
                    }
                    break;
                }
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
            let mut drop = false;
            if let Some(r) = self.ranges.front() {
                if r.range <= range {
                    drop = true;
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
