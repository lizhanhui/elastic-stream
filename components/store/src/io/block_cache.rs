use std::{
    cell::UnsafeCell, collections::BTreeMap, f32::consts::E, ops::Bound, rc::Rc, sync::Arc,
    time::Instant,
};

use slog::{error, info, trace, Logger};
use thiserror::Error;

use super::buf::AlignedBuf;

#[derive(Error, Debug)]
enum CacheError {
    #[error("Cache miss")]
    Miss,
}

#[derive(Debug)]
pub(crate) struct Entry {
    buf: Arc<AlignedBuf>,
    hit: usize,
    last_hit_instant: Instant,
}

impl Entry {
    fn new(buf: Arc<AlignedBuf>) -> Self {
        Self {
            buf,
            hit: 0,
            last_hit_instant: Instant::now(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct BlockCache {
    log: Logger,
    wal_offset: u64,
    entries: BTreeMap<u32, Rc<UnsafeCell<Entry>>>,
}

pub(crate) struct MissedEntry {
    pub(crate) wal_offset: u64,
    pub(crate) len: u32,
}

impl BlockCache {
    pub(crate) fn new(log: Logger, offset: u64) -> Self {
        Self {
            log,
            wal_offset: offset,
            entries: BTreeMap::new(),
        }
    }

    /// Add a new entry to the cache.
    /// The newly added entry will replace the existing entry if the start wal_offset conflicts.
    ///
    /// # Note
    /// * The upper layer should ensure the cached entries are not overlapping after a new entry is added.
    ///
    /// # Arguments
    /// * `buf` - The buffer to be added to the cache.
    pub(crate) fn add_entry(&mut self, buf: Arc<AlignedBuf>) {
        trace!(
            self.log,
            "Add block cache entry: [{}, {})",
            buf.wal_offset,
            buf.wal_offset + buf.limit() as u64
        );
        debug_assert!(buf.wal_offset >= self.wal_offset);
        let from = (buf.wal_offset - self.wal_offset) as u32;
        let entry = Rc::new(UnsafeCell::new(Entry::new(buf)));

        // The replace occurs when the new entry overlaps with the existing entry.
        self.entries.insert(from, entry);
    }

    /// Get cached entries from the cache.
    /// If the cache couldn't meet the query needs, it will return the missed entries.
    ///
    /// # Arguments
    /// * `wal_offset` - The start wal_offset of the query.
    /// * `len` - The length of the query.
    ///
    /// # Returns
    /// * `Ok` - The cached entries.
    /// * `Err` - The missed entries.
    pub(crate) fn try_get_entry(
        &self,
        wal_offset: u64,
        len: u32,
    ) -> Result<Vec<Arc<AlignedBuf>>, Vec<MissedEntry>> {
        let from = wal_offset.checked_sub(self.wal_offset);

        if let Some(from) = from {
            let from = from as u32;
            let to = from + len;

            let start_cursor = self.entries.upper_bound(Bound::Included(&from));
            let start_key = start_cursor.key().unwrap_or(&from);

            let search: Vec<_> = self
                .entries
                .range(start_key..&to)
                .filter(|(_k, entry)| {
                    let item = unsafe { &mut *entry.get() };
                    if item.buf.covers_partial(wal_offset, len) {
                        return true;
                    }
                    return false;
                })
                .collect();

            // Return a complete missed entry if the search result is empty.
            if search.is_empty() {
                return Err(vec![MissedEntry { wal_offset, len }]);
            }

            // Return partial missed entries if the search result is not cover the specified range.
            let mut missed_entries = Vec::new();
            let mut last_end = from;

            search.iter().for_each(|(k, entry)| {
                let item = unsafe { &mut *entry.get() };
                if **k > last_end {
                    missed_entries.push(MissedEntry {
                        wal_offset: self.wal_offset + last_end as u64,
                        len: *k - last_end,
                    });
                }
                last_end = *k + item.buf.limit() as u32;
            });

            if last_end < to {
                missed_entries.push(MissedEntry {
                    wal_offset: self.wal_offset + last_end as u64,
                    len: to - last_end,
                });
            }

            if !missed_entries.is_empty() {
                return Err(missed_entries);
            }

            let search: Vec<_> = search
                .into_iter()
                .map(|(_k, entry)| {
                    let item = unsafe { &mut *entry.get() };
                    item.hit += 1;
                    item.last_hit_instant = Instant::now();
                    Arc::clone(&item.buf)
                })
                .collect();

            Ok(search)
        } else {
            error!(
                self.log,
                "Invalid wal_offset: {}, cache wal_offset: {}", wal_offset, self.wal_offset
            );

            Err(vec![MissedEntry { wal_offset, len }])
        }
    }

    /// Remove cache entries if `Predicate` returns `true`.
    ///
    /// # Arguments
    /// * `pred` - Predicate that return true if the entry is supposed to be dropped and false to reserve.
    pub(crate) fn remove<F>(&mut self, pred: F)
    where
        F: Fn(&Entry) -> bool,
    {
        self.entries.drain_filter(|_k, v| {
            let entry = unsafe { &*v.get() };
            if pred(entry) {
                info!(
                    self.log,
                    "Remove block cache entry [{}, {})",
                    entry.buf.wal_offset,
                    entry.buf.wal_offset + entry.buf.limit() as u64
                );
                true
            } else {
                false
            }
        });
    }
}

#[cfg(test)]
mod tests {}
