use lazy_static::lazy_static;
use log::{error, info, trace};
use minstant::{Anchor, Instant};
use std::cmp::Ordering;
use std::{cell::UnsafeCell, cmp, collections::BTreeMap, ops::Bound, rc::Rc, sync::Arc};

use super::buf::AlignedBuf;

lazy_static! {
    static ref ANCHOR: Anchor = Anchor::new();
}

#[derive(Debug)]
pub(crate) struct Entry {
    /// hit count of the entry.
    /// this field will be updated in [`BlockCache::try_get_entries`] and used to determine which entry should be dropped.
    hit: usize,

    /// The last time the entry is hit.
    /// this field will be updated in [`BlockCache::try_get_entries`] and used to determine which entry should be dropped.
    last_hit_instant: Instant,

    /// The number of strong references to the entry.
    /// The entry shouldn't be dropped if the strong_rc is not zero.
    strong_rc: usize,

    /// Some if the entry is not loaded yet.
    /// None if the entry is loaded that has a valid cached buf.
    entry_range: Option<EntryRange>,

    /// None if the entry is not loaded yet.
    buf: Option<Arc<AlignedBuf>>,
}

impl Entry {
    fn new(buf: Arc<AlignedBuf>) -> Self {
        Self {
            buf: Some(buf),
            hit: 0,
            strong_rc: 0,
            last_hit_instant: Instant::now(),
            entry_range: None,
        }
    }

    fn new_loading_entry(entry_range: EntryRange) -> Self {
        Self {
            buf: None,
            hit: 0,
            strong_rc: 0,
            last_hit_instant: Instant::now(),
            entry_range: Some(entry_range),
        }
    }

    /// Judge if the cached entry covers specified EntryRange partially.
    ///
    /// Test the cached buf or loading entry whether it covers the specified region partially.
    ///
    /// # Arguments
    /// * `entry_range` - The specified region.
    ///
    /// # Returns
    /// `true` if the cache hit partially;
    /// `false` if the cache has no overlap with the specified region.
    pub(crate) fn covers_partial(&self, entry_range: &EntryRange) -> bool {
        if let Some(buf) = &self.buf {
            buf.covers_partial(entry_range.wal_offset, entry_range.len)
        } else if let Some(loading_entry_range) = &self.entry_range {
            // When comparing with a loading entry, we should consider the aligned length
            loading_entry_range.wal_offset < entry_range.wal_offset + entry_range.len as u64
                && entry_range.wal_offset
                    < loading_entry_range.wal_offset + loading_entry_range.len as u64
        } else {
            false
        }
    }

    /// Return the length of the cached entry.
    pub(crate) fn len(&self) -> u32 {
        if let Some(buf) = &self.buf {
            buf.limit() as u32
        } else if let Some(loading_entry_range) = &self.entry_range {
            loading_entry_range.len
        } else {
            0
        }
    }

    /// Return the occupied size of the cached entry, aka the capacity of the cached entry.
    pub(crate) fn capacity(&self) -> u32 {
        if let Some(buf) = &self.buf {
            buf.capacity as u32
        } else if let Some(loading_entry_range) = &self.entry_range {
            loading_entry_range.len
        } else {
            0
        }
    }

    /// Check the entry whether it is loaded.
    pub(crate) fn is_loaded(&self) -> bool {
        self.buf.is_some()
    }

    /// Check the entry whether it is strong referenced.
    pub(crate) fn is_strong_referenced(&self) -> bool {
        self.strong_rc > 0
    }

    pub(crate) fn wal_offset(&self) -> u64 {
        if let Some(buf) = &self.buf {
            buf.wal_offset
        } else if let Some(loading_entry_range) = &self.entry_range {
            loading_entry_range.wal_offset
        } else {
            0
        }
    }

    /// Score the cache entry, based on the hit count and last hit instant.
    /// The score is used to determine which cache entry should be dropped,
    /// a higher score means a lower priority to be dropped.
    pub(crate) fn score(&self) -> u64 {
        let hit = self.hit;
        let last_hit_instant = self.last_hit_instant;

        let timestamp = last_hit_instant.as_unix_nanos(&ANCHOR);

        // Use a formula to calculate the score, the hit count and timestamp will have a positive effect on the score.
        // Which means higher hit count and recently access lead to higher score.
        // Each cache hit will give a bonus of approximately one second.
        timestamp + ((hit as u64) << 27)
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct EntryRange {
    /// The start wal_offset of the entry which is a absolute wal_offset.
    /// It's expected to be aligned to the IO alignment.
    pub(crate) wal_offset: u64,

    /// It's expected to be aligned to the IO alignment.
    pub(crate) len: u32,
}

impl EntryRange {
    /// The new EntryRange is aligned to the specified alignment.
    pub(crate) fn new(wal_offset: u64, len: u32, alignment: usize) -> Self {
        // Alignment must be positive
        debug_assert_ne!(0, alignment);
        // Alignment must be power of 2.
        debug_assert_eq!(0, alignment & (alignment - 1));
        let from = wal_offset / alignment as u64 * alignment as u64;
        let to = wal_offset + len as u64;

        let aligned_to = (to + alignment as u64 - 1) / alignment as u64 * alignment as u64;

        Self {
            wal_offset: from,
            len: (aligned_to - from) as u32,
        }
    }
}

pub trait MergeRange<T> {
    /// Merge the entries to bigger continuous ranges as possible.
    /// This may reduce the number of loading io tasks.
    ///
    /// # Arguments
    /// * `self` - The entries to be merged.
    ///
    /// # Returns
    /// * The continuous ranges of the missed entries.
    fn merge(self) -> Vec<T>;
}

impl MergeRange<EntryRange> for Vec<EntryRange> {
    /// The merge algorithm supports merging disorderly, repeated entries as well as overlapping entries.
    fn merge(self) -> Vec<EntryRange> {
        let mut ranges = self;
        ranges.sort_by_key(|r| r.wal_offset);

        let mut merged_ranges: Vec<EntryRange> = Vec::with_capacity(ranges.len());

        // Iterate the sorted ranges from the end.
        for range in ranges.into_iter() {
            if let Some(last_m) = merged_ranges.last_mut() {
                if last_m.wal_offset + last_m.len as u64 >= range.wal_offset {
                    // Adjust the length of the last merged range.
                    last_m.len = cmp::max(
                        range.wal_offset + range.len as u64 - last_m.wal_offset,
                        last_m.len as u64,
                    ) as u32;
                    continue;
                }
            }
            merged_ranges.push(range);
        }
        merged_ranges
    }
}

#[derive(Debug)]
pub(crate) struct BlockCache {
    // The start wal_offset of the block cache, usually it is the start wal_offset of some segment.
    wal_offset: u64,

    // The key of the map is a relative wal_offset from the start wal_offset of the block cache.
    entries: BTreeMap<u32, Rc<UnsafeCell<Entry>>>,

    // A map whose keys are always sorted. The key of the map is score and the value is wal_offset.
    score_list: skiplist::OrderedSkipList<Rc<UnsafeCell<Entry>>>,

    // The size of the block cache.
    cache_size: u32,

    alignment: usize,
}

impl BlockCache {
    pub(crate) fn new(config: &Arc<config::Configuration>, offset: u64) -> Self {
        Self {
            wal_offset: offset,
            entries: BTreeMap::new(),
            score_list: unsafe {
                skiplist::OrderedSkipList::with_comp(
                    |a: &Rc<UnsafeCell<Entry>>, b: &Rc<UnsafeCell<Entry>>| {
                        let entry_a = &*a.get();
                        let entry_b = &*b.get();

                        // If the wal_offset is the same, the entry is the same.
                        if entry_a.wal_offset() == entry_b.wal_offset() {
                            return Ordering::Equal;
                        }

                        // And then compare by score and wal offset.
                        let score_order = entry_a.score().cmp(&entry_b.score());
                        if score_order == Ordering::Equal {
                            return entry_a.wal_offset().cmp(&entry_b.wal_offset());
                        }
                        score_order
                    },
                )
            },
            cache_size: 0,
            alignment: config.store.alignment,
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
            "Add block cache entry: [{}, {})",
            buf.wal_offset,
            buf.wal_offset + buf.limit() as u64
        );
        debug_assert!(buf.wal_offset >= self.wal_offset);
        let from = (buf.wal_offset - self.wal_offset) as u32;

        // Fetch previous entry if exists.
        let pre_entry = self.entries.get(&from);

        let mut new_entry = Entry::new(buf);

        if let Some(pre_entry) = pre_entry {
            self.score_list.remove(pre_entry);

            // The strong reference count of the replaced entry should be inherited by the new entry.
            let pre_entry = unsafe { &*pre_entry.get() };
            new_entry.strong_rc = pre_entry.strong_rc;

            // Decrease the cached size if the replaced entry exists and is loaded.
            if pre_entry.is_loaded() {
                self.cache_size -= pre_entry.capacity();
            }
        }

        // Increase the cached size.
        self.cache_size += new_entry.capacity();
        let entry = Rc::new(UnsafeCell::new(new_entry));

        // The replace occurs when the new entry overlaps with the existing entry.
        self.entries.insert(from, entry.clone());
        self.score_list.insert(entry.clone());
    }

    /// Add a loading entry to the cache.
    /// The upper layer should ensure that a loading io task is submitted for the entry.
    /// # Arguments
    /// * `entry_range` - The entry range to be added to the cache.
    pub(crate) fn add_loading_entry(&mut self, entry_range: EntryRange) {
        trace!(
            "Add loading block cache entry: [{}, {})",
            entry_range.wal_offset,
            entry_range.wal_offset + entry_range.len as u64
        );

        debug_assert!(entry_range.wal_offset >= self.wal_offset);
        let from = (entry_range.wal_offset - self.wal_offset) as u32;
        let entry = Rc::new(UnsafeCell::new(Entry::new_loading_entry(entry_range)));

        // The replace occurs when the new entry overlaps with the existing entry.
        let pre_entry = self.entries.insert(from, entry.clone());

        // Decrease the cached size if the replaced entry exists and is loaded.
        if let Some(pre_entry) = pre_entry {
            self.score_list.remove(&pre_entry);

            let pre_entry = unsafe { &*pre_entry.get() };

            if pre_entry.is_loaded() {
                self.cache_size -= pre_entry.capacity();
            }
        }
    }

    /// Strong reference the entries that cover the given range.
    ///
    /// # Arguments
    /// * `entry_range` - The range of the entries to be referenced.
    /// * `ref_count` - The reference count to be increased or decreased.
    pub(crate) fn strong_reference_entries(&self, entry_range: EntryRange, ref_count: isize) {
        let wal_offset = entry_range.wal_offset;
        let len = entry_range.len;

        let from = wal_offset.checked_sub(self.wal_offset);

        if let Some(from) = from {
            let from = from as u32;
            let to = from + len;

            // The start cursor is the first entry whose start wal_offset is less than or equal the query wal_offset.
            // This behavior is aim to resolve the case that the query wal_offset is in the middle of an entry.
            let start_cursor = self.entries.upper_bound(Bound::Included(&from));
            let start_key = start_cursor.key().unwrap_or(&from);

            self.entries.range(start_key..&to).for_each(|(_k, entry)| {
                let item = unsafe { &mut *entry.get() };
                if item.covers_partial(&entry_range) {
                    // Increase the reference count if the ref_count is positive, otherwise decrease it.
                    if ref_count > 0 {
                        item.strong_rc += ref_count as usize;
                    } else {
                        item.strong_rc -= (-ref_count) as usize;
                    }
                }
            });
        }
    }

    /// Get cached entries from the cache.
    /// If the cache couldn't meet the query needs, it will return the missed entries.
    ///
    /// # Arguments
    /// * `wal_offset` - The start wal_offset of the query.
    /// * `len` - The length of the query.
    ///
    /// # Returns
    /// * `Ok` - The cached entries. If the ok result is None, it means the caller should wait for the ongoing io tasks.
    /// * `Err` - The missed entries, may split the request into multiple missed ranges.
    pub(crate) fn try_get_entries(
        &mut self,
        entry_range: EntryRange,
    ) -> Result<Option<Vec<Arc<AlignedBuf>>>, Vec<EntryRange>> {
        let wal_offset = entry_range.wal_offset;
        let len = entry_range.len;

        let from = wal_offset.checked_sub(self.wal_offset);

        if let Some(from) = from {
            let from = from as u32;
            let to = from + len;

            // The start cursor is the first entry whose start wal_offset is less than or equal the query wal_offset.
            // This behavior is aim to resolve the case that the query wal_offset is in the middle of an entry.
            let start_cursor = self.entries.upper_bound(Bound::Included(&from));
            let start_key = start_cursor.key().unwrap_or(&from);

            let search: Vec<_> = self
                .entries
                .range(start_key..&to)
                .filter(|(_k, entry)| {
                    let item = unsafe { &mut *entry.get() };
                    if item.covers_partial(&entry_range) {
                        return true;
                    }
                    false
                })
                .collect();

            // Return a complete missed entry if the search result is empty.
            if search.is_empty() {
                return Err(vec![entry_range]);
            }

            // Return partially missed entries if the search result does not cover the specified range.
            let mut missed_entries: Vec<_> = Vec::new();
            let mut last_end = from;

            search.iter().for_each(|(k, entry)| {
                let item = unsafe { &mut *entry.get() };
                if **k > last_end {
                    missed_entries.push(EntryRange {
                        wal_offset: self.wal_offset + last_end as u64,
                        len: **k - last_end,
                    });
                }
                last_end = **k + item.len();
            });

            if last_end < to {
                // The caller may request read only a part of the last entry, if last_end is not aligned to the block size.
                // In this case, we shouldn't return the missed entry to avoid the last writable entry being dropped by read IO.
                if last_end % self.alignment as u32 == 0 {
                    missed_entries.push(EntryRange {
                        wal_offset: self.wal_offset + last_end as u64,
                        len: to - last_end,
                    });
                }
            }

            if !missed_entries.is_empty() {
                return Err(missed_entries);
            }

            let search_len = search.len();
            let buf_res: Vec<_> = search
                .into_iter()
                .flat_map(|(_k, entry)| {
                    let item = unsafe { &mut *entry.get() };

                    // remove the entry from the score list and insert it again to update the score.
                    self.score_list.remove(entry);

                    // Although here we may not return the buffer to the caller,
                    // we still increase the hit count to reduce the chance of being dropped.
                    item.hit += 1;
                    item.last_hit_instant = Instant::now();

                    self.score_list.insert(entry.clone());

                    &item.buf
                })
                .map(Arc::clone)
                .collect();

            if buf_res.len() != search_len {
                // There is some loading entry in the search result, the caller should wait for the loading.
                // So we return a empty ok result to indicate the caller to wait.
                return Ok(None);
            }

            Ok(Some(buf_res))
        } else {
            error!(
                "Invalid wal_offset: {}, cache wal_offset: {}",
                wal_offset, self.wal_offset
            );

            Err(vec![entry_range])
        }
    }

    /// Retrieve the lowest score of entry from the cache.
    /// Only entries that are loaded and not referenced by any reader will be counted.
    ///
    /// # Returns
    /// * the lowest score of entry from the cache.
    pub(crate) fn min_score(&self) -> u64 {
        if self.score_list.is_empty() {
            return 0;
        }

        for i in 0..self.score_list.len() {
            let entry = unsafe { &*self.score_list.get(i).unwrap().get() };
            if !entry.is_strong_referenced() {
                return entry.score();
            }
        }
        0
    }

    /// Remove a entry with the lowest score from the cache.
    /// Only entries that are loaded and not referenced by any reader will be counted.
    ///
    /// # Returns
    /// * `Some` - The removed entry.
    /// * `None` - The entry is not found.
    pub(crate) fn remove_by_score(&mut self) -> Option<&Entry> {
        if self.score_list.is_empty() {
            return None;
        }

        // let mut entry_to_remove = Some(unsafe { &*self.score_list.front().unwrap().get() });
        let mut entry_to_remove = None;

        for i in 0..self.score_list.len() {
            let entry = unsafe { &*self.score_list.get(i).unwrap().get() };
            if !entry.is_strong_referenced() {
                entry_to_remove = Some(entry);
                break;
            }
        }

        if let Some(entry) = entry_to_remove {
            self.remove_by(entry.wal_offset());
        }

        entry_to_remove
    }

    /// Remove a specific entry from the cache.
    ///
    /// # Arguments
    /// * `wal_offset` - The wal_offset of the entry to be removed.
    ///
    /// # Returns
    /// * `Some` - The removed entry.
    /// * `None` - The entry is not found.
    pub(crate) fn remove_by(&mut self, wal_offset: u64) -> Option<&Entry> {
        let from = wal_offset.checked_sub(self.wal_offset);

        if let Some(from) = from {
            let from = from as u32;
            let entry = self.entries.remove(&from);

            if let Some(entry) = entry {
                self.score_list.remove(&entry);

                let entry = unsafe { &*entry.get() };

                if entry.is_loaded() {
                    self.cache_size -= entry.capacity();
                }

                return Some(entry);
            }
            return None;
        }
        None
    }

    /// Remove cache entries if `Predicate` returns `true`.
    ///
    /// # Arguments
    /// * `pred` - Predicate that return true if the entry is supposed to be dropped and false to reserve.
    pub(crate) fn remove<F>(&mut self, pred: F)
    where
        F: Fn(&Entry) -> bool,
    {
        self.entries.retain(|_k, v| {
            let entry = unsafe { &*v.get() };
            if pred(entry) {
                info!(
                    "Remove block cache entry [{}, {})",
                    entry.wal_offset(),
                    entry.wal_offset() + entry.len() as u64
                );

                self.score_list.remove(v);

                // Decrease the cache size.
                self.cache_size -= entry.capacity();
                false
            } else {
                true
            }
        });
    }

    pub(crate) fn buf_of_last_cache_entry(&mut self) -> Option<Arc<AlignedBuf>> {
        if let Some(entry) = self.entries.last_entry() {
            let v = Rc::clone(entry.get());
            let entry = unsafe { &*v.get() };
            if let Some(ref buf) = entry.buf {
                return Some(Arc::clone(buf));
            }
        }
        None
    }

    /// Fetch the wal offset of the last entry in the cache.
    pub(crate) fn wal_offset_of_last_cache_entry(&self) -> u64 {
        if let Some((k, _)) = self.entries.last_key_value() {
            return *k as u64 + self.wal_offset;
        }
        0u64
    }

    /// Fetch the current cache size.
    pub(crate) fn cache_size(&self) -> u32 {
        self.cache_size
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Index;
    use std::{
        error::Error,
        sync::{atomic::Ordering, Arc},
    };

    use rand::{seq::SliceRandom, thread_rng};

    use crate::io::{
        block_cache::{Entry, EntryRange, MergeRange},
        buf::AlignedBuf,
    };

    use super::BlockCache;

    /// Test merge missed entry ranges.
    #[test]
    fn test_merge_entries() {
        let block_size = 4096;

        // Case one: add 16 entries, and merge to one range.
        let mut missed_entries: Vec<_> = (0..16)
            .map(|n| EntryRange {
                wal_offset: n * block_size as u64,
                len: block_size,
            })
            .collect();
        let mut rng = thread_rng();
        missed_entries.shuffle(&mut rng);

        let merged = missed_entries.merge();
        assert_eq!(1, merged.len());
        assert_eq!(0, merged[0].wal_offset);
        assert_eq!(16 * block_size, merged[0].len);

        // Case two: add 16 entries, and merge to two ranges.
        let mut missed_entries: Vec<_> = (0..8)
            .map(|n| EntryRange {
                wal_offset: n * block_size as u64,
                len: block_size,
            })
            .collect();

        let start_wal_offset = 1024 * block_size as u64;
        (0..8).for_each(|n| {
            missed_entries.push(EntryRange {
                wal_offset: start_wal_offset + n * block_size as u64,
                len: block_size,
            });
        });

        missed_entries.shuffle(&mut rng);

        let merged = missed_entries.merge();
        assert_eq!(2, merged.len());
        assert_eq!(0, merged[0].wal_offset);
        assert_eq!(8 * block_size, merged[0].len);
        assert_eq!(start_wal_offset, merged[1].wal_offset);
        assert_eq!(8 * block_size, merged[1].len);

        // Case three: no merge
        let mut missed_entries: Vec<_> = (0..8)
            .map(|n| EntryRange {
                wal_offset: n * 3 * block_size as u64,
                len: block_size,
            })
            .collect();

        missed_entries.shuffle(&mut rng);
        let merged = missed_entries.merge();
        assert_eq!(8, merged.len());
        (0..8).for_each(|n| {
            assert_eq!(n * 3 * block_size as u64, merged[n as usize].wal_offset);
        });

        // Case four: discard the redundant range.
        let mut missed_entries: Vec<_> = (0..8)
            .map(|n| EntryRange {
                wal_offset: n * block_size as u64,
                len: block_size,
            })
            .collect();

        (0..8).for_each(|n| {
            missed_entries.push(EntryRange {
                wal_offset: n * block_size as u64,
                len: block_size,
            });
        });

        missed_entries.shuffle(&mut rng);
        let merged = missed_entries.merge();
        assert_eq!(1, merged.len());
        assert_eq!(0, merged[0].wal_offset);
        assert_eq!(8 * block_size, merged[0].len);

        // Case five: handle the case that the ranges are overlapped.
        let mut missed_entries: Vec<_> = vec![
            // Add a range [0, 2 * block_size)
            EntryRange {
                wal_offset: 0,
                len: 2 * block_size,
            },
            // Add a range [block_size, 3 * block_size)
            EntryRange {
                wal_offset: block_size as u64,
                len: 3 * block_size,
            },
            // Add a range [1 * block_size, 2* block_size)
            EntryRange {
                wal_offset: block_size as u64,
                len: 2 * block_size,
            },
            // Add a range [3 * block_size, 3 * block_size)
            EntryRange {
                wal_offset: 3 * block_size as u64,
                len: 3 * block_size,
            },
        ];

        missed_entries.shuffle(&mut rng);
        let merged = missed_entries.merge();
        assert_eq!(1, merged.len());
        assert_eq!(0, merged[0].wal_offset);
        assert_eq!(6 * block_size, merged[0].len);
    }

    /// Test add entry.
    #[test]
    fn test_add_entry() {
        let start_wal_offset = 1024 * 1024 * 1024;
        let cfg = config::Configuration::default();
        let config = Arc::new(cfg);
        let mut block_cache = BlockCache::new(&config, start_wal_offset);
        let block_size = 4096;
        for n in 0..16 {
            let buf = Arc::new(
                AlignedBuf::new(
                    start_wal_offset + n * block_size as u64,
                    block_size,
                    block_size,
                )
                .unwrap(),
            );
            buf.limit.store(block_size, Ordering::Relaxed);
            block_cache.add_entry(buf);
        }

        assert_eq!(16, block_cache.entries.len());
        assert_eq!(16, block_cache.score_list.len());

        // Make sure the entries are sorted by score.
        for i in 0..16 {
            let entry = block_cache.score_list.index(i);
            assert_eq!(
                unsafe { &*entry.get() }.wal_offset(),
                start_wal_offset + i as u64 * block_size as u64
            );
        }

        // Assert the
        assert_eq!(
            start_wal_offset + 15 * block_size as u64,
            block_cache.wal_offset_of_last_cache_entry()
        )
    }

    /// Test score some cached entries.
    #[test]
    fn test_score_cached_entries() {
        let block_size = 4096;
        let mut entries = vec![];
        for n in 0..16 {
            let buf =
                Arc::new(AlignedBuf::new(n * block_size as u64, block_size, block_size).unwrap());
            buf.limit.store(block_size, Ordering::Relaxed);
            let mut entry = Entry::new(buf);
            entry.hit = n as usize;
            entries.push(entry);
        }
        let mut rng = thread_rng();
        entries.shuffle(&mut rng);

        entries.sort_by(|a, b| {
            let a_score = a.score();
            let b_score = b.score();
            a_score.cmp(&b_score)
        });

        // Assert the entries are sorted by score.
        for (n, entry) in entries.iter().enumerate().take(16) {
            assert_eq!(n, entry.hit);
        }
    }

    /// Test cache size.
    #[test]
    fn test_cache_size() {
        let cfg = config::Configuration::default();
        let config = Arc::new(cfg);
        let mut block_cache = BlockCache::new(&config, 0);
        let block_size = 4096;
        let total = 16;
        for n in 0..total {
            let buf =
                Arc::new(AlignedBuf::new(n * block_size as u64, block_size, block_size).unwrap());
            buf.limit.store(block_size, Ordering::Relaxed);
            block_cache.add_entry(buf);
        }

        assert_eq!(total as u32 * block_size as u32, block_cache.cache_size());

        // Check the score list.
        // All entries are sorted by order of addition at the beginning.
        assert_eq!(total as usize, block_cache.score_list.len());
        assert_eq!(
            0,
            unsafe { &*block_cache.score_list.front().unwrap().get() }.wal_offset()
        );
        assert_eq!(
            (total - 1) * block_size as u64,
            unsafe { &*block_cache.score_list.back().unwrap().get() }.wal_offset()
        );

        // Test cache size after remove.
        block_cache.remove(|entry| entry.wal_offset() == 0);
        assert_eq!(15 * block_size as u32, block_cache.cache_size());

        assert_eq!(15usize, block_cache.score_list.len());
        assert_eq!(
            block_size as u64,
            unsafe { &*block_cache.score_list.front().unwrap().get() }.wal_offset()
        );
        assert_eq!(
            15 * block_size as u64,
            unsafe { &*block_cache.score_list.back().unwrap().get() }.wal_offset()
        );

        // Add a loading entry and test cache size.
        block_cache.add_loading_entry(EntryRange {
            wal_offset: 0,
            len: block_size as u32,
        });
        assert_eq!(15 * block_size as u32, block_cache.cache_size());

        assert_eq!(15usize, block_cache.score_list.len());
        assert_eq!(
            block_size as u64,
            unsafe { &*block_cache.score_list.front().unwrap().get() }.wal_offset()
        );
        assert_eq!(
            15 * block_size as u64,
            unsafe { &*block_cache.score_list.back().unwrap().get() }.wal_offset()
        );

        // Replace a loading entry and test cache size.
        let buf = Arc::new(AlignedBuf::new(0, block_size, block_size).unwrap());
        buf.limit.store(block_size, Ordering::Relaxed);
        block_cache.add_entry(buf);

        assert_eq!(16 * block_size as u32, block_cache.cache_size());

        assert_eq!(16usize, block_cache.score_list.len());
        assert_eq!(
            block_size as u64,
            unsafe { &*block_cache.score_list.front().unwrap().get() }.wal_offset()
        );
        assert_eq!(
            0,
            unsafe { &*block_cache.score_list.back().unwrap().get() }.wal_offset()
        );

        // Replace a entry with a bigger one
        {
            // Remove the second entry.
            block_cache.remove(|entry| entry.wal_offset() == block_size as u64);
            // Replace the first entry with a bigger one.
            let buf = Arc::new(AlignedBuf::new(0, block_size * 2, block_size * 2).unwrap());
            buf.limit.store(block_size * 2, Ordering::Relaxed);
            block_cache.add_entry(buf);
            assert_eq!(16 * block_size as u32, block_cache.cache_size());

            assert_eq!(15usize, block_cache.score_list.len());
            assert_eq!(
                2 * block_size as u64,
                unsafe { &*block_cache.score_list.front().unwrap().get() }.wal_offset()
            );
            assert_eq!(
                0,
                unsafe { &*block_cache.score_list.back().unwrap().get() }.wal_offset()
            );
        }

        // Replace a entry with a smaller one
        {
            // Replace the first entry with a smaller one.
            let buf = Arc::new(AlignedBuf::new(0, block_size, block_size / 2).unwrap());
            buf.limit.store(block_size, Ordering::Relaxed);
            block_cache.add_entry(buf);
            assert_eq!(15 * block_size as u32, block_cache.cache_size());
            assert_eq!(15usize, block_cache.score_list.len());
        }

        // Replace a entry with a loading one
        {
            // Replace the first entry with a loading one.
            block_cache.add_loading_entry(EntryRange {
                wal_offset: 0,
                len: block_size as u32,
            });
            assert_eq!(14 * block_size as u32, block_cache.cache_size());
            assert_eq!(14usize, block_cache.score_list.len());
        }

        // Record the cache size by capacity rather than the limit size.
        {
            let buf = Arc::new(AlignedBuf::new(0, block_size * 2, block_size / 2).unwrap());
            buf.limit.store(block_size, Ordering::Relaxed);
            block_cache.add_entry(buf);
            assert_eq!(16 * block_size as u32, block_cache.cache_size());
            assert_eq!(15usize, block_cache.score_list.len());
        }
    }

    /// Test get entry.
    #[test]
    fn test_get_entry() {
        // Case one: total hit in a big cached entry
        let cfg = config::Configuration::default();
        let config = Arc::new(cfg);
        let mut block_cache = BlockCache::new(&config, 0);
        let block_size = 4096;

        let buf = Arc::new(AlignedBuf::new(4096, block_size * 1024, block_size).unwrap());
        buf.increase_written(block_size * 1024);

        block_cache.add_entry(buf);
        assert_eq!(1, block_cache.score_list.len());
        assert_eq!(
            4096,
            unsafe { &*block_cache.score_list.back().unwrap().get() }.wal_offset()
        );

        let score = block_cache.min_score();

        let hit = block_cache
            .try_get_entries(EntryRange {
                wal_offset: 4096 * 2,
                len: 4096 * 10,
            })
            .unwrap()
            .unwrap();
        assert_eq!(1, hit.len());
        assert_eq!(block_size * 1024, hit[0].limit());

        let score_after_get = block_cache.min_score();
        assert!(score_after_get > score);

        // Case two: hit partially in two cached entries
        let mut block_cache = BlockCache::new(&config, 0);
        let target_entry = EntryRange {
            wal_offset: 0,
            len: 4096 * 10,
        };

        // Add entry 1: [4096, 4096 * 2)
        let wal_offset = 4096;
        let len = 4096 * 2;
        let buf = Arc::new(AlignedBuf::new(wal_offset, len, block_size).unwrap());
        buf.increase_written(len);
        block_cache.add_entry(buf);

        // Add entry 2: [4096 * 4, 4096)
        let wal_offset = 4096 * 4;
        let len = 4096;
        let buf = Arc::new(AlignedBuf::new(wal_offset, len, block_size).unwrap());
        buf.increase_written(len);
        block_cache.add_entry(buf);

        assert_eq!(2, block_cache.score_list.len());
        let min_score = block_cache.min_score();
        let max_score = unsafe { &*block_cache.score_list.back().unwrap().get() }.score();

        let hit = block_cache.try_get_entries(target_entry).unwrap_err();

        // Miss will leave the score list untouched
        assert_eq!(2, block_cache.score_list.len());
        assert_eq!(min_score, block_cache.min_score());
        assert_eq!(
            max_score,
            unsafe { &*block_cache.score_list.back().unwrap().get() }.score()
        );

        // Miss [0, 4096)
        assert_eq!(3, hit.len());
        assert_eq!(0, hit[0].wal_offset);
        assert_eq!(4096, hit[0].len);

        // Miss [4096 * 3, 4096)
        assert_eq!(4096 * 3, hit[1].wal_offset);
        assert_eq!(4096, hit[1].len);

        // Miss [4096 * 5, 4096 * 5)
        assert_eq!(4096 * 5, hit[2].wal_offset);
        assert_eq!(4096 * 5, hit[2].len);

        // Try add loading entry
        hit.iter().for_each(|r| {
            block_cache.add_loading_entry(*r);
        });

        let pending_hit = block_cache.try_get_entries(target_entry);
        assert!(pending_hit.is_ok());
        assert!(pending_hit.unwrap().is_none());

        // Pending hit will renew the score of cached entries
        assert_eq!(5, block_cache.score_list.len());
        assert!(block_cache.min_score() > max_score);
        let last_score = unsafe { &*block_cache.score_list.back().unwrap().get() }.score();

        // Complete the loading entry
        hit.iter().for_each(|r| {
            let buf = Arc::new(AlignedBuf::new(r.wal_offset, r.len as usize, block_size).unwrap());
            buf.increase_written(r.len as usize);
            block_cache.add_entry(buf);
        });

        // Hit again, with 5 entries returned
        let hit = block_cache.try_get_entries(target_entry).unwrap().unwrap();
        assert_eq!(5, hit.len());

        // Hit will renew the score of cached entries
        assert_eq!(5, block_cache.score_list.len());
        assert!(block_cache.min_score() > last_score);
    }

    #[test]
    fn test_get_last_writable_entry() {
        let cfg = config::Configuration::default();
        let config = Arc::new(cfg);
        let mut block_cache = BlockCache::new(&config, 0);
        let block_size = 4096;

        let buf = Arc::new(AlignedBuf::new(0, block_size, block_size).unwrap());
        buf.increase_written(1024);

        block_cache.add_entry(buf);
        assert_eq!(1, block_cache.score_list.len());
        let score = unsafe { &*block_cache.score_list.back().unwrap().get() }.score();

        let target_entry = EntryRange {
            wal_offset: 0,
            len: 512,
        };
        let hit = block_cache.try_get_entries(target_entry).unwrap().unwrap();
        assert_eq!(1, hit.len());
        assert_eq!(1024, hit[0].limit());

        assert_eq!(1, block_cache.score_list.len());
        assert!(block_cache.min_score() > score);
    }

    #[test]
    fn test_buf_of_last_cache_entry() -> Result<(), Box<dyn Error>> {
        let cfg = config::Configuration::default();
        let config = Arc::new(cfg);
        let mut block_cache = BlockCache::new(&config, 0);
        assert!(block_cache.buf_of_last_cache_entry().is_none());
        let buf = Arc::new(AlignedBuf::new(0, 4096, 512)?);
        block_cache.add_entry(Arc::clone(&buf));

        let buf_ = block_cache.buf_of_last_cache_entry().unwrap();
        assert!(Arc::ptr_eq(&buf, &buf_));
        Ok(())
    }

    #[test]
    fn test_remove() {
        let cfg = config::Configuration::default();
        let config = Arc::new(cfg);
        let mut block_cache = BlockCache::new(&config, 0);
        let block_size = 4096usize;

        block_cache.add_loading_entry(EntryRange::new(0, block_size as u32, block_size));

        for n in 1..16 {
            let buf =
                Arc::new(AlignedBuf::new(n * block_size as u64, block_size, block_size).unwrap());
            buf.limit.store(block_size, Ordering::Relaxed);
            block_cache.add_entry(buf);
        }
        assert_eq!(16, block_cache.entries.len());
        assert_eq!(15, block_cache.score_list.len());

        block_cache.remove_by_score();
        assert_eq!(15, block_cache.entries.len());
        assert!(block_cache.entries.get(&(block_size as u32)).is_none());

        assert_eq!(14, block_cache.score_list.len());
        assert_eq!(
            2 * block_size as u64,
            unsafe { &*block_cache.score_list.front().unwrap().get() }.wal_offset()
        );

        block_cache.remove_by(15 * block_size as u64);
        assert_eq!(14, block_cache.entries.len());
        assert!(block_cache.entries.get(&(15 * block_size as u32)).is_none());

        assert_eq!(13, block_cache.score_list.len());
        assert_eq!(
            14 * block_size as u64,
            unsafe { &*block_cache.score_list.back().unwrap().get() }.wal_offset()
        );
    }
}
