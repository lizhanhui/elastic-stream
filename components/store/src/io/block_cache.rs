use std::{cell::UnsafeCell, collections::BTreeMap, rc::Rc, sync::Arc, time::Instant};

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
    offset: u64,
    entries: BTreeMap<u32, Rc<UnsafeCell<Entry>>>,
}

impl BlockCache {
    pub(crate) fn new(offset: u64) -> Self {
        Self {
            offset,
            entries: BTreeMap::new(),
        }
    }

    pub(crate) fn add_entry(&mut self, buf: Arc<AlignedBuf>) {
        debug_assert!(buf.offset >= self.offset);
        let from = (buf.offset - self.offset) as u32;
        let entry = Rc::new(UnsafeCell::new(Entry::new(buf)));
        self.entries.insert(from, entry);
    }

    pub(crate) fn get_entry(&self, offset: u64, len: u32) -> Option<Arc<AlignedBuf>> {
        let to = offset.checked_sub(self.offset).expect("out of bound") as u32;
        let search = self.entries.range(..to).rev().try_find(|(_k, entry)| {
            let item = unsafe { &mut *entry.get() };
            if item.buf.covers(offset, len) {
                item.hit += 1;
                item.last_hit_instant = Instant::now();
                Ok(true)
            } else if item.buf.offset > offset {
                Err(CacheError::Miss)
            } else {
                Ok(false)
            }
        });

        if let Ok(Some((_, entry))) = search {
            let item = unsafe { &mut *entry.get() };
            return Some(Arc::clone(&item.buf));
        }
        None
    }

    /// Remove cache entries if `Predicate` returns `true`.
    ///
    /// #Arguments
    /// * `pred` - Predicate that return true if the entry is supposed to be dropped and false to reserve.
    pub(crate) fn remove<F>(&mut self, pred: F)
    where
        F: Fn(&Entry) -> bool,
    {
        self.entries.drain_filter(|_k, v| {
            let entry = unsafe { &*v.get() };
            pred(entry)
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        sync::{atomic::Ordering, Arc},
    };

    use crate::io::buf::AlignedBuf;

    #[test]
    fn test_hit() -> Result<(), Box<dyn Error>> {
        let log = test_util::terminal_logger();
        let mut block_cache = super::BlockCache::new(0);
        let block_size = 4096;
        for n in (0..16).into_iter() {
            let buf = Arc::new(AlignedBuf::new(
                log.clone(),
                n * block_size as u64,
                block_size,
                block_size,
            )?);
            buf.written.store(block_size, Ordering::Relaxed);
            block_cache.add_entry(buf);
        }

        let buf = block_cache.get_entry(1024, 1024);
        assert_eq!(true, buf.is_some());

        block_cache.remove(|e| e.hit == 0);

        let buf = block_cache.get_entry(1024, 1024);
        assert_eq!(true, buf.is_some());

        let buf = block_cache.get_entry(8192, 1024);
        assert_eq!(true, buf.is_none());

        Ok(())
    }
}
