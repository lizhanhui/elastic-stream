use std::{collections::HashMap, rc::Rc, sync::Arc, time::Instant};

use super::buf::AlignedBuf;

#[derive(Debug, PartialEq, Eq)]
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

#[derive(Debug, Eq)]
pub(crate) struct BlockCache {
    offset: u64,
    block_size: u32,
    entries: HashMap<u32, Rc<Entry>>,
}

impl BlockCache {
    pub(crate) fn new(offset: u64, block_size: u32) -> Self {
        Self {
            offset,
            block_size,
            entries: HashMap::new(),
        }
    }

    pub(crate) fn add_entry(&mut self, buf: Arc<AlignedBuf>) {
        debug_assert!(buf.offset >= self.offset);
        let entry = Rc::new(Entry::new(buf));
        let from = (entry.buf.offset - self.offset) as u32;
        let mut delta = 0;
        loop {
            self.entries.insert(from + delta, Rc::clone(&entry));
            delta += self.block_size;
            if delta > entry.buf.written as u32 {
                break;
            }
        }
    }

    pub(crate) fn get_entry(&self, offset: u64, len: u32) -> Option<Arc<AlignedBuf>> {
        let key = (offset / self.block_size as u64 * self.block_size as u64 - self.offset) as u32;
        if let Some(entry) = self.entries.get(&key) {
            if entry.buf.offset + entry.buf.written as u64 >= offset + len as u64 {
                return Some(Arc::clone(&entry.buf));
            }
        }
        None
    }

    pub(crate) fn remove<F>(&mut self, pred: F)
    where
        F: Fn(&Rc<Entry>) -> bool,
    {
        self.entries.drain_filter(|k, v| pred(v));
    }
}

impl PartialEq for BlockCache {
    fn eq(&self, other: &Self) -> bool {
        true
    }
}
