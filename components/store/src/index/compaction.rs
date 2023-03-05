use std::{
    error::Error,
    ffi::{CStr, CString},
    sync::atomic::{AtomicUsize, Ordering},
};

use rocksdb::{
    compaction_filter::CompactionFilter,
    compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory},
    CompactionDecision,
};

pub(crate) struct IndexCompactionFilter {
    name: CString,
}

impl IndexCompactionFilter {
    pub(crate) fn new(n: usize) -> Result<Self, Box<dyn Error>> {
        let name = format!("round-{}", n);
        let name = CString::new(name)?;
        Ok(Self { name })
    }
}

impl CompactionFilter for IndexCompactionFilter {
    fn filter(&mut self, level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        // println!(
        //     "Level: {} Checking {} --> {}",
        //     level,
        //     String::from_utf8_lossy(key),
        //     String::from_utf8_lossy(value)
        // );
        CompactionDecision::Keep
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}

pub(crate) struct IndexCompactionFilterFactory {
    name: CString,
    round: AtomicUsize,
}

impl IndexCompactionFilterFactory {
    pub(crate) fn new(name: &str) -> Result<Self, Box<dyn Error>> {
        let name = CString::new(name)?;
        Ok(Self {
            name,
            round: AtomicUsize::new(0),
        })
    }
}

impl CompactionFilterFactory for IndexCompactionFilterFactory {
    type Filter = IndexCompactionFilter;

    fn create(&mut self, context: CompactionFilterContext) -> Self::Filter {
        if context.is_full_compaction {
            println!("Full Compaction");
        }

        if context.is_manual_compaction {
            println!("Manual Compaction");
        }

        IndexCompactionFilter::new(self.round.fetch_add(1, Ordering::Relaxed)).unwrap()
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}
