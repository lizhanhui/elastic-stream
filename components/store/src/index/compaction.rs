use std::{
    ffi::{CStr, CString},
    io::Cursor,
    rc::Rc,
};

use bytes::Buf;
use rocksdb::{
    compaction_filter::CompactionFilter,
    compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory},
    CompactionDecision,
};

use slog::{info, trace, Logger};

use super::MinOffset;

pub(crate) struct IndexCompactionFilter {
    log: Logger,
    name: CString,
    min_offset: u64,
}

impl IndexCompactionFilter {
    pub(crate) fn new(log: Logger, name: CString, min_offset: u64) -> Self {
        Self {
            log,
            name,
            min_offset,
        }
    }
}

impl CompactionFilter for IndexCompactionFilter {
    fn filter(&mut self, _level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        if key.len() != 8 {
            return CompactionDecision::Keep;
        }

        if value.len() < 8 {
            return CompactionDecision::Remove;
        }
        let mut rdr = Cursor::new(&value[..]);
        let offset = rdr.get_u64();
        if offset < self.min_offset {
            trace!(
                self.log,
                "Removed {} -> {}, min-offset: {}",
                Cursor::new(&key[..]).get_u64(),
                offset,
                self.min_offset
            );
            CompactionDecision::Remove
        } else {
            CompactionDecision::Keep
        }
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}

pub(crate) struct IndexCompactionFilterFactory {
    log: Logger,
    name: CString,
    min_offset: Rc<dyn MinOffset>,
}

impl IndexCompactionFilterFactory {
    pub(crate) fn new(log: Logger, name: CString, min_offset: Rc<dyn MinOffset>) -> Self {
        Self {
            log,
            name,
            min_offset,
        }
    }
}

impl CompactionFilterFactory for IndexCompactionFilterFactory {
    type Filter = IndexCompactionFilter;

    fn create(&mut self, context: CompactionFilterContext) -> Self::Filter {
        info!(
            self.log,
            "Created a `IndexCompactionFilter`: full_compaction: {}, manual_compaction: {}",
            context.is_full_compaction,
            context.is_manual_compaction
        );

        IndexCompactionFilter::new(
            self.log.clone(),
            self.name.clone(),
            self.min_offset.min_offset(),
        )
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}
