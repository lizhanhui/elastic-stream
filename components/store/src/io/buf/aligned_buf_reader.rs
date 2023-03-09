use slog::Logger;

use crate::error::StoreError;

use super::AlignedBuf;

pub(crate) struct AlignedBufReader {
    log: Logger,
}

impl AlignedBufReader {
    pub(crate) fn alloc_read_buf(
        log: Logger,
        offset: u64,
        len: usize,
        alignment: u64,
    ) -> Result<AlignedBuf, StoreError> {
        // Alignment must be positive
        debug_assert_ne!(0, alignment);
        // Alignment must be power of 2.
        debug_assert_eq!(0, alignment & (alignment - 1));
        let from = offset / alignment * alignment;
        let to = (offset + len as u64 + alignment - 1) / alignment * alignment;
        AlignedBuf::new(log, from, (to - from) as usize, alignment as usize)
    }
}
