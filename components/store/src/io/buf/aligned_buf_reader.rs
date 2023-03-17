use slog::Logger;

use crate::error::StoreError;

use super::AlignedBuf;

pub(crate) struct AlignedBufReader {
    log: Logger,
}

impl AlignedBufReader {
    /// Create a new AlignedBufReader.
    /// The specified `wal_offset` doesn't need to be aligned,
    /// but the returned AlignedBuf's `wal_offset` will be aligned.
    pub(crate) fn alloc_read_buf(
        log: Logger,
        wal_offset: u64,
        len: usize,
        alignment: u64,
    ) -> Result<AlignedBuf, StoreError> {
        // Alignment must be positive
        debug_assert_ne!(0, alignment);
        // Alignment must be power of 2.
        debug_assert_eq!(0, alignment & (alignment - 1));
        let from = wal_offset / alignment * alignment;
        let to = (wal_offset + len as u64 + alignment - 1) / alignment * alignment;
        AlignedBuf::new(log, from, (to - from) as usize, alignment as usize)
    }
}
