pub(crate) struct Indexer {}

impl Indexer {
    pub(crate) fn new() -> Self {
        Self {}
    }

    pub(crate) fn index(&self, offset: u64, wal_offset: u64, hash: u64) {}

    /// Returns offset in WAL. All record index are atomic-flushed to RocksDB.
    pub(crate) fn flushed_wal_offset(&self) -> u64 {
        0
    }

    /// Flush record index in cache into RocksDB using atomic-flush.
    pub(crate) fn flush(&self) {}
}
