#[derive(Debug)]
pub(crate) struct RecordHandle {
    pub(crate) wal_offset: u64,
    pub(crate) len: u32,
    pub(crate) hash: u64,
}
