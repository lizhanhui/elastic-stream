#[derive(Debug)]
pub(crate) struct RecordHandle {
    pub(crate) offset: u64,
    pub(crate) len: u32,
    pub(crate) hash: u64,
}
