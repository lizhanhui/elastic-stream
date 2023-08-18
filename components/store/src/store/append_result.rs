#[derive(Debug)]
pub struct AppendResult {
    pub stream_id: u64,
    pub range_index: u32,
    pub offset: u64,
    pub last_offset_delta: u32,
    pub wal_offset: u64,
    pub bytes_len: u32,
}
