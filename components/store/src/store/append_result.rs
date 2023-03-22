#[derive(Debug)]
pub struct AppendResult {
    pub stream_id: i64,
    pub offset: i64,
    pub wal_offset: u64,
}