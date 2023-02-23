pub struct AppendRecordRequest {
    pub stream_id: i64,
    pub offset: i64,
    pub buffer: bytes::Bytes,
}
