use crate::BufSlice;

#[derive(Debug)]
pub struct FetchResult {
    pub stream_id: i64,
    pub offset: i64,
    pub payload: Vec<BufSlice>,
}
