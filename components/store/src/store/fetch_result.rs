use crate::io::task::SingleFetchResult;

#[derive(Debug)]
pub struct FetchResult {
    /// Stream ID of the record.
    pub stream_id: u64,

    /// Range index of the record.
    pub range: u32,

    /// Minimum logic index of all records.
    pub start_offset: u64,

    /// Maximum logic index of all records.
    pub end_offset: u64,

    /// Total length of the record payload.
    pub total_len: usize,

    /// A fetch result may be splitted into multiple `SingleFetchResult`s.
    /// Each `SingleFetchResult` contains a single record.
    pub results: Vec<SingleFetchResult>,
}
