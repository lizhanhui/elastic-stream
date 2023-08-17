use crate::io::task::SingleFetchResult;

#[derive(Debug)]
pub struct FetchResult {
    pub stream_id: u64,
    pub offset: i64,

    /// A fetch result may be splitted into multiple `SingleFetchResult`s.
    /// Each `SingleFetchResult` contains a single record.
    pub results: Vec<SingleFetchResult>,
}
