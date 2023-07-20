use bytes::Bytes;

use crate::resource::Resource;

#[derive(Debug, Clone)]
pub struct ListResourceResult {
    pub resources: Vec<Resource>,
    pub version: i64,
    pub continuation: Option<Bytes>,
}
