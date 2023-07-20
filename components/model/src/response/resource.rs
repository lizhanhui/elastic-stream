use bytes::Bytes;

use crate::resource::{Resource, ResourceEvent};

#[derive(Debug, Clone)]
pub struct ListResourceResult {
    pub resources: Vec<Resource>,
    pub version: i64,
    pub continuation: Option<Bytes>,
}

#[derive(Debug, Clone)]
pub struct WatchResourceResult {
    pub events: Vec<ResourceEvent>,
    pub version: i64,
}
