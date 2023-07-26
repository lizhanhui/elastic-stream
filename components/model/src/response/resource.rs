use bytes::Bytes;

use crate::resource::{Resource, ResourceEvent};

#[derive(Debug, Clone)]
pub struct ListResourceResult {
    /// Resources that match the query.
    pub resources: Vec<Resource>,

    /// Resource version of the resource list, which can be used for watch
    pub version: i64,

    /// If the result is truncated, the continuation is the token to be used for next list.
    /// If empty, no more resources can be listed.
    pub continuation: Option<Bytes>,
}

#[derive(Debug, Clone)]
pub struct WatchResourceResult {
    /// Resource events that match the query, including add, modify, delete.
    pub events: Vec<ResourceEvent>,

    /// Resource version when the request is processed. This version can be used for next watch.
    pub version: i64,
}
