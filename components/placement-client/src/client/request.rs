use std::fmt::Display;

use bytes::Bytes;

#[derive(Debug)]
pub(crate) enum Request {
    ListRange { partition_id: i64 },

    Heartbeat { client_id: String },
}

impl Request {
    pub(crate) fn build_frame_header(&self) -> Option<Bytes> {
        match *self {
            Self::ListRange { partition_id:_ } => {}
            Self::Heartbeat { .. } => {}
        }
        None
    }
}

impl Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Request::Heartbeat { .. } => write!(f, "Heartbeat"),
            Request::ListRange { .. } => write!(f, "ListRange"),
        }
    }
}
