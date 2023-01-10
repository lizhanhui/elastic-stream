use bytes::Bytes;

#[derive(Debug)]
pub(crate) enum Request {
    ListRange { partition_id: i64 },

    Heartbeat,
}

impl Request {
    pub(crate) fn build_frame_header(&self) -> Option<Bytes> {
        match *self {
            Self::ListRange { partition_id } => {}
            Self::Heartbeat => {}
        }
        None
    }
}
