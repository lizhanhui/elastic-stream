use std::io;

use crate::node::Node;
use codec::error::FrameError;
use flatbuffers::InvalidFlatbuffer;
use model::Status;
use thiserror::Error;
use tokio::sync::oneshot;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Internal system error: {0:?}")]
    Internal(Status),

    #[error("Unexpected response: {0}")]
    UnexpectedResponse(String),

    #[error("ConnectTimeout")]
    ConnectTimeout(#[from] io::Error),

    #[error("Codec error")]
    Codec(#[from] FrameError),

    #[error("Oneshot receive error")]
    Recv(#[from] oneshot::error::RecvError),

    #[error("Flatbuffers got invalid data")]
    Parse(#[from] InvalidFlatbuffer),

    #[error("Connection reset: {0}")]
    ConnectionReset(String),

    #[error("Placement managers have no enough data nodes")]
    DataNodeNotAvailable,

    #[error("Requested node is not playing leader role any more")]
    LeadershipChanged { nodes: Vec<Node> },
}
