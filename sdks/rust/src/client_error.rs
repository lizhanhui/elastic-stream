use std::io;

use codec::error::FrameError;
use flatbuffers::InvalidFlatbuffer;
use thiserror::Error;
use tokio::sync::oneshot;

use crate::node::Node;

#[derive(Debug, Error)]
pub enum ClientError {
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
