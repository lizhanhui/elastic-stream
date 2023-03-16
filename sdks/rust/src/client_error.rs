use std::io;

use codec::error::FrameError;
use flatbuffers::InvalidFlatbuffer;
use thiserror::Error;
use tokio::sync::oneshot;

#[derive(Debug, Error)]
pub enum ClientError {
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
}
