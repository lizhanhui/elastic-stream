use std::net::SocketAddr;

use codec::error::FrameError;
use thiserror::Error;
use tokio::time::error::Elapsed;

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("Failed to encode frame")]
    EncodeFrame(#[from] FrameError),

    #[error("Network IO")]
    Network(#[from] std::io::Error),

    #[error("TCP connection is not established")]
    NotConnected,

    #[error("Connecting to {target} timeout, elapsed {elapsed}")]
    Timeout {
        target: SocketAddr,
        elapsed: Elapsed,
    },
}
