use std::io;

use model::error;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("ConnectTimeout")]
    ConnectTimeout(#[from] io::Error),
}
