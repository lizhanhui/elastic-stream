//! Define various error types for this crate.
//!
//! Though some developers prefer to have their errors in each module, this crate takes the strategy of
//! defining errors centrally. Namely, all errors live in this module with the hope of having a consistent
//! and coherent heirarchy of errors.
//!

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("Disk of `{0}` is full")]
    DiskFull(String),

    #[error("Request path `{0}` is invalid")]
    InvalidPath(String),

    #[error("`{0}`")]
    NotFound(String),

    #[error("Internal IO error")]
    IO(#[from] std::io::Error),

    #[error("Create to create IO_Uring instance")]
    IO_URING,
}

#[derive(Debug, Error)]
pub enum ReadError {}

#[derive(Debug, Error)]
pub enum PutError {
    #[error("Failed to send PutRequest")]
    SubmissionQueue,

    #[error("Recv from oneshot channel failed")]
    ChannelRecv,

    #[error("Internal error")]
    Internal,
}
