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
