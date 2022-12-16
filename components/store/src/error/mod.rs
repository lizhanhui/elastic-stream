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
