use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("Disk of `{0}` is full")]
    DiskFull(String),
}
