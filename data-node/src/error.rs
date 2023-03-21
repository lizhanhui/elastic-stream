use codec::frame::OperationCode;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("Unsupported operation `{0}`")]
    Unsupported(OperationCode),

    #[error("Failed to acquire stream range from placement managers")]
    AcquireRange,

    #[error("Failed to seal a stream range")]
    Seal,
}

#[derive(Debug, Error)]
pub enum LaunchError {
    #[error("No cores available to bind")]
    NoCoresAvailable,
}
