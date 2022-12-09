use codec::frame::OperationCode;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("Unsupported operation `{0}`")]
    Unsupported(OperationCode),
}
