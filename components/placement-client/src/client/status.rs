use bytes::Bytes;

use super::error_code::ErrorCode;

#[derive(Debug, Clone)]
pub struct Status {
    pub code: ErrorCode,
    pub message: String,
    pub details: Option<Bytes>,
}

impl Status {
    pub fn ok() -> Self {
        Self {
            code: ErrorCode::Ok,
            message: "OK".to_owned(),
            details: None,
        }
    }

    pub fn internal(message: String) -> Self {
        Self {
            code: ErrorCode::Internal,
            message,
            details: None,
        }
    }

    pub fn invalid_request(message: String) -> Self {
        Self {
            code: ErrorCode::InvalidRequest,
            message,
            details: None,
        }
    }
}
