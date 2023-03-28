use bytes::Bytes;
use protocol::rpc::header::ErrorCode;

#[derive(Debug, Clone)]
pub struct Status {
    pub code: ErrorCode,
    pub message: String,
    pub details: Option<Bytes>,
}

impl Status {
    pub fn ok() -> Self {
        Self {
            code: ErrorCode::OK,
            message: "OK".to_owned(),
            details: None,
        }
    }

    pub fn decode() -> Self {
        Self {
            code: ErrorCode::DECODE,
            message: "Decoding frame header failure".to_owned(),
            details: None,
        }
    }

    pub fn pm_internal(message: String) -> Self {
        Self {
            code: ErrorCode::PM_INTERNAL_SERVER_ERROR,
            message,
            details: None,
        }
    }

    pub fn dn_internal(message: String) -> Self {
        Self {
            code: ErrorCode::DN_INTERNAL_SERVER_ERROR,
            message,
            details: None,
        }
    }

    pub fn bad_request(message: String) -> Self {
        Self {
            code: ErrorCode::BAD_REQUEST,
            message,
            details: None,
        }
    }
}
