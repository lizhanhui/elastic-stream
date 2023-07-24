use std::fmt::Display;

use flatbuffers::InvalidFlatbuffer;
use protocol::rpc::header::ErrorCode;
use thiserror::Error;

#[derive(Debug)]
pub struct EsError {
    pub code: ErrorCode,
    pub message: String,
    source: Option<anyhow::Error>,
}

impl EsError {
    pub fn new(code: ErrorCode, message: &str) -> Self {
        Self {
            code,
            message: message.to_string(),
            source: None,
        }
    }

    pub fn unexpected(message: &str) -> Self {
        Self {
            code: ErrorCode::UNEXPECTED,
            message: message.to_string(),
            source: None,
        }
    }

    pub fn set_source(mut self, src: impl Into<anyhow::Error>) -> Self {
        debug_assert!(self.source.is_none(), "the source error has been set");

        self.source = Some(src.into());
        self
    }
}

impl Display for EsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let source = self.source.as_ref().map(|e| e.to_string());
        write!(
            f,
            "code: {:?}, message: {}, source: {:?}",
            self.code, self.message, source
        )
    }
}

impl std::error::Error for EsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|v| v.as_ref())
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum RangeError {
    #[error("The range has already been sealed")]
    AlreadySealed(u64),
}

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("Last range is already sealed")]
    AlreadySealed,

    #[error("Range index does not match")]
    RangeIndexMismatch { target: i32, actual: i32 },

    #[error("Bad offset")]
    SealBadOffset,

    #[error("Current range-server does not have the stream/range to seal")]
    SealWrongServer,
}

#[derive(Debug, Error)]
pub enum RecordError {
    #[error("Required record field is missing")]
    RequiredFieldMissing,

    #[error("The stream id of the record does not match the stream id of the record batch")]
    StreamIdMismatch,

    #[error("Parse header for record error")]
    ParseHeader,
}

#[derive(Debug, Error)]
pub enum WriterError {
    #[error("Network IO timeout when sending records")]
    Timeout,
}

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("The record batch is empty")]
    EmptyBatch,
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("Build record error")]
    BuildRecord(#[from] RecordError),

    #[error("The total length of record batch is not equal to the length of the data")]
    DataLengthMismatch,

    #[error("The magic number of the record batch is invalid")]
    InvalidMagic,

    #[error("The format of the record batch is invalid")]
    InvalidDataFormat,

    #[error("Failed to parse append request payload")]
    Flatbuffer(#[from] InvalidFlatbuffer),
}
