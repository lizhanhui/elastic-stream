use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum RangeError {
    #[error("The range has already been sealed")]
    AlreadySealed(u64),
}

#[derive(Debug, Error)]
pub enum StreamError {}

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
}
