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
}

#[derive(Debug, Error)]
pub enum ProducerError {
    #[error("Network IO timeout when sending records")]
    Timeout,
}
