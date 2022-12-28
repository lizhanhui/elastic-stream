use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum RangeError {
    #[error("The range has already been sealed")]
    AlreadySealed(u64),
}

#[derive(Debug, Error)]
pub enum StreamError {}
