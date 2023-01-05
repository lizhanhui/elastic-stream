use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum CommandError {}

#[derive(Debug, Error, PartialEq)]
pub enum ClientError {
    #[error("Bad address")]
    BadAddress,

    #[error("Connection to `{0}` is refused")]
    ConnectionRefused(String),

    #[error("Timeout on connecting `{0}`")]
    ConnectTimeout(String),

    #[error("Failed to establish TCP connection. Cause: `{0}`")]
    ConnectFailure(String),

    #[error("Failed to disable Nagle's algorithm")]
    DisableNagleAlgorithm,
}

#[derive(Debug, Error)]
pub enum ListRangeError {
    #[error("Internal client error")]
    Internal,
}
