use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum CommandError {}

#[derive(Debug, Error, PartialEq)]
pub enum ClientError {
    #[error("Bad address")]
    BadAddress,

    #[error("Timeout on connecting `{0}`")]
    ConnectTimeout(String),

    #[error("Failed to establish TCP connection. Cause: `{0}`")]
    ConnectFailure(String),
}
