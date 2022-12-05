use thiserror::Error;

#[derive(Debug, Error)]
pub enum RangeError {}

#[derive(Debug, Error)]
pub enum StreamError {}
