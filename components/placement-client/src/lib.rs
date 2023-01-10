use std::fmt::{Display, Formatter, Result};

pub mod client;
pub mod error;
pub mod generated;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SessionState {
    Active,
    Closing,
}

impl Display for SessionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match *self {
            SessionState::Active => {
                write!(f, "Active")
            }
            SessionState::Closing => {
                write!(f, "Closing")
            }
        }
    }
}
