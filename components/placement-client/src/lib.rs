use std::fmt::{Display, Formatter, Result};

pub mod client;
pub mod error;

#[derive(Debug, Clone, Copy)]
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
