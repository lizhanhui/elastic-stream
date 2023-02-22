//! Implement operations offered by `Store` trait.
//!
//!

use pin_project::pin_project;

mod get;
pub mod append;
mod scan;

#[pin_project]
pub struct Append<Op> {
    #[pin]
    pub(crate) inner: Op,
}

pub struct Get {}

pub struct Scan {}
