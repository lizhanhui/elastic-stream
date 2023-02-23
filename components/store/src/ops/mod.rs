//! Implement operations offered by `Store` trait.
//!
//!

use pin_project::pin_project;

pub mod append;
mod get;
mod scan;

#[pin_project]
pub struct Append<Op> {
    #[pin]
    pub(crate) inner: Op,
}

pub struct Get {}

pub struct Scan {}
