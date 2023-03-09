//! Implement operations offered by `Store` trait.
//!
//!

use pin_project::pin_project;

pub mod append;
pub mod fetch;
pub mod scan;

#[pin_project]
pub struct Append<Op> {
    #[pin]
    pub(crate) inner: Op,
}

#[pin_project]
pub struct Fetch<Op> {
    #[pin]
    pub(crate) inner: Op,
}

pub struct Scan {}
