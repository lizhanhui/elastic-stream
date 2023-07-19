#![feature(io_error_more)]
#![feature(io_slice_advance)]
#![feature(slice_first_last_chunk)]
#![feature(slice_pattern)]

pub mod bytes;
pub mod crc32;
pub(crate) mod handle_joiner;
pub mod metrics;

pub use crate::handle_joiner::HandleJoiner;
