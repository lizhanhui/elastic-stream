#![feature(io_error_more)]

pub mod crc32;
pub mod fs;

/// Create directories recursively if missing
pub use crate::fs::mkdirs_if_missing;
