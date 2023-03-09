#![feature(io_error_more)]

pub mod crc32;
pub mod fs;
pub use crate::fs::mkdirs_if_missing;
