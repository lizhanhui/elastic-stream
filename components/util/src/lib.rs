#![feature(io_error_more)]

pub mod crc32;
pub mod fs;
pub mod test;

pub use crate::fs::mkdirs_if_missing;

pub use crate::test::fs::create_random_path;
pub use crate::test::fs::DirectoryRemovalGuard;
pub use crate::test::log::terminal_logger;
pub use crate::test::run_listener;
