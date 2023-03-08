mod block_cache;
pub(crate) mod buf;
mod context;
mod offset_manager;
mod options;
mod record;
mod segment;
pub(crate) mod task;
mod uring;
mod wal;
mod write_window;

use crc::{Crc, CRC_32_ISCSI};

pub(crate) const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
pub(crate) use self::options::Options;
pub(crate) use self::task::{IoTask, ReadTask, WriteTask};
pub(crate) use self::uring::IO;
pub(crate) use self::write_window::{WriteWindow, WriteWindowError};
