use std::sync::Arc;

use super::{buf::AlignedBuf, IoTask};

/// IO context for `Read` and `Write`.
pub(crate) struct Context {
    /// io_uring opcode
    pub(crate) opcode: u8,

    /// Associated buffer to write or read into.
    pub(crate) buf: Arc<AlignedBuf>,

    /// Original starting WAL offset to read. This field makes sense iff opcode is `Read`.
    /// This field represents the real starting WAL offset of a read operation.
    pub(crate) wal_offset: Option<u64>,

    /// Original read length. This field makes sense iff opcode is `Read`.
    /// This field represents the real read length of a read operation.
    pub(crate) len: Option<u32>,
}

impl Context {
    /// Create write context
    pub(crate) fn write_ctx(opcode: u8, buf: Arc<AlignedBuf>) -> *mut Self {
        Box::into_raw(Box::new(Self {
            opcode,
            buf,
            wal_offset: None,
            len: None,
        }))
    }

    /// Create read context
    pub(crate) fn read_ctx(opcode: u8, buf: Arc<AlignedBuf>, offset: u64, len: u32) -> *mut Self {
        Box::into_raw(Box::new(Self {
            opcode,
            buf,
            wal_offset: Some(offset),
            len: Some(len),
        }))
    }
}
