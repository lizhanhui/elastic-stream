use std::sync::Arc;

use io_uring::opcode;

use super::buf::AlignedBuf;

/// IO context for `Read` and `Write`.
pub(crate) struct Context {
    /// io_uring opcode
    pub(crate) opcode: u8,

    /// Associated buffer to write or read into.
    pub(crate) buf: Arc<AlignedBuf>,

    /// Original starting WAL offset to read. This field makes sense iff opcode is `Read`.
    /// This field represents the real starting WAL offset of a read operation.
    pub(crate) wal_offset: u64,

    /// Original read length. This field makes sense iff opcode is `Read`.
    /// This field represents the real read length of a read operation.
    pub(crate) len: u32,

    /// The start time of this context
    /// This field is used to calculate the duration from creation to completion of this operation
    pub(crate) start_time: minstant::Instant,

    /// For io_uring_register file
    pub(crate) fd: i32,
    ///
    pub(crate) sparse_offset: i32,
}

impl Context {
    /// Create write context
    pub(crate) fn write_ctx(
        opcode: u8,
        buf: Arc<AlignedBuf>,
        wal_offset: u64,
        len: u32,
        start_time: minstant::Instant,
    ) -> *mut Self {
        Box::into_raw(Box::new(Self {
            opcode,
            buf,
            wal_offset,
            len,
            start_time,
            fd: 0,
            sparse_offset: -1,
        }))
    }

    #[inline(always)]
    pub(crate) fn is_partial_write(&self) -> bool {
        if self.opcode != opcode::Write::CODE && self.opcode != opcode::Writev::CODE {
            return false;
        }

        // Note the underlying buf may expand to a larger size, so we need to check the len of the context.
        self.buf.partial() || self.len < self.buf.limit() as u32
    }

    /// Create read context
    pub(crate) fn read_ctx(
        opcode: u8,
        buf: Arc<AlignedBuf>,
        wal_offset: u64,
        len: u32,
        start_time: minstant::Instant,
    ) -> *mut Self {
        Box::into_raw(Box::new(Self {
            opcode,
            buf,
            wal_offset,
            len,
            start_time,
            fd: 0,
            sparse_offset: -1,
        }))
    }

    pub(crate) fn register_ctx(
        opcode: u8,
        wal_offset: u64,
        fd: i32,
        sparse_offset: i32,
        alignment: usize,
    ) -> Box<Self> {
        let buf = Arc::new(AlignedBuf::new(0, alignment, alignment).unwrap());
        Box::new(Self {
            opcode,
            buf,
            wal_offset,
            fd,
            sparse_offset,
            len: alignment as u32,
            start_time: minstant::Instant::now(),
        })
    }
}
