use std::sync::Arc;

use super::buf::AlignedBuf;

pub(crate) struct OpState {
    pub(crate) opcode: u8,

    pub(crate) buf: Arc<AlignedBuf>,

    /// Original starting WAL offset to read. This field makes sense iff opcode is `Read`.
    pub(crate) offset: Option<u64>,

    /// Original read length. This field makes sense iff opcode is `Read`.
    pub(crate) len: Option<u32>,
}
