use std::sync::Arc;

use super::buf::AlignedBuf;

pub(crate) struct OpState {
    pub(crate) opcode: u8,

    pub(crate) buf: Arc<AlignedBuf>,

    pub(crate) offset: Option<u64>,
    pub(crate) len: Option<u32>,
}
