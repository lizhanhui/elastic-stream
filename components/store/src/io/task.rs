use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{error::AppendError, ops::append::AppendResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ReadTask {
    /// Offset, in term of WAL, of the record to read.
    pub(crate) offset: u64,

    /// Number of bytes to read.
    pub(crate) len: u32,
}

pub(crate) struct WriteTask {
    /// Stream ID of the record.
    pub(crate) stream_id: i64,
    /// Logical primary index offset
    pub(crate) offset: i64,

    /// `Record` serialized.
    ///
    /// Note: An application `Record` may be splitted into multiple WAL blocks/records,
    /// with enhancing digest/checksum and integrity guarantee.
    pub(crate) buffer: Bytes,

    pub(crate) observer: oneshot::Sender<Result<AppendResult, AppendError>>,
}

impl WriteTask {
    // Fetch the total length of the record, incuding the header.
    pub(crate) fn total_len(&self) -> u32 {
        self.buffer.len() as u32 + 4 /* CRC */ + 3 /* Record Size */ + 1 /* Record Type */
    }
}

pub(crate) enum IoTask {
    Read(ReadTask),
    Write(WriteTask),
}
