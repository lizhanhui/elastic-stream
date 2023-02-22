use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ReadTask {
    /// Offset, in term of WAL, of the record to read.
    pub(crate) offset: u64,

    /// Number of bytes to read.
    pub(crate) len: u32,
}

pub(crate) struct WriteTask {
    /// Stream ID of the record.
    stream_id: u64,
    /// Logical primary index offset
    offset: u64,

    /// `Record` serialized.
    ///
    /// Note: An application `Record` may be splitted into multiple WAL blocks/records,
    /// with enhancing digest/checksum and integrity guarantee.
    buffer: Bytes,
}

pub(crate) enum IoTask {
    Read(ReadTask),
    Write(WriteTask),
}

fn foo() {}
