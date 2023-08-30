use std::io::Cursor;

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Index entry in RocksDB
pub(crate) struct Record {
    // Record index, the key of record in RocksDB
    pub(crate) index: RecordIndex,

    // Record details, used to locate the record in WAL
    pub(crate) handle: RecordHandle,
}

impl Record {
    /// End offset of nested entries in the pointed `Record`
    pub(crate) fn end_offset(&self) -> u64 {
        match self.handle.ext {
            HandleExt::Hash(..) => self.index.offset + 1,
            HandleExt::BatchSize(len) => self.index.offset + len as u64,
        }
    }
}

pub(crate) struct RecordIndex {
    /// Stream ID of the record.
    pub(crate) stream_id: u64,

    /// Range index of the record.
    pub(crate) range: u32,

    /// Logic offset of the record.
    pub(crate) offset: u64,
}

impl TryFrom<&[u8]> for RecordIndex {
    type Error = String;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != 20 {
            return Err(format!("Key of index entry should contain stream_id[8B], range[4B], offset[8B], but got {}B", value.len()));
        }
        let mut cursor = Cursor::new(value);
        let stream_id = cursor.get_u64();
        let range = cursor.get_u32();
        let offset = cursor.get_u64();
        Ok(Self {
            stream_id,
            range,
            offset,
        })
    }
}

impl From<&RecordIndex> for Bytes {
    fn from(index: &RecordIndex) -> Self {
        let mut value_buf = BytesMut::with_capacity(20);
        value_buf.put_u64(index.stream_id);
        value_buf.put_u32(index.range);
        value_buf.put_u64(index.offset);
        value_buf.freeze()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RecordHandle {
    /// WAL offset
    pub(crate) wal_offset: u64,

    /// Bytes of the `Record` in WAL
    pub(crate) len: u32,

    /// Extended information of the record.
    pub(crate) ext: HandleExt,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HandleExt {
    /// Hash of the `Record` tag if it contains a single entry.
    Hash(u64),

    /// Number of the nested entries included in the pointed `Record`.
    BatchSize(u32),
}

impl HandleExt {
    #[allow(dead_code)]
    pub(crate) fn count(&self) -> u32 {
        match self {
            HandleExt::Hash(_) => 1,
            HandleExt::BatchSize(len) => *len,
        }
    }
}

impl TryFrom<&[u8]> for RecordHandle {
    type Error = String;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < 12 {
            return Err(format!("Value of index entry should at least contain offset[8B], length_type[4B], but got {}B", value.len()));
        }
        let mut cursor = Cursor::new(value);
        let offset = cursor.get_u64();
        let length_type = cursor.get_u32();
        let ty = length_type & 0xFF;
        let len = length_type >> 8;
        let ext = match ty {
            0 => {
                debug_assert!(
                    cursor.remaining() >= 8,
                    "Extended field should be u64, hash of record tag"
                );
                HandleExt::Hash(cursor.get_u64())
            }
            1 => {
                debug_assert!(
                    cursor.remaining() >= 4,
                    "Extended field should be u32, number of nested entries"
                );
                HandleExt::BatchSize(cursor.get_u32())
            }
            _ => {
                unreachable!("Unknown type");
            }
        };
        Ok(Self {
            wal_offset: offset,
            len,
            ext,
        })
    }
}

impl From<&RecordHandle> for Bytes {
    fn from(handle: &RecordHandle) -> Self {
        let mut value_buf = BytesMut::with_capacity(20);
        value_buf.put_u64(handle.wal_offset);
        let mut length_type = handle.len << 8;
        match handle.ext {
            HandleExt::Hash(hash) => {
                value_buf.put_u32(length_type);
                value_buf.put_u64(hash);
            }
            HandleExt::BatchSize(len) => {
                // set type
                length_type |= 1;

                value_buf.put_u32(length_type);
                value_buf.put_u32(len);
            }
        };
        value_buf.freeze()
    }
}
