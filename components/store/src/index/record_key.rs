use std::io::Cursor;

use bytes::Buf;

pub(crate) struct RecordKey {
    #[allow(dead_code)]
    pub(crate) stream_id: u64,
    #[allow(dead_code)]
    pub(crate) range_index: u32,
    pub(crate) start_offset: u64,
}

impl RecordKey {
    #[allow(dead_code)]
    pub(crate) fn new(stream_id: u64, range_index: u32, start_offset: u64) -> Self {
        Self {
            stream_id,
            range_index,
            start_offset,
        }
    }
}

impl From<&[u8]> for RecordKey {
    fn from(value: &[u8]) -> Self {
        debug_assert!(
            value.len() >= 20,
            "Value of index entry should at least contain offset[8B], length-type[4B]"
        );
        let mut cursor = Cursor::new(value);
        let stream_id = cursor.get_u64();
        let range_index = cursor.get_u32();
        let start_offset = cursor.get_u64();
        Self {
            stream_id,
            range_index,
            start_offset,
        }
    }
}
