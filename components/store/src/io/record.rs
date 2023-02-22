

/// Type of the
///
/// +---------+-----------+-----------+--- ... ---+
/// |CRC (4B) | Size (3B) | Type (1B) | Payload   |
/// +---------+-----------+-----------+--- ... ---+
///
/// CRC = 32bit hash computed over the payload using CRC
/// Size = Length of the payload data
/// Type = Type of record
///        (ZeroType, FullType, FirstType, LastType, MiddleType )
///        The type is used to group a bunch of records together to represent
///        blocks that are larger than BlockSize
/// Payload = Byte stream as long as specified by the payload size
#[repr(u8)]
pub(crate) enum RecordType {
    Zero = 0,
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}


