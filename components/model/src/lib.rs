pub mod error;

use std::time::Duration;

use error::{RangeError, StreamError};

pub trait Record<'a> {
    fn partition() -> Option<u32>;

    fn offset() -> Option<u64>;

    fn data() -> &'a [u8];
}

pub trait Range {
    fn sealed() -> bool;

    fn seal() -> Result<(), RangeError>;
}

pub trait Stream {
    /// Associate type: Range.
    type R: Range;

    fn open() -> Result<Vec<Self::R>, StreamError>;

    fn close();

    fn delete() -> Result<(), StreamError>;
}

pub trait Reader {
    /// Associate type: Record
    type R;

    fn pread(offset: u64, len: usize, timeout: Duration) -> Result<Vec<Self::R>, StreamError>;
}

pub trait Writer {
    /// Associate type: Record
    type R;

    fn append(record: Self::R, timeout: Duration) -> Result<u64, StreamError>;

    fn append_batch(records: &[Self::R], timeout: Duration) -> Result<Vec<u64>, StreamError>;
}
