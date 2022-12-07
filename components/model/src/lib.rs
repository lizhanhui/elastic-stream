pub mod error;

use error::{RangeError, StreamError};

pub trait Record<'a> {
    fn partition(&self) -> Option<u32>;

    fn offset(&self) -> Option<u64>;

    fn data(&self) -> &'a [u8];
}

pub trait Range {
    fn sealed(&self) -> bool;

    fn seal(&mut self) -> Result<(), RangeError>;
}

pub trait Stream<T> {
    /// Associate type: Range.
    type R: Range;

    fn open(&mut self) -> Result<Vec<Self::R>, StreamError>;

    fn close(&mut self);

    fn delete(&mut self) -> Result<(), StreamError>;

    fn get<'a>(&self, offset: u64) -> Result<Option<T>, StreamError>
    where
        T: Record<'a>;

    fn scan<'a>(&self, offset: u64, len: usize) -> Result<Option<Vec<T>>, StreamError>
    where
        T: Record<'a>;

    fn append<'a>(&mut self, record: &[T]) -> Result<(), StreamError>
    where
        T: Record<'a>;
}
