#![feature(type_alias_impl_trait)]

pub mod error;
pub mod util;

use self::{
    ops::{Get, Put, Scan},
    option::{ReadOptions, WriteOptions},
};
use futures::Future;

pub mod cursor;
pub mod elastic;
pub mod ops;
pub mod option;
pub mod segment;

pub struct Record {
    pub buffer: bytes::Bytes,
}

pub trait Store {
    /// Inner operation that actually puts record into store.
    type PutOp;

    /// Put a new record into store.
    ///
    /// * `options` - Write options, specifying how the record is written to persistent medium.
    /// * `record` - Data record to append.
    fn put(&self, options: WriteOptions, record: Record) -> Put<Self::PutOp>
    where
        <Self as Store>::PutOp: Future;

    /// Retrieve a single existing record at the given partition and offset.
    /// * `options` - Read options, specifying target partition and offset.
    fn get(&self, options: ReadOptions) -> Get;

    /// Scan a range of partition for matched records.
    fn scan(&self, options: ReadOptions) -> Scan;
}
