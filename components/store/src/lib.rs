//! # Design Overview and Theory
//!
//! `store` crate follows "thread-per-core" paradigm to achieve the goal of scaling linearly with the addition and evolution of hardware.
//!  This design pattern makes execution of threads independent from one another as much as possible, which means avoiding software locks
//!  and even atomic instructions. Read [SPDK](https://spdk.io/doc/concurrency.html) for a detailed explanation and analysis.
//!
//! There are other libraries and products adopting this design:
//! - [Datadog Glommio](https://www.datadoghq.com/blog/engineering/introducing-glommio/)
//! - [ScyllaDB](https://www.scylladb.com/) and [Seastar](https://seastar.io/)
//! - [Redpanda](https://redpanda.com/blog/tpc-buffers)
//! - [SPDK](https://spdk.io/doc/concurrency.html)
//!
//! # Implementation
//! In production, this crate is supposed to run on Linux with modern kernel, offering full-fledged io-uring feature. It also works for macOS
//!  and legacy Linux where io-uring is not available through falling back to kqueue and epoll respectively.
//!
//! # Rust Note
//! `TAIT` feature is employed to wrap async methods into Future, which would then be wrapped into `tower`-like service and layers. As a result,
//! nightly rust-toolchain is required for the moment.
#![feature(type_alias_impl_trait)]

pub mod error;
pub mod util;

use self::{
    ops::{Get, Put, Scan},
    option::{ReadOptions, WriteOptions},
};
use error::PutError;
use futures::Future;
use ops::put::PutResult;

pub mod cursor;
pub mod ops;
pub mod option;
pub mod segment;

mod io;
mod store;

pub use crate::store::ElasticStore;

pub struct Record {
    pub buffer: bytes::Bytes,
}

/// Definition of core storage trait.
///
///
pub trait Store {
    /// Inner operation that actually puts record into store.
    type PutOp;

    /// Put a new record into store.
    ///
    /// * `options` - Write options, specifying how the record is written to persistent medium.
    /// * `record` - Data record to append.
    fn put(&self, options: WriteOptions, record: Record) -> Put<Self::PutOp>
    where
        <Self as Store>::PutOp: Future<Output = Result<PutResult, PutError>>;

    /// Retrieve a single existing record at the given partition and offset.
    /// * `options` - Read options, specifying target partition and offset.
    fn get(&self, options: ReadOptions) -> Get;

    /// Scan a range of partition for matched records.
    fn scan(&self, options: ReadOptions) -> Scan;
}
