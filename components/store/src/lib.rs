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
#![feature(drain_filter)]
#![feature(hash_drain_filter)]
#![feature(btree_drain_filter)]
#![feature(try_find)]

pub mod error;
pub mod util;

use self::{
    ops::{Append, Fetch, Scan},
    option::{ReadOptions, WriteOptions},
};
use error::{AppendError, FetchError};
use futures::Future;
use ops::append::AppendResult;
use ops::fetch::FetchResult;

pub mod cursor;
pub mod ops;
pub mod option;

mod index;
mod io;
mod offset_manager;
mod request;
mod store;

pub use crate::io::buf::buf_slice::BufSlice;
pub use crate::store::ElasticStore;
pub use request::AppendRecordRequest;

/// Definition of core storage trait.
///
///
pub trait Store {
    /// Inner operation that actually appends record into store.
    type AppendOp;
    /// Inner operation that actually fetches record from store.
    type FetchOp;

    /// Append a new record into store.
    ///
    /// * `options` - Write options, specifying how the record is written to persistent medium.
    /// * `record` - Data record to append.
    fn append(&self, options: WriteOptions, request: AppendRecordRequest) -> Append<Self::AppendOp>
    where
        <Self as Store>::AppendOp: Future<Output = Result<AppendResult, AppendError>>;

    /// Retrieve a single existing record at the given stream and offset.
    /// * `options` - Read options, specifying target stream and offset.
    fn fetch(&self, options: ReadOptions) -> Fetch<Self::FetchOp>
    where
        <Self as Store>::FetchOp: Future<Output = Result<FetchResult, FetchError>>;

    /// Scan a range of stream for matched records.
    fn scan(&self, options: ReadOptions) -> Scan;
}
