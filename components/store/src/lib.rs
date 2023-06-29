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
//! In production, this crate is supposed to run on Linux with modern kernel, offering full-fledged io-uring feature.
#![allow(incomplete_features)]
#![feature(type_alias_impl_trait)]
#![feature(extract_if)]
#![feature(hash_extract_if)]
#![feature(btree_extract_if)]
#![feature(try_find)]
#![feature(btree_cursors)]
#![feature(async_fn_in_trait)]
#![feature(result_flattening)]
#![feature(iterator_try_collect)]
#![feature(io_error_more)]

pub mod error;
pub mod util;

use std::sync::Arc;

use self::option::{ReadOptions, WriteOptions};
use error::{AppendError, FetchError, StoreError};
use model::range::RangeMetadata;

pub mod cursor;
pub mod ops;
pub mod option;

mod index;
mod io;
mod offset_manager;
mod request;
mod store;

pub use crate::io::buf::buf_slice::BufSlice;
pub use crate::io::record::RECORD_PREFIX_LENGTH;
pub use crate::ops::fetch::Fetch;
pub use crate::store::append_result::AppendResult;
pub use crate::store::elastic_store::ElasticStore;
pub use crate::store::fetch_result::FetchResult;
pub use request::AppendRecordRequest;

/// Definition of core storage trait.
pub trait Store {
    /// Append a new record into store.
    ///
    /// * `options` - Write options, specifying how the record is written to persistent medium.
    /// * `record` - Data record to append.
    async fn append(
        &self,
        options: WriteOptions,
        request: AppendRecordRequest,
    ) -> Result<AppendResult, AppendError>;

    /// Retrieve a single existing record at the given stream and offset.
    /// * `options` - Read options, specifying target stream and offset.
    fn fetch(&self, options: ReadOptions) -> Fetch;

    /// List all stream ranges in the store
    ///
    /// if `filter` returns true, the range is kept in the final result vector; dropped otherwise.
    async fn list<F>(&self, filter: F) -> Result<Vec<RangeMetadata>, StoreError>
    where
        F: Fn(&RangeMetadata) -> bool;

    /// List all ranges pertaining to the specified stream in the store
    ///
    /// if `filter` returns true, the range is kept in the final result vector; dropped otherwise.
    async fn list_by_stream<F>(
        &self,
        stream_id: i64,
        filter: F,
    ) -> Result<Vec<RangeMetadata>, StoreError>
    where
        F: Fn(&RangeMetadata) -> bool;

    /// Seal stream range in metadata column family after cross check with placement driver.
    async fn seal(&self, range: RangeMetadata) -> Result<(), StoreError>;

    /// Create a stream range in metadata.
    async fn create(&self, range: RangeMetadata) -> Result<(), StoreError>;

    /// Max record offset in the store of the specified stream.
    fn max_record_offset(&self, stream_id: i64, range: u32) -> Result<Option<u64>, StoreError>;

    fn id(&self) -> i32;

    fn config(&self) -> Arc<config::Configuration>;
}
