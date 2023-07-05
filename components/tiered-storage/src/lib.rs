#![feature(async_fn_in_trait)]
#![feature(map_try_insert)]

pub mod object_manager;
pub mod object_storage;
mod range_accumulator;
mod range_offload;

use bytes::Bytes;
use store::error::FetchError;

#[cfg(test)]
use mockall::{automock, predicate::*};

#[cfg_attr(test, automock)]
pub trait TieredStorage {
    fn add_range(&self, stream_id: u64, range_index: u32, start_offset: u64, end_offset: u64);

    /// new record arrived notify
    fn new_record_arrived(
        &self,
        stream_id: u64,
        range_index: u32,
        end_offset: u64,
        record_size: u32,
    );
}

pub trait RangeFetcher {
    async fn fetch(
        &self,
        stream_id: u64,
        range_index: u32,
        start_offset: u64,
        end_offset: u64,
        max_size: u32,
    ) -> Result<RangeFetchResult, FetchError>;
}

pub struct MockRangeFetcher;

impl RangeFetcher for MockRangeFetcher {
    async fn fetch(
        &self,
        _stream_id: u64,
        _range_index: u32,
        _start_offset: u64,
        _end_offset: u64,
        _max_size: u32,
    ) -> Result<RangeFetchResult, FetchError> {
        unimplemented!()
    }
}

pub struct RangeFetchResult {
    pub payload: Vec<Bytes>,
    pub end_offset: u64,
}

pub trait ObjectManager {
    fn campaign(&self, stream_id: u64, range_index: u32);

    fn commit_object(&self, object_metadata: ObjectMetadata);

    fn get_objects(
        &self,
        stream_id: u64,
        range_index: u32,
        start_offset: u64,
        end_offset: u64,
    ) -> Vec<ObjectMetadata>;
}

#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub stream_id: u64,
    pub range_index: u32,
    pub start_offset: u64,
    pub end_offset_delta: u32,
    pub sparse_index: Bytes,
}

impl ObjectMetadata {
    pub fn new(stream_id: u64, range_index: u32, start_offset: u64) -> Self {
        Self {
            stream_id,
            range_index,
            start_offset,
            end_offset_delta: 0,
            sparse_index: Bytes::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RangeKey {
    stream_id: u64,
    range_index: u32,
}

impl RangeKey {
    pub fn new(stream_id: u64, range_index: u32) -> Self {
        Self {
            stream_id,
            range_index,
        }
    }
}