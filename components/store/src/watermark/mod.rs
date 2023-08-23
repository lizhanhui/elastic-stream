pub(crate) mod manager;
pub(crate) mod range_descriptor;
pub(crate) mod stream_descriptor;
pub(crate) mod wal_watermark;

/// Expose minimum and offload WAL watermarks.
///
/// WAL file sequence would periodically check and purge deprecated segment files. Once a segment file is removed,
/// min offset watermark of the WAL is be updated. Their index entries, that map to the removed file should be
/// compacted away.
///
/// If tiered storage feature is active, data will be offloaded to object storage service(OSS, S3) asynchronously.
/// `offload` watermark is used to track the upload progress.
pub trait Watermark {
    fn min(&self) -> u64;

    fn offload(&self) -> u64;

    fn set_min(&self, value: u64);

    fn set_offload(&self, value: u64);
}
pub(crate) use wal_watermark::WalWatermark;

pub trait WatermarkManager {
    fn on_index(&mut self, stream_id: u64, range: u32, offset: u64);

    fn trim_stream(&mut self, stream_id: u64, offset: u64);

    fn on_data_offload(&mut self, stream_id: u64, range: u32, offset: u64, delta: u32);

    fn add_range(&mut self, stream_id: u64, range: u32, start: u64, end: Option<u64>);

    fn seal_range(&mut self, stream_id: u64, range: u32, end: u64);

    fn trim_range(&mut self, stream_id: u64, range: u32, start: u64);

    fn delete_range(&mut self, stream_id: u64, range: u32);

    fn min_wal_offset(&mut self) -> u64;

    fn offload_wal_offset(&mut self) -> u64;
}

pub(crate) struct OffloadSlice {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

pub(crate) use manager::DefaultWatermarkManager;
