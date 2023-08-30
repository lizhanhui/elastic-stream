use std::{
    ffi::CString,
    fs,
    io::Cursor,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::{error, info, trace, warn};
use rocksdb::{
    BlockBasedOptions, ColumnFamilyDescriptor, DBCompressionType, FlushOptions, IteratorMode,
    Options, ReadOptions, WriteOptions, DB,
};
use tokio::sync::mpsc;

use config::Configuration;
use model::range::RangeMetadata;

use crate::index::record::{Record, RecordIndex};
use crate::{error::StoreError, watermark::Watermark};

use super::{record::RecordHandle, Indexer};

const INDEX_COLUMN_FAMILY: &str = "index";
const METADATA_COLUMN_FAMILY: &str = "metadata";

/// Key-value in metadata column family, flagging WAL offset, prior to which primary index are already built.
const WAL_CHECKPOINT: &str = "wal_checkpoint";

/// Representation of a range in metadata column family: {range-prefix: 1B}{stream_id:8B}{begin:8B} --> {status: 1B}[{end: 8B}].
///
/// If a range is sealed, its status is changed from `0` to `1` and logical `end` will be appended.
const RANGE_PREFIX: u8 = b'r';

pub(crate) struct DefaultIndexer {
    /// RocksDB instance
    db: DB,

    /// Write options used when put index and checkpoint WAL metadata.
    ///
    /// Note that WAL is disabled to achieve best performance and `Atomic Flush` is used to keep these two consistent.
    /// It is possible that index records and updates to checkpoint WAL may be lost in case of power failure or OS crash, but
    /// recovery procedure will rebuild them all on reboot.
    write_opts: WriteOptions,

    /// Number of index records put to RocksDB without manual flush.
    count: AtomicUsize,

    /// Trigger manual DB flush after `flush_threshold` index records are put.
    flush_threshold: usize,
}

impl DefaultIndexer {
    /// Build RocksDB column family options for index.
    ///
    fn build_index_column_family_options(
        min_offset: Arc<dyn Watermark>,
    ) -> Result<Options, StoreError> {
        let mut index_cf_opts = Options::default();
        index_cf_opts.enable_statistics();
        index_cf_opts.set_write_buffer_size(128 * 1024 * 1024);
        index_cf_opts.set_max_write_buffer_number(4);
        index_cf_opts.set_compression_type(DBCompressionType::None);
        {
            // 128MiB block cache
            let cache = rocksdb::Cache::new_lru_cache(128 << 20);
            let mut table_opts = BlockBasedOptions::default();
            table_opts.set_block_cache(&cache);
            table_opts.set_block_size(128 << 10);
            index_cf_opts.set_block_based_table_factory(&table_opts);
        }

        let compaction_filter_name = CString::new("index-compaction-filter").map_err(|_e| {
            StoreError::Internal(
                "Failed to create index compaction filter name in CString".to_owned(),
            )
        })?;

        let index_compaction_filter_factory = super::compaction::IndexCompactionFilterFactory::new(
            compaction_filter_name,
            min_offset,
        );

        index_cf_opts.set_compaction_filter_factory(index_compaction_filter_factory);
        Ok(index_cf_opts)
    }

    /// Construct a new `Index`.
    ///
    /// # Arguments
    ///
    /// * `path` - Path where RocksDB store shall live
    /// * `min_offset` - Pointer to struct where current minimum WAL offset can be retrieved
    /// * `flush_threshold` - Flush index and metadata records every N operations.
    pub(crate) fn new(
        config: &Arc<Configuration>,
        min_offset: Arc<dyn Watermark>,
        flush_threshold: usize,
    ) -> Result<Self, StoreError> {
        let path = config.store.path.metadata_path();
        if !path.exists() {
            info!("Create directory: {:?}", path);
            fs::create_dir_all(path.as_path())?;
        }

        let index_cf_opts = Self::build_index_column_family_options(Arc::clone(&min_offset))?;
        let index_cf = ColumnFamilyDescriptor::new(INDEX_COLUMN_FAMILY, index_cf_opts);

        let mut metadata_cf_opts = Options::default();
        metadata_cf_opts.enable_statistics();
        metadata_cf_opts.create_if_missing(true);
        metadata_cf_opts.optimize_for_point_lookup(16 << 20);

        // Set compaction filter for ranges
        let range_compaction_filter_name =
            CString::new("range-compaction-filter").map_err(|_e| {
                StoreError::Internal(
                    "Failed to create range-compaction filter name in CString".to_owned(),
                )
            })?;
        let range_compaction_filter_factory = super::compaction::IndexCompactionFilterFactory::new(
            range_compaction_filter_name,
            Arc::clone(&min_offset),
        );
        metadata_cf_opts.set_compaction_filter_factory(range_compaction_filter_factory);
        let metadata_cf = ColumnFamilyDescriptor::new(METADATA_COLUMN_FAMILY, metadata_cf_opts);

        let mut db_opts = Options::default();
        db_opts.set_atomic_flush(true);
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.enable_statistics();
        db_opts.set_stats_dump_period_sec(10);
        db_opts.set_stats_persist_period_sec(10);
        // Threshold of all memtable across column families added in size
        db_opts.set_db_write_buffer_size(1024 * 1024 * 1024);
        db_opts.set_max_background_jobs(2);

        if let Some(ref cpu_set) = config.store.rocksdb.cpu_set {
            let mut env = rocksdb::Env::new().map_err(|e| {
                StoreError::Configuration(format!(
                    "Failed to create default Env for RocksDB: {}",
                    e
                ))
            })?;
            env.set_cpu_set(&config::parse_cpu_set(cpu_set)[..]);
            db_opts.set_env(&env);
        }

        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(true);
        write_opts.set_sync(false);

        let db = DB::open_cf_descriptors(&db_opts, path, vec![index_cf, metadata_cf])
            .map_err(|e| StoreError::RocksDB(e.into_string()))?;

        Ok(Self {
            db,
            write_opts,
            count: AtomicUsize::new(0),
            flush_threshold,
        })
    }

    #[cfg(test)]
    pub(crate) fn scan_records(
        &self,
        stream_id: u64,
        range: u32,
        offset: u64,
        max_bytes: u32,
    ) -> Result<Option<Vec<Record>>, StoreError> {
        let mut read_opts = ReadOptions::default();
        let lower = self.build_index_key(stream_id, range, offset);
        read_opts.set_iterate_lower_bound(&lower[..]);

        let mut upper = BytesMut::with_capacity(8 + 4);
        upper.put_u64(stream_id);
        upper.put_u32(range + 1);
        read_opts.set_iterate_upper_bound(upper.freeze());

        self.scan_records_from(read_opts, max_bytes)
    }

    fn scan_records_from(
        &self,
        read_opts: ReadOptions,
        max_bytes: u32,
    ) -> Result<Option<Vec<Record>>, StoreError> {
        let mut bytes_c = 0_u32;
        match self.db.cf_handle(INDEX_COLUMN_FAMILY) {
            Some(cf) => {
                let indexes: Vec<_> = self
                    .db
                    .iterator_cf_opt(cf, read_opts, IteratorMode::Start)
                    .flatten()
                    .map_while(|(k, v)| {
                        if bytes_c >= max_bytes {
                            return None;
                        }

                        let index = match RecordIndex::try_from(&*k) {
                            Ok(index) => index,
                            Err(error) => {
                                warn!("Deserialize index key failed: {}", error);
                                return None;
                            }
                        };

                        let handle = match RecordHandle::try_from(&*v) {
                            Ok(handle) => handle,
                            Err(error) => {
                                warn!("Deserialize index value failed: {}", error);
                                return None;
                            }
                        };
                        bytes_c += handle.len;
                        Some(Record { index, handle })
                    })
                    .collect();

                if indexes.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(indexes))
                }
            }
            None => Err(StoreError::RocksDB(format!(
                "No column family: `{}`",
                INDEX_COLUMN_FAMILY
            ))),
        }
    }

    /// Compaction is synchronous and should execute in its own thread.
    #[allow(dead_code)]
    pub(crate) fn compact(&self) {
        if let Some(cf) = self.db.cf_handle(INDEX_COLUMN_FAMILY) {
            self.db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);
        }
    }

    /// Returns the largest offset less than or equal to the given offset. The function follows the following rules:
    /// 1. `None` is returned if the specific offset < min offset or offset > max offset.
    /// 2. The current offset key is returned if it exists.
    /// 3. Otherwise, the left key is returned.
    fn retrieve_left_key(
        &self,
        stream_id: u64,
        range: u32,
        offset: u64,
    ) -> Result<Option<Record>, StoreError> {
        let max_key = self.retrieve_max_key(stream_id, range)?;
        if let Some(record) = max_key {
            if offset > record.index.offset {
                return Ok(None);
            }

            if offset == record.index.offset {
                return Ok(Some(record));
            }
        } else {
            return Ok(None);
        }

        match self.db.cf_handle(INDEX_COLUMN_FAMILY) {
            Some(cf) => {
                let mut read_opts = ReadOptions::default();
                // If the current offset has a key, return it.
                // So we add 1 to the offset to achieve this behavior.
                let upper = self.build_index_key(stream_id, range, offset + 1);
                read_opts.set_iterate_upper_bound(&upper[..]);

                let mut lower = BytesMut::with_capacity(8 + 4 + 8);
                lower.put_u64(stream_id);
                lower.put_u32(range);
                lower.put_u64(0);
                read_opts.set_iterate_lower_bound(lower.freeze());

                // Reverse scan from the offset, find a lower key
                let mut reverse_itr = self.db.iterator_cf_opt(cf, read_opts, IteratorMode::End);
                let lower_entry = reverse_itr.next();
                lower_entry
                    .transpose()
                    .map_err(|e| StoreError::RocksDB(e.into_string()))
                    .map(|opt| {
                        if let Some((k, v)) = opt {
                            let index = match RecordIndex::try_from(&*k) {
                                Ok(index) => index,
                                Err(error) => {
                                    warn!("Deserialize index key failed: {}", error);
                                    return None;
                                }
                            };

                            let handle = match RecordHandle::try_from(&*v) {
                                Ok(handle) => handle,
                                Err(error) => {
                                    warn!("Deserialize index value failed: {}", error);
                                    return None;
                                }
                            };
                            Some(Record { index, handle })
                        } else {
                            None
                        }
                    })
            }
            None => Err(StoreError::RocksDB(format!(
                "No column family: `{}`",
                INDEX_COLUMN_FAMILY
            ))),
        }
    }

    fn build_index_key(&self, stream_id: u64, range: u32, offset: u64) -> Bytes {
        Bytes::from(&RecordIndex {
            stream_id,
            range,
            offset,
        })
    }
}

impl Indexer for DefaultIndexer {
    ///
    /// # Arguments
    ///
    /// * `stream_id` - Stream Identifier
    /// * `range` - Range index
    /// * `offset` Logical offset within the stream, similar to row-id within a database table.
    /// * `handle` - Pointer to record-batch in WAL.
    ///
    fn index(&self, record: &Record) -> Result<(), StoreError> {
        match self.db.cf_handle(INDEX_COLUMN_FAMILY) {
            Some(cf) => {
                let key_buf = Into::<Bytes>::into(&record.index);
                let value_buf = Into::<Bytes>::into(&record.handle);

                self.db
                    .put_cf_opt(cf, &key_buf[..], &value_buf[..], &self.write_opts)
                    .map_err(|e| StoreError::RocksDB(e.into_string()))?;
                self.advance_wal_checkpoint(record.handle.wal_offset)?;
                let prev = self.count.fetch_add(1, Ordering::Relaxed);
                if prev + 1 >= self.flush_threshold {
                    self.flush(false)?;
                    info!("Advanced WAL checkpoint to {}", record.handle.wal_offset);
                    self.count.store(0, Ordering::Relaxed);
                }
                Ok(())
            }
            None => Err(StoreError::RocksDB("No column family".to_owned())),
        }
    }

    ///
    /// Scan WAL offset of the given offset.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - Stream ID
    /// * `range` - Range index
    /// * `offset` - Logical offset to look up
    /// * `end` - Valid range upper boundary
    fn scan_wal_offset(
        &self,
        stream_id: u64,
        range: u32,
        offset: u64,
        end: Option<u64>,
    ) -> Option<u64> {
        let lower = self.build_index_key(stream_id, range, offset);

        let mut upper = BytesMut::with_capacity(8 + 4 + 8);
        upper.put_u64(stream_id);
        upper.put_u32(range);
        if let Some(end) = end {
            upper.put_u64(end);
        } else {
            upper.put_u64(u64::MAX);
        }

        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_lower_bound(&lower[..]);
        read_opts.set_iterate_upper_bound(upper.freeze());
        if let Ok(Some(vec)) = self.scan_records_from(read_opts, 1) {
            if let Some(record) = vec.first() {
                return Some(record.handle.wal_offset);
            }
        }
        None
    }

    /// The specific offset is not guaranteed to exist,
    /// because it may have been compacted or point to a intermediate position of a record batch.
    /// So the `scan_record_handles_left_shift` will left-shift the offset as a lower bound to scan the records
    fn scan_record_left_shift(
        &self,
        stream_id: u64,
        range: u32,
        offset: u64,
        max_offset: u64,
        max_bytes: u32,
    ) -> Result<Option<Vec<Record>>, StoreError> {
        let left_key_record = self.retrieve_left_key(stream_id, range, offset)?;
        let left_key = left_key_record
            .map(|entry| Bytes::from(&entry.index))
            .unwrap_or(self.build_index_key(stream_id, range, offset));
        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_lower_bound(&left_key[..]);
        read_opts.set_iterate_upper_bound((stream_id + 1).to_be_bytes());

        let upper_key = self.build_index_key(stream_id, range, max_offset);
        read_opts.set_iterate_upper_bound(&upper_key[..]);

        self.scan_records_from(read_opts, max_bytes)
    }

    /// Returns WAL checkpoint offset.
    ///
    /// Note we can call this method as frequently as we need as all operation is memory
    fn get_wal_checkpoint(&self) -> Result<u64, StoreError> {
        if let Some(metadata_cf) = self.db.cf_handle(METADATA_COLUMN_FAMILY) {
            match self
                .db
                .get_cf(metadata_cf, WAL_CHECKPOINT)
                .map_err(|e| StoreError::RocksDB(e.into_string()))?
            {
                Some(value) => {
                    if value.len() < 8 {
                        error!("Value of wal_checkpoint is corrupted. Expecting 8 bytes in big endian, actual: {:?}", value);
                        return Err(StoreError::DataCorrupted);
                    }
                    let mut cursor = Cursor::new(&value[..]);
                    let wal_offset = cursor.get_u64();
                    trace!("Checkpoint WAL-offset={}", wal_offset);
                    Ok(wal_offset)
                }
                None => {
                    info!("No KV entry for wal_checkpoint yet. Default wal_checkpoint to 0");
                    Ok(0)
                }
            }
        } else {
            info!("No column family metadata yet. Default wal_checkpoint to 0");
            Ok(0)
        }
    }

    /// Update checkpoint WAL offset, indicating records prior to it should have been properly indexed.
    ///
    /// Note the indexes are possibly stored only in RocksDB memtable. Updated checkpoint WAL is the same.
    /// We are employing `Atomic Flush` of RocksDB to ensure data consistency without using WAL.
    fn advance_wal_checkpoint(&self, offset: u64) -> Result<(), StoreError> {
        let cf = self
            .db
            .cf_handle(METADATA_COLUMN_FAMILY)
            .ok_or(StoreError::RocksDB(
                "Metadata should have been created as we have create_if_missing enabled".to_owned(),
            ))?;
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64(offset);
        self.db
            .put_cf_opt(cf, WAL_CHECKPOINT, &buf[..], &self.write_opts)
            .map_err(|e| StoreError::RocksDB(e.into_string()))
    }

    /// Flush record index in cache into RocksDB using atomic-flush.
    ///
    /// Normally, we do not have invoke this function as frequently as insertion of entry, mapping stream offset to WAL
    /// offset. The reason behind this is that DB is having `AtomicFlush` enabled. As long as we put mapping entries
    /// first and then update checkpoint `offset` of WAL, integrity of index entries will be guaranteed after recovery
    /// from `checkpoint` WAL offset.
    ///
    /// Once a memtable is full and flushed to SST files, we can guarantee that all mapping entries prior to checkpoint
    /// `offset` of WAL are already persisted.
    ///
    /// To minimize the amount of WAL data to recover, for example, in case of planned reboot, we shall update checkpoint
    /// offset and invoke this method before stopping.
    fn flush(&self, wait: bool) -> Result<(), StoreError> {
        info!("AtomicFlush RocksDB column families");
        let mut flush_opt = FlushOptions::default();
        flush_opt.set_wait(wait);
        if let Some((index, metadata)) = self
            .db
            .cf_handle(INDEX_COLUMN_FAMILY)
            .zip(self.db.cf_handle(METADATA_COLUMN_FAMILY))
        {
            self.db
                .flush_cfs_opt(&[index, metadata], &flush_opt)
                .map_err(|e| StoreError::RocksDB(e.into_string()))
        } else {
            unreachable!("index or metadata column family handle is not found")
        }
    }

    fn retrieve_max_key(&self, stream_id: u64, range: u32) -> Result<Option<Record>, StoreError> {
        match self.db.cf_handle(INDEX_COLUMN_FAMILY) {
            Some(cf) => {
                let mut read_opts = ReadOptions::default();
                let mut lower = BytesMut::with_capacity(12);
                lower.put_u64(stream_id);
                lower.put_u32(range);
                read_opts.set_iterate_lower_bound(lower.freeze());

                let mut upper = BytesMut::with_capacity(12);
                upper.put_u64(stream_id);
                upper.put_u32(range + 1);
                read_opts.set_iterate_upper_bound(upper.freeze());

                let mut iter = self.db.iterator_cf_opt(cf, read_opts, IteratorMode::End);
                iter.next()
                    .transpose()
                    .map_err(|e| StoreError::RocksDB(e.into_string()))
                    .map(|opt| {
                        if let Some((k, v)) = opt {
                            let index = match RecordIndex::try_from(&*k) {
                                Ok(index) => index,
                                Err(error) => {
                                    warn!("Deserialize index key failed: {}", error);
                                    return None;
                                }
                            };

                            let handle = match RecordHandle::try_from(&*v) {
                                Ok(handle) => handle,
                                Err(error) => {
                                    warn!("Deserialize index value failed: {}", error);
                                    return None;
                                }
                            };
                            Some(Record { index, handle })
                        } else {
                            None
                        }
                    })
            }
            None => Err(StoreError::RocksDB(format!(
                "No column family: `{}`",
                INDEX_COLUMN_FAMILY
            ))),
        }
    }
}

impl super::LocalRangeManager for DefaultIndexer {
    fn list_by_stream(&self, stream_id: u64, tx: mpsc::UnboundedSender<RangeMetadata>) {
        let mut prefix = BytesMut::with_capacity(9);
        prefix.put_u8(RANGE_PREFIX);
        prefix.put_u64(stream_id);

        if let Some(cf) = self.db.cf_handle(METADATA_COLUMN_FAMILY) {
            let mut read_opts = ReadOptions::default();
            read_opts.set_iterate_lower_bound(&prefix[..]);
            read_opts.set_prefix_same_as_start(true);
            self.db
                .iterator_cf_opt(cf, read_opts, IteratorMode::Start)
                .flatten()
                .map_while(|(k, v)| {
                    if !k.starts_with(&prefix[..]) {
                        None
                    } else {
                        // prefix + stream-id + range-index
                        debug_assert_eq!(k.len(), 1 + 8 + 4);

                        let mut key_reader = Cursor::new(&k[..]);
                        let _prefix = key_reader.get_u8();
                        debug_assert_eq!(_prefix, RANGE_PREFIX);

                        let _stream_id = key_reader.get_u64();
                        debug_assert_eq!(stream_id, _stream_id);

                        let id = key_reader.get_i32();

                        if v.len() == 8 {
                            let mut value_reader = Cursor::new(&v[..]);
                            let start = value_reader.get_u64();
                            Some(RangeMetadata::new(stream_id, id, 0, start, None))
                        } else {
                            debug_assert_eq!(v.len(), 8 + 8);
                            let mut value_reader = Cursor::new(&v[..]);
                            let start = value_reader.get_u64();
                            let end = value_reader.get_u64();
                            debug_assert!(start <= end, "Range start <= end should hold");
                            Some(RangeMetadata::new(stream_id, id, 0, start, Some(end)))
                        }
                    }
                })
                .for_each(|range| {
                    if tx.send(range).is_err() {
                        error!(
                            "Channel to transfer range for stream={} is closed",
                            stream_id
                        );
                    }
                });
        }
    }

    fn list(&self, tx: mpsc::UnboundedSender<RangeMetadata>) {
        let mut prefix = BytesMut::with_capacity(1);
        prefix.put_u8(RANGE_PREFIX);

        if let Some(cf) = self.db.cf_handle(METADATA_COLUMN_FAMILY) {
            let mut read_opts = ReadOptions::default();
            read_opts.set_iterate_lower_bound(&prefix[..]);
            read_opts.set_prefix_same_as_start(true);
            self.db
                .iterator_cf_opt(cf, read_opts, IteratorMode::Start)
                .flatten()
                .map_while(|(k, v)| {
                    if !k.starts_with(&prefix[..]) {
                        None
                    } else {
                        // prefix + stream-id + range-index
                        debug_assert_eq!(k.len(), 1 + 8 + 4);

                        let mut key_reader = Cursor::new(&k[..]);
                        let _prefix = key_reader.get_u8();
                        debug_assert_eq!(_prefix, RANGE_PREFIX);

                        let _stream_id = key_reader.get_u64();

                        let id = key_reader.get_i32();

                        if v.len() == 8 {
                            let mut value_reader = Cursor::new(&v[..]);
                            let start = value_reader.get_u64();
                            Some(RangeMetadata::new(_stream_id, id, 0, start, None))
                        } else {
                            debug_assert_eq!(v.len(), 8 + 8);
                            let mut value_reader = Cursor::new(&v[..]);
                            let start = value_reader.get_u64();
                            let end = value_reader.get_u64();
                            Some(RangeMetadata::new(_stream_id, id, 0, start, Some(end)))
                        }
                    }
                })
                .for_each(|range| {
                    if tx.send(range).is_err() {
                        warn!("Channel to transfer stream ranges is closed");
                    }
                });
        }
    }

    fn seal(&self, stream_id: u64, range: &RangeMetadata) -> Result<(), StoreError> {
        debug_assert!(
            range.has_end(),
            "The metadata to seal range does not have end offset"
        );
        let end = range.end().ok_or(StoreError::Internal("".to_owned()))?;
        debug_assert!(end >= range.start(), "End of range cannot less than start");
        // prefix + stream-id + range-index
        let mut key_buf = BytesMut::with_capacity(1 + 8 + 4);
        key_buf.put_u8(RANGE_PREFIX);
        key_buf.put_u64(stream_id);
        key_buf.put_i32(range.index());

        // start, [end] offset.
        let mut value_buf = BytesMut::with_capacity(8 + 8);
        value_buf.put_u64(range.start());
        value_buf.put_u64(end);

        if let Some(cf) = self.db.cf_handle(METADATA_COLUMN_FAMILY) {
            self.db
                .put_cf_opt(cf, &key_buf[..], &value_buf[..], &self.write_opts)
                .map_err(|e| StoreError::RocksDB(e.into_string()))
        } else {
            Err(StoreError::RocksDB(format!(
                "No column family: {}",
                METADATA_COLUMN_FAMILY
            )))
        }
    }

    fn add(&self, stream_id: u64, range: &RangeMetadata) -> Result<(), StoreError> {
        let mut key_buf = BytesMut::with_capacity(1 + 8 + 4);
        key_buf.put_u8(RANGE_PREFIX);
        key_buf.put_u64(stream_id);
        key_buf.put_i32(range.index());

        let mut value_buf = BytesMut::with_capacity(8 + 8);
        value_buf.put_u64(range.start());
        if let Some(end) = range.end() {
            value_buf.put_u64(end);
        }

        if let Some(cf) = self.db.cf_handle(METADATA_COLUMN_FAMILY) {
            self.db
                .put_cf_opt(cf, &key_buf[..], &value_buf[..], &self.write_opts)
                .map_err(|e| StoreError::RocksDB(e.into_string()))
        } else {
            Err(StoreError::RocksDB(format!(
                "No column family: {}",
                METADATA_COLUMN_FAMILY
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
    };

    use crate::index::record::{Record, RecordIndex};
    use crate::{
        index::{
            record::{HandleExt, RecordHandle},
            Indexer,
        },
        watermark::Watermark,
    };

    struct SampleWatermark {
        min: AtomicU64,
        offload: AtomicU64,
    }

    impl Watermark for SampleWatermark {
        fn min(&self) -> u64 {
            self.min.load(Ordering::Relaxed)
        }

        fn offload(&self) -> u64 {
            self.offload.load(Ordering::Relaxed)
        }

        fn set_min(&self, offset: u64) {
            self.min.store(offset, Ordering::Relaxed);
        }

        fn set_offload(&self, offload: u64) {
            self.offload.store(offload, Ordering::Relaxed);
        }
    }

    fn new_indexer() -> Result<super::DefaultIndexer, Box<dyn Error>> {
        let path = tempfile::tempdir()?;
        let mut config = config::Configuration::default();
        config
            .store
            .path
            .set_base(path.path().as_os_str().to_str().unwrap());
        let config = Arc::new(config);
        let watermark = Arc::new(SampleWatermark {
            min: AtomicU64::new(0),
            offload: AtomicU64::new(0),
        });
        let indexer = super::DefaultIndexer::new(&config, watermark as Arc<dyn Watermark>, 128)?;
        Ok(indexer)
    }

    #[test]
    fn test_wal_checkpoint() -> Result<(), Box<dyn Error>> {
        let indexer = new_indexer()?;
        assert_eq!(0, indexer.get_wal_checkpoint()?);
        let wal_offset = 100;
        indexer.advance_wal_checkpoint(wal_offset)?;
        assert_eq!(wal_offset, indexer.get_wal_checkpoint()?);
        Ok(())
    }

    #[test]
    fn test_retrieve_max_key() -> Result<(), Box<dyn Error>> {
        // Case one: have a left key
        let indexer = new_indexer()?;
        let start_offset = 10;
        let stream_id = 1;
        let range = 0;

        indexer.index(&Record {
            index: RecordIndex {
                stream_id,
                range,
                offset: start_offset,
            },
            handle: RecordHandle {
                wal_offset: 1024,
                len: 128,
                ext: HandleExt::Hash(10),
            },
        })?;
        indexer.index(&Record {
            index: RecordIndex {
                stream_id,
                range,
                offset: start_offset + 1,
            },
            handle: RecordHandle {
                wal_offset: 1024,
                len: 128,
                ext: HandleExt::Hash(10),
            },
        })?;

        // Case one: have a max key
        let max_key = indexer.retrieve_max_key(stream_id, range).unwrap().unwrap();
        assert_eq!(
            (stream_id, range, start_offset + 1),
            (
                max_key.index.stream_id,
                max_key.index.range,
                max_key.index.offset
            )
        );

        //Case two: no max key
        let max_key = indexer.retrieve_max_key(stream_id, range + 1).unwrap();
        assert!(max_key.is_none());

        Ok(())
    }

    #[test]
    fn test_retrieve_left_key() -> Result<(), Box<dyn Error>> {
        // Case one: have a left key
        let indexer = new_indexer()?;
        let left_offset = 10;
        let stream_id = 1;
        let range = 0;

        indexer.index(&Record {
            index: RecordIndex {
                stream_id,
                range,
                offset: left_offset,
            },
            handle: RecordHandle {
                wal_offset: 1024,
                len: 128,
                ext: HandleExt::Hash(10),
            },
        })?;
        indexer.index(&Record {
            index: RecordIndex {
                stream_id,
                range,
                offset: left_offset + 2,
            },
            handle: RecordHandle {
                wal_offset: 1024,
                len: 128,
                ext: HandleExt::Hash(10),
            },
        })?;
        indexer.index(&Record {
            index: RecordIndex {
                stream_id,
                range,
                offset: left_offset + 4,
            },
            handle: RecordHandle {
                wal_offset: 1024,
                len: 128,
                ext: HandleExt::Hash(10),
            },
        })?;

        let left_key = indexer
            .retrieve_left_key(stream_id, range, left_offset + 1)
            .unwrap()
            .unwrap();
        assert_eq!(
            (stream_id, range, left_offset),
            (
                left_key.index.stream_id,
                left_key.index.range,
                left_key.index.offset
            )
        );

        // Case two: the specific key is equal to the left key
        let left_key = indexer
            .retrieve_left_key(stream_id, range, left_offset + 2)
            .unwrap()
            .unwrap();
        assert_eq!(
            (stream_id, range, left_offset + 2),
            (
                left_key.index.stream_id,
                left_key.index.range,
                left_key.index.offset
            )
        );

        // Case three: no left key
        let left_key = indexer
            .retrieve_left_key(stream_id, range, left_offset - 1)
            .unwrap();
        assert!(left_key.is_none());

        // Case four: the smallest key
        let left_key = indexer
            .retrieve_left_key(stream_id, range, left_offset)
            .unwrap()
            .unwrap();
        assert_eq!(
            (stream_id, range, left_offset),
            (
                left_key.index.stream_id,
                left_key.index.range,
                left_key.index.offset
            )
        );

        // Case five: the biggest key
        let left_key = indexer
            .retrieve_left_key(stream_id, range, left_offset + 4)
            .unwrap()
            .unwrap();

        assert_eq!(
            (stream_id, range, left_offset + 4),
            (
                left_key.index.stream_id,
                left_key.index.range,
                left_key.index.offset
            )
        );

        // Case six: the biggest key + 1

        let left_key = indexer
            .retrieve_left_key(stream_id, range, left_offset + 5)
            .unwrap();
        assert!(left_key.is_none());

        Ok(())
    }

    #[test]
    fn test_index_scan_from_left_key() -> Result<(), Box<dyn Error>> {
        let indexer = new_indexer()?;
        let stream_id = 0;
        let range = 0;
        const CNT: u64 = 1024;
        (1..CNT)
            .flat_map(|n| {
                indexer.index(&Record {
                    index: RecordIndex {
                        stream_id,
                        range,
                        offset: n * 10,
                    },
                    handle: RecordHandle {
                        wal_offset: n * 128,
                        len: 128,
                        ext: HandleExt::Hash(10),
                    },
                })
            })
            .count();

        // The record handle physical offset is 128, 256, 384, ...
        // While the logical offset is 10, 20, 30, ..., which means each record batch contains 10 records.

        // Case one: scan from a exist key
        let indexes = indexer.scan_record_left_shift(stream_id, range, 10, 100, 128 * 2)?;
        assert!(indexes.is_some());
        let indexes = indexes.unwrap();
        assert_eq!(2, indexes.len());

        indexes.into_iter().enumerate().for_each(|(i, record)| {
            assert_eq!(stream_id, record.index.stream_id);
            assert_eq!(range, record.index.range);
            assert_eq!(((i + 1) * 10) as u64, record.index.offset);

            assert_eq!(((i + 1) * 128) as u64, record.handle.wal_offset);
            assert_eq!(128, record.handle.len);
            assert_eq!(HandleExt::Hash(10), record.handle.ext);
        });

        // Case two: scan from a left key
        let indexes = indexer.scan_record_left_shift(0, range, 12, 100, 128 * 2)?;
        assert!(indexes.is_some());
        let handles = indexes.unwrap();
        assert_eq!(2, handles.len());

        handles.into_iter().enumerate().for_each(|(i, record)| {
            assert_eq!(stream_id, record.index.stream_id);
            assert_eq!(range, record.index.range);
            assert_eq!(((i + 1) * 10) as u64, record.index.offset);

            assert_eq!(((i + 1) * 128) as u64, record.handle.wal_offset);
            assert_eq!(128, record.handle.len);
            assert_eq!(HandleExt::Hash(10), record.handle.ext);
        });

        // Case three: scan from a key smaller than the smallest key
        let indexes = indexer.scan_record_left_shift(0, range, 1, 100, 128 * 2)?;
        assert!(indexes.is_some());
        let indexes = indexes.unwrap();
        assert_eq!(2, indexes.len());

        indexes.into_iter().enumerate().for_each(|(i, record)| {
            assert_eq!(stream_id, record.index.stream_id);
            assert_eq!(range, record.index.range);
            assert_eq!(((i + 1) * 10) as u64, record.index.offset);

            assert_eq!(((i + 1) * 128) as u64, record.handle.wal_offset);
            assert_eq!(128, record.handle.len);
            assert_eq!(HandleExt::Hash(10), record.handle.ext);
        });

        // Case four: scan from a key bigger than the biggest key

        let indexes = indexer.scan_record_left_shift(0, range, CNT * 11, 100, 128 * 2)?;
        assert!(indexes.is_none());

        // Case five: scan with a max offset
        let indexes = indexer.scan_record_left_shift(0, range, 10, 40, 128 * 10)?;
        // Three records are scanned
        assert!(indexes.is_some());
        let indexes = indexes.unwrap();
        assert_eq!(3, indexes.len());
        assert_eq!(indexes[0].index.offset, 10);
        assert_eq!(indexes[0].handle.wal_offset, 128);
        assert_eq!(indexes[1].index.offset, 20);
        assert_eq!(indexes[1].handle.wal_offset, 256);
        assert_eq!(indexes[2].index.offset, 30);
        assert_eq!(indexes[2].handle.wal_offset, 384);

        Ok(())
    }

    #[test]
    fn test_index_scan() -> Result<(), Box<dyn Error>> {
        let indexer = new_indexer()?;
        let stream_id = 0;
        let range = 0;
        const CNT: u64 = 1024;
        (0..CNT)
            .flat_map(|n| {
                indexer.index(&Record {
                    index: RecordIndex {
                        stream_id,
                        range,
                        offset: n,
                    },
                    handle: RecordHandle {
                        wal_offset: n,
                        len: 128,
                        ext: HandleExt::Hash(10),
                    },
                })
            })
            .count();

        // Case one: scan ten records from the indexer
        let indexes = indexer.scan_records(0, range, 0, 10 * 128)?;
        assert!(indexes.is_some());
        let indexes = indexes.unwrap();
        assert_eq!(10, indexes.len());
        indexes.into_iter().enumerate().for_each(|(i, record)| {
            assert_eq!(stream_id, record.index.stream_id);
            assert_eq!(range, record.index.range);
            assert_eq!(i as u64, record.index.offset);

            assert_eq!(i as u64, record.handle.wal_offset);
            assert_eq!(128, record.handle.len);
            assert_eq!(HandleExt::Hash(10), record.handle.ext);
        });

        // Case two: scan 0 bytes from the indexer
        let indexes = indexer.scan_records(0, range, 0, 0)?;
        assert!(indexes.is_none());

        // Case three: return at least one record even if the bytes is not enough
        let indexes = indexer.scan_records(0, range, 0, 5)?;
        assert!(indexes.is_some());
        let indexes = indexes.unwrap();
        assert_eq!(1, indexes.len());

        Ok(())
    }

    #[test]
    fn test_compaction() -> Result<(), Box<dyn Error>> {
        let path = tempfile::tempdir()?;
        let mut config = config::Configuration::default();
        config
            .store
            .path
            .set_base(path.path().as_os_str().to_str().unwrap());
        let config = Arc::new(config);
        let watermark = Arc::new(SampleWatermark {
            min: AtomicU64::new(0),
            offload: AtomicU64::new(0),
        });
        let indexer =
            super::DefaultIndexer::new(&config, Arc::clone(&watermark) as Arc<dyn Watermark>, 128)?;
        let stream_id = 0;
        let range = 0;
        const CNT: u64 = 1024;
        (0..CNT)
            .flat_map(|n| {
                indexer.index(&Record {
                    index: RecordIndex {
                        stream_id,
                        range,
                        offset: n,
                    },
                    handle: RecordHandle {
                        wal_offset: n,
                        len: 128,
                        ext: HandleExt::Hash(10),
                    },
                })
            })
            .count();

        indexer.flush(true)?;
        indexer.compact();

        let indexes = indexer.scan_records(0, range, 0, 10)?.unwrap();
        assert_eq!(0, indexes[0].index.offset);
        assert_eq!(0, indexes[0].handle.wal_offset);
        watermark.set_min(10);

        indexer.compact();
        let _indexes = indexer.scan_records(0, range, 0, 10)?.unwrap();
        Ok(())
    }
}
