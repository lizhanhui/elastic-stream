mod compaction;

pub(crate) struct Indexer {}

impl Indexer {
    pub(crate) fn new() -> Self {
        Self {}
    }

    pub(crate) fn index(&self, _offset: u64, _wal_offset: u64, _hash: u64) {}

    /// Returns offset in WAL. All record index are atomic-flushed to RocksDB.
    pub(crate) fn flushed_wal_offset(&self) -> u64 {
        0
    }

    /// Flush record index in cache into RocksDB using atomic-flush.
    ///
    /// Normally, we do not have invoke this function as frequently as insertion of entry, mapping stream offset to WAL
    /// offset. Reason behind this that DB is having `AtomicFlush` enabled. As long as we put mapping entries first and
    /// then update checkpoint `offset` of WAL.
    ///
    /// Once a memtable is full and flushed to SST files, we can guarantee that all mapping entries prior to checkpoint
    /// `offset` of WAL are already persisted.
    ///
    /// To minimize the amount of WAL data to recover, for example, in case of planned reboot, we shall update checkpoint
    /// offset and invoke this method before stopping.
    pub(crate) fn flush(&self) {}
}

pub trait MinOffset {
    fn min_offset(&self) -> u64;
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        ffi::CString,
        path::Path,
        rc::Rc,
        sync::atomic::{AtomicU64, Ordering},
        time::Instant,
    };

    use bytes::{BufMut, BytesMut};
    use rocksdb::{
        BlockBasedOptions, ColumnFamilyDescriptor, DBCompressionType, IteratorMode, Options,
        ReadOptions, WriteOptions, DB,
    };

    use super::{compaction, MinOffset};

    struct TestStore {
        _min_offset: AtomicU64,
    }

    impl TestStore {
        fn new() -> Self {
            Self {
                _min_offset: AtomicU64::new(u64::MAX),
            }
        }

        fn set_min_offset(&self, min_offset: u64) {
            self._min_offset.store(min_offset, Ordering::Relaxed);
        }
    }

    impl MinOffset for TestStore {
        fn min_offset(&self) -> u64 {
            self._min_offset.load(Ordering::Relaxed)
        }
    }

    #[test]
    fn test_rocksdb_setup() -> Result<(), Box<dyn Error>> {
        let log = util::terminal_logger();
        let path = "/tmp/rocksdb";
        let path = Path::new(path);
        std::fs::create_dir_all(path)?;
        let _dir_guard = util::DirectoryRemovalGuard::new(&path);

        let mut cf_opts = Options::default();
        cf_opts.enable_statistics();
        cf_opts.set_write_buffer_size(128 * 1024 * 1024);
        cf_opts.set_max_write_buffer_number(4);
        cf_opts.set_compression_type(DBCompressionType::None);
        {
            // 128MiB block cache
            let cache = rocksdb::Cache::new_lru_cache(128 << 20)?;
            let mut table_opts = BlockBasedOptions::default();
            table_opts.set_block_cache(&cache);
            table_opts.set_block_size(128 << 10);
            cf_opts.set_block_based_table_factory(&table_opts);
        }

        let compaction_filter_name = CString::new("index-compaction-filter")?;

        let store = Rc::new(TestStore::new());

        let index_compaction_filter_factory = compaction::IndexCompactionFilterFactory::new(
            log.clone(),
            compaction_filter_name,
            Rc::clone(&store) as Rc<dyn MinOffset>,
        );

        cf_opts.set_compaction_filter_factory(index_compaction_filter_factory);

        let cf = ColumnFamilyDescriptor::new("index", cf_opts);

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

        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(true);
        write_opts.set_sync(false);

        let db = DB::open_cf_descriptors(&db_opts, path, vec![cf])?;
        let cf = db.cf_handle("index").unwrap();

        db.put_cf_opt(cf, "abc", "def", &write_opts)?;
        match db.get_cf(cf, "abc") {
            Ok(Some(v)) => {
                let value = String::from_utf8_lossy(&v[..]);
                println!("value = {}", value);
            }
            Ok(None) => {}
            Err(_e) => {}
        }
        const N: u64 = 100_000;
        let now = Instant::now();
        (0..N)
            .into_iter()
            .map(|n| {
                let mut value = BytesMut::with_capacity(20);
                value.put_u64(n);
                value.put_u32(0);
                value.put_i64(0);
                db.put_cf_opt(cf, &n.to_be_bytes(), &value, &write_opts)?;
                Ok::<(), rocksdb::Error>(())
            })
            .flatten()
            .count();
        db.flush()?;
        let elapsed = now.elapsed();
        println!(
            "Inserting {} KV entries costs {}ms, avg: {}us",
            N,
            elapsed.as_millis(),
            elapsed.as_micros() / N as u128,
        );

        let cnt = db
            .iterator_cf_opt(cf, ReadOptions::default(), IteratorMode::Start)
            .count();

        store.set_min_offset(5000);
        db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);

        let diff = cnt
            - db.iterator_cf_opt(cf, ReadOptions::default(), IteratorMode::Start)
                .count();
        assert_eq!(5000, diff);
        Ok(())
    }
}
