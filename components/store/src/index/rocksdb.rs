#[cfg(test)]
mod tests {
    use std::{
        env,
        error::Error,
        ffi::CString,
        rc::Rc,
        sync::{atomic::{AtomicU64, Ordering}, Arc},
        time::Instant,
    };

    use bytes::{BufMut, BytesMut};
    use rocksdb::{
        BlockBasedOptions, ColumnFamilyDescriptor, DBCompressionType, IteratorMode, Options,
        ReadOptions, WriteOptions, DB,
    };
    use uuid::Uuid;

    use crate::index::MinOffset;

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
        let log = test_util::terminal_logger();
        let data_path = test_util::create_random_path()?;
        let db_path = data_path.as_path();
        let _dir_guard = test_util::DirectoryRemovalGuard::new(log.clone(), &db_path);

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

        let store = Arc::new(TestStore::new());

        let index_compaction_filter_factory =
            crate::index::compaction::IndexCompactionFilterFactory::new(
                log.clone(),
                compaction_filter_name,
                Arc::clone(&store) as Arc<dyn MinOffset>,
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

        let db = DB::open_cf_descriptors(&db_opts, db_path, vec![cf])?;
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

        const MIN_OFFSET: u64 = 50;
        store.set_min_offset(MIN_OFFSET);
        db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);

        let diff = cnt
            - db.iterator_cf_opt(cf, ReadOptions::default(), IteratorMode::Start)
                .count();
        assert_eq!(MIN_OFFSET, diff as u64);
        Ok(())
    }
}
