mod compaction;

pub(crate) struct Indexer {}

impl Indexer {
    pub(crate) fn new() -> Self {
        Self {}
    }

    pub(crate) fn index(&self, offset: u64, wal_offset: u64, hash: u64) {}

    /// Returns offset in WAL. All record index are atomic-flushed to RocksDB.
    pub(crate) fn flushed_wal_offset(&self) -> u64 {
        0
    }

    /// Flush record index in cache into RocksDB using atomic-flush.
    pub(crate) fn flush(&self) {}
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::Path, time::Instant};

    use bytes::{BufMut, BytesMut};
    use rocksdb::{
        BlockBasedOptions, ColumnFamilyDescriptor, DBCompressionType, FlushOptions, Options,
        WriteOptions, DB,
    };

    use super::compaction;

    #[test]
    fn test_rocksdb_setup() -> Result<(), Box<dyn Error>> {
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

        let index_compaction_filter_factory =
            compaction::IndexCompactionFilterFactory::new("index-compaction-filter")?;

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
            Err(e) => {}
        }
        const N: u64 = 100_000;
        let now = Instant::now();
        (0..N)
            .into_iter()
            .map(|n| {
                let data = &n as *const u64 as *const u8;
                let key = unsafe { std::slice::from_raw_parts(data, 8) };

                let mut value = BytesMut::with_capacity(20);
                value.put_u64(n);
                value.put_u32(0);
                value.put_i64(0);
                db.put_cf_opt(cf, key, &value, &write_opts)?;
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
        db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }
}
