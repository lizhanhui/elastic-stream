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
    use std::{error::Error, path::Path};

    use rocksdb::{ColumnFamilyDescriptor, Options, DB};

    use super::compaction;

    #[test]
    fn test_rocksdb_setup() -> Result<(), Box<dyn Error>> {
        let path = "/tmp/rocksdb";
        let path = Path::new(path);
        std::fs::create_dir_all(path)?;
        let _dir_guard = util::DirectoryRemovalGuard::new(&path);

        let mut cf_opts = Options::default();
        cf_opts.set_max_write_buffer_number(16);

        let index_compaction_filter_factory =
            compaction::IndexCompactionFilterFactory::new("index-compaction-filter")?;

        cf_opts.set_compaction_filter_factory(index_compaction_filter_factory);

        let cf = ColumnFamilyDescriptor::new("index", cf_opts);

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        {
            let db = DB::open_cf_descriptors(&db_opts, path, vec![cf])?;

            let cf = db.cf_handle("index").unwrap();

            db.put_cf(cf, "abc", "def")?;

            match db.get_cf(cf, "abc") {
                Ok(Some(v)) => {
                    let value = String::from_utf8_lossy(&v[..]);
                    println!("value = {}", value);
                }
                Ok(None) => {}
                Err(e) => {}
            }

            (0..100)
                .into_iter()
                .map(|n| {
                    let key = format!("k{:08}", n);
                    let value = format!("v{:08}", n);
                    db.put_cf(cf, key, value)?;
                    Ok::<(), rocksdb::Error>(())
                })
                .flatten()
                .count();

            db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);
        }

        Ok(())
    }
}
