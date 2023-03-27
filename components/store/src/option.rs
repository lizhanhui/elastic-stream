//! Options of various kinds to modify their action behaviors
//!

use nix::sys::stat;
use std::path::Path;

use crate::error::StoreError;

#[derive(Debug, Clone, PartialEq)]
pub struct WalPath {
    pub(crate) path: String,

    /// Target size of total files under the path, in byte.
    pub(crate) target_size: u64,

    /// For read, it's preferred to read expected amount of data and
    /// reply on storage driver to merge automatically.
    ///
    /// Refer to https://aws.amazon.com/premiumsupport/knowledge-center/ebs-calculate-optimal-io-size/
    pub(crate) block_size: usize,
}

impl WalPath {
    pub fn new(path: &str, target_size: u64) -> Result<Self, StoreError> {
        let dir_path = Path::new(path);
        if !dir_path.exists() {
            std::fs::create_dir_all(dir_path)?;
        }

        let p = Path::new(path);
        let file_stat = stat::stat(p).map_err(|e| StoreError::System(e as i32))?;

        Ok(Self {
            path: path.to_owned(),
            target_size,
            block_size: file_stat.st_blksize as usize,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoreOptions {
    pub(crate) create_if_missing: bool,
    pub(crate) wal_path: WalPath,
    pub(crate) metadata_path: String,
    pub(crate) destroy_on_exit: bool,
    pub(crate) command_queue_depth: usize,
}

impl StoreOptions {
    pub fn new(store_path: &WalPath, metadata_path: String) -> Self {
        Self {
            create_if_missing: true,
            wal_path: store_path.clone(),
            destroy_on_exit: false,
            command_queue_depth: 1024,
            metadata_path,
        }
    }
}

/// Options that control write operations
#[derive(Debug)]
pub struct WriteOptions {
    /// If true, the write will be flushed from operating system buffer cache(through fsync or fdatasync)
    /// and replicated to replica-group peers before the write is considered complete.
    ///
    /// If this flag is true, writes will require relatively more amount of time.
    ///
    /// If this flag is false, some recent writes may be lost on machine/process crashes, or failing-over.
    ///
    /// Default: true
    pub sync: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self { sync: true }
    }
}

#[derive(Debug, Default)]
pub struct ReadOptions {
    /// Target stream
    pub stream_id: i64,

    /// Logical offset, from which to read records
    pub offset: i64,

    /// The maximum time in milliseconds to wait for the read response.
    pub max_wait_ms: i32,

    /// The maximum bytes of the read response.
    pub max_bytes: i32,
}

#[cfg(test)]
mod tests {
    use crate::error::StoreError;

    use super::WalPath;

    #[test]
    fn test_alignment() -> Result<(), StoreError> {
        let wal_path = WalPath::new("/tmp", 1234)?;
        assert_eq!(true, wal_path.block_size <= 4096);
        Ok(())
    }
}
