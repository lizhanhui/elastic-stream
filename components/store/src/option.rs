//! Options of various kinds to modify their action behaviors
//!

#[derive(Debug, Clone, PartialEq)]
pub struct StorePath {
    pub(crate) path: String,

    /// Target size of total files under the path, in byte.
    pub(crate) target_size: u64,
}

impl StorePath {
    pub fn new(path: &str, target_size: u64) -> Self {
        Self {
            path: path.to_owned(),
            target_size,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoreOptions {
    pub(crate) create_if_missing: bool,
    pub(crate) store_path: StorePath,
    pub(crate) destroy_on_exit: bool,
    pub(crate) command_queue_depth: usize,
}

impl StoreOptions {
    pub fn new(store_path: &StorePath) -> Self {
        Self {
            create_if_missing: true,
            store_path: store_path.clone(),
            destroy_on_exit: false,
            command_queue_depth: 1024,
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
    /// Target partition
    pub(crate) partition_id: i64,

    /// Logical offset, from which to read records
    pub(crate) offset: i64,

    /// Maximum number of records to read.
    pub(crate) limit: Option<usize>,
}
