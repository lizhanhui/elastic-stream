use crate::option::WalPath;

const DEFAULT_MAX_IO_DEPTH: u32 = 4096;
const DEFAULT_SQPOLL_IDLE_MS: u32 = 2000;
const DEFAULT_SQPOLL_CPU: u32 = 1;
const DEFAULT_MAX_BOUNDED_URING_WORKER_COUNT: u32 = 2;
const DEFAULT_MAX_UNBOUNDED_URING_WORKER_COUNT: u32 = 2;

pub(crate) const DEFAULT_LOG_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;

const DEFAULT_READ_BLOCK_SIZE: u32 = 1024 * 128;

pub(crate) const DEFAULT_MAX_CACHE_SIZE: u64 = 1024 * 1024 * 1024;

const DEFAULT_MIN_PREALLOCATED_SEGMENT_FILES: usize = 1;

#[derive(Debug, Clone)]
pub(crate) struct Options {
    /// A list of paths where write-ahead-log segment files can be put into, with its target_size considered.
    ///
    /// Newer data is placed into paths specified earlier in the vector while the older data are gradually moved
    /// to paths specified later in the vector.
    ///
    /// For example, we have a SSD device with 100GiB for hot data as well as 1TiB S3-backed tiered storage.
    /// Configuration for it should be `[{"/ssd", 100GiB}, {"/s3", 1TiB}]`.
    pub(crate) wal_paths: Vec<WalPath>,

    pub(crate) metadata_path: String,

    pub(crate) io_depth: u32,

    pub(crate) sqpoll_idle_ms: u32,

    /// Bind the kernel's poll thread to the specified cpu.
    pub(crate) sqpoll_cpu: u32,

    pub(crate) max_workers: [u32; 2],

    pub(crate) segment_size: u64,

    pub(crate) max_cache_size: u64,

    pub(crate) alignment: usize,

    pub(crate) read_block_size: u32,

    pub(crate) min_preallocated_segment_files: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            wal_paths: Vec::new(),
            metadata_path: String::new(),
            io_depth: DEFAULT_MAX_IO_DEPTH,
            sqpoll_idle_ms: DEFAULT_SQPOLL_IDLE_MS,
            sqpoll_cpu: DEFAULT_SQPOLL_CPU,
            max_workers: [
                DEFAULT_MAX_BOUNDED_URING_WORKER_COUNT,
                DEFAULT_MAX_UNBOUNDED_URING_WORKER_COUNT,
            ],
            segment_size: DEFAULT_LOG_SEGMENT_SIZE,
            alignment: 4096,
            read_block_size: DEFAULT_READ_BLOCK_SIZE,
            min_preallocated_segment_files: DEFAULT_MIN_PREALLOCATED_SEGMENT_FILES,
            max_cache_size: DEFAULT_MAX_CACHE_SIZE,
        }
    }
}

impl Options {
    pub fn add_wal_path(&mut self, wal_path: WalPath) {
        self.alignment = wal_path.block_size;
        self.wal_paths.push(wal_path);
    }
}
