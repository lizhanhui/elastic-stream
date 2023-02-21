mod record;
mod segment;
mod task;

use crate::error::StoreError;
use crate::option::WalPath;
use crossbeam::channel::{Receiver, Sender};
use io_uring::{
    opcode::{self, Write},
    squeue, types,
};
use segment::LogSegmentFile;
use slog::{error, info, trace, warn, Logger};
use std::{
    collections::{BTreeMap, VecDeque},
    os::fd::AsRawFd,
    path::Path,
};
use task::{IoTask, ReadTask, WriteTask};

const DEFAULT_MAX_IO_DEPTH: u32 = 4096;
const DEFAULT_SQPOLL_IDLE_MS: u32 = 2000;
const DEFAULT_SQPOLL_CPU: u32 = 1;
const DEFAULT_MAX_BOUNDED_URING_WORKER_COUNT: u32 = 2;
const DEFAULT_MAX_UNBOUNDED_URING_WORKER_COUNT: u32 = 2;

const DEFAULT_LOG_SEGMENT_FILE_SIZE: u64 = 1024u64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct Options {
    /// A list of paths where write-ahead-log segment files can be put into, with its target_size considered.
    ///
    /// Newer data is placed into paths specified earlier in the vector while the older data are gradually moved
    /// to paths specified later in the vector.
    ///
    /// For example, we have a SSD device with 100GiB for hot data as well as 1TiB S3-backed tiered storage.
    /// Configuration for it should be `[{"/ssd", 100GiB}, {"/s3", 1TiB}]`.
    wal_paths: Vec<WalPath>,

    io_depth: u32,

    sqpoll_idle_ms: u32,

    /// Bind the kernel's poll thread to the specified cpu.
    sqpoll_cpu: u32,

    max_workers: [u32; 2],

    file_size: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            wal_paths: Vec::new(),
            io_depth: DEFAULT_MAX_IO_DEPTH,
            sqpoll_idle_ms: DEFAULT_SQPOLL_IDLE_MS,
            sqpoll_cpu: DEFAULT_SQPOLL_CPU,
            max_workers: [
                DEFAULT_MAX_BOUNDED_URING_WORKER_COUNT,
                DEFAULT_MAX_UNBOUNDED_URING_WORKER_COUNT,
            ],
            file_size: DEFAULT_LOG_SEGMENT_FILE_SIZE,
        }
    }
}

pub(crate) struct IO {
    options: Options,

    /// Full fledged I/O Uring instance with setup of `SQPOLL` and `IOPOLL` features
    ///
    /// This io_uring instance is supposed to take up two CPU processors/cores. Namely, both the kernel and user-land are performing
    /// busy polling, submitting and reaping requests.
    ///
    /// With `IOPOLL`, the kernel thread is polling block device drivers for completed tasks, thus no interrupts are required any more on
    /// IO completion.
    ///
    /// With `SQPOLL`, once application thread submits the IO request to `SQ` and compare-and-swap queue head, kernel thread would `see`
    /// and start to process them without `io_uring_enter` syscall.
    ///
    /// At the time of writing(kernel 5.15 and 5.19), `io_uring` instance with `IOPOLL` feature is restricted to file descriptor opened
    /// with `O_DIRECT`. That is, `Currently, this feature is usable only  on  a file  descriptor opened using the O_DIRECT flag.`
    ///
    /// As a result, Opcode `OpenAt` and`OpenAt2` are not compatible with this `io_uring` instance.
    /// `Fallocate64`, for some unknown reason, is not working either.
    poll_ring: io_uring::IoUring,

    /// I/O Uring instance for write-ahead-log segment file management.
    ///
    /// Unlike `poll_uring`, this instance is used to create/fallocate/delete log segment files because these opcodes are not
    /// properly supported by the instance armed with the `IOPOLL` feature.
    ring: io_uring::IoUring,

    pub(crate) sender: Sender<IoTask>,
    receiver: Receiver<IoTask>,
    log: Logger,

    segments: VecDeque<LogSegmentFile>,
    // Runtime data structure
}

impl IO {
    pub(crate) fn new(options: &mut Options, log: Logger) -> Result<Self, StoreError> {
        if options.wal_paths.is_empty() {
            return Err(StoreError::Configuration("WAL path required".to_owned()));
        }

        // Ensure WAL directories exists.
        for dir in &options.wal_paths {
            util::mkdirs_if_missing(&dir.path)?;
        }

        let ring = io_uring::IoUring::builder().dontfork().build(32).map_err(|e| {
            error!(log, "Failed to build I/O Uring instance for write-ahead-log segment file management: {:#?}", e);
            StoreError::IoUring
        })?;

        let poll_ring = io_uring::IoUring::builder()
            .dontfork()
            .setup_iopoll()
            .setup_sqpoll(options.sqpoll_idle_ms)
            .setup_sqpoll_cpu(options.sqpoll_cpu)
            .setup_r_disabled()
            .build(options.io_depth)
            .map_err(|e| {
                error!(log, "Failed to build polling I/O Uring instance: {:#?}", e);
                StoreError::IoUring
            })?;

        let submitter = poll_ring.submitter();
        submitter.register_iowq_max_workers(&mut options.max_workers)?;
        submitter.register_enable_rings()?;
        trace!(log, "Polling I/O Uring instance created");

        let (sender, receiver) = crossbeam::channel::unbounded();

        Ok(Self {
            options: options.clone(),
            poll_ring,
            ring,
            sender,
            receiver,
            log,
            segments: VecDeque::new(),
        })
    }

    fn load_wal_segment_files(&mut self) -> Result<(), StoreError> {
        let mut segment_files: Vec<_> = self
            .options
            .wal_paths
            .iter()
            .rev()
            .map(|wal_path| Path::new(&wal_path.path).read_dir())
            .flatten()
            .flatten() // Note Result implements FromIterator trait, so `flatten` applies and potential `Err` will be propagated.
            .flatten()
            .map(|entry| {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.file_type().is_dir() {
                        warn!(self.log, "Skip {:?} as it is a directory", entry.path());
                        None
                    } else {
                        let path = entry.path();
                        let path = path.as_path();
                        if let Some(offset) = LogSegmentFile::parse_offset(path) {
                            let log_segment_file = LogSegmentFile::new(
                                offset,
                                entry.path().to_str()?,
                                metadata.len(),
                                segment::Medium::SSD,
                            );
                            Some(log_segment_file)
                        } else {
                            error!(
                                self.log,
                                "Failed to parse offset from file name: {:?}",
                                entry.path()
                            );
                            None
                        }
                    }
                } else {
                    None
                }
            })
            .filter_map(|f| f)
            .collect();

        // Sort log segment file by file name.
        segment_files.sort();

        for mut segment_file in segment_files.into_iter() {
            segment_file.open()?;
            self.segments.push_back(segment_file);
        }

        Ok(())
    }

    fn load(&mut self) -> Result<(), StoreError> {
        self.load_wal_segment_files()?;
        Ok(())
    }

    fn launch_indexer(&self) -> Result<(), StoreError> {
        Ok(())
    }

    fn alloc_segment(&mut self) -> Result<bool, StoreError> {
        let offset = if self.segments.is_empty() {
            0
        } else {
            if let Some(last) = self.segments.back() {
                last.offset + self.options.file_size
            } else {
                unreachable!("Should-not-reach-here")
            }
        };
        let dir = self
            .options
            .wal_paths
            .first()
            .ok_or(StoreError::AllocLogSegment)?;
        let path = Path::new(&dir.path);
        let path = path.join(LogSegmentFile::format(offset));
        let segment = LogSegmentFile::new(
            offset,
            path.to_str().ok_or(StoreError::AllocLogSegment)?,
            self.options.file_size,
            segment::Medium::SSD,
        );
        self.segments.push_back(segment);
        Ok(true)
    }

    fn acquire_writable_segment(&mut self) -> Option<&mut LogSegmentFile> {
        let mut create = true;
        if let Some(file) = self.segments.back() {
            if !file.is_full() {
                create = false;
            }
        }

        if create {
            self.alloc_segment();
        }
        // Reuse previously allocated segment file since it's not full yet.
        self.segments.back_mut()
    }

    fn segment_file_of(&self, offset: u64, len: u32) -> Option<&LogSegmentFile> {
        for segment in self.segments.iter().rev() {
            if segment.offset < offset {
                return Some(segment);
            }
        }
        None
    }

    pub(crate) fn run(&mut self) -> Result<(), StoreError> {
        self.load()?;

        // let (tx, rx) = tokio::sync::oneshot::channel();
        self.launch_indexer()?;

        let io_depth = self.poll_ring.params().sq_entries();
        let mut in_flight_requests = 0;

        let wal_dir = self
            .options
            .wal_paths
            .first()
            .expect("Failed to acquire tier-0 WAL directory")
            .clone();
        let wal_path = Path::new(&wal_dir.path);

        let file_size = self.options.file_size;

        let mut current_segment = self
            .acquire_writable_segment()
            .ok_or(StoreError::AllocLogSegment)?;
        current_segment.open()?;

        let mut pos = current_segment.offset + current_segment.written;

        let mut io_tasks: BTreeMap<u64, u32> = BTreeMap::new();

        loop {
            let mut pending = vec![];
            loop {
                if in_flight_requests >= io_depth {
                    break;
                }

                // if the log segment file is full, break loop.

                if in_flight_requests == 0 {
                    if let Ok(io_task) = self.receiver.recv() {
                        pending.push(io_task);
                        in_flight_requests += 1;
                    } else {
                        break;
                    }
                } else {
                    if let Ok(io_task) = self.receiver.try_recv() {
                        pending.push(io_task);
                        in_flight_requests += 1;
                    } else {
                        break;
                    }
                }
            }

            let entries: Vec<_> = pending
                .into_iter()
                .map(|io_task| match io_task {
                    IoTask::Read(ReadTask {
                        offset,
                        len,
                        buffer,
                    }) => {
                        let segment = self
                            .segment_file_of(offset, len)
                            .ok_or(StoreError::AllocLogSegment)?;
                        let sqe = opcode::Read::new(
                            types::Fd(segment.fd.ok_or(StoreError::AllocLogSegment)?),
                            buffer,
                            len,
                        )
                        .offset((offset - segment.offset) as i64)
                        .build()
                        .user_data(0);
                        Ok::<squeue::Entry, StoreError>(sqe)
                    }
                    IoTask::Write(WriteTask {
                        stream_id,
                        offset,
                        buffer,
                    }) => {
                        let ptr = pos;
                        let file_offset = pos - current_segment.offset;
                        if file_offset > current_segment.size {
                            // A few issues are still not resolved
                            // todo!("buffer is NOT 4k-aligned for now");
                            // todo!("Switch current log segment file");
                        }

                        let sqe = Write::new(
                            types::Fd(
                                current_segment
                                    .fd
                                    .expect("LogSegmentFile should have opened"),
                            ),
                            buffer.as_ptr(),
                            buffer.len() as u32,
                        )
                        .offset64(file_offset as libc::off_t)
                        .build()
                        .user_data(ptr);
                        io_tasks.insert(ptr, buffer.len() as u32);
                        pos += buffer.len() as u64;
                        Ok(sqe)
                    }
                })
                .flatten()
                .collect();

            if !entries.is_empty() {
                unsafe {
                    self.poll_ring
                        .submission()
                        .push_multiple(&entries)
                        .map_err(|e| StoreError::IoUring)?
                };
            }

            if let Ok(_reaped) = self.poll_ring.submit_and_wait(1) {
                let mut completion = self.poll_ring.completion();
                loop {
                    while let Some(_cqe) = completion.next() {
                        in_flight_requests -= 1;
                    }

                    // This will flush any entries consumed in this iterator and will make available new entries in the queue
                    // if the kernel has produced some entries in the meantime.
                    completion.sync();

                    if completion.is_empty() {
                        break;
                    }
                }
            } else {
                break;
            }
        }
        Ok(())
    }
}

impl AsRawFd for IO {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.poll_ring.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs::File;

    use std::env;
    use uuid::Uuid;

    use crate::io::segment::LogSegmentFile;

    #[test]
    fn test_load_wals() -> Result<(), Box<dyn Error>> {
        let mut options = super::Options::default();
        let uuid = Uuid::new_v4();
        let mut wal_dir = env::temp_dir();
        wal_dir.push(uuid.simple().to_string());
        let wal_dir = wal_dir.as_path();
        std::fs::create_dir_all(wal_dir)?;
        let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir);

        let logger = util::terminal_logger();

        // Prepare log segment files
        let files: Vec<_> = (0..10)
            .into_iter()
            .map(|i| {
                let f = wal_dir.join(LogSegmentFile::format(i * 100));
                File::create(f.as_path())
            })
            .flatten()
            .collect();
        assert_eq!(10, files.len());

        let wal_dir = super::WalPath::new(wal_dir.to_str().unwrap(), 1234);
        options.wal_paths.push(wal_dir);

        let mut io = super::IO::new(&mut options, logger.clone())?;
        io.load_wal_segment_files()?;
        assert_eq!(files.len(), io.segments.len());
        Ok(())
    }
}
