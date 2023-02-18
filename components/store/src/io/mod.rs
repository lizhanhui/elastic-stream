mod segment;

use crate::error::StoreError;
use crate::option::WalPath;
use crossbeam::channel::{Receiver, Sender};
use segment::{LogSegmentFile, Status, TimeRange};
use slog::{error, info, trace, warn, Logger};
use std::{collections::VecDeque, os::fd::AsRawFd, path::Path};

const DEFAULT_MAX_IO_DEPTH: u32 = 4096;
const DEFAULT_SQPOLL_IDLE_MS: u32 = 2000;
const DEFAULT_SQPOLL_CPU: u32 = 1;
const DEFAULT_MAX_BOUNDED_URING_WORKER_COUNT: u32 = 2;
const DEFAULT_MAX_UNBOUNDED_URING_WORKER_COUNT: u32 = 2;

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

    pub(crate) sender: Sender<()>,
    receiver: Receiver<()>,
    log: Logger,

    segments: VecDeque<LogSegmentFile>,
}

impl IO {
    pub(crate) fn new(options: &mut Options, log: Logger) -> Result<Self, StoreError> {
        if options.wal_paths.is_empty() {
            return Err(StoreError::Configuration("WAL path required".to_owned()));
        }

        // Ensure WAL directories exists.
        for dir in &options.wal_paths {
            util::fs::mkdirs_if_missing(&dir.path)?;
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

    fn load_wals(&mut self) -> Result<(), StoreError> {
        self.options
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
                        let log_segment_file = LogSegmentFile::new(
                            entry.path().as_os_str().to_str().unwrap(),
                            metadata.len() as u32,
                            segment::Medium::SSD,
                        );
                        Some(log_segment_file)
                    }
                } else {
                    None
                }
            })
            .filter_map(|f| f)
            .for_each(|f| {
                // f.open();
                info!(self.log, "Adding {:?}", f);
                self.segments.push_back(f);
            });

        Ok(())
    }

    fn load(&mut self) -> Result<(), StoreError> {
        self.load_wals()?;
        Ok(())
    }

    pub(crate) fn run(&mut self) -> Result<(), StoreError> {
        self.load()?;

        let io_depth = self.poll_ring.params().sq_entries();
        let mut in_flight_requests = 0;
        loop {
            loop {
                if in_flight_requests >= io_depth {
                    break;
                }

                if let Ok(_) = self.receiver.try_recv() {
                    in_flight_requests += 1;
                    todo!("Convert item to IO task");
                }
            }

            if let Ok(_reaped) = self.poll_ring.submit_and_wait(1) {
                let mut completion = self.poll_ring.completion();
                while let Some(_cqe) = completion.next() {
                    in_flight_requests -= 1;
                }
                completion.sync();
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
    use std::{error::Error, path::Path};

    #[test]
    fn test_load_wals() -> Result<(), Box<dyn Error>> {
        let mut options = super::Options::default();
        let wal_dir = super::WalPath::new("/tmp", 1234);
        options.wal_paths.push(wal_dir);
        let logger = util::terminal_logger();
        let mut io = super::IO::new(&mut options, logger.clone())?;
        io.load_wals()?;
        Ok(())
    }
}
