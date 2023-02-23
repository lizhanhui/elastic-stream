pub(crate) mod buf;
mod record;
mod segment;
mod state;
pub(crate) mod task;

use crate::error::AppendError;
use crate::option::WalPath;
use crate::{error::StoreError, ops::append::AppendResult};
use buf::RecordBuf;
use crossbeam::channel::{Receiver, Sender, TryRecvError};
use io_uring::{
    opcode::{self, Write},
    squeue, types,
};
use segment::LogSegmentFile;
use slog::{error, info, trace, warn, Logger};
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    os::fd::AsRawFd,
    path::Path,
};
use task::IoTask;

use self::segment::Status;
use self::state::OpState;

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

impl Options {
    pub fn add_wal_path(&mut self, wal_path: WalPath) {
        self.wal_paths.push(wal_path);
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

    pub(crate) sender: Option<Sender<IoTask>>,
    receiver: Receiver<IoTask>,
    log: Logger,

    segments: VecDeque<LogSegmentFile>,

    // Runtime data structure
    channel_disconnected: bool,
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
            sender: Some(sender),
            receiver,
            log,
            segments: VecDeque::new(),
            channel_disconnected: false,
        })
    }

    fn load_wal_segment_files(&mut self) -> Result<(), StoreError> {
        let mut segment_files: Vec<_> = self
            .options
            .wal_paths
            .iter()
            .rev()
            .flat_map(|wal_path| Path::new(&wal_path.path).read_dir())
            .flatten() // Note Result implements FromIterator trait, so `flatten` applies and potential `Err` will be propagated.
            .flatten()
            .flat_map(|entry| {
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
                                segment::Medium::Ssd,
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

    fn alloc_segment(&mut self) -> Result<(), StoreError> {
        let offset = if self.segments.is_empty() {
            0
        } else if let Some(last) = self.segments.back() {
            last.offset + self.options.file_size
        } else {
            unreachable!("Should-not-reach-here")
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
            segment::Medium::Ssd,
        );
        self.segments.push_back(segment);
        Ok(())
    }

    fn acquire_writable_segment(&mut self) -> Option<&mut LogSegmentFile> {
        let mut create = true;

        if let Some(file) = self.segments.back() {
            if !file.is_full() {
                create = false;
            }
        }

        if create {
            if let Err(e) = self.alloc_segment() {
                error!(self.log, "Failed to allocate LogSegmentFile: {:?}", e);
                return None;
            }
        }
        // Reuse previously allocated segment file since it's not full yet.
        self.segments.back_mut()
    }

    fn segment_file_of(&mut self, offset: u64) -> Option<&mut LogSegmentFile> {
        self.segments
            .iter_mut()
            .rev()
            .find(|segment| segment.offset <= offset && (segment.offset + segment.size > offset))
    }

    fn validate_io_task(io_task: &mut IoTask, log: &Logger) -> bool {
        if let IoTask::Write(ref mut task) = io_task {
            if 0 == task.buffer.len() {
                warn!(log, "WriteTask buffer length is 0");
                return false;
            }
        }
        return true;
    }

    fn on_bad_request(io_task: IoTask) {
        match io_task {
            IoTask::Read(_) => {
                todo!()
            }
            IoTask::Write(write_task) => {
                let _ = write_task.observer.send(Err(AppendError::Internal));
            }
        }
    }

    fn receive_io_tasks(&mut self, inflight: &mut u32, io_depth: u32) -> Vec<IoTask> {
        let mut tasks = vec![];
        loop {
            if *inflight >= io_depth {
                break;
            }

            // if the log segment file is full, break loop.

            if *inflight == 0 {
                // Block the thread until at least one IO task arrives
                match self.receiver.recv() {
                    Ok(mut io_task) => {
                        if !IO::validate_io_task(&mut io_task, &self.log) {
                            IO::on_bad_request(io_task);
                            continue;
                        }
                        tasks.push(io_task);
                        *inflight += 1;
                    }
                    Err(_e) => {
                        info!(self.log, "Channel for submitting IO task disconnected");
                        self.channel_disconnected = true;
                        break;
                    }
                }
            } else {
                match self.receiver.try_recv() {
                    Ok(mut io_task) => {
                        if !IO::validate_io_task(&mut io_task, &self.log) {
                            IO::on_bad_request(io_task);
                            continue;
                        }
                        tasks.push(io_task);
                        *inflight += 1;
                    }
                    Err(TryRecvError::Empty) => {
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        info!(self.log, "Channel for submitting IO task disconnected");
                        self.channel_disconnected = true;
                        break;
                    }
                }
            }
        }
        tasks
    }

    fn writable_segment_count(&self) -> usize {
        self.segments
            .iter()
            .rev() // from back to front
            .take_while(|segment| segment.status != Status::Read)
            .count()
    }

    fn convert_task_to_sqe(
        &mut self,
        tasks: Vec<IoTask>,
        tag: &mut u64,
        pos: &mut u64,
        alignment: u32,
        inflight_tasks: &mut HashMap<u64, OpState>,
    ) -> Vec<squeue::Entry> {
        tasks
            .into_iter()
            .flat_map(|io_task| match io_task {
                IoTask::Read(read_task) => {
                    let segment = self
                        .segment_file_of(read_task.offset)
                        .ok_or(StoreError::OffsetOutOfRange(read_task.offset))?;

                    let mut record_buf = RecordBuf::new();
                    record_buf.alloc(read_task.len as usize, alignment as usize)?;
                    let buf = record_buf
                        .write_buf()
                        .expect("Should have allocated aligned memory for read");
                    let ptr = buf.ptr;
                    let state = OpState {
                        task: io_task,
                        record_buf,
                    };
                    inflight_tasks.insert(*tag, state);

                    let sqe = opcode::Read::new(
                        types::Fd(segment.fd.ok_or(StoreError::AllocLogSegment)?),
                        ptr,
                        read_task.len,
                    )
                    .offset((read_task.offset - segment.offset) as i64)
                    .build()
                    .user_data(*tag);
                    *tag += 1;
                    Ok::<squeue::Entry, StoreError>(sqe)
                }
                IoTask::Write(task) => {
                    loop {
                        if let Some(segment) = self.segment_file_of(*pos) {
                            let buf_len = task.buffer.len();
                            let file_offset = *pos - segment.offset;
                            if !segment.can_hold(buf_len as u64) {
                                segment.append_footer(pos);
                                // Switch to a new log segment
                                continue;
                            }

                            let mut record_buf = RecordBuf::new();
                            record_buf.alloc(buf_len, alignment as usize)?;
                            let buf = record_buf.write_buf().ok_or(StoreError::MemoryAlignment)?;
                            unsafe { std::ptr::copy(task.buffer.as_ptr(), buf.ptr, buf_len) };

                            let sqe = Write::new(
                                types::Fd(segment.fd.expect("LogSegmentFile should have opened")),
                                buf.ptr,
                                task.buffer.len() as u32,
                            )
                            .offset64(file_offset as libc::off_t)
                            .build()
                            .user_data(*tag);

                            // Track state of write op
                            let state = OpState {
                                task: IoTask::Write(task),
                                record_buf,
                            };
                            inflight_tasks.insert(*tag, state);
                            *tag += 1;
                            *pos += buf_len as u64;
                            break Ok(sqe);
                        } else {
                            self.alloc_segment()?;
                        }
                    }
                }
            })
            .collect()
    }

    pub(crate) fn run(io: RefCell<IO>) -> Result<(), StoreError> {
        let log = io.borrow().log.clone();

        io.borrow_mut().load()?;

        // let (tx, rx) = tokio::sync::oneshot::channel();
        io.borrow().launch_indexer()?;

        let alignment = io
            .borrow()
            .options
            .wal_paths
            .first()
            .ok_or(StoreError::Configuration(String::from(
                "Need at least one WAL path",
            )))?
            .block_size;

        let io_depth = io.borrow().poll_ring.params().sq_entries();

        // Number of inflight IO tasks submitted to block layer.
        let mut inflight = 0;

        let wal_dir = io
            .borrow()
            .options
            .wal_paths
            .first()
            .expect("Failed to acquire tier-0 WAL directory")
            .clone();
        let _wal_path = Path::new(&wal_dir.path);

        let _file_size = io.borrow().options.file_size;

        // `pos` will be initialized when loading store during start-up procedure.
        let mut pos = {
            let mut io_mut = io.borrow_mut();
            let segment = io_mut
                .acquire_writable_segment()
                .ok_or(StoreError::AllocLogSegment)?;
            segment.open()?;
            segment.offset + segment.written
        };

        // Mapping between tag to OpState
        let mut inflight_tasks = HashMap::new();
        let mut tag: u64 = 0;

        let cqe_wanted = 1;

        // Mapping between segment offset to Status
        //
        // Status migration: OpenAt --> Fallocate --> ReadWrite. Once the status of segment is driven to ReadWrite,
        // this mapping should be removed.
        let mut file_op: HashMap<u64, Status> = HashMap::new();

        // Main loop
        loop {
            // Check if we need to create a new log segment
            {}

            let entries = {
                let mut io_mut = io.borrow_mut();

                // Receive IO tasks from channel
                let tasks = io_mut.receive_io_tasks(&mut inflight, io_depth);
                trace!(log, "Received {} IO requests from channel", tasks.len());

                // Convert IO tasks into io_uring entries
                io_mut.convert_task_to_sqe(
                    tasks,
                    &mut tag,
                    &mut pos,
                    alignment,
                    &mut inflight_tasks,
                )
            };

            if 0 == inflight && io.borrow().channel_disconnected {
                info!(
                    log,
                    "Now that all IO requests are served and channel disconnects, stop main loop"
                );
                break;
            }

            // Submit io_uring entries into submission queue.
            trace!(
                log,
                "Get {} incoming SQE(s) to submit, inflight: {}",
                entries.len(),
                inflight
            );
            if !entries.is_empty() {
                let cnt = entries.len();
                unsafe {
                    io.borrow_mut()
                        .poll_ring
                        .submission()
                        .push_multiple(&entries)
                        .map_err(|e| {
                            info!(
                                log,
                                "Failed to push SQE entries into submission queue: {:?}", e
                            );
                            StoreError::IoUring
                        })?
                };
                trace!(log, "Pushed {} SQEs into submission queue", cnt);
            }

            // Wait complete asynchronous IO
            trace!(
                log,
                "Waiting for at least {}/{} CQE(s) to reap",
                cqe_wanted,
                inflight
            );
            let now = std::time::Instant::now();
            match io.borrow().poll_ring.submit_and_wait(cqe_wanted) {
                Ok(_reaped) => {
                    trace!(
                        log,
                        "io_uring_enter waited {}us to reap completed IO CQE(s)",
                        now.elapsed().as_micros()
                    );
                }
                Err(e) => {
                    error!(log, "io_uring_enter got an error: {:?}", e);

                    // Fatal errors, crash the process and let watchdog to restart.
                    panic!("io_uring_enter returns error {:?}", e);
                }
            }

            // Reap CQE(s)
            {
                let mut io_mut = io.borrow_mut();
                let mut completion = io_mut.poll_ring.completion();
                let prev = inflight;
                loop {
                    for cqe in completion.by_ref() {
                        inflight -= 1;
                        let tag = cqe.user_data();
                        if let Some(state) = inflight_tasks.remove(&tag) {
                            on_complete(tag, state, cqe.result(), &log);
                        }
                    }
                    // This will flush any entries consumed in this iterator and will make available new entries in the queue
                    // if the kernel has produced some entries in the meantime.
                    completion.sync();

                    if completion.is_empty() {
                        break;
                    }
                }
                trace!(log, "Reaped {} CQE(s)", prev - inflight);
            }
        }
        info!(log, "Main loop quit");
        Ok(())
    }
}

/// Process reaped IO completion.
///
/// # Arguments
///
/// * `tag` - Used to generate additional IO read if not all data are read yet.
/// * `state` - Operation state, including original IO request, buffer and response observer.
/// * `result` - Result code, exactly same to system call return value.
/// * `log` - Logger instance.
///
fn on_complete(_tag: u64, state: OpState, result: i32, log: &Logger) {
    match state.task {
        IoTask::Write(write) => {
            if -1 != result {
                trace!(log, "{} bytes written", result);
                let append_result = AppendResult {
                    stream_id: write.stream_id,
                    offset: write.offset,
                };
                if let Err(_e) = write.observer.send(Ok(append_result)) {
                    error!(log, "Failed to write append result to oneshot channel");
                }
            } else {
                error!(log, "Internal IO error: errno: {}", result);
                if write
                    .observer
                    .send(Err(AppendError::System(result)))
                    .is_err()
                {
                    error!(log, "Failed to propagate system error {} to caller", result);
                }
            }
        }
        IoTask::Read(_read) => {
            // Note reads performed in chunks of `BlockSize`, as a result, we have to
            // parse `RecordType` of each records to ensure the whole requested IO has been fulfilled.
            // If the target data spans two or more blocks, we need to generate an additional read request.
            todo!()
        }
    }
}

impl AsRawFd for IO {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.poll_ring.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fs::File;

    use bytes::BytesMut;
    use slog::info;
    use std::env;
    use std::path::PathBuf;
    use tokio::sync::oneshot;
    use uuid::Uuid;

    use crate::io::segment::LogSegmentFile;
    use crate::io::DEFAULT_LOG_SEGMENT_FILE_SIZE;
    use crate::option::WalPath;
    use crate::{error::StoreError, io::segment::Status};

    use super::task::{IoTask, WriteTask};

    fn create_io(wal_dir: WalPath) -> Result<super::IO, StoreError> {
        let mut options = super::Options::default();
        let logger = util::terminal_logger();
        options.wal_paths.push(wal_dir);
        super::IO::new(&mut options, logger.clone())
    }

    fn random_wal_dir() -> Result<PathBuf, StoreError> {
        let uuid = Uuid::new_v4();
        let mut wal_dir = env::temp_dir();
        wal_dir.push(uuid.simple().to_string());
        let wal_path = wal_dir.as_path();
        std::fs::create_dir_all(wal_path)?;
        Ok(wal_dir)
    }

    #[test]
    fn test_load_wals() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
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

        let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir.as_path());
        let wal_dir = super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?;

        let mut io = create_io(wal_dir)?;
        io.load_wal_segment_files()?;
        assert_eq!(files.len(), io.segments.len());
        Ok(())
    }

    #[test]
    fn test_alloc_segment() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir.as_path());
        let mut io = create_io(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;
        io.alloc_segment()?;
        assert_eq!(1, io.segments.len());

        io.alloc_segment()?;
        assert_eq!(
            DEFAULT_LOG_SEGMENT_FILE_SIZE,
            io.segments.get(1).unwrap().offset
        );
        Ok(())
    }

    #[test]
    fn test_writable_segment_count() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir.as_path());
        let mut io = create_io(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;
        io.alloc_segment()?;
        assert_eq!(1, io.writable_segment_count());
        io.alloc_segment()?;
        assert_eq!(2, io.writable_segment_count());

        io.segments.front_mut().unwrap().status = Status::Read;
        assert_eq!(1, io.writable_segment_count());

        Ok(())
    }

    #[test]
    fn test_acquire_writable_segment() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir.as_path());
        let mut io = create_io(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;
        io.acquire_writable_segment()
            .ok_or(StoreError::AllocLogSegment)?;
        assert_eq!(1, io.segments.len());
        // Verify `acquire_writable_segment()` is reentrant
        io.acquire_writable_segment()
            .ok_or(StoreError::AllocLogSegment)?;
        assert_eq!(1, io.segments.len());
        Ok(())
    }

    #[test]
    fn test_segment_file_of() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir.as_path());
        let mut io = create_io(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;
        io.acquire_writable_segment()
            .ok_or(StoreError::AllocLogSegment)?;
        assert_eq!(1, io.segments.len());

        // Ensure we can get the right
        let segment = io
            .segment_file_of(io.options.file_size - 1)
            .ok_or(StoreError::AllocLogSegment)?;
        assert_eq!(0, segment.offset);

        let segment = io.segment_file_of(0).ok_or(StoreError::AllocLogSegment)?;
        assert_eq!(0, segment.offset);

        Ok(())
    }

    #[test]
    fn test_receive_io_tasks() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir.as_path());
        let mut io = create_io(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;
        let sender = io.sender.take().unwrap();
        let mut buffer = BytesMut::with_capacity(128);
        buffer.resize(128, 65);
        let buffer = buffer.freeze();

        // Send IoTask to channel
        (0..16)
            .into_iter()
            .flat_map(|_| {
                let (tx, _rx) = oneshot::channel();
                let io_task = IoTask::Write(WriteTask {
                    stream_id: 0,
                    offset: 0,
                    buffer: buffer.clone(),
                    observer: tx,
                });
                sender.send(io_task)
            })
            .count();

        let mut in_flight_requests = 0;

        // Device IO queue depths
        let io_depth = 16;

        let tasks = io.receive_io_tasks(&mut in_flight_requests, io_depth);
        assert_eq!(16, tasks.len());

        drop(sender);

        // Mock that some in-flight IO tasks were reaped
        in_flight_requests = 8;

        let tasks = io.receive_io_tasks(&mut in_flight_requests, io_depth);

        assert_eq!(true, tasks.is_empty());
        assert_eq!(true, io.channel_disconnected);

        Ok(())
    }

    #[test]
    fn test_to_sqe() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir.as_path());
        let mut io = create_io(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;

        let segment = io.acquire_writable_segment().unwrap();
        segment.open()?;

        let buffer = BytesMut::with_capacity(128);
        let buffer = buffer.freeze();

        // Send IoTask to channel
        let tasks: Vec<_> = (0..16)
            .into_iter()
            .map(|_| {
                let (tx, _rx) = oneshot::channel();
                IoTask::Write(WriteTask {
                    stream_id: 0,
                    offset: 0,
                    buffer: buffer.clone(),
                    observer: tx,
                })
            })
            .collect();

        let mut tag = 0;
        let mut pos = 0;
        let alignment = 512;

        let mut inflight_tasks = HashMap::new();

        let entries =
            io.convert_task_to_sqe(tasks, &mut tag, &mut pos, alignment, &mut inflight_tasks);
        assert_eq!(16, entries.len());
        Ok(())
    }

    #[test]
    fn test_run() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir.as_path());
        let mut io = create_io(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;
        let sender = io
            .sender
            .take()
            .ok_or(StoreError::Configuration("IO channel".to_owned()))?;
        let io = RefCell::new(io);
        let handle = std::thread::spawn(move || {
            let _ = super::IO::run(io);
            println!("Module io stopped");
        });

        let mut buffer = BytesMut::with_capacity(4096);
        buffer.resize(4096, 65);
        let buffer = buffer.freeze();

        (0..16)
            .into_iter()
            .map(|i| {
                let (tx, _rx) = oneshot::channel();
                IoTask::Write(WriteTask {
                    stream_id: 0,
                    offset: i as i64,
                    buffer: buffer.clone(),
                    observer: tx,
                })
            })
            .for_each(|task| {
                sender.send(task).unwrap();
            });

        drop(sender);

        handle.join().map_err(|_| StoreError::AllocLogSegment)?;

        Ok(())
    }
}
