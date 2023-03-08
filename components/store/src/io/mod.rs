mod block_cache;
pub(crate) mod buf;
mod context;
mod record;
mod segment;
pub(crate) mod task;
mod wal;
mod write_window;

use crate::error::{AppendError, StoreError};
use crate::index::{Indexer, MinOffset};
use crate::ops::append::AppendResult;
use crate::option::WalPath;
use crossbeam::channel::{Receiver, Sender, TryRecvError};
use io_uring::register;
use io_uring::{opcode, squeue, types};
use segment::LogSegment;
use slog::{debug, error, info, trace, warn, Logger};
use std::collections::{BTreeMap, HashSet};
use std::fs::OpenOptions;
use std::os::unix::prelude::{FileExt, OpenOptionsExt};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::{
    cell::{RefCell, UnsafeCell},
    collections::{HashMap, VecDeque},
    os::fd::AsRawFd,
    path::Path,
};
use task::IoTask;

use self::buf::{AlignedBufReader, AlignedBufWriter};
use self::context::IOContext;
use self::record::RecordType;
use self::segment::Status;
use self::task::WriteTask;
use self::wal::WAL;
use self::write_window::WriteWindow;
use crc::{Crc, CRC_32_ISCSI};

const DEFAULT_MAX_IO_DEPTH: u32 = 4096;
const DEFAULT_SQPOLL_IDLE_MS: u32 = 2000;
const DEFAULT_SQPOLL_CPU: u32 = 1;
const DEFAULT_MAX_BOUNDED_URING_WORKER_COUNT: u32 = 2;
const DEFAULT_MAX_UNBOUNDED_URING_WORKER_COUNT: u32 = 2;

const DEFAULT_LOG_SEGMENT_FILE_SIZE: u64 = 1024u64 * 1024 * 1024;

const DEFAULT_READ_BLOCK_SIZE: u32 = 1024 * 128;

const DEFAULT_MIN_PREALLOCATED_SEGMENT_FILES: usize = 1;

pub(crate) const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

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

    alignment: usize,

    read_block_size: u32,

    min_preallocated_segment_files: usize,
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
            alignment: 4096,
            read_block_size: DEFAULT_READ_BLOCK_SIZE,
            min_preallocated_segment_files: DEFAULT_MIN_PREALLOCATED_SEGMENT_FILES,
        }
    }
}

impl Options {
    pub fn add_wal_path(&mut self, wal_path: WalPath) {
        self.alignment = wal_path.block_size;
        self.wal_paths.push(wal_path);
    }
}

struct WalOffsetManager {
    min: AtomicU64,
}

impl WalOffsetManager {
    fn new() -> Self {
        Self {
            min: AtomicU64::new(u64::MAX),
        }
    }

    fn set_min_offset(&self, min: u64) {
        self.min.store(min, Ordering::Relaxed);
    }
}

impl MinOffset for WalOffsetManager {
    fn min_offset(&self) -> u64 {
        self.min.load(Ordering::Relaxed)
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
    data_ring: io_uring::IoUring,

    /// Sender of the IO task channel.
    ///
    /// Assumed to be taken by wrapping structs, which will offer APIs with semantics of choice.
    pub(crate) sender: Option<Sender<IoTask>>,

    /// Receiver of the IO task channel.
    ///
    /// According to our design, there is only one instance.
    receiver: Receiver<IoTask>,

    /// Logger instance.
    log: Logger,

    /// A WAL instance that manages the lifecycle of write-ahead-log segments.
    ///
    /// Note new segment are appended to the back; while oldest segments are popped from the front.
    wal: WAL,

    // Following fields are runtime data structure
    /// Flag indicating if the IO channel is disconnected, aka, all senders are dropped.
    channel_disconnected: bool,

    tag: u64,
    inflight_data_tasks: HashMap<u64, IOContext>,

    /// Number of inflight data tasks that are submitted to `data_uring` and not yet reaped.
    inflight: usize,
    write_inflight: usize,

    /// Pending IO tasks received from IO channel.
    ///
    /// Before converting `IoTask`s into io_uring SQEs, we need to ensure these tasks are bearing valid offset and
    /// length if they are read; In case the tasks are write, we need to ensure targeting log segment file has enough
    /// space for the incoming buffers.
    ///
    /// If there is no writable log segment files available or the write is so fast that preallocated ones are depleted
    /// before a new segment file is ready, writes, though very unlikely, will stall.
    pending_data_tasks: VecDeque<IoTask>,

    buf_writer: UnsafeCell<AlignedBufWriter>,

    /// Tracks write requests that are dispatched to underlying storage device and
    /// the completed ones;
    ///
    /// Advances continuous boundary if possible.
    write_window: WriteWindow,

    /// Inflight write tasks that are not yet acknowledged
    inflight_write_tasks: BTreeMap<u64, WriteTask>,

    /// Offsets of blocks that are partially filled with data and are still inflight.
    barrier: HashSet<u64>,

    blocked: HashMap<u64, squeue::Entry>,

    wal_offset_manager: Rc<WalOffsetManager>,
}

/// Check if required opcodes are supported by the host operation system.
///
/// # Arguments
/// * `probe` - Probe result, which contains all features that are supported.
///
fn check_io_uring(probe: &register::Probe) -> Result<(), StoreError> {
    let codes = [
        opcode::OpenAt::CODE,
        opcode::Fallocate64::CODE,
        opcode::Write::CODE,
        opcode::Read::CODE,
        opcode::Close::CODE,
        opcode::UnlinkAt::CODE,
    ];
    for code in &codes {
        if !probe.is_supported(*code) {
            return Err(StoreError::OpCodeNotSupported(*code));
        }
    }
    Ok(())
}

impl IO {
    /// Create new `IO` instance.
    ///
    /// Behavior of the IO instance can be tuned through `Options`.
    pub(crate) fn new(options: &mut Options, log: Logger) -> Result<Self, StoreError> {
        if options.wal_paths.is_empty() {
            return Err(StoreError::Configuration("WAL path required".to_owned()));
        }

        // Ensure WAL directories exists.
        for dir in &options.wal_paths {
            util::mkdirs_if_missing(&dir.path)?;
        }

        let control_ring = io_uring::IoUring::builder().dontfork().build(32).map_err(|e| {
            error!(log, "Failed to build I/O Uring instance for write-ahead-log segment file management: {:#?}", e);
            StoreError::IoUring
        })?;

        let data_ring = io_uring::IoUring::builder()
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

        let mut probe = register::Probe::new();

        let submitter = data_ring.submitter();
        submitter.register_iowq_max_workers(&mut options.max_workers)?;
        submitter.register_probe(&mut probe)?;
        submitter.register_enable_rings()?;

        check_io_uring(&probe)?;

        trace!(log, "Polling I/O Uring instance created");

        let (sender, receiver) = crossbeam::channel::unbounded();

        Ok(Self {
            options: options.clone(),
            data_ring,
            sender: Some(sender),
            receiver,
            write_window: WriteWindow::new(log.clone(), 0),
            buf_writer: UnsafeCell::new(AlignedBufWriter::new(log.clone(), 0, options.alignment)),
            wal: WAL::new(
                options.clone().wal_paths,
                control_ring,
                options.file_size,
                log.clone(),
            ),
            log,
            channel_disconnected: false,
            tag: 0,
            inflight_data_tasks: HashMap::new(),
            inflight: 0,
            write_inflight: 0,
            pending_data_tasks: VecDeque::new(),
            inflight_write_tasks: BTreeMap::new(),
            barrier: HashSet::new(),
            blocked: HashMap::new(),
            wal_offset_manager: Rc::new(WalOffsetManager::new()),
        })
    }

    fn load(&mut self) -> Result<(), StoreError> {
        self.wal.load_from_paths()?;
        Ok(())
    }

    fn recover(&mut self, offset: u64) -> Result<(), StoreError> {
        let pos = self.wal.recover(offset)?;

        // Reset offset of write buffer
        self.buf_writer.get_mut().offset(pos);

        // Reset committed WAL offset
        self.write_window.reset_committed(pos);

        Ok(())
    }

    fn validate_io_task(io_task: &mut IoTask, log: &Logger) -> bool {
        if let IoTask::Write(ref mut task) = io_task {
            if task.buffer.is_empty() {
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

    fn receive_io_tasks(&mut self) -> usize {
        let mut received = 0;
        let io_depth = self.data_ring.params().sq_entries() as usize;
        loop {
            // TODO: Find a better estimation.
            //
            // it might be some kind of pessimistic here as read/write may be merged after grouping.
            // As we might configure a larger IO depth value, finding a more precise metric is left to
            // the next development iteration.
            //
            // A better metric is combining actual SQEs number with received tasks together, so we may
            // receive tasks according merging result of the previous iteration.
            //
            // Note cloud providers count IOPS in a complex way:
            // https://aws.amazon.com/premiumsupport/knowledge-center/ebs-calculate-optimal-io-size/
            //
            // For example, if the application is performing small I/O operations of 32 KiB:
            // 1. Amazon EBS merges sequential (physically contiguous) operations to the maximum I/O size of 256 KiB.
            //    In this scenario, Amazon EBS counts only 1 IOPS to perform 8 I/O operations submitted by the operating system.
            // 2. Amazon EBS counts random I/O operations separately. A single, random I/O operation of 32 KiB counts as 1 IOPS.
            //    In this scenario, Amazon EBS counts 8 random, 32 KiB I/O operations as 8 IOPS submitted by the OS.
            //
            // Amazon EBS splits I/O operations larger than the maximum 256 KiB into smaller operations.
            // For example, if the I/O size is 500 KiB, Amazon EBS splits the operation into 2 IOPS.
            // The first one is 256 KiB and the second one is 244 KiB.
            if self.inflight + received >= io_depth {
                break received;
            }

            // if the log segment file is full, break loop.

            if self.inflight + received == 0 {
                // Block the thread until at least one IO task arrives
                match self.receiver.recv() {
                    Ok(mut io_task) => {
                        if !IO::validate_io_task(&mut io_task, &self.log) {
                            IO::on_bad_request(io_task);
                            continue;
                        }
                        self.pending_data_tasks.push_back(io_task);
                        received += 1;
                    }
                    Err(_e) => {
                        info!(self.log, "Channel for submitting IO task disconnected");
                        self.channel_disconnected = true;
                        break received;
                    }
                }
            } else {
                match self.receiver.try_recv() {
                    Ok(mut io_task) => {
                        if !IO::validate_io_task(&mut io_task, &self.log) {
                            IO::on_bad_request(io_task);
                            continue;
                        }
                        self.pending_data_tasks.push_back(io_task);
                        received += 1;
                    }
                    Err(TryRecvError::Empty) => {
                        break received;
                    }
                    Err(TryRecvError::Disconnected) => {
                        info!(self.log, "Channel for submitting IO task disconnected");
                        self.channel_disconnected = true;
                        break received;
                    }
                }
            }
        }
    }

    fn calculate_write_buffers(&self) -> Vec<usize> {
        let mut requirement: VecDeque<_> = self
            .pending_data_tasks
            .iter()
            .map(|task| match task {
                IoTask::Write(task) => {
                    debug_assert!(task.buffer.len() > 0);
                    task.buffer.len() + 4 /* CRC */ + 3 /* Record Size */ + 1 /* Record Type */
                }
                _ => 0,
            })
            .filter(|n| *n > 0)
            .collect();
        self.wal.calculate_write_buffers(&mut requirement)
    }

    fn build_write_sqe(&mut self, entries: &mut Vec<squeue::Entry>) {
        // Add previously blocked entries.
        self.blocked
            .drain_filter(|offset, _entry| !self.barrier.contains(offset))
            .for_each(|(_, entry)| {
                entries.push(entry);
            });

        let writer = self.buf_writer.get_mut();
        writer
            .take()
            .into_iter()
            .map(|buf| {
                let ptr = buf.as_ptr();
                if let Some(segment) = self.wal.segment_file_of(buf.offset) {
                    debug_assert_eq!(Status::ReadWrite, segment.status);
                    if let Some(sd) = segment.sd.as_ref() {
                        debug_assert!(buf.offset >= segment.offset);
                        debug_assert!(
                            buf.offset + buf.write_pos() as u64 <= segment.offset + segment.size
                        );
                        let file_offset = buf.offset - segment.offset;
                        let sqe = opcode::Write::new(types::Fd(sd.fd), ptr, buf.write_pos() as u32)
                            .offset64(file_offset as libc::off_t)
                            .build()
                            .user_data(self.tag);
                        // Track write requests
                        self.write_window.add(buf.offset, buf.write_pos() as u32)?;

                        // Check barrier
                        if self.barrier.contains(&buf.offset) {
                            // Submit SQE to io_uring when the blocking IO task completed.
                            self.blocked.insert(buf.offset, sqe);
                        } else {
                            // Insert barrier, blocking future write to this aligned block issued to `io_uring` until `sqe` is reaped.
                            if buf.partial() {
                                self.barrier.insert(buf.offset);
                            }
                            entries.push(sqe);
                        }
                        let ioc = IOContext {
                            opcode: opcode::Write::CODE,
                            buf,
                            offset: None,
                            len: None,
                        };
                        self.inflight_data_tasks.insert(self.tag, ioc);
                        self.tag += 1;
                    } else {
                        // fatal errors
                        let msg =
                            format!("Segment {} should be open and with valid FD", segment.path);
                        error!(self.log, "{}", msg);
                        panic!("{}", msg);
                    }
                } else {
                    error!(self.log, "");
                }
                Ok::<(), write_window::WriteWindowError>(())
            })
            .flatten()
            .count();
    }

    fn build_sqe(&mut self, entries: &mut Vec<squeue::Entry>) {
        let log = self.log.clone();
        let alignment = self.options.alignment;
        let buf_list = self.calculate_write_buffers();
        let left = self.buf_writer.get_mut().remaining();

        buf_list
            .iter()
            .enumerate()
            .map(|(idx, n)| {
                if 0 == idx {
                    if *n > left {
                        self.buf_writer.get_mut().reserve(*n - left)
                    } else {
                        Ok(())
                    }
                } else {
                    self.buf_writer.get_mut().reserve(*n)
                }
            })
            .flatten()
            .count();

        'task_loop: while let Some(io_task) = self.pending_data_tasks.pop_front() {
            match io_task {
                IoTask::Read(task) => {
                    // TODO: Check if there is an on-going read IO covering this request.
                    let segment = match self.wal.segment_file_of(task.offset) {
                        Some(segment) => segment,
                        None => {
                            // Consume io_task directly
                            todo!("Return error to caller directly")
                        }
                    };

                    if let Some(sd) = segment.sd.as_ref() {
                        if let Ok(buf) = AlignedBufReader::alloc_read_buf(
                            self.log.clone(),
                            task.offset,
                            task.len as usize,
                            alignment as u64,
                        ) {
                            let ptr = buf.as_ptr() as *mut u8;
                            let sqe = opcode::Read::new(types::Fd(sd.fd), ptr, task.len)
                                .offset((task.offset - segment.offset) as i64)
                                .build()
                                .user_data(self.tag);
                            self.tag += 1;
                            entries.push(sqe);

                            let ioc = IOContext {
                                opcode: opcode::Read::CODE,
                                buf: Arc::new(buf),
                                offset: Some(task.offset),
                                len: Some(task.len),
                            };
                            self.inflight_data_tasks.insert(self.tag, ioc);
                        }
                    } else {
                        self.pending_data_tasks.push_front(IoTask::Read(task));
                    }
                }
                IoTask::Write(task) => {
                    let writer = unsafe { &mut *self.buf_writer.get() };
                    loop {
                        if let Some(segment) = self.wal.segment_file_of(writer.offset) {
                            if segment.status != Status::ReadWrite {
                                self.pending_data_tasks.push_front(IoTask::Write(task));
                                break 'task_loop;
                            }

                            if let Some(_sd) = segment.sd.as_ref() {
                                let payload_length = task.buffer.len();
                                if !segment.can_hold(payload_length as u64) {
                                    segment.append_footer(writer);
                                    // Switch to a new log segment
                                    continue;
                                }
                                segment.append_full_record(writer, &task.buffer[..]);
                                break;
                            } else {
                                error!(log, "LogSegmentFile {} with read_write status does not have valid FD", segment.path);
                                unreachable!("LogSegmentFile {} should have been with a valid FD if its status is read_write", segment.path);
                            }
                        } else {
                            self.pending_data_tasks.push_front(IoTask::Write(task));
                            break 'task_loop;
                        }
                    }
                    self.build_write_sqe(entries);
                }
            }
        }
    }

    fn await_data_task_completion(&self, mut wanted: usize) {
        if self.inflight == 0 {
            trace!(
                self.log,
                "No inflight data task. Skip `await_data_task_completion`"
            );
            return;
        }

        trace!(
            self.log,
            "Waiting for at least {}/{} CQE(s) to reap",
            wanted,
            self.inflight
        );

        if wanted > self.inflight {
            wanted = self.inflight;
        }

        let now = std::time::Instant::now();
        match self.data_ring.submit_and_wait(wanted) {
            Ok(_reaped) => {
                trace!(
                    self.log,
                    "io_uring_enter waited {}us to reap completed data CQE(s)",
                    now.elapsed().as_micros()
                );
            }
            Err(e) => {
                error!(self.log, "io_uring_enter got an error: {:?}", e);

                // Fatal errors, crash the process and let watchdog to restart.
                panic!("io_uring_enter returns error {:?}", e);
            }
        }
    }

    fn reap_data_tasks(&mut self) {
        if 0 == self.inflight {
            return;
        }
        let committed = self.write_window.committed;
        let mut cache_entries = vec![];
        {
            let mut completion = self.data_ring.completion();
            let mut count = 0;
            loop {
                for cqe in completion.by_ref() {
                    count += 1;
                    let tag = cqe.user_data();
                    if let Some(state) = self.inflight_data_tasks.remove(&tag) {
                        // Remove barrier
                        self.barrier.remove(&state.buf.offset);

                        if let Err(e) =
                            on_complete(&mut self.write_window, &state, cqe.result(), &self.log)
                        {
                            match e {
                                StoreError::System(errno) => {
                                    error!(
                                        self.log,
                                        "io_uring opcode `{}` failed. errno: `{}`",
                                        state.opcode,
                                        errno
                                    );

                                    // TODO: Check if the errno is recoverable...
                                }
                                _ => {}
                            }
                        } else {
                            // Add block cache
                            cache_entries.push(Arc::clone(&state.buf));
                        }
                    }
                }
                // This will flush any entries consumed in this iterator and will make available new entries in the queue
                // if the kernel has produced some entries in the meantime.
                completion.sync();

                if completion.is_empty() {
                    break;
                }
            }
            debug_assert!(self.inflight >= count);
            self.inflight -= count;
            trace!(self.log, "Reaped {} data CQE(s)", count);
        }

        // Add to block cache
        for buf in cache_entries {
            if let Some(segment) = self.wal.segment_file_of(buf.offset) {
                segment.block_cache.add_entry(buf);
            }
        }

        if self.write_window.committed > committed {
            self.acknowledge_write_tasks();
        }
    }

    fn acknowledge_write_tasks(&mut self) {
        let committed = self.write_window.committed;
        loop {
            if let Some((offset, _)) = self.inflight_write_tasks.first_key_value() {
                if *offset < committed {
                    break;
                }
            } else {
                break;
            }

            if let Some((_, task)) = self.inflight_write_tasks.pop_first() {
                let append_result = AppendResult {
                    stream_id: task.stream_id,
                    offset: task.offset,
                };
                if let Err(e) = task.observer.send(Ok(append_result)) {
                    error!(self.log, "Failed to propagate AppendResult `{:?}`", e);
                }
            }
        }
    }

    fn should_quit(&self) -> bool {
        0 == self.inflight
            && self.pending_data_tasks.is_empty()
            && self.wal.control_task_num() == 0
            && self.channel_disconnected
    }

    fn submit_data_tasks(&mut self, entries: &Vec<squeue::Entry>) -> Result<(), StoreError> {
        // Submit io_uring entries into submission queue.
        trace!(
            self.log,
            "Get {} incoming SQE(s) to submit, inflight: {}",
            entries.len(),
            self.inflight
        );
        if !entries.is_empty() {
            let cnt = entries.len();
            unsafe {
                self.data_ring
                    .submission()
                    .push_multiple(entries)
                    .map_err(|e| {
                        info!(
                            self.log,
                            "Failed to push SQE entries into submission queue: {:?}", e
                        );
                        StoreError::IoUring
                    })?
            };
            self.inflight += cnt;
            trace!(self.log, "Pushed {} SQEs into submission queue", cnt);
        }
        Ok(())
    }

    pub(crate) fn run(io: RefCell<IO>) -> Result<(), StoreError> {
        let log = io.borrow().log.clone();
        let offset_manager = Rc::clone(&io.borrow().wal_offset_manager);
        let indexer = Indexer::new(log.clone(), "/tmp/rocksdb", offset_manager)?;
        io.borrow_mut().load()?;
        let pos = indexer.flushed_wal_offset();
        io.borrow_mut().recover(pos)?;

        let min_preallocated_segment_files = io.borrow().options.min_preallocated_segment_files;

        let cqe_wanted = 1;

        // Main loop
        loop {
            // Check if we need to create a new log segment
            loop {
                if io.borrow().wal.writable_segment_count() >= min_preallocated_segment_files + 1 {
                    break;
                }
                io.borrow_mut().wal.try_open()?;
            }

            // check if we have expired segment files to close and delete
            {
                io.borrow_mut().wal.try_close()?;
            }

            let mut entries = vec![];
            {
                let mut io_mut = io.borrow_mut();

                // Receive IO tasks from channel
                let cnt = io_mut.receive_io_tasks();
                trace!(log, "Received {} IO requests from channel", cnt);

                // Convert IO tasks into io_uring entries
                io_mut.build_sqe(&mut entries);
            }

            if !entries.is_empty() {
                io.borrow_mut().submit_data_tasks(&entries)?;
            } else {
                let io_borrow = io.borrow();
                if !io_borrow.should_quit() {
                    if !io_borrow.pending_data_tasks.is_empty() {
                        io_borrow.wal.await_control_task_completion();
                    }
                } else {
                    info!(
                        log,
                        "Now that all IO requests are served and channel disconnects, stop main loop"
                    );
                    break;
                }
            }

            // Wait complete asynchronous IO
            io.borrow().await_data_task_completion(cqe_wanted);

            {
                let mut io_mut = io.borrow_mut();
                // Reap data CQE(s)
                io_mut.reap_data_tasks();
                // Perform file operation
                io_mut.wal.reap_control_tasks()?;
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
/// * `state` - Operation state, including original IO request, buffer and response observer.
/// * `result` - Result code, exactly same to system call return value.
/// * `log` - Logger instance.
fn on_complete(
    write_window: &mut WriteWindow,
    ioc: &IOContext,
    result: i32,
    log: &Logger,
) -> Result<(), StoreError> {
    match ioc.opcode {
        opcode::Write::CODE => {
            if result < 0 {
                error!(
                    log,
                    "Write to WAL range `[{}, {})` failed",
                    ioc.buf.offset,
                    ioc.buf.write_pos()
                );
                return Err(StoreError::System(-result));
            } else {
                write_window
                    .commit(ioc.buf.offset, ioc.buf.write_pos() as u32)
                    .map_err(|_e| StoreError::WriteWindow)?;
            }
            Ok(())
        }
        opcode::Read::CODE => Ok(()),
        _ => Ok(()),
    }
}

impl AsRawFd for IO {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.data_ring.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use std::cell::RefCell;
    use std::env;
    use std::path::PathBuf;
    use tokio::sync::oneshot;
    use uuid::Uuid;

    use super::task::{IoTask, WriteTask};

    use crate::error::StoreError;
    use crate::option::WalPath;

    fn create_io(wal_dir: WalPath) -> Result<super::IO, StoreError> {
        let mut options = super::Options::default();
        let logger = util::terminal_logger();
        options.add_wal_path(wal_dir);
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

        io.receive_io_tasks();
        assert_eq!(16, io.pending_data_tasks.len());
        io.pending_data_tasks.clear();

        drop(sender);

        // Mock that some in-flight IO tasks were reaped
        io.inflight = 0;

        io.receive_io_tasks();

        assert_eq!(true, io.pending_data_tasks.is_empty());
        assert_eq!(true, io.channel_disconnected);

        Ok(())
    }

    #[test]
    fn test_build_sqe() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir.as_path());
        let mut io = create_io(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;

        let segment = io.wal.alloc_segment()?;
        segment.open()?;

        let len = 4088;
        let mut buffer = BytesMut::with_capacity(len);
        buffer.resize(len, 65);
        let buffer = buffer.freeze();

        // Send IoTask to channel
        (0..16)
            .into_iter()
            .map(|n| {
                let (tx, _rx) = oneshot::channel();
                IoTask::Write(WriteTask {
                    stream_id: 0,
                    offset: n,
                    buffer: buffer.clone(),
                    observer: tx,
                })
            })
            .for_each(|io_task| {
                io.pending_data_tasks.push_back(io_task);
            });

        let mut entries = Vec::new();

        io.build_sqe(&mut entries);
        assert!(!entries.is_empty());
        Ok(())
    }

    #[test]
    fn test_run() -> Result<(), StoreError> {
        let (tx, rx) = oneshot::channel();
        let handle = std::thread::spawn(move || {
            let wal_dir = random_wal_dir().unwrap();
            let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir.as_path());
            let mut io =
                create_io(super::WalPath::new(wal_dir.to_str().unwrap(), 1234).unwrap()).unwrap();
            let sender = io
                .sender
                .take()
                .ok_or(StoreError::Configuration("IO channel".to_owned()))
                .unwrap();
            tx.send(sender);
            let io = RefCell::new(io);

            let _ = super::IO::run(io);
            println!("Module io stopped");
        });

        let sender = rx
            .blocking_recv()
            .map_err(|_| StoreError::Internal("Internal error".to_owned()))?;

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
