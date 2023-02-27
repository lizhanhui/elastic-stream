pub(crate) mod buf;
mod record;
mod segment;
mod state;
pub(crate) mod task;

use crate::error::AppendError;
use crate::index::Indexer;
use crate::option::WalPath;
use crate::{error::StoreError, ops::append::AppendResult};
use buf::RecordBuf;
use crossbeam::channel::{Receiver, Sender, TryRecvError};
use io_uring::register;
use io_uring::{
    opcode::{self, Write},
    squeue, types,
};
use segment::LogSegmentFile;
use slog::{debug, error, info, trace, warn, Logger};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::prelude::{FileExt, OpenOptionsExt};
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    os::fd::AsRawFd,
    path::Path,
};
use task::IoTask;

use self::record::RecordType;
use self::segment::Status;
use self::state::OpState;
use crc::{Algorithm, Crc, CRC_32_ISCSI};

const DEFAULT_MAX_IO_DEPTH: u32 = 4096;
const DEFAULT_SQPOLL_IDLE_MS: u32 = 2000;
const DEFAULT_SQPOLL_CPU: u32 = 1;
const DEFAULT_MAX_BOUNDED_URING_WORKER_COUNT: u32 = 2;
const DEFAULT_MAX_UNBOUNDED_URING_WORKER_COUNT: u32 = 2;

const DEFAULT_LOG_SEGMENT_FILE_SIZE: u64 = 1024u64 * 1024 * 1024;

const DEFAULT_MIN_PREALLOCATED_SEGMENT_FILES: usize = 1;

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

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
            min_preallocated_segment_files: DEFAULT_MIN_PREALLOCATED_SEGMENT_FILES,
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
    data_ring: io_uring::IoUring,

    /// I/O Uring instance for write-ahead-log segment file management.
    ///
    /// Unlike `data_uring`, this instance is used to open/fallocate/close/delete log segment files because these opcodes are not
    /// properly supported by the instance armed with the `IOPOLL` feature.
    control_ring: io_uring::IoUring,

    pub(crate) sender: Option<Sender<IoTask>>,
    receiver: Receiver<IoTask>,
    log: Logger,

    segments: VecDeque<LogSegmentFile>,

    // Runtime data structure
    channel_disconnected: bool,
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
            control_ring,
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

    /// Return whether has reached end of the WAL
    fn scan_record(
        segment: &mut LogSegmentFile,
        pos: &mut u64,
        log: &Logger,
    ) -> Result<bool, StoreError> {
        let mut file_pos = *pos - segment.offset;
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOATIME)
            .open(segment.path.to_owned())?;

        let mut meta_buf = [0; 4];

        let mut buf = bytes::BytesMut::new();
        let mut last_found = false;
        // Find the last continuous record
        loop {
            file.read_exact_at(&mut meta_buf, file_pos)?;
            file_pos += 4;
            let crc = u32::from_be_bytes(meta_buf);

            file.read_exact_at(&mut meta_buf, file_pos)?;
            file_pos += 4;
            let len_type = u32::from_be_bytes(meta_buf);
            let len = (len_type >> 8) as usize;

            // Verify the parsed `len` makes sense.
            if file_pos + len as u64 > segment.size {
                info!(
                    log,
                    "Got an invalid record length: `{}`. Stop scanning WAL", len
                );
                last_found = true;
                break;
            }

            let record_type = (len_type & 0xFF) as u8;
            if let Ok(t) = RecordType::try_from(record_type) {
                match t {
                    RecordType::Zero => {
                        // TODO: validate CRC for Padding?
                        // Padding
                        file_pos += len as u64;
                        // Should have reached EOF
                        debug_assert_eq!(segment.size, file_pos);

                        segment.written = segment.size;
                        segment.status = Status::Read;
                        info!(log, "Reached EOF of {}", segment.path);
                    }
                    RecordType::Full => {
                        // Full record
                        buf.resize(len, 0);
                        file.read_exact_at(buf.as_mut(), file_pos)?;

                        let ckm = CRC32C.checksum(buf.as_ref());
                        if ckm != crc {
                            segment.written = file_pos - 4 - 4;
                            segment.status = Status::ReadWrite;
                            info!(log, "Found a record failing CRC32c. Expecting: `{:#08x}`, Actual: `{:#08x}`", crc, ckm);
                            last_found = true;
                            break;
                        }
                        file_pos += len as u64;
                    }
                    RecordType::First => {
                        unimplemented!("Support of RecordType::First not implemented")
                    }
                    RecordType::Middle => {
                        unimplemented!("Support of RecordType::Middle not implemented")
                    }
                    RecordType::Last => {
                        unimplemented!("Support of RecordType::Last not implemented")
                    }
                }
            } else {
                last_found = true;
                break;
            }

            buf.resize(len, 0);
        }
        *pos = segment.offset + file_pos;
        Ok(last_found)
    }

    fn recover(&mut self, offset: u64) -> Result<u64, StoreError> {
        let mut pos = offset;
        let log = self.log.clone();
        info!(log, "Start to recover WAL segment files");
        let mut need_scan = true;
        for segment in &mut self.segments {
            if segment.offset + segment.size <= offset {
                segment.status = Status::Read;
                segment.written = segment.size;
                debug!(log, "Mark {} as read-only", segment.path);
                continue;
            }

            if !need_scan {
                segment.written = 0;
                segment.status = Status::ReadWrite;
                debug!(log, "Mark {} as read-write", segment.path);
                continue;
            }

            if Self::scan_record(segment, &mut pos, &log)? {
                need_scan = false;
                info!(log, "Recovery completed at `{}`", pos);
            }
        }
        info!(log, "Recovery of WAL segment files completed");
        Ok(pos)
    }

    fn alloc_segment(&mut self) -> Result<&mut LogSegmentFile, StoreError> {
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
        self.segments.back_mut().ok_or(StoreError::AllocLogSegment)
    }

    fn segment_file_of(&mut self, offset: u64) -> Option<&mut LogSegmentFile> {
        self.segments
            .iter_mut()
            .rev()
            .find(|segment| segment.offset <= offset && (segment.offset + segment.size > offset))
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

    fn receive_io_tasks(
        &mut self,
        inflight: &mut u32,
        io_depth: u32,
        tasks: &mut VecDeque<IoTask>,
    ) {
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
                        tasks.push_back(io_task);
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
                        tasks.push_back(io_task);
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
        tasks: &mut VecDeque<IoTask>,
        tag: &mut u64,
        pos: &mut u64,
        alignment: u32,
        inflight_tasks: &mut HashMap<u64, OpState>,
        entries: &mut Vec<squeue::Entry>,
    ) {
        let log = self.log.clone();
        'task_loop: while let Some(io_task) = tasks.pop_front() {
            match io_task {
                IoTask::Read(task) => {
                    let segment = match self.segment_file_of(task.offset) {
                        Some(segment) => segment,
                        None => {
                            // Consume io_task directly
                            todo!("Return error to caller directly")
                        }
                    };

                    if let Some(fd) = segment.fd {
                        let mut record_buf = RecordBuf::new();
                        if let Err(_e) = record_buf.alloc(task.len as usize, alignment as usize) {
                            // Put the IO task back, such that a second attempt is possible in the next loop round.
                            tasks.push_front(IoTask::Read(task));
                            error!(log, "Failed to alloc {}-aligned memory", alignment);
                            break 'task_loop;
                        }

                        let buf = record_buf
                            .write_buf()
                            .expect("Should have allocated aligned memory for read");
                        let ptr = buf.ptr;
                        let state = OpState {
                            task: io_task,
                            record_buf,
                        };

                        let sqe = opcode::Read::new(types::Fd(fd), ptr, task.len)
                            .offset((task.offset - segment.offset) as i64)
                            .build()
                            .user_data(*tag);
                        *tag += 1;
                        entries.push(sqe);

                        inflight_tasks.insert(*tag, state);
                    } else {
                        tasks.push_front(IoTask::Read(task));
                    }
                }
                IoTask::Write(task) => {
                    loop {
                        if let Some(segment) = self.segment_file_of(*pos) {
                            if segment.status != Status::ReadWrite {
                                tasks.push_front(IoTask::Write(task));
                                break 'task_loop;
                            }

                            if let Some(fd) = segment.fd {
                                let buf_len = task.buffer.len();
                                let file_offset = *pos - segment.offset;
                                if !segment.can_hold(buf_len as u64) {
                                    segment.append_footer(pos);
                                    // Switch to a new log segment
                                    continue;
                                }

                                let mut record_buf = RecordBuf::new();
                                if let Err(_e) = record_buf.alloc(buf_len, alignment as usize) {
                                    tasks.push_front(IoTask::Write(task));
                                    error!(log, "Failed to alloc {}-aligned memory", alignment);
                                    break 'task_loop;
                                }

                                if let Some(buf) = record_buf.write_buf() {
                                    unsafe {
                                        std::ptr::copy(task.buffer.as_ptr(), buf.ptr, buf_len)
                                    };

                                    let sqe = Write::new(
                                        types::Fd(fd),
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
                                    entries.push(sqe);
                                    break;
                                } else {
                                    error!(log, "RecordBuf does not have a valid ptr after successful allocation");
                                    unreachable!();
                                }
                            } else {
                                error!(log, "LogSegmentFile {} with read_write status does not have valid FD", segment.path);
                                unreachable!("LogSegmentFile {} should have been with a valid FD if its status is read_write", segment.path);
                            }
                        } else {
                            tasks.push_front(IoTask::Write(task));
                            break 'task_loop;
                        }
                    }
                }
            }
        }
    }

    fn async_open(&mut self, file_op: &mut HashMap<u64, Status>) -> Result<(), StoreError> {
        let log = self.log.clone();
        let segment = self.alloc_segment()?;
        let offset = segment.offset;
        debug_assert_eq!(segment.status, Status::OpenAt);
        file_op.insert(offset, segment.status);
        info!(
            log,
            "About to create/open LogSegmentFile: `{}`", segment.path
        );
        let sqe = opcode::OpenAt::new(
            types::Fd(libc::AT_FDCWD),
            segment.path.as_ptr() as *const i8,
        )
        .flags(libc::O_CREAT | libc::O_RDWR | libc::O_DIRECT)
        .mode(libc::S_IRWXU | libc::S_IRWXG)
        .build()
        .user_data(offset);
        unsafe {
            self.control_ring.submission().push(&sqe).map_err(|e| {
                error!(
                    self.log,
                    "Failed to push OpenAt SQE to submission queue: {:?}", e
                );
                StoreError::IoUring
            })?
        };
        Ok(())
    }

    /// Delete segments when their backed file is deleted
    fn delete_segments(&mut self, offsets: Vec<u64>) {
        if offsets.is_empty() {
            return;
        }

        self.segments.retain(|segment| {
            !(segment.status == Status::UnlinkAt && offsets.contains(&segment.offset))
        });
    }

    fn on_file_op_completion(
        &mut self,
        offset: u64,
        result: i32,
        op_table: &mut HashMap<u64, Status>,
    ) -> Result<(), StoreError> {
        let log = self.log.clone();
        let mut to_remove = vec![];
        if let Some(segment) = self.segment_file_of(offset) {
            if -1 == result {
                error!(log, "Failed to `{}` {}", segment.status, segment.path);
                return Err(StoreError::System(result));
            }
            match segment.status {
                Status::OpenAt => {
                    info!(
                        log,
                        "LogSegmentFile: `{}` is created and open with FD: {}",
                        segment.path,
                        result
                    );
                    segment.fd = Some(result);
                    segment.status = Status::Fallocate64;

                    info!(
                        log,
                        "About to fallocate LogSegmentFile: `{}` with FD: {}", segment.path, result
                    );
                    let sqe = opcode::Fallocate64::new(types::Fd(result), segment.size as i64)
                        .offset(0)
                        .mode(0)
                        .build()
                        .user_data(offset);
                    unsafe {
                        self.control_ring.submission().push(&sqe).map_err(|e| {
                            error!(
                                log,
                                "Failed to submit Fallocate SQE to io_uring SQ: {:?}", e
                            );
                            StoreError::IoUring
                        })
                    }?;
                    op_table.insert(offset, Status::Fallocate64);
                }
                Status::Fallocate64 => {
                    info!(
                        log,
                        "Fallocate of LogSegmentFile `{}` completed", segment.path
                    );
                    segment.status = Status::ReadWrite;
                }
                Status::Close => {
                    info!(log, "LogSegmentFile: `{}` is closed", segment.path);
                    segment.fd = None;

                    info!(log, "About to delete LogSegmentFile `{}`", segment.path);
                    let sqe = opcode::UnlinkAt::new(
                        types::Fd(libc::AT_FDCWD),
                        segment.path.as_ptr() as *const i8,
                    )
                    .build()
                    .flags(squeue::Flags::empty())
                    .user_data(offset);
                    unsafe {
                        self.control_ring.submission().push(&sqe).map_err(|e| {
                            error!(log, "Failed to push Unlink SQE to SQ: {:?}", e);
                            StoreError::IoUring
                        })
                    }?;
                }
                Status::UnlinkAt => {
                    info!(log, "LogSegmentFile: `{}` is deleted", segment.path);
                    to_remove.push(offset)
                }
                _ => {}
            };
        }

        // It's OK to submit 0 entry.
        self.control_ring.submit().map_err(|e| {
            error!(log, "Failed to submit SQEs to SQ: {:?}", e);
            StoreError::IoUring
        })?;

        self.delete_segments(to_remove);

        Ok(())
    }

    fn async_fallocate(&mut self, file_op: &mut HashMap<u64, Status>) -> Result<(), StoreError> {
        // Map of segment offset to syscall result
        let mut m = HashMap::new();
        {
            let mut cq = self.control_ring.completion();
            loop {
                for cqe in cq.by_ref() {
                    m.insert(cqe.user_data(), cqe.result());
                }
                cq.sync();
                if cq.is_empty() {
                    break;
                }
            }
        }

        m.into_iter()
            .flat_map(|(offset, result)| {
                if file_op.remove(&offset).is_none() {
                    error!(
                        self.log,
                        "`file_op` map should have a record for log segment with offset: {}",
                        offset
                    );
                    return Err(StoreError::Internal(
                        "file_op misses expected in-progress offset-status entry".to_owned(),
                    ));
                }
                self.on_file_op_completion(offset, result, file_op)
            })
            .count();

        Ok(())
    }

    fn async_close(&mut self, file_op: &mut HashMap<u64, Status>) -> Result<(), StoreError> {
        let to_close: HashMap<_, _> = self
            .segments
            .iter()
            .take_while(|segment| segment.status == Status::Close)
            .filter(|segment| !file_op.contains_key(&segment.offset))
            .map(|segment| {
                info!(self.log, "About to close LogSegmentFile: {}", segment.path);
                (segment.offset, segment.fd)
            })
            .collect();

        for (offset, fd) in to_close {
            if let Some(fd) = fd {
                let sqe = opcode::Close::new(types::Fd(fd)).build().user_data(offset);
                unsafe {
                    self.control_ring.submission().push(&sqe).map_err(|e| {
                        error!(self.log, "Failed to submit close SQE to SQ: {:?}", e);
                        StoreError::IoUring
                    })
                }?;
            }
        }

        self.control_ring.submit().map_err(|e| {
            error!(self.log, "io_uring_enter failed when submit: {:?}", e);
            StoreError::IoUring
        })?;

        Ok(())
    }

    pub(crate) fn run(io: RefCell<IO>) -> Result<(), StoreError> {
        let log = io.borrow().log.clone();

        let indexer = Indexer::new();
        io.borrow_mut().load()?;
        let pos = indexer.flushed_wal_offset();
        let mut pos = io.borrow_mut().recover(pos)?;

        let alignment = io
            .borrow()
            .options
            .wal_paths
            .first()
            .ok_or(StoreError::Configuration(String::from(
                "Need at least one WAL path",
            )))?
            .block_size;

        let io_depth = io.borrow().data_ring.params().sq_entries();
        let min_preallocated_segment_files = io.borrow().options.min_preallocated_segment_files;

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

        // Preallocate a log segment file according to pos
        {
            // TODO: if pre-allocated file num is less than configured
            let mut io_mut = io.borrow_mut();
            let segment = io_mut.alloc_segment()?;
            segment.open()?;
        }

        // Mapping between tag to OpState
        let mut inflight_data_tasks = HashMap::new();
        let mut tag: u64 = 0;

        let cqe_wanted = 1;

        // Mapping between segment offset to file operation `Status`
        //
        // File `Status` migration road-map: OpenAt --> Fallocate --> ReadWrite -> Read -> Close -> Unlink.
        // Once the status of segment is driven to ReadWrite,
        // this mapping should be removed.
        let mut inflight_control_tasks: HashMap<u64, Status> = HashMap::new();

        let mut pending_tasks = VecDeque::new();

        // Main loop
        loop {
            // Check if we need to create a new log segment
            loop {
                if io.borrow().writable_segment_count() >= min_preallocated_segment_files + 1 {
                    break;
                }
                io.borrow_mut().async_open(&mut inflight_control_tasks)?;
            }

            // check if we have expired log segments to close and delete
            {
                io.borrow_mut().async_close(&mut inflight_control_tasks)?;
            }

            let mut entries = vec![];

            {
                let mut io_mut = io.borrow_mut();

                // Receive IO tasks from channel
                io_mut.receive_io_tasks(&mut inflight, io_depth, &mut pending_tasks);
                trace!(
                    log,
                    "Received {} IO requests from channel",
                    pending_tasks.len()
                );

                // Convert IO tasks into io_uring entries
                io_mut.convert_task_to_sqe(
                    &mut pending_tasks,
                    &mut tag,
                    &mut pos,
                    alignment,
                    &mut inflight_data_tasks,
                    &mut entries,
                );
            }

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
                        .data_ring
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
            } else if !pending_tasks.is_empty() && !inflight_control_tasks.is_empty() {
                let now = std::time::Instant::now();
                match io.borrow().control_ring.submit_and_wait(1) {
                    Ok(_) => {
                        warn!(
                            log,
                            "Waiting {}us for control plane file system operation",
                            now.elapsed().as_micros()
                        );
                    }
                    Err(e) => {
                        error!(log, "io_uring_enter got an error: {:?}", e);

                        // Fatal errors, crash the process and let watchdog to restart.
                        panic!("io_uring_enter returns error {:?}", e);
                    }
                };
            }

            // Wait complete asynchronous IO
            trace!(
                log,
                "Waiting for at least {}/{} CQE(s) to reap",
                cqe_wanted,
                inflight
            );
            let now = std::time::Instant::now();
            match io.borrow().data_ring.submit_and_wait(cqe_wanted) {
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
                let mut completion = io_mut.data_ring.completion();
                let prev = inflight;
                loop {
                    for cqe in completion.by_ref() {
                        inflight -= 1;
                        let tag = cqe.user_data();
                        if let Some(state) = inflight_data_tasks.remove(&tag) {
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

            // Perform file operation
            {
                if !inflight_control_tasks.is_empty() {
                    io.borrow_mut()
                        .async_fallocate(&mut inflight_control_tasks)?;
                }
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
        self.data_ring.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{HashMap, VecDeque};
    use std::error::Error;
    use std::fs::File;

    use bytes::BytesMut;
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
    fn test_segment_file_of() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir.as_path());
        let mut io = create_io(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;
        io.alloc_segment()?;
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

        let mut tasks = VecDeque::new();
        io.receive_io_tasks(&mut in_flight_requests, io_depth, &mut tasks);
        assert_eq!(16, tasks.len());
        tasks.clear();

        drop(sender);

        // Mock that some in-flight IO tasks were reaped
        in_flight_requests = 8;

        io.receive_io_tasks(&mut in_flight_requests, io_depth, &mut tasks);

        assert_eq!(true, tasks.is_empty());
        assert_eq!(true, io.channel_disconnected);

        Ok(())
    }

    #[test]
    fn test_to_sqe() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir.as_path());
        let mut io = create_io(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;

        let segment = io.alloc_segment().unwrap();
        segment.open()?;

        let buffer = BytesMut::with_capacity(128);
        let buffer = buffer.freeze();

        // Send IoTask to channel
        let mut tasks: VecDeque<_> = (0..16)
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

        let mut entries = Vec::new();

        io.convert_task_to_sqe(
            &mut tasks,
            &mut tag,
            &mut pos,
            alignment,
            &mut inflight_tasks,
            &mut entries,
        );
        assert_eq!(16, entries.len());
        Ok(())
    }

    #[test]
    fn test_delete_segments() -> Result<(), Box<dyn Error>> {
        let wal_dir = random_wal_dir()?;
        let _wal_dir_guard = util::DirectoryRemovalGuard::new(wal_dir.as_path());
        let mut io = create_io(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;
        io.alloc_segment()?;
        assert_eq!(1, io.segments.len());
        let offsets = io
            .segments
            .iter_mut()
            .map(|segment| {
                segment.status = Status::UnlinkAt;
                segment.offset
            })
            .collect();
        io.delete_segments(offsets);
        assert_eq!(0, io.segments.len());
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
