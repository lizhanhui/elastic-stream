use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    os::{fd::FromRawFd, unix::prelude::FileExt},
    path::Path,
};

use crate::{
    error::StoreError,
    io::record::RecordType,
    io::segment::{LogSegment, Medium, SegmentDescriptor, Status, FOOTER_LENGTH},
    option::WalPath,
};

use io_uring::{opcode, squeue, types};
use slog::{debug, error, info, warn, Logger};

/// A WAL contains a list of log segments, and supports open, close, alloc, and other operations.
pub(crate) struct Wal {
    /// The WAL path if the log segments are on file system.
    wal_paths: Vec<WalPath>,

    /// I/O Uring instance for write-ahead-log segment file management.
    ///
    /// Unlike `data_uring`, this instance is used to open/fallocate/close/delete log segment files because these opcodes are not
    /// properly supported by the instance armed with the `IOPOLL` feature.
    control_ring: io_uring::IoUring,

    /// Mapping of on-going file operations between segment offset to file operation `Status`.
    ///
    /// File `Status` migration road-map: OpenAt --> Fallocate --> ReadWrite -> Read -> Close -> Unlink.
    /// Once the status of segment is driven to ReadWrite,
    /// this mapping should be removed.
    inflight_control_tasks: HashMap<u64, Status>,

    /// The block paths if the log segments are on block devices.
    /// The block device is not supported yet.
    block_paths: Option<String>,

    /// The container of the log segments.
    segments: VecDeque<LogSegment>,

    file_size: u64,

    /// Logger instance.
    log: Logger,
}

impl Wal {
    pub(crate) fn new(
        wal_paths: Vec<WalPath>,
        control_ring: io_uring::IoUring,
        file_size: u64,
        log: Logger,
    ) -> Self {
        Self {
            segments: VecDeque::new(),
            wal_paths,
            block_paths: None,
            log,
            control_ring,
            file_size,
            inflight_control_tasks: HashMap::new(),
        }
    }

    /// All the segments will be opened after loading from WAL.
    pub(crate) fn load_from_paths(&mut self) -> Result<(), StoreError> {
        let mut segment_files: Vec<_> = self
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
                        if let Some(offset) = LogSegment::parse_offset(path) {
                            let log_segment_file = LogSegment::new(offset, metadata.len(), path);
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
            .flatten()
            .collect();

        // Sort log segment file by file name.
        segment_files.sort();

        for mut segment_file in segment_files.into_iter() {
            segment_file.open()?;
            self.segments.push_back(segment_file);
        }

        Ok(())
    }

    pub(crate) fn segment_file_of(&mut self, offset: u64) -> Option<&mut LogSegment> {
        self.segments
            .iter_mut()
            .rev()
            .find(|segment| segment.offset <= offset && (segment.offset + segment.size > offset))
    }

    /// Return whether has reached end of the WAL
    fn scan_record(
        segment: &mut LogSegment,
        pos: &mut u64,
        log: &Logger,
    ) -> Result<bool, StoreError> {
        let mut file_pos = *pos - segment.offset;
        let sd = segment.sd.as_ref().ok_or(StoreError::NotOpened)?;

        // Open the file with the given `fd`.
        let file = unsafe { File::from_raw_fd(sd.fd) };

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
                        info!(log, "Reached EOF of {}", segment);
                    }
                    RecordType::Full => {
                        // Full record
                        buf.resize(len, 0);
                        file.read_exact_at(buf.as_mut(), file_pos)?;

                        let ckm = util::crc32::crc32(buf.as_ref());
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

    pub(crate) fn recover(&mut self, offset: u64) -> Result<u64, StoreError> {
        let mut pos = offset;
        let log = self.log.clone();
        info!(log, "Start to recover WAL segment files");
        let mut need_scan = true;
        for segment in self.segments.iter_mut() {
            if segment.offset + segment.size <= offset {
                segment.status = Status::Read;
                segment.written = segment.size;
                debug!(log, "Mark {} as read-only", segment);
                continue;
            }

            if !need_scan {
                segment.written = 0;
                segment.status = Status::ReadWrite;
                debug!(log, "Mark {} as read-write", segment);
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

    pub(crate) fn try_open(&mut self) -> Result<(), StoreError> {
        let log = self.log.clone();
        let segment = self.alloc_segment()?;
        let offset = segment.offset;
        debug_assert_eq!(segment.status, Status::OpenAt);
        info!(log, "About to create/open LogSegmentFile: `{}`", segment);
        let status = segment.status;
        self.inflight_control_tasks.insert(offset, status);
        let sqe = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), segment.path.as_ptr())
            .flags(libc::O_CREAT | libc::O_RDWR | libc::O_DIRECT | libc::O_DSYNC)
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
        self.segments.push_back(segment);
        Ok(())
    }

    pub(crate) fn try_close(&mut self) -> Result<(), StoreError> {
        let to_close: Vec<&LogSegment> = self
            .segments
            .iter()
            .take_while(|segment| segment.status == Status::Close)
            .filter(|segment| !self.inflight_control_tasks.contains_key(&segment.offset))
            .collect();

        for segment in to_close {
            if let Some(sd) = segment.sd.as_ref() {
                let sqe = opcode::Close::new(types::Fd(sd.fd))
                    .build()
                    .user_data(segment.offset);
                info!(self.log, "About to close LogSegmentFile: {}", segment);
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

    /// Open a new segment without uring
    /// Currently only used in tests
    /// TODO: a better way to handle open
    pub(crate) fn open_segment(&mut self) -> Result<(), StoreError> {
        let mut segment = self.alloc_segment()?;
        segment.open()?;
        self.segments.push_back(segment);
        Ok(())
    }

    fn alloc_segment(&mut self) -> Result<LogSegment, StoreError> {
        let offset = if self.segments.is_empty() {
            0
        } else if let Some(last) = self.segments.back() {
            last.offset + self.file_size
        } else {
            unreachable!("Should-not-reach-here")
        };
        let dir = self.wal_paths.first().ok_or(StoreError::AllocLogSegment)?;
        let path = Path::new(&dir.path);
        let path = path.join(LogSegment::format(offset));

        let segment = LogSegment::new(offset, self.file_size, path.as_path())?;

        Ok(segment)
    }

    pub(crate) fn writable_segment_count(&self) -> usize {
        self.segments
            .iter()
            .rev() // from back to front
            .take_while(|segment| segment.status != Status::Read)
            .count()
    }

    /// Delete segments when their backed file is deleted
    pub(crate) fn delete_segments(&mut self, offsets: Vec<u64>) {
        if offsets.is_empty() {
            return;
        }

        self.segments.retain(|segment| {
            !(segment.status == Status::UnlinkAt && offsets.contains(&segment.offset))
        });
    }

    pub(crate) fn control_task_num(&self) -> usize {
        self.inflight_control_tasks.len()
    }

    pub(crate) fn await_control_task_completion(&self) {
        let now = std::time::Instant::now();
        match self.control_ring.submit_and_wait(1) {
            Ok(_) => {
                info!(
                    self.log,
                    "Waiting {}us for control plane file system operation",
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

    pub(crate) fn calculate_write_buffers(&self, requirement: &mut VecDeque<usize>) -> Vec<usize> {
        let mut write_buf_list = vec![];
        let mut size = 0;
        self.segments
            .iter()
            .rev()
            .filter(|segment| segment.writable())
            .rev()
            .for_each(|segment| {
                if requirement.is_empty() {
                    return;
                }

                let remaining = segment.remaining() as usize;
                while let Some(n) = requirement.front() {
                    if size + n + FOOTER_LENGTH as usize > remaining {
                        write_buf_list.push(remaining);
                        size = 0;
                        return;
                    } else {
                        size += n;
                        requirement.pop_front();
                    }
                }

                if size > 0 {
                    write_buf_list.push(size);
                    size = 0;
                }
            });
        write_buf_list
    }

    pub(crate) fn reap_control_tasks(&mut self) -> Result<(), StoreError> {
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
                if self.inflight_control_tasks.remove(&offset).is_none() {
                    error!(
                        self.log,
                        "`file_op` map should have a record for log segment with offset: {}",
                        offset
                    );
                    return Err(StoreError::Internal(
                        "file_op misses expected in-progress offset-status entry".to_owned(),
                    ));
                }
                self.on_file_op_completion(offset, result)
            })
            .count();

        Ok(())
    }

    fn on_file_op_completion(&mut self, offset: u64, result: i32) -> Result<(), StoreError> {
        let log = self.log.clone();
        let mut to_remove = vec![];
        if let Some(segment) = self.segment_file_of(offset) {
            if -1 == result {
                error!(log, "LogSegment file operation failed: {}", segment);
                return Err(StoreError::System(result));
            }
            match segment.status {
                Status::OpenAt => {
                    info!(
                        log,
                        "LogSegmentFile: `{}` is created and open with FD: {}", segment, result
                    );
                    segment.sd = Some(SegmentDescriptor {
                        medium: Medium::Ssd,
                        fd: result,
                        base_ptr: 0,
                    });
                    segment.status = Status::Fallocate64;

                    info!(
                        log,
                        "About to fallocate LogSegmentFile: `{}` with FD: {}", segment, result
                    );
                    let sqe = opcode::Fallocate64::new(types::Fd(result), segment.size as i64)
                        .offset(0)
                        .mode(libc::FALLOC_FL_ZERO_RANGE)
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
                    self.inflight_control_tasks
                        .insert(offset, Status::Fallocate64);
                }
                Status::Fallocate64 => {
                    info!(log, "Fallocate of LogSegmentFile `{}` completed", segment);
                    segment.status = Status::ReadWrite;
                }
                Status::Close => {
                    info!(log, "LogSegmentFile: `{}` is closed", segment);
                    segment.sd = None;

                    info!(log, "About to delete LogSegmentFile `{}`", segment);
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
                    info!(log, "LogSegmentFile: `{}` is deleted", segment);
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
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::error::Error;
    use std::fs::File;
    use std::path::PathBuf;

    use bytes::BytesMut;
    use slog::error;
    use tokio::sync::oneshot;

    use crate::error::StoreError;
    use crate::io::{
        options::DEFAULT_LOG_SEGMENT_FILE_SIZE,
        segment::{LogSegment, Status},
        task::{IoTask, WriteTask},
        Options,
    };
    use crate::option::WalPath;

    use super::Wal;

    fn create_wal(wal_dir: WalPath) -> Result<Wal, StoreError> {
        let logger = test_util::terminal_logger();
        let control_ring = io_uring::IoUring::builder().dontfork().build(32).map_err(|e| {
            error!(logger, "Failed to build I/O Uring instance for write-ahead-log segment file management: {:#?}", e);
            StoreError::IoUring
        })?;

        let mut options = Options::default();
        options.add_wal_path(wal_dir);

        Ok(Wal::new(
            options.wal_paths,
            control_ring,
            options.file_size,
            logger,
        ))
    }

    fn random_wal_dir() -> Result<PathBuf, StoreError> {
        test_util::create_random_path().map_err(|e| StoreError::IO(e))
    }

    #[test]
    fn test_load_wals() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        // Prepare log segment files
        let files: Vec<_> = (0..10)
            .into_iter()
            .map(|i| {
                let f = wal_dir.join(LogSegment::format(i * 100));
                File::create(f.as_path())
            })
            .flatten()
            .collect();
        assert_eq!(10, files.len());

        let log = test_util::terminal_logger();
        let _wal_dir_guard = test_util::DirectoryRemovalGuard::new(log, wal_dir.as_path());
        let wal_dir = super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?;

        let mut wal = create_wal(wal_dir)?;
        wal.load_from_paths()?;
        assert_eq!(files.len(), wal.segments.len());
        Ok(())
    }

    #[test]
    fn test_alloc_segment() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        let log = test_util::terminal_logger();
        let _wal_dir_guard = test_util::DirectoryRemovalGuard::new(log, wal_dir.as_path());
        let mut wal = create_wal(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;
        let segment = wal.alloc_segment()?;
        assert_eq!(0, segment.offset);
        wal.segments.push_back(segment);

        let segment = wal.alloc_segment()?;
        assert_eq!(DEFAULT_LOG_SEGMENT_FILE_SIZE, segment.offset);
        Ok(())
    }

    #[test]
    fn test_writable_segment_count() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        let log = test_util::terminal_logger();
        let _wal_dir_guard = test_util::DirectoryRemovalGuard::new(log, wal_dir.as_path());
        let mut wal = create_wal(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;
        let segment = wal.alloc_segment()?;
        wal.segments.push_back(segment);
        assert_eq!(1, wal.writable_segment_count());
        let segment = wal.alloc_segment()?;
        wal.segments.push_back(segment);
        assert_eq!(2, wal.writable_segment_count());

        wal.segments.front_mut().unwrap().status = Status::Read;
        assert_eq!(1, wal.writable_segment_count());

        Ok(())
    }

    #[test]
    fn test_segment_file_of() -> Result<(), StoreError> {
        let wal_dir = random_wal_dir()?;
        let log = test_util::terminal_logger();
        let _wal_dir_guard = test_util::DirectoryRemovalGuard::new(log, wal_dir.as_path());
        let mut wal = create_wal(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;
        let file_size = wal.file_size;
        let segment = wal.alloc_segment()?;
        wal.segments.push_back(segment);
        let segment = wal.alloc_segment()?;
        wal.segments.push_back(segment);
        assert_eq!(2, wal.segments.len());

        // Ensure we can get the right
        let segment = wal
            .segment_file_of(wal.file_size - 1)
            .ok_or(StoreError::AllocLogSegment)?;
        assert_eq!(0, segment.offset);

        let segment = wal.segment_file_of(0).ok_or(StoreError::AllocLogSegment)?;
        assert_eq!(0, segment.offset);

        let segment = wal
            .segment_file_of(wal.file_size)
            .ok_or(StoreError::AllocLogSegment)?;

        assert_eq!(file_size, segment.offset);

        Ok(())
    }

    #[test]
    fn test_delete_segments() -> Result<(), Box<dyn Error>> {
        let wal_dir = random_wal_dir()?;
        let log = test_util::terminal_logger();
        let _wal_dir_guard = test_util::DirectoryRemovalGuard::new(log, wal_dir.as_path());
        let mut wal = create_wal(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;
        let segment = wal.alloc_segment()?;
        wal.segments.push_back(segment);
        assert_eq!(1, wal.segments.len());
        let offsets = wal
            .segments
            .iter_mut()
            .map(|segment| {
                segment.status = Status::UnlinkAt;
                segment.offset
            })
            .collect();
        wal.delete_segments(offsets);
        assert_eq!(0, wal.segments.len());
        Ok(())
    }

    #[test]
    fn test_calculate_write_buffers() -> Result<(), StoreError> {
        let log = test_util::terminal_logger();
        let wal_dir = random_wal_dir()?;
        let _wal_dir_guard = test_util::DirectoryRemovalGuard::new(log, wal_dir.as_path());
        let mut wal = create_wal(super::WalPath::new(wal_dir.to_str().unwrap(), 1234)?)?;
        (0..3)
            .into_iter()
            .map(|_| wal.open_segment().err())
            .flatten()
            .count();
        wal.segments.iter_mut().for_each(|segment| {
            segment.status = Status::ReadWrite;
        });

        let len = 4096;
        let mut buf = BytesMut::with_capacity(len);
        buf.resize(len, 65);
        let buf = buf.freeze();

        let mut pending_data_tasks: VecDeque<IoTask> = VecDeque::new();
        (0..16).into_iter().for_each(|i| {
            let (tx, _rx) = oneshot::channel();
            pending_data_tasks.push_back(IoTask::Write(WriteTask {
                stream_id: 0,
                offset: i,
                buffer: buf.clone(),
                observer: tx,
            }));
        });

        let requirement: VecDeque<_> = pending_data_tasks
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

        // Case when there are multiple writable log segment files
        let buffers = wal.calculate_write_buffers(&mut requirement.clone());
        assert_eq!(1, buffers.len());
        assert_eq!(Some(&65664), buffers.first());

        // Case when the remaining of the first writable segment file can hold a record
        let segment = wal.segments.front_mut().unwrap();
        segment.written = segment.size - 4096 - 8 - crate::io::segment::FOOTER_LENGTH;
        let buffers = wal.calculate_write_buffers(&mut requirement.clone());
        assert_eq!(2, buffers.len());
        assert_eq!(Some(&4128), buffers.first());
        assert_eq!(Some(&61560), buffers.iter().nth(1));

        // Case when the last writable log segment file cannot hold a record
        let segment = wal.segments.front_mut().unwrap();
        segment.written = segment.size - 4096 - 4;
        let buffers = wal.calculate_write_buffers(&mut requirement.clone());
        assert_eq!(2, buffers.len());
        assert_eq!(Some(&4100), buffers.first());
        assert_eq!(Some(&65664), buffers.iter().nth(1));

        // Case when the is only one writable segment file and it cannot hold all records
        wal.segments.iter_mut().for_each(|segment| {
            segment.status = Status::Read;
            segment.written = segment.size;
        });
        let segment = wal.segments.back_mut().unwrap();
        segment.status = Status::ReadWrite;
        segment.written = segment.size - 4096;

        let buffers = wal.calculate_write_buffers(&mut requirement.clone());
        assert_eq!(1, buffers.len());
        assert_eq!(Some(&4096), buffers.first());

        Ok(())
    }
}
