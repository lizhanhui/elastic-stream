use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    fmt::Display,
    fs::{File, OpenOptions},
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
        unix::prelude::{FileExt, OpenOptionsExt},
    },
    path::Path,
    time::SystemTime,
};

use bytes::{BufMut, BytesMut};
use derivative::Derivative;
use io_uring::{opcode, squeue, types};
use nix::fcntl;
use slog::{debug, error, info, trace, warn, Logger};

use crate::{
    error::StoreError,
    io::{record::RecordType, CRC32C},
    option::WalPath,
};

use super::{block_cache::BlockCache, buf::AlignedBufWriter, record::RECORD_PREFIX_LENGTH};

// CRC(4B) + length(3B) + Type(1B) + earliest_record_time(8B) + latest_record_time(8B)
pub(crate) const FOOTER_LENGTH: u64 = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TimeRange {
    pub(crate) begin: SystemTime,
    pub(crate) end: Option<SystemTime>,
}

impl TimeRange {
    pub(crate) fn new(begin: SystemTime) -> Self {
        Self { begin, end: None }
    }
}

/// `LogSegment` consists of fixed length blocks, which are groups of variable-length records.
///
/// The writer writes and reader reads in chunks of blocks. `BlockSize` are normally multiple of the
/// underlying storage block size, which is medium and cloud-vendor specific. By default, `BlockSize` is
/// `256KiB`.
#[derive(Derivative)]
#[derivative(PartialEq, Eq, Debug)]
pub(crate) struct LogSegment {
    /// Log segment offset in bytes, it's the absolute offset in the whole WAL.
    pub(crate) offset: u64,

    /// Fixed log segment file size
    /// offset + size = next log segment start offset
    pub(crate) size: u64,

    /// Position where this log segment has been written.
    /// It's a relative position in this log segment.
    pub(crate) written: u64,

    /// Consists of the earliest record time and the latest record time.
    pub(crate) time_range: Option<TimeRange>,

    /// The block cache layer on top of the log segment, is used to
    /// cache the most recently read or write blocks depending on the cache strategy.
    #[derivative(PartialEq = "ignore")]
    pub(crate) block_cache: BlockCache,

    /// The status of the log segment.
    pub(crate) status: Status,

    /// The path of the log segment, if the fd is a file descriptor.
    pub(crate) path: String,

    /// The underlying descriptor of the log segment.
    /// Currently, it's a file descriptor with `O_DIRECT` flag.
    pub(crate) sd: Option<SegmentDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentDescriptor {
    /// The underlying storage medium of the log segment.
    pub(crate) medium: Medium,

    /// The raw file descriptor of the log segment.
    /// It's a file descriptor or a block device descriptor.
    pub(crate) fd: RawFd,

    /// The base address of the log segment, always zero if the fd is a file descriptor.
    pub(crate) base_ptr: u64,
}

/// Write-ahead-log segment file status.
///
/// `Status` indicates the opcode allowed on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    // Once a `LogSegmentFile` is constructed, there is no file in fs yet.
    // Need to `open` with `O_CREATE` flag.
    OpenAt,

    // Given `LogSegmentFile` are fixed in length, it would accelerate IO performance if space is pre-allocated.
    Fallocate64,

    // Now the segment file is ready for read/write.
    ReadWrite,

    // Once the segment file is full, it turns immutable, thus, read-only.
    Read,

    // When data in the segment file expires and ref-count turns `0`, FD shall be closed and the file deleted.
    Close,

    // Delete the file to reclaim disk space.
    UnlinkAt,
}

/// A WAL contains a list of log segments, and supports open, close, alloc, and other operations.
pub(crate) struct WAL {
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

// TODO: a better display format is needed.
impl Display for LogSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LogSegment {{ offset: {}, size: {}, written: {}, time_range: {:?} }}",
            self.offset, self.size, self.written, self.time_range
        )
    }
}

impl Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::OpenAt => {
                write!(f, "open")
            }
            Self::Fallocate64 => {
                write!(f, "fallocate")
            }
            Self::ReadWrite => {
                write!(f, "read/write")
            }
            Self::Read => {
                write!(f, "read")
            }
            Self::Close => {
                write!(f, "close")
            }
            Self::UnlinkAt => {
                write!(f, "unlink")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Medium {
    Ssd,
    Hdd,
    S3,
}

impl LogSegment {
    pub(crate) fn new(offset: u64, size: u64, path: String) -> Self {
        Self {
            offset,
            size,
            written: 0,
            time_range: None,
            block_cache: BlockCache::new(offset, 4096),
            sd: None,
            status: Status::OpenAt,
            path,
        }
    }

    pub(crate) fn format(offset: u64) -> String {
        format!("{:0>20}", offset)
    }

    pub(crate) fn parse_offset(path: &Path) -> Option<u64> {
        let file_name = path.file_name()?;
        let file_name = file_name.to_str()?;
        file_name.parse::<u64>().ok()
    }

    pub(crate) fn open(&mut self) -> Result<(), StoreError> {
        if self.sd.is_some() {
            return Ok(());
        }

        // Open the file for direct read/write
        let mut opts = OpenOptions::new();
        let file = opts
            .create(true)
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(Path::new(&self.path))?;
        let metadata = file.metadata()?;

        let mut sd_status = Status::OpenAt;

        if self.size != metadata.len() {
            debug_assert!(0 == metadata.len(), "LogSegmentFile is corrupted");
            fcntl::fallocate(
                file.as_raw_fd(),
                fcntl::FallocateFlags::empty(),
                0,
                self.size as libc::off_t,
            )
            .map_err(|errno| StoreError::System(errno as i32))?;
            sd_status = Status::ReadWrite;
        } else {
            // We assume the log segment file is read-only. The recovery/apply procedure would update status accordingly.
            sd_status = Status::Read;
        }

        self.sd = Some(SegmentDescriptor {
            medium: Medium::Ssd,
            fd: file.into_raw_fd(),
            base_ptr: 0,
        });

        // TODO: read time_range from meta-blocks

        Ok(())
    }

    pub fn is_full(&self) -> bool {
        self.written >= self.size
    }

    pub(crate) fn can_hold(&self, payload_length: u64) -> bool {
        self.status != Status::Read
            && self.written + RECORD_PREFIX_LENGTH + payload_length + FOOTER_LENGTH <= self.size
    }

    pub(crate) fn append_footer(
        &mut self,
        writer: &mut AlignedBufWriter,
    ) -> Result<(), StoreError> {
        let padding_length = self.size - self.written - RECORD_PREFIX_LENGTH - 8 - 8;
        let length_type: u32 = RecordType::Zero.with_length(padding_length as u32 + 8 + 8);
        let earliest: u64 = 0;
        let latest: u64 = 0;

        // Fill padding with 0

        let mut buf = BytesMut::with_capacity(padding_length as usize + 16);
        if padding_length > 0 {
            buf.resize(padding_length as usize, 0);
        }
        buf.put_u64(earliest);
        buf.put_u64(latest);
        let buf = buf.freeze();
        let crc: u32 = CRC32C.checksum(&buf[..]);
        writer.write_u32(crc)?;
        writer.write_u32(length_type)?;
        writer.write(&buf[..=padding_length as usize])?;

        Ok(())
    }

    pub(crate) fn remaining(&self) -> u64 {
        if Status::ReadWrite != self.status {
            return 0;
        } else {
            debug_assert!(self.size >= self.written);
            return self.size - self.written;
        }
    }

    pub(crate) fn append_full_record(
        &self,
        writer: &mut AlignedBufWriter,
        payload: &[u8],
    ) -> Result<(), StoreError> {
        let crc = CRC32C.checksum(payload);
        let length_type = RecordType::Full.with_length(payload.len() as u32);
        writer.write_u32(crc)?;
        writer.write_u32(length_type)?;
        writer.write(payload)?;
        Ok(())
    }
}

impl Drop for LogSegment {
    fn drop(&mut self) {
        // Close these open FD
        if let Some(sd) = self.sd.as_ref() {
            let _file = unsafe { File::from_raw_fd(sd.fd) };
        }
    }
}

impl PartialOrd for LogSegment {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let this = Path::new(&self.path);
        let that = Path::new(&other.path);
        let lhs = match this.file_name() {
            Some(name) => name,
            None => return None,
        };
        let rhs = match that.file_name() {
            Some(name) => name,
            None => return None,
        };

        lhs.partial_cmp(rhs)
    }
}

impl Ord for LogSegment {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.partial_cmp(other) {
            Some(res) => res,
            None => {
                unreachable!("Should not reach here");
            }
        }
    }
}

impl WAL {
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
                            let log_segment_file = LogSegment::new(
                                offset,
                                metadata.len(),
                                entry.path().to_str()?.to_string(),
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

    pub(crate) fn recover(&mut self, offset: u64) -> Result<u64, StoreError> {
        let mut pos = offset;
        let log = self.log.clone();
        info!(log, "Start to recover WAL segment files");
        let mut need_scan = true;
        for segment in self.segments.iter_mut() {
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

    pub(crate) fn try_open(&mut self) -> Result<(), StoreError> {
        let log = self.log.clone();
        let segment = self.alloc_segment()?;
        let offset = segment.offset;
        debug_assert_eq!(segment.status, Status::OpenAt);
        info!(log, "About to create/open LogSegmentFile: `{}`", segment);
        let status = segment.status;
        let ptr = segment.path.as_ptr() as *const i8;
        self.inflight_control_tasks.insert(offset, status);
        let sqe = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), ptr)
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
                info!(self.log, "About to close LogSegmentFile: {}", segment.path);
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
        let segment = LogSegment::new(
            offset,
            self.file_size,
            path.to_str()
                .ok_or(StoreError::AllocLogSegment)?
                .to_string(),
        );

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
            .filter(|segment| !segment.is_full())
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
                    segment.sd = Some(SegmentDescriptor {
                        medium: Medium::Ssd,
                        fd: result,
                        base_ptr: 0,
                    });
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
                    self.inflight_control_tasks
                        .insert(offset, Status::Fallocate64);
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
                    segment.sd = None;

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
}

#[cfg(test)]
mod tests {
    use super::LogSegment;

    #[test]
    fn test_format_number() {
        assert_eq!(
            LogSegment::format(0).len(),
            LogSegment::format(std::u64::MAX).len()
        );
    }
}
