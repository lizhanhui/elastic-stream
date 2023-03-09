use bytes::{BufMut, BytesMut};
use derivative::Derivative;
use nix::fcntl;
use slog::Logger;
use std::{
    cmp::Ordering,
    ffi::CString,
    fmt::{self, Display},
    fs::{File, OpenOptions},
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
        unix::prelude::{OpenOptionsExt, OsStrExt},
    },
    path::Path,
    time::SystemTime,
};

use crate::{error::StoreError, io::record::RecordType};

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
    pub(crate) path: CString,

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

// TODO: a better display format is needed.
impl Display for LogSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = self.path.to_str().map_err(|_| fmt::Error)?;
        write!(
            f,
            "LogSegment {{ path: {}, offset: {}, size: {}, written: {}, time_range: {:?} }}",
            path, self.offset, self.size, self.written, self.time_range
        )
    }
}

impl Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::OpenAt => {
                write!(f, "open")
            }
            Self::Fallocate64 => {
                write!(f, "fallocate")
            }
            Self::ReadWrite => {
                write!(f, "read|write")
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
    pub(crate) fn new(
        log: Logger,
        offset: u64,
        size: u64,
        path: &Path,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            offset,
            size,
            written: 0,
            time_range: None,
            block_cache: BlockCache::new(log, offset),
            sd: None,
            status: Status::OpenAt,
            path: CString::new(path.as_os_str().as_bytes())
                .map_err(|e| StoreError::InvalidPath(e.to_string()))?,
        })
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
            .custom_flags(libc::O_RDWR | libc::O_DIRECT | libc::O_DSYNC)
            .open(Path::new(
                self.path
                    .to_str()
                    .map_err(|e| StoreError::InvalidPath(e.to_string()))?,
            ))?;
        let metadata = file.metadata()?;

        let mut status = Status::OpenAt;

        if self.size != metadata.len() {
            debug_assert!(0 == metadata.len(), "LogSegmentFile is corrupted");
            fcntl::fallocate(
                file.as_raw_fd(),
                fcntl::FallocateFlags::empty(),
                0,
                self.size as libc::off_t,
            )
            .map_err(|errno| StoreError::System(errno as i32))?;
            status = Status::ReadWrite;
        } else {
            // We assume the log segment file is read-only. The recovery/apply procedure would update status accordingly.
            status = Status::Read;
        }

        self.status = status;
        self.sd = Some(SegmentDescriptor {
            medium: Medium::Ssd,
            fd: file.into_raw_fd(),
            base_ptr: 0,
        });

        // TODO: read time_range from meta-blocks

        Ok(())
    }

    pub fn writable(&self) -> bool {
        self.written < self.size && self.status == Status::ReadWrite
    }

    pub(crate) fn can_hold(&self, payload_length: u64) -> bool {
        self.status != Status::Read
            && self.written + RECORD_PREFIX_LENGTH + payload_length + FOOTER_LENGTH <= self.size
    }

    pub(crate) fn append_footer(
        &mut self,
        writer: &mut AlignedBufWriter,
    ) -> Result<u64, StoreError> {
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
        let crc: u32 = util::crc32::crc32(&buf[..]);
        writer.write_u32(crc)?;
        writer.write_u32(length_type)?;
        writer.write(&buf[..=padding_length as usize])?;

        self.written = self.size;
        Ok(self.offset + self.written)
    }

    pub(crate) fn remaining(&self) -> u64 {
        if Status::ReadWrite != self.status {
            return 0;
        } else {
            debug_assert!(self.size >= self.written);
            return self.size - self.written;
        }
    }

    pub(crate) fn append_record(
        &mut self,
        writer: &mut AlignedBufWriter,
        payload: &[u8],
    ) -> Result<u64, StoreError> {
        let crc = util::crc32::crc32(payload);
        let length_type = RecordType::Full.with_length(payload.len() as u32);
        writer.write_u32(crc)?;
        writer.write_u32(length_type)?;
        writer.write(payload)?;
        self.written += 4 + 4 + payload.len() as u64;
        Ok(self.offset + self.written)
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
        self.offset.partial_cmp(&other.offset)
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

#[cfg(test)]
mod tests {
    use crate::io::{buf::AlignedBufWriter, record::RecordType};
    use bytes::BytesMut;
    use std::error::Error;

    use super::{LogSegment, Status};

    #[test]
    fn test_format_number() {
        assert_eq!(
            LogSegment::format(0).len(),
            LogSegment::format(std::u64::MAX).len()
        );
    }

    #[test]
    fn test_append_record() -> Result<(), Box<dyn Error>> {
        let tmp = std::env::temp_dir();
        let log = test_util::terminal_logger();
        let mut segment = super::LogSegment::new(log, 0, 1024 * 1024, tmp.as_path())?;
        segment.status = Status::ReadWrite;

        let log = test_util::terminal_logger();
        let mut buf_writer = AlignedBufWriter::new(log, 0, 512);
        buf_writer.reserve(1024)?;

        let mut data = BytesMut::with_capacity(256);
        data.resize(256, 65);

        let pos = segment.append_record(&mut buf_writer, &data)?;
        assert_eq!(pos, 4 + 4 + data.len() as u64);

        let buffers = buf_writer.take();
        let buf = buffers.first().unwrap();
        let crc = buf.read_u32(0)?;
        assert_eq!(crc, util::crc32::crc32(&data));
        let length_type = buf.read_u32(4)?;
        let (len, t) = RecordType::parse(length_type)?;
        assert_eq!(t, RecordType::Full);
        assert_eq!(len as usize, data.len());

        let payload = buf.slice(8..);
        assert_eq!(&data, payload);
        Ok(())
    }
}
