use std::{
    cmp::Ordering,
    fmt::Display,
    fs::{File, OpenOptions},
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
        unix::prelude::OpenOptionsExt,
    },
    path::Path,
    time::SystemTime,
};

use nix::fcntl;

use crate::error::StoreError;

// magic_code(4bytes) + earliest_record_time(8bytes) + latest_record_time(8bytes)
const FOOTER_LEN: u64 = 20;

/// Write-ahead-log segment file status.
///
/// `Status` indicates the opcode allowed on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    // Once a `LogSegmentFile` is constructed, there is no file in fs yet.
    // Need to `open` with `O_CREAT` flag.
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

/// `LogSegmentFile` consists of fixed length blocks, which are groups of variable-length records.
///
/// The writer writes and reader reads in chunks of blocks. `BlockSize` are normally multiple of the
/// underlying storage block size, which is medium and cloud-vendor specific. By default, `BlockSize` is
/// `256KiB`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogSegmentFile {
    pub(crate) offset: u64,
    pub(crate) path: String,

    /// Fixed log segment file size
    pub(crate) size: u64,

    pub(crate) medium: Medium,
    pub(crate) status: Status,

    // Use File or RawFd?
    pub(crate) fd: Option<RawFd>,

    /// Position where this log segment file has been written.
    pub(crate) written: u64,

    pub(crate) time_range: Option<TimeRange>,
}

impl LogSegmentFile {
    pub(crate) fn new(offset: u64, path: &str, size: u64, medium: Medium) -> Self {
        Self {
            offset,
            path: path.to_owned(),
            size,
            medium,
            status: Status::OpenAt,
            fd: None,
            written: 0,
            time_range: None,
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

    pub(crate) fn with_offset(
        wal_dir: &Path,
        offset: u64,
        file_size: u64,
    ) -> Option<LogSegmentFile> {
        let file_path = wal_dir.join(Self::format(offset));
        Some(LogSegmentFile::new(
            offset,
            file_path.to_str()?,
            file_size,
            Medium::Ssd,
        ))
    }

    pub(crate) fn open(&mut self) -> Result<(), StoreError> {
        if self.fd.is_some() {
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

        if self.size != metadata.len() {
            debug_assert!(0 == metadata.len(), "LogSegmentFile is corrupted");
            fcntl::fallocate(
                file.as_raw_fd(),
                fcntl::FallocateFlags::empty(),
                0,
                self.size as libc::off_t,
            )
            .map_err(|errno| StoreError::System(errno as i32))?;
            self.status = Status::ReadWrite;
        } else {
            // We assume the log segment file is read-only. The recovery/apply procedure would update status accordingly.
            self.status = Status::Read;
        }
        self.fd = Some(file.into_raw_fd());

        // Read time_range from meta-blocks

        Ok(())
    }

    pub fn is_full(&self) -> bool {
        self.written >= self.size
    }

    pub(crate) fn can_hold(&self, len: u64) -> bool {
        self.status != Status::Read && self.written + FOOTER_LEN + len <= self.size
    }

    pub fn append_footer(&mut self, pos: &mut u64) {
        *pos += self.size - self.written;
        self.written = self.size;
        todo!("Padding footer");
    }
}

impl Drop for LogSegmentFile {
    fn drop(&mut self) {
        // Close these open FD
        if let Some(fd) = self.fd {
            let _file = unsafe { File::from_raw_fd(fd) };
        }
    }
}

impl PartialOrd for LogSegmentFile {
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

impl Ord for LogSegmentFile {
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
    use super::LogSegmentFile;

    #[test]
    fn test_format_number() {
        assert_eq!(
            LogSegmentFile::format(0).len(),
            LogSegmentFile::format(std::u64::MAX).len()
        );
    }
}
