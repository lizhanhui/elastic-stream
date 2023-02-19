use std::{
    fs::File,
    os::{
        fd::{FromRawFd, IntoRawFd, RawFd},
        unix::prelude::OpenOptionsExt,
    },
    path::Path,
    time::SystemTime,
};

use crate::error::StoreError;

/// Write-ahead-log segment file status.
///
/// `Status` indicates the opcode allowed on it.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Status {
    OpenAt,
    Fallocate64,
    ReadWrite,
    Read,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Medium {
    SSD,
    HDD,
    S3,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TimeRange {
    pub(crate) begin: SystemTime,
    pub(crate) end: Option<SystemTime>,
}

impl TimeRange {
    pub(crate) fn new(begin: SystemTime) -> Self {
        Self { begin, end: None }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LogSegmentFile {
    pub(crate) path: String,
    pub(crate) size: u64,
    pub(crate) medium: Medium,
    pub(crate) status: Status,

    // Use File or RawFd?
    pub(crate) fd: Option<RawFd>,

    /// Position where this log segment file has been written.
    pub(crate) written: u32,

    pub(crate) time_range: Option<TimeRange>,
}

impl LogSegmentFile {
    pub(crate) fn new(path: &str, size: u64, medium: Medium) -> Self {
        Self {
            path: path.to_owned(),
            size,
            medium,
            status: Status::OpenAt,
            fd: None,
            written: 0,
            time_range: None,
        }
    }

    pub(crate) fn open(&mut self) -> Result<(), StoreError> {
        if self.fd.is_some() {
            return Ok(());
        }

        // Open the file for direct read/write
        let mut opts = std::fs::OpenOptions::new();
        let file = opts
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(Path::new(&self.path))?;
        let metadata = file.metadata()?;
        self.size = metadata.len();
        if 0 == self.size {
            self.status = Status::Fallocate64;
        } else {
            self.status = Status::Read;
        }
        self.fd = Some(file.into_raw_fd());

        // Read time_range from meta-blocks

        Ok(())
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
