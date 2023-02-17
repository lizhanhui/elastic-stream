use std::{os::fd::RawFd, time::SystemTime};

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
    pub(crate) size: u32,
    pub(crate) medium: Medium,
    pub(crate) status: Status,
    pub(crate) fd: Option<RawFd>,

    /// Position where this log segment file has been written.
    pub(crate) written: u32,

    pub(crate) time_range: Option<TimeRange>,
}

impl LogSegmentFile {
    pub(crate) fn new(path: &str, size: u32, medium: Medium) -> Self {
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
}
