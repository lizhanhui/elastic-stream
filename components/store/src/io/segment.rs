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
    path: String,
    size: u32,
    pub(crate) status: Status,
    pub(crate) fd: Option<RawFd>,
    pub(crate) time_range: Option<TimeRange>,
}

impl LogSegmentFile {
    pub(crate) fn new(path: &str, size: u32) -> Self {
        Self {
            path: path.to_owned(),
            size,
            status: Status::OpenAt,
            fd: None,
            time_range: None,
        }
    }

    pub(crate) fn set_status(&mut self, status: Status) {
        self.status = status;
    }

    pub(crate) fn set_time_range(&mut self, range: TimeRange) {
        self.time_range = Some(range);
    }
}
