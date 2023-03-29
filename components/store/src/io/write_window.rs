use std::collections::{btree_map::Entry, BTreeMap};

use slog::{error, trace, Logger};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub(crate) enum WriteWindowError {
    #[error("A previous entry(offset: {offset:?}, length: {length:?}) exists")]
    Existed { offset: u64, length: u32 },

    #[error(
        "Invalid commit attempt (offset: {offset:?}, previous_length: {previous_length:?}, attempted_length: {attempted_length:?})"
    )]
    InvalidCommit {
        offset: u64,
        previous_length: u32,
        attempted_length: u32,
    },
}

/// Window manager that manages concurrent direct IO writes.
///
/// A write would make a range of WAL data persistent. The range is represented
/// as [offset, offset + len). `io_uring` allows up to configured IO_DEPTH number of
/// writes submitted to underlying device and get them done concurrently regardless of
/// submit order, unless their SQEs are flagged with `IOSQE_IO_LINK` and within the
/// same submit batch.
///
/// To achieve maximum IOPS/throughput, we do not use linking requests and manage flush
/// progress manually.
///
/// Here is how we manage the concurrent window:
///
/// 1. Once a SQE is generated and prepared to submit, we add the range [offset, offset + len)
///    into a tree map in form of offset --> len;
/// 2. On reaping a CQE, we compare the completed IO with the minimum node of submitted tree-map.
/// 2.1 If the completed range offset != committed, meaning we are not expanding continuous writes,
///     we insert it to completed tree map directly.
/// 2.2 If these two ranges are identical, and offset of them equals to the previously committed
///     , then increase the committed and we are safe to acknowledge all pending requests whose
///     spans are less than the committed.
/// 2.3  Once we found an expanding IO completion, we need to loop to advance more.
pub(crate) struct WriteWindow {
    log: Logger,

    /// An offset in WAL, all data prior to it should have been completely written. All write requests whose
    /// ranges fall into [0, committed) can be safely acknowledged.
    pub(crate) committed: u64,

    /// Writes that have been submitted to IO device.
    submitted: BTreeMap<u64, u32>,

    /// Writes that are completed but disjoint with the prior.
    completed: BTreeMap<u64, u32>,
}

impl WriteWindow {
    pub(crate) fn new(log: Logger, committed: u64) -> Self {
        Self {
            log,
            committed,
            submitted: BTreeMap::new(),
            completed: BTreeMap::new(),
        }
    }

    /// Reset committed WAL offset, expected to be called once on initialization.
    pub(crate) fn reset_committed(&mut self, committed: u64) {
        self.committed = committed;
    }

    pub(crate) fn add(&mut self, offset: u64, length: u32) -> Result<(), WriteWindowError> {
        trace!(
            self.log,
            "Add inflight WAL write: [{}, {})",
            offset,
            offset + length as u64
        );
        match self.submitted.entry(offset) {
            Entry::Vacant(e) => {
                e.insert(length);
                Ok(())
            }
            Entry::Occupied(e) => Err(WriteWindowError::Existed {
                offset,
                length: *e.get(),
            }),
        }
    }

    fn advance(&mut self) -> Result<u64, WriteWindowError> {
        while let Some((completed_offset, completed_len)) = self.completed.first_key_value() {
            if *completed_offset > self.committed {
                // Some writes, within the gap, are NOT yet completed
                break;
            }

            if let Some((submitted_offset, submitted_len)) = self.submitted.first_key_value() {
                if completed_offset != submitted_offset {
                    break;
                }
                debug_assert_eq!(completed_len, submitted_len, "If wal_offset of submitted range equals to that of the completed, their length must be the same");
            } else {
                break;
            }
            self.completed.pop_first();
            if let Some((wal_offset, length)) = self.submitted.pop_first() {
                self.committed = wal_offset + length as u64;
                trace!(
                    self.log,
                    "Advance committed position of WAL to: {}",
                    self.committed
                );
            }
        }
        Ok(self.committed)
    }

    pub(crate) fn commit(&mut self, offset: u64, length: u32) -> Result<u64, WriteWindowError> {
        trace!(
            self.log,
            "Try to commit WAL [{}, {})",
            offset,
            offset + length as u64
        );
        match self.completed.entry(offset) {
            Entry::Vacant(e) => {
                e.insert(length);
            }
            Entry::Occupied(mut e) => {
                let prev_len = e.get_mut();
                if length >= *prev_len {
                    trace!(
                        self.log,
                        "Completed WAL range expanded from [{}, {}) to [{}, {})",
                        offset,
                        offset + *prev_len as u64,
                        offset,
                        offset + length as u64
                    );
                    *prev_len = length;
                } else {
                    // Should not reach here, as [offset, e.get()) truly includes [offset, length). This violates
                    // barrier mechanism.
                    error!(
                        self.log,
                        "Unexpected invalid commit. Previously completed range[{}, {}), attempted range: [{}, {})",
                        offset,
                        offset + *prev_len as u64,
                        offset,
                        offset + length as u64
                    );
                    return Err(WriteWindowError::InvalidCommit {
                        offset,
                        previous_length: *prev_len,
                        attempted_length: length,
                    });
                }
            }
        };
        self.advance()
    }
}

#[cfg(test)]
mod tests {
    use super::WriteWindowError;

    #[test]
    fn test_write_window() -> Result<(), WriteWindowError> {
        let log = test_util::terminal_logger();
        let mut window = super::WriteWindow::new(log, 0);
        window.add(0, 10)?;
        window.commit(0, 10)?;
        assert_eq!(10, window.committed);

        window.add(0, 20)?;
        window.commit(0, 20)?;
        assert_eq!(20, window.committed);

        window.add(20, 10)?;
        window.commit(20, 10)?;
        assert_eq!(30, window.committed);

        // Gap
        window.add(40, 10)?;
        window.commit(40, 10)?;
        assert_eq!(30, window.committed);

        Ok(())
    }

    #[test]
    fn test_write_window_add() -> Result<(), WriteWindowError> {
        let log = test_util::terminal_logger();
        let mut window = super::WriteWindow::new(log, 0);
        window.add(0, 10)?;
        match window.add(0, 20) {
            Ok(_) => {
                panic!("Should have raised an error");
            }
            Err(e) => {
                assert_eq!(
                    e,
                    WriteWindowError::Existed {
                        offset: 0,
                        length: 10
                    }
                );
            }
        }
        Ok(())
    }
}
