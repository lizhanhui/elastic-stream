use std::collections::{btree_map::Entry, BTreeMap};

use slog::{error, Logger};
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum WriteWindowError {
    #[error("A previous entry(offset: {offset:?}, length: {length:?}) exists")]
    Existed { offset: u64, length: u32 },

    #[error("Mismatch length at offset {offset:?}: expected: {expected:?}, actual: {actual:?}")]
    Length {
        offset: u64,
        expected: u32,
        actual: u32,
    },

    #[error(
        "Duplicated commit (offset: {offset:?}, length: {length:?}, dup_length: {dup_length:?})"
    )]
    DuplicatedCommit {
        offset: u64,
        length: u32,
        dup_length: u32,
    },

    #[error("Internal error: {0}")]
    Internal(String),
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
    committed: u64,

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

    pub(crate) fn reset_committed(&mut self, committed: u64) {
        self.committed = committed;
    }

    pub(crate) fn add(&mut self, offset: u64, length: u32) -> Result<(), WriteWindowError> {
        match self.submitted.entry(offset) {
            Entry::Vacant(e) => {
                e.insert(length);
                Ok(())
            }
            Entry::Occupied(e) => Err(WriteWindowError::Existed { offset, length }),
        }
    }

    fn advance(&mut self) -> Result<u64, WriteWindowError> {
        loop {
            if let Some((completed_offset, completed_length)) = self.completed.first_key_value() {
                if *completed_offset != self.committed {
                    break;
                }

                if let Some((submitted_offset, submitted_length)) = self.submitted.first_key_value()
                {
                    if completed_offset != submitted_offset {
                        break;
                    }

                    if completed_length != submitted_length {
                        return Err(WriteWindowError::Length {
                            offset: *completed_offset,
                            expected: *completed_length,
                            actual: *submitted_length,
                        });
                    }
                } else {
                    break;
                }
            } else {
                break;
            }

            self.submitted.pop_first();
            if let Some((_, length)) = self.completed.pop_first() {
                self.committed += length as u64;
            }
        }
        Ok(self.committed)
    }

    pub(crate) fn commit(&mut self, offset: u64, length: u32) -> Result<u64, WriteWindowError> {
        match self.completed.entry(offset) {
            Entry::Vacant(e) => {
                e.insert(length);
            }
            Entry::Occupied(e) => {
                error!(
                    self.log,
                    "Duplicated commit. offset: {}, length: {}, dup_length: {}",
                    offset,
                    e.get(),
                    length
                );
                return Err(WriteWindowError::DuplicatedCommit {
                    offset,
                    length: *e.get(),
                    dup_length: length,
                });
            }
        };
        self.advance()
    }
}
