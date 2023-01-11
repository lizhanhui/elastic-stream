use crate::error::RangeError;

pub trait Range {
    fn sealed(&self) -> bool;

    fn seal(&mut self) -> Result<u64, RangeError>;
}

/// Representation of a partition range in form of `[start, end)` in which `start` is inclusive and `end` is exclusive.
/// If `start` == `end`, there will be no valid records in the range.
///
/// At the beginning, `end` will be `None` and it would grow as more slots are taken from the range.
/// Once the range is sealed, it becomes immutable and its right boundary becomes fixed.
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct PartitionRange {
    /// The start slot index, inclusive.
    start: u64,

    /// The next slot index to allocate for the incoming record.
    next: u64,

    /// The end of the range, exclusive
    end: Option<u64>,
}

impl PartitionRange {
    pub fn new(start: u64, next: u64, end: Option<u64>) -> Self {
        Self { start, next, end }
    }

    /// Expand the range by one.
    pub fn take_slot(&mut self) -> Result<u64, RangeError> {
        match self.end {
            None => {
                let index = self.next;
                self.next += 1;
                Ok(index)
            }
            Some(offset) => Err(RangeError::AlreadySealed(offset)),
        }
    }

    /// Length of the range.
    /// That is, number of records in the partition range.
    pub fn len(&self) -> u64 {
        self.next - self.start
    }
}

impl Range for PartitionRange {
    fn sealed(&self) -> bool {
        self.end.is_some()
    }

    fn seal(&mut self) -> Result<u64, RangeError> {
        match self.end {
            None => {
                self.end = Some(self.next);
                Ok(self.next)
            }
            Some(offset) => Err(RangeError::AlreadySealed(offset)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_take_slot() {
        let mut range = PartitionRange::new(0, 0, None);
        assert_eq!(range.sealed(), false);
        assert_eq!(range.len(), 0);

        assert_eq!(range.take_slot(), Ok(0));
        assert_eq!(range.len(), 1);
        assert_eq!(range.sealed(), false);

        assert_eq!(range.take_slot(), Ok(1));
        assert_eq!(range.len(), 2);
        assert_eq!(range.sealed(), false);

        assert_eq!(range.take_slot(), Ok(2));
        assert_eq!(range.len(), 3);
        assert_eq!(range.sealed(), false);

        assert_eq!(range.seal(), Ok(3));
        assert_eq!(range.sealed(), true);
        assert_eq!(range.seal(), Err(RangeError::AlreadySealed(3)));
        assert_eq!(range.take_slot(), Err(RangeError::AlreadySealed(3)));
        assert_eq!(range.len(), 3);
        assert_eq!(range.sealed(), true);
    }
}
