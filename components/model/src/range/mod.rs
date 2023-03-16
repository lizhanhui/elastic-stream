use std::fmt::{self, Display, Formatter};

use derivative::Derivative;

use crate::{data_node::DataNode, error::RangeError};

pub trait Range {
    fn sealed(&self) -> bool;

    fn seal(&mut self) -> Result<u64, RangeError>;
}

/// Representation of a stream range in form of `[start, end)` in which `start` is inclusive and `end` is exclusive.
/// If `start` == `end`, there will be no valid records in the range.
///
/// At the beginning, `end` will be `None` and it would grow as more slots are taken from the range.
/// Once the range is sealed, it becomes immutable and its right boundary becomes fixed.
#[derive(Derivative)]
#[derivative(Debug, PartialEq, Clone)]
pub struct StreamRange {
    stream_id: i64,

    id: i32,

    /// The start slot index, inclusive.
    start: u64,

    /// The next slot index to allocate for the incoming record.
    next: u64,

    /// The end of the range, exclusive
    end: Option<u64>,

    /// List of data nodes, that all have identical records within the range.
    #[derivative(PartialEq = "ignore")]
    replica: Vec<DataNode>,
}

impl StreamRange {
    pub fn new(stream_id: i64, id: i32, start: u64, next: u64, end: Option<u64>) -> Self {
        Self {
            stream_id,
            id,
            start,
            next,
            end,
            replica: vec![],
        }
    }

    pub fn replica(&self) -> &Vec<DataNode> {
        &self.replica
    }

    pub fn replica_mut(&mut self) -> &mut Vec<DataNode> {
        &mut self.replica
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
    /// That is, number of records in the stream range.
    pub fn len(&self) -> u64 {
        self.next - self.start
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> Option<u64> {
        self.end
    }
}

impl Range for StreamRange {
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

impl Display for StreamRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{stream-id={}, range-id={}}}=[{}, {}, {})",
            self.stream_id,
            self.id,
            self.start,
            self.next,
            self.end.unwrap_or(0)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_take_slot() {
        let mut range = StreamRange::new(0, 0, 0, 0, None);
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
