use crate::range::StreamRange;

/// Stream is the basic storage unit in the system that store records in an append-only fashion.
pub struct Stream {
    id: i64,
    ranges: Vec<StreamRange>,
}

impl Stream {
    pub fn new(id: i64, ranges: Vec<StreamRange>) -> Self {
        Self { id, ranges }
    }

    pub fn open(id: i64) -> Self {
        Self { id, ranges: vec![] }
    }

    pub fn seal(&self, committed: u64) {
        todo!()
    }

    pub fn range(&self, index: i32) -> Option<StreamRange> {
        self.ranges
            .iter()
            .try_find(|&range| Some(range.id() == index))
            .flatten()
            .map(|range| range.clone())
    }
}
