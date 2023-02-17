use crate::range::StreamRange;

/// Stream is the basic storage unit in the system that store records in an append-only fashion.
pub struct Stream {
    id: i64,
    ranges: Vec<StreamRange>,
}

impl Stream {
    fn open(id: i64) -> Self {
        Self { id, ranges: vec![] }
    }
}
