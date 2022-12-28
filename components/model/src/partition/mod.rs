use crate::range::PartitionRange;

pub struct Partition {
    id: u64,
    ranges: Vec<PartitionRange>,
}

impl Partition {
    fn open(id: u64) -> Self {
        Self { id, ranges: vec![] }
    }
}
