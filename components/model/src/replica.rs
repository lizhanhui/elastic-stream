use protocol::rpc::header::ReplicaProgressT;

#[derive(Debug, Clone)]
pub struct ReplicaProgress {
    pub stream_id: u64,
    pub range_index: u32,
    pub confirm_offset: u64,
}

impl ReplicaProgress {
    pub fn new(stream_id: u64, range_index: u32, confirm_offset: u64) -> ReplicaProgress {
        ReplicaProgress {
            stream_id,
            range_index,
            confirm_offset,
        }
    }
}

impl From<&ReplicaProgress> for ReplicaProgressT {
    fn from(progress: &ReplicaProgress) -> ReplicaProgressT {
        let mut t = ReplicaProgressT::default();
        t.stream_id = progress.stream_id as i64;
        t.range_index = progress.range_index as i32;
        t.confirm_offset = progress.confirm_offset as i64;
        t
    }
}
