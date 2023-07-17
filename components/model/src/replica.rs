use protocol::rpc::header::RangeProgressT;

#[derive(Debug, Clone)]
pub struct RangeProgress {
    pub stream_id: u64,
    pub range_index: u32,
    pub confirm_offset: u64,
}

impl RangeProgress {
    pub fn new(stream_id: u64, range_index: u32, confirm_offset: u64) -> RangeProgress {
        RangeProgress {
            stream_id,
            range_index,
            confirm_offset,
        }
    }
}

impl From<&RangeProgress> for RangeProgressT {
    fn from(progress: &RangeProgress) -> RangeProgressT {
        let mut t = RangeProgressT::default();
        t.stream_id = progress.stream_id as i64;
        t.range_index = progress.range_index as i32;
        t.confirm_offset = progress.confirm_offset as i64;
        t
    }
}
