use flat_model::{BatchRecordMeta, RecordMeta};

#[path = "generated/model_generated.rs"]
pub mod flat_model;
#[path = "generated/rpc_generated.rs"]
pub mod rpc;

pub fn root_as_batch_record_meta(buf: &[u8]) -> Result<BatchRecordMeta, flatbuffers::InvalidFlatbuffer> {
    flatbuffers::root::<BatchRecordMeta>(buf)
}

pub fn root_as_record_meta(buf: &[u8]) -> Result<RecordMeta, flatbuffers::InvalidFlatbuffer> {
    flatbuffers::root::<RecordMeta>(buf)
}