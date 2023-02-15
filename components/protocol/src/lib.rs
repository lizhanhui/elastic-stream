use flat_model::{RecordBatchMeta, RecordMeta};

#[path = "generated/model_generated.rs"]
pub mod flat_model;
#[path = "generated/rpc_generated.rs"]
pub mod rpc;

pub fn root_as_record_batch_meta(buf: &[u8]) -> Result<RecordBatchMeta, flatbuffers::InvalidFlatbuffer> {
    flatbuffers::root::<RecordBatchMeta>(buf)
}

pub fn root_as_record_meta(buf: &[u8]) -> Result<RecordMeta, flatbuffers::InvalidFlatbuffer> {
    flatbuffers::root::<RecordMeta>(buf)
}