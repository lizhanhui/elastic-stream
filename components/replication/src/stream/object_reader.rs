use std::{cmp::min, rc::Rc, time::Instant};

use bytes::{BufMut, BytesMut};
use log::{debug, info, warn};
use model::{error::EsError, object::ObjectMetadata};
use opendal::{
    services::{Fs, S3},
    Operator,
};
use protocol::rpc::header::ErrorCode;

use serde::Deserialize;
use tokio::sync::oneshot;

use crate::stream::metrics::METRICS;

use super::{object_stream::ObjectMetadataManager, records_block::RecordsBlock};

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub(crate) trait ObjectReader {
    async fn read_first_object_blocks(
        &self,
        start_offset: u64,
        end_offset: Option<u64>,
        size_hint: u32,
        object_metadata_manager: &ObjectMetadataManager,
    ) -> Result<Vec<RecordsBlock>, EsError>;
}

#[derive(Debug)]
pub(crate) struct DefaultObjectReader {
    object_reader: Rc<AsyncObjectReader>,
}

impl DefaultObjectReader {
    pub(crate) fn new(object_reader: Rc<AsyncObjectReader>) -> Self {
        Self { object_reader }
    }
}

impl ObjectReader for DefaultObjectReader {
    async fn read_first_object_blocks(
        &self,
        start_offset: u64,
        end_offset: Option<u64>,
        size_hint: u32,
        object_metadata_manager: &ObjectMetadataManager,
    ) -> Result<Vec<RecordsBlock>, EsError> {
        let start = Instant::now();
        let (object, range) = if let Some((object, range)) =
            object_metadata_manager.find_first(start_offset, end_offset, size_hint)
        {
            (object, range)
        } else {
            return Err(EsError::new(
                ErrorCode::OBJECT_NOT_FOUND,
                "Cannot find object for the range.",
            ));
        };
        let mut position = range.0;
        let size = range.1 - range.0;
        debug!("fetch {:?} blocks in range {:?}", object.key, range);
        let mut object_blocks = self.object_reader.read(&object, range).await?;
        if object_blocks.is_empty() {
            return Err(EsError::new(
                ErrorCode::NO_MATCH_RECORDS_IN_OBJECT,
                "Object reader return empty block",
            ));
        }
        let first = object_blocks.first_mut().unwrap();
        first.trim(start_offset, end_offset);
        if object_blocks.len() > 1 {
            let last = object_blocks.last_mut().unwrap();
            last.trim(start_offset, end_offset);
        }

        // add position hint
        for block in &object_blocks {
            let end_offset = block.end_offset();
            if end_offset == (object.start_offset + object.end_offset_delta as u64) {
                position = 0;
            }
            object_metadata_manager.add_position_hint(end_offset, position);
            position += block.size();
        }
        METRICS.with(|m| m.record_fetch_object(size, start.elapsed().as_micros() as u64));
        // TODO: double check block continuous.
        Ok(object_blocks)
    }
}

#[derive(Deserialize, Debug)]
struct ObjectStorageConfig {
    pub endpoint: String,
    #[serde(default = "default_empty_string")]
    pub bucket: String,
    #[serde(default = "default_empty_string")]
    pub region: String,
    #[serde(default = "default_empty_string")]
    pub access_key_id: String,
    #[serde(default = "default_empty_string")]
    pub secret_access_key: String,
}

fn default_empty_string() -> String {
    "".to_owned()
}

#[derive(Debug)]
pub(crate) struct AsyncObjectReader {
    op: Option<Operator>,
}

impl AsyncObjectReader {
    pub(crate) fn new() -> Self {
        let op = match envy::prefixed("ES_OBJ_").from_env::<ObjectStorageConfig>() {
            Ok(config) => {
                if config.endpoint.starts_with("fs://") {
                    let mut builder = Fs::default();
                    builder.root("/tmp/");
                    info!("start local test fs operator");
                    Some(Operator::new(builder).unwrap().finish())
                } else {
                    let mut s3_builder = S3::default();
                    s3_builder.root("/");
                    s3_builder.bucket(&config.bucket);
                    s3_builder.region(&config.region);
                    s3_builder.endpoint(&config.endpoint);
                    s3_builder.access_key_id(&config.access_key_id);
                    s3_builder.secret_access_key(&config.secret_access_key);
                    info!(
                        "start object operator with endpoint: {}, bucket: {}, region: {}",
                        &config.endpoint, &config.bucket, &config.region
                    );
                    Some(Operator::new(s3_builder).unwrap().finish())
                }
            }
            Err(e) => {
                warn!(
                    "read object storage config fail: {}, start without object storage",
                    e
                );
                None
            }
        };
        Self { op }
    }

    async fn read(
        &self,
        object: &ObjectMetadata,
        range: (u32, u32),
    ) -> Result<Vec<RecordsBlock>, EsError> {
        if self.op.is_none() {
            return Err(EsError::new(
                ErrorCode::OBJECT_OPERATOR_UNINITIALIZED,
                "Operator is not initialized",
            ));
        }
        // // TODO: dispatch task to different thread.
        let (tx, rx) = oneshot::channel();
        let object_key = object.key.clone().unwrap();
        self.read0(object_key, range, object.data_len, tx);
        rx.await.expect("object read rx await fail")
    }

    fn read0(
        &self,
        object_key: String,
        range: (u32, u32),
        object_data_len: u32,
        tx: oneshot::Sender<Result<Vec<RecordsBlock>, EsError>>,
    ) {
        let op = self.op.as_ref().unwrap().clone();
        tokio_uring::spawn(async move {
            let mut all_bytes = BytesMut::new();
            let mut start_pos = range.0 as u64;
            let mut end_pos = range.1 as u64;
            loop {
                if start_pos >= end_pos {
                    let _ = tx.send(Err(EsError::unexpected(
                        "read to object end still cannot read a complete RecordBatch",
                    )));
                    return;
                }
                let read_bytes = match op.range_read(&object_key, start_pos..end_pos).await {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx.send(Err(EsError::new(
                            ErrorCode::OBJECT_PARSE_ERROR,
                            "parse block fail",
                        )
                        .set_source(e)));
                        break;
                    }
                };
                all_bytes.put_slice(&read_bytes);
                let bytes = all_bytes.freeze();
                let rst = match RecordsBlock::parse(bytes.clone(), 1024 * 1024, true) {
                    Ok(blocks) => {
                        if blocks.is_empty() {
                            // the read range may only contains a part of one RecordBatch,
                            // so we need to read the next range until read a complete RecordBatch.
                            let new_end_pos =
                                min(end_pos + end_pos - start_pos, object_data_len as u64);
                            start_pos = end_pos;
                            end_pos = new_end_pos;
                            all_bytes = BytesMut::from(&bytes[..]);
                            continue;
                        } else {
                            Ok(blocks)
                        }
                    }
                    Err(e) => Err(
                        EsError::new(ErrorCode::OBJECT_PARSE_ERROR, "parse block fail")
                            .set_source(e),
                    ),
                };
                let _ = tx.send(rst);
                break;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};
    use model::{object::gen_footer, record::flat_record::FlatRecordBatch, RecordBatch};

    use crate::stream::replication_range::vec_bytes_to_bytes;

    use super::*;
    use std::{env, error::Error};

    #[test]
    fn test_read_first_object_blocks() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async move {
            let data_len = write_object("test_read_first_object_blocks").await;
            env::set_var("ES_OBJ_ENDPOINT", "fs://");

            let object_metadata_manager = ObjectMetadataManager::new();
            let mut object_metadata = ObjectMetadata::new(1, 2, 3, 100);
            object_metadata.key = Some("test_read_first_object_blocks".to_owned());
            object_metadata.data_len = data_len;
            object_metadata.end_offset_delta = 278;
            object_metadata_manager.add_object_metadata(&object_metadata);

            let async_reader = Rc::new(AsyncObjectReader::new());
            let obj_reader = DefaultObjectReader::new(async_reader.clone());

            let blocks = obj_reader
                .read_first_object_blocks(243, Some(260), 100, &object_metadata_manager)
                .await
                .unwrap();
            assert_eq!(1, blocks.len());
            assert_eq!(243, blocks[0].start_offset());
            assert_eq!(263, blocks[0].end_offset());

            // TODO: check hint
        });
        Ok(())
    }

    #[test]
    fn test_async_object_reader_read() -> Result<(), Box<dyn Error>> {
        // mock object data with size 200
        tokio_uring::start(async move {
            let data_len = write_object("test_async_object_reader_read").await;
            env::set_var("ES_OBJ_ENDPOINT", "fs://");
            let obj_reader = AsyncObjectReader::new();
            let mut object_metadata = ObjectMetadata::new(1, 2, 3, 100);
            object_metadata.key = Some("test_async_object_reader_read".to_owned());
            object_metadata.data_len = data_len;
            let rst = obj_reader.read(&object_metadata, (0, 100)).await.unwrap();
            assert_eq!(1, rst.len());
            assert_eq!(233, rst[0].start_offset());
            assert_eq!(243, rst[0].end_offset());
        });
        Ok(())
    }

    async fn write_object(path: &str) -> u32 {
        let mut object_bytes = BytesMut::new();
        let mut data_len = 0;
        let mut records0 = new_record_batch_bytes(233, 10, 200);
        data_len += records0.len() as u32;
        let mut records1 = new_record_batch_bytes(243, 20, 100);
        data_len += records1.len() as u32;
        let mut records2 = new_record_batch_bytes(263, 15, 100);
        data_len += records2.len() as u32;

        object_bytes.put(&mut records0);
        object_bytes.put(&mut records1);
        object_bytes.put(&mut records2);
        object_bytes.put(gen_footer(data_len, 0));

        let mut fs_builder = Fs::default();
        fs_builder.root("/tmp/");
        let op = Operator::new(fs_builder).unwrap().finish();
        op.write(path, object_bytes.freeze()).await.unwrap();
        data_len
    }

    fn new_record_batch_bytes(base_offset: u64, count: u32, payload_size: u32) -> Bytes {
        let builder = RecordBatch::new_builder()
            .with_stream_id(1)
            .with_range_index(0)
            .with_base_offset(base_offset as i64)
            .with_last_offset_delta(count as i32)
            .with_payload(BytesMut::zeroed(payload_size as usize).freeze());
        let flat: FlatRecordBatch = builder.build().unwrap().into();
        vec_bytes_to_bytes(&flat.encode().0)
    }
}
