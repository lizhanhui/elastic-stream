use std::{cell::RefCell, collections::BTreeMap, ops::Bound, rc::Rc};

use bytes::Bytes;
use log::{debug, info, warn};
use model::object::ObjectMetadata;
use opendal::{
    services::{Fs, S3},
    Operator,
};

use crate::{error::ObjectReadError, Error};
use serde::Deserialize;
use tokio::sync::oneshot;

use super::records_block::RecordsBlock;

pub(crate) trait ObjectReader {
    async fn read_first_object_blocks(
        &self,
        start_offset: u64,
        end_offset: Option<u64>,
        size_hint: u32,
        object_metadata_manager: &ObjectMetadataManager,
    ) -> Result<Vec<RecordsBlock>, ObjectReadError>;
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
    ) -> Result<Vec<RecordsBlock>, ObjectReadError> {
        let (object, range) = if let Some((object, range)) =
            object_metadata_manager.find_first(start_offset, end_offset, size_hint)
        {
            (object, range)
        } else {
            return Err(ObjectReadError::NotFound(Error::new(
                0,
                "Cannot find object for the range.",
            )));
        };
        let mut position = range.0;
        debug!("fetch {:?} blocks in range {:?}", object.key, range);
        let mut object_blocks = self.object_reader.read(&object, range).await?;
        if object_blocks.is_empty() {
            return Err(ObjectReadError::Unexpected(Error::new(
                0,
                "Object reader return empty block",
            )));
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
    ) -> Result<Vec<RecordsBlock>, ObjectReadError> {
        if self.op.is_none() {
            return Err(ObjectReadError::Unexpected(Error::new(
                0,
                "Operator is not initialized",
            )));
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
        _object_data_len: u32,
        tx: oneshot::Sender<Result<Vec<RecordsBlock>, ObjectReadError>>,
    ) {
        let op = self.op.as_ref().unwrap().clone();
        tokio_uring::spawn(async move {
            // TODO: 最后一个 block 可能没有数据，每个 block 必须读到数据,object_reader 负责。
            let rst = op
                .range_read(&object_key, range.0 as u64..range.1 as u64)
                .await;
            let rst = match rst {
                Ok(b) => match RecordsBlock::parse(Bytes::from(b), 1024 * 1024, true) {
                    Ok(blocks) => Ok(blocks),
                    Err(_) => Err(ObjectReadError::Unexpected(Error::new(
                        0,
                        "parse block fail",
                    ))),
                },
                Err(_) => Err(ObjectReadError::ReqStoreFail(Error::new(
                    0,
                    "req store fail",
                ))),
            };
            let _ = tx.send(rst);
        });
    }
}

#[derive(Debug)]
pub(crate) struct ObjectMetadataManager {
    metadata_map: RefCell<BTreeMap<u64, ObjectMetadata>>,
    // TODO: clean
    // start_offset -> object position hint map
    offset_to_position: RefCell<BTreeMap<u64, u32>>,
}

impl ObjectMetadataManager {
    pub(crate) fn new() -> Self {
        Self {
            metadata_map: RefCell::new(BTreeMap::new()),
            offset_to_position: RefCell::new(BTreeMap::new()),
        }
    }

    pub(crate) fn add_object_metadata(&self, metadata: &ObjectMetadata) {
        let mut metadata_map = self.metadata_map.borrow_mut();
        metadata_map
            .entry(metadata.start_offset)
            .or_insert_with(|| metadata.clone());
    }

    fn add_position_hint(&self, start_offset: u64, position: u32) {
        let mut offset_to_position = self.offset_to_position.borrow_mut();
        offset_to_position
            .entry(start_offset)
            .or_insert_with(|| position);
    }

    fn find_first(
        &self,
        start_offset: u64,
        end_offset: Option<u64>,
        size_hint: u32,
    ) -> Option<(ObjectMetadata, (u32, u32))> {
        let metadata_map = self.metadata_map.borrow();
        let cursor = metadata_map.upper_bound(Bound::Included(&start_offset));

        let object = cursor.value()?;

        let object_end_offset = object.start_offset + object.end_offset_delta as u64;
        if object_end_offset <= start_offset {
            // object is before start_offset
            return None;
        }
        let position = self
            .offset_to_position
            .borrow()
            .upper_bound(Bound::Included(&start_offset))
            .key_value()
            .filter(|(offset, _)| *offset >= &object.start_offset) // filter offset in current object
            .map(|(_, position)| position)
            .cloned();
        if let Some(range) = object.find_bound(start_offset, end_offset, size_hint, position) {
            Some((object.clone(), range))
        } else {
            panic!("object#find_bound should not return None");
        }
    }
}
