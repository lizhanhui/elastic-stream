use std::{cell::RefCell, cmp::min, collections::BTreeMap, ops::Bound, rc::Rc};

use log::error;
use model::{error::EsError, object::ObjectMetadata};
use protocol::rpc::header::ErrorCode;

use crate::stream::FetchDataset;

use super::{object_reader::ObjectReader, records_block::RecordsBlock, Stream};

pub(crate) struct ObjectStream<S, R> {
    stream: Rc<S>,
    object_metadata_manager: ObjectMetadataManager,
    object_reader: R,
}

impl<S, R> ObjectStream<S, R>
where
    S: Stream + 'static,
    R: ObjectReader + 'static,
{
    pub(crate) fn new(stream: Rc<S>, object_reader: R) -> Rc<Self> {
        Rc::new(Self {
            stream,
            object_metadata_manager: ObjectMetadataManager::new(),
            object_reader,
        })
    }

    async fn fetch0(
        &self,
        start_offset: u64,
        end_offset: u64,
        batch_max_bytes: u32,
    ) -> Result<super::FetchDataset, EsError> {
        let mut start_offset = start_offset;
        let mut remaining_size = batch_max_bytes;
        let mut final_blocks = vec![];
        loop {
            let dataset = self
                .stream
                .fetch(start_offset, end_offset, remaining_size)
                .await?;
            let (records_blocks, objects) = match dataset {
                FetchDataset::Full(blocks) => (blocks, vec![]),
                FetchDataset::Partial(blocks) => (blocks, vec![]),
                FetchDataset::Mixin(blocks, objects) => (blocks, objects),
                FetchDataset::Overflow(blocks) => (blocks, vec![]),
            };
            objects.iter().for_each(|object| {
                self.object_metadata_manager.add_object_metadata(object);
            });
            // Fetch ([0, 100), size=1000), there are 3 cases for underline stream fetch result:
            // 1. records_blocks contains ([0, 100), size <= 1000) records, then return.
            // 2. records_blocks are empty blocks contains [100, 100) records, then we read ([0, end_offset >= 100), size >= 1000) from object storage
            // 3. records_blocks only contain partial records [50, 100),  and we need read [0, end_offset >= 50) from object storage
            // - [0, 30) is already fulfill the request size, then return.
            // - [0, 60) is not fulfill the request size, then combine [0, 60) and [50, 100) to [0, 100) and return.
            let mut records_block = merge_blocks(records_blocks);
            let blocks_start_offset = records_block.start_offset();
            if start_offset < blocks_start_offset {
                while !(start_offset >= blocks_start_offset || remaining_size == 0) {
                    let mut object_blocks = self
                        .object_reader
                        .read_first_object_blocks(
                            start_offset,
                            None,
                            remaining_size,
                            &self.object_metadata_manager,
                        )
                        .await?;
                    let object_blocks_end_offset = object_blocks
                        .last()
                        .ok_or_else(|| {
                            error!("Object blocks is empty");
                            EsError::new(
                                ErrorCode::NO_MATCH_RECORDS_IN_OBJECT,
                                "Object blocks is empty",
                            )
                        })?
                        .end_offset();
                    let object_blocks_len = object_blocks.iter().map(|b| b.size()).sum();
                    start_offset = object_blocks_end_offset;
                    remaining_size -= min(object_blocks_len, remaining_size);
                    final_blocks.append(&mut object_blocks);
                }
            }
            if records_block.start_offset() <= start_offset {
                records_block.trim(start_offset, None);
                if !records_block.is_empty() {
                    remaining_size -= min(records_block.size(), remaining_size);
                    start_offset = records_block.end_offset();
                    final_blocks.push(records_block);
                }
            }
            if start_offset >= end_offset || remaining_size == 0 {
                break;
            }
        }
        check_records_sequence(&final_blocks)?;
        Ok(FetchDataset::Overflow(final_blocks))
    }
}

fn merge_blocks(blocks: Vec<RecordsBlock>) -> RecordsBlock {
    if blocks.is_empty() {
        return RecordsBlock::empty_block(u64::MAX);
    }
    let end_offset = blocks.last().unwrap().end_offset();
    let records_count = blocks.iter().map(|b| b.records.len()).sum();
    let mut records = Vec::with_capacity(records_count);
    for mut block in blocks.into_iter() {
        records.append(&mut block.records);
    }
    if records.is_empty() {
        RecordsBlock::empty_block(end_offset)
    } else {
        RecordsBlock::new(records)
    }
}

fn check_records_sequence(blocks: &[RecordsBlock]) -> Result<(), EsError> {
    let mut expect_next_offset = None;
    for block in blocks.iter() {
        if let Some(next_offset) = expect_next_offset {
            if block.start_offset() != next_offset {
                return Err(EsError::new(
                    ErrorCode::RECORDS_BLOCKS_NOT_CONTINUOUS,
                    "Blocks is not continuous",
                ));
            }
            expect_next_offset = Some(block.end_offset());
        } else {
            expect_next_offset = Some(block.end_offset());
        }
    }
    Ok(())
}

/// delegate Stream trait to inner stream beside #fetch
impl<S, R> Stream for ObjectStream<S, R>
where
    S: Stream + 'static,
    R: ObjectReader + 'static,
{
    async fn fetch(
        &self,
        start_offset: u64,
        end_offset: u64,
        batch_max_bytes: u32,
    ) -> Result<super::FetchDataset, EsError> {
        self.fetch0(start_offset, end_offset, batch_max_bytes).await
    }

    async fn open(&self) -> Result<(), EsError> {
        self.stream.open().await
    }

    async fn close(&self) {
        self.stream.close().await
    }

    fn start_offset(&self) -> u64 {
        self.stream.start_offset()
    }

    fn confirm_offset(&self) -> u64 {
        self.stream.confirm_offset()
    }

    fn next_offset(&self) -> u64 {
        self.stream.next_offset()
    }

    async fn append(&self, record_batch: model::RecordBatch) -> Result<u64, EsError> {
        self.stream.append(record_batch).await
    }

    async fn trim(&self, new_start_offset: u64) -> Result<(), EsError> {
        self.stream.trim(new_start_offset).await
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

    pub(crate) fn add_position_hint(&self, start_offset: u64, position: u32) {
        let mut offset_to_position = self.offset_to_position.borrow_mut();
        offset_to_position
            .entry(start_offset)
            .or_insert_with(|| position);
    }

    pub(crate) fn find_first(
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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use bytes::{BufMut, Bytes, BytesMut};
    use mockall::predicate::{self, eq};

    use crate::stream::{object_reader::MockObjectReader, records_block::BlockRecord, MockStream};

    use super::*;

    #[monoio::test]
    async fn test_fetch_remote_exactly_match() -> Result<(), Box<dyn Error>> {
        let mut inner_stream = MockStream::new();
        let object_reader = MockObjectReader::new();

        inner_stream.expect_fetch().returning(|start_offset, _, _| {
            if start_offset == 100 {
                Ok(FetchDataset::Full(vec![new_records_block(100, 200, 1000)]))
            } else {
                Ok(FetchDataset::Full(vec![new_records_block(300, 400, 10)]))
            }
        });
        let stream = ObjectStream::new(Rc::new(inner_stream), object_reader);
        // offset match
        let dataset = stream.fetch(100, 200, 10).await.unwrap();
        match dataset {
            FetchDataset::Overflow(blocks) => {
                assert_eq!(1, blocks.len());
                assert_eq!(100, blocks[0].start_offset());
                assert_eq!(200, blocks[0].end_offset());
                assert_eq!(1000, blocks[0].size());
            }
            _ => panic!("unexpected dataset"),
        }

        // size match
        let dataset = stream.fetch(300, 500, 10).await.unwrap();
        match dataset {
            FetchDataset::Overflow(blocks) => {
                assert_eq!(1, blocks.len());
                assert_eq!(300, blocks[0].start_offset());
                assert_eq!(400, blocks[0].end_offset());
                assert_eq!(10, blocks[0].size());
            }
            _ => panic!("unexpected dataset"),
        }
        Ok(())
    }

    #[monoio::test]
    async fn test_fetch_mixin() -> Result<(), Box<dyn Error>> {
        let mut inner_stream = MockStream::new();
        let mut object_reader = MockObjectReader::new();

        inner_stream
            .expect_fetch()
            .with(eq(100), eq(200), eq(1000))
            .times(1)
            .returning(|_, _, _| Ok(FetchDataset::Full(vec![new_records_block(150, 200, 10)])));
        object_reader
            .expect_read_first_object_blocks()
            .with(eq(100), eq(None), eq(1000), predicate::always())
            .times(1)
            .returning(|_, _, _, _| Ok(vec![new_records_block(100, 120, 100)]));
        object_reader
            .expect_read_first_object_blocks()
            .with(eq(120), eq(None), eq(900), predicate::always())
            .times(1)
            .returning(|_, _, _, _| Ok(vec![new_records_block(120, 150, 100)]));
        let stream = ObjectStream::new(Rc::new(inner_stream), object_reader);

        let dataset = stream.fetch(100, 200, 1000).await.unwrap();
        match dataset {
            FetchDataset::Overflow(blocks) => {
                assert_eq!(3, blocks.len());
                assert_eq!(100, blocks[0].start_offset());
                assert_eq!(120, blocks[0].end_offset());
                assert_eq!(100, blocks[0].size());
                assert_eq!(120, blocks[1].start_offset());
                assert_eq!(150, blocks[1].end_offset());
                assert_eq!(100, blocks[1].size());
                assert_eq!(150, blocks[2].start_offset());
                assert_eq!(200, blocks[2].end_offset());
                assert_eq!(10, blocks[2].size());
            }
            _ => panic!("unexpected dataset"),
        }
        Ok(())
    }

    #[monoio::test]
    async fn test_fetch_object_fulfil_size() -> Result<(), Box<dyn Error>> {
        let mut inner_stream = MockStream::new();
        let mut object_reader = MockObjectReader::new();

        inner_stream
            .expect_fetch()
            .with(eq(100), eq(200), eq(1000))
            .times(1)
            .returning(|_, _, _| Ok(FetchDataset::Full(vec![new_records_block(160, 200, 10)])));
        object_reader
            .expect_read_first_object_blocks()
            .with(eq(100), eq(None), eq(1000), predicate::always())
            .times(1)
            .returning(|_, _, _, _| Ok(vec![new_records_block(100, 120, 100)]));
        object_reader
            .expect_read_first_object_blocks()
            .with(eq(120), eq(None), eq(900), predicate::always())
            .times(1)
            .returning(|_, _, _, _| Ok(vec![new_records_block(120, 150, 1000)]));
        let stream = ObjectStream::new(Rc::new(inner_stream), object_reader);

        let dataset = stream.fetch(100, 200, 1000).await.unwrap();
        match dataset {
            FetchDataset::Overflow(blocks) => {
                assert_eq!(2, blocks.len());
                assert_eq!(100, blocks[0].start_offset());
                assert_eq!(120, blocks[0].end_offset());
                assert_eq!(100, blocks[0].size());
                assert_eq!(120, blocks[1].start_offset());
                assert_eq!(150, blocks[1].end_offset());
                assert_eq!(1000, blocks[1].size());
            }
            _ => panic!("unexpected dataset"),
        }
        Ok(())
    }

    #[test]
    fn test_find_first() {
        let om = ObjectMetadataManager::new();
        let mut object_metadata = ObjectMetadata::new(1, 2, 3, 100);
        object_metadata.end_offset_delta = 10;
        object_metadata.data_len = 1000;
        object_metadata.sparse_index = sparse_index(vec![(5, 300), (8, 700)]);
        om.add_object_metadata(&object_metadata);

        let (obj, (start_pos, end_pos)) = om.find_first(105, Some(106), 1000).unwrap();
        assert_eq!(100, obj.start_offset);
        assert_eq!(300, start_pos);
        assert_eq!(700, end_pos);

        let (_obj, (start_pos, end_pos)) = om.find_first(105, None, 1000).unwrap();
        assert_eq!(300, start_pos);
        assert_eq!(1000, end_pos);

        om.add_position_hint(106, 500);
        let (_obj, (start_pos, end_pos)) = om.find_first(107, None, 1000).unwrap();
        assert_eq!(500, start_pos);
        assert_eq!(1000, end_pos);

        let (_obj, (start_pos, end_pos)) = om.find_first(107, None, 100).unwrap();
        assert_eq!(500, start_pos);
        assert_eq!(700, end_pos);
    }

    #[test]
    fn test_merge_blocks() {
        // merge continuous blocks
        let blocks = vec![new_records_block(10, 20, 1), new_records_block(20, 30, 1)];
        let block = merge_blocks(blocks);
        assert_eq!(10, block.start_offset());
        assert_eq!(30, block.end_offset());
        assert_eq!(2, block.size());

        // merge empty blocks
        let blocks = vec![RecordsBlock::empty_block(30)];
        let block = merge_blocks(blocks);
        assert_eq!(30, block.start_offset());
        assert_eq!(30, block.end_offset());
        assert_eq!(0, block.size());
        assert!(block.is_empty());
    }

    fn new_records_block(start_offset: u64, end_offset: u64, size: usize) -> RecordsBlock {
        let data = BytesMut::zeroed(size).freeze();
        RecordsBlock::new(vec![BlockRecord {
            start_offset,
            end_offset_delta: (end_offset - start_offset) as u32,
            data: vec![data],
        }])
    }

    fn sparse_index(offset_position_vec: Vec<(u32, u32)>) -> Bytes {
        let mut bytes = BytesMut::with_capacity(offset_position_vec.len() * 8);
        for (offset, position) in offset_position_vec.into_iter() {
            bytes.put_u32(offset);
            bytes.put_u32(position);
        }
        bytes.freeze()
    }
}
