use std::{cmp::min, rc::Rc};

use log::{debug, error, warn};
use model::{error::EsError, RecordBatch};
use protocol::rpc::header::ErrorCode;

use super::{
    cache::{block_size_align, BlockCache, HotCache, Readahead},
    records_block::RecordsBlock,
    FetchDataset, Stream,
};

pub(crate) struct CacheStream<S> {
    stream_id: u64,
    stream: Rc<S>,
    hot_cache: Rc<HotCache>,
    block_cache: Rc<BlockCache>,
}

impl<S> CacheStream<S>
where
    S: Stream + 'static,
{
    pub(crate) fn new(
        stream_id: u64,
        stream: Rc<S>,
        hot_cache: Rc<HotCache>,
        block_cache: Rc<BlockCache>,
    ) -> Rc<Self> {
        Rc::new(Self {
            stream_id,
            stream,
            hot_cache,
            block_cache,
        })
    }

    async fn fetch0(
        &self,
        mut start_offset: u64,
        end_offset: u64,
        mut batch_max_bytes: u32,
    ) -> Result<FetchDataset, EsError> {
        let stream_start_offset = self.start_offset();
        let stream_end_offset = self.confirm_offset();
        if start_offset < stream_start_offset || end_offset > stream_end_offset {
            return Err(EsError::new(
                ErrorCode::OFFSET_OUT_OF_RANGE_BOUNDS,
                &format!(
                    "[{start_offset}, {end_offset} is out of stream bound [{stream_start_offset},{stream_end_offset})",
                ),
            ));
        }

        let mut blocks = vec![];

        // 1. try read from hot cache.
        let hot_block =
            self.hot_cache
                .get_block(self.stream_id, start_offset, end_offset, batch_max_bytes);
        start_offset = hot_block.end_offset();
        batch_max_bytes -= min(hot_block.size(), batch_max_bytes);

        blocks.push(hot_block);
        if start_offset >= end_offset || batch_max_bytes == 0 {
            // fullfil by hot cache.
            return Ok(FetchDataset::Full(blocks));
        }

        // 2. try read from block cache
        let (block, readahead) =
            self.block_cache
                .get_block(self.stream_id, start_offset, end_offset, batch_max_bytes);
        start_offset = block.end_offset();
        batch_max_bytes -= min(block.size(), batch_max_bytes);
        if !block.is_empty() {
            blocks.push(block);
        }

        // 2.1 trigger background readahead.
        if let Some(readahead) = readahead {
            self.background_readahead(readahead);
        }
        if start_offset >= end_offset || batch_max_bytes == 0 {
            // fullfil by block cache.
            return Ok(FetchDataset::Full(blocks));
        }

        // 3. read from stream.
        // double the end_offset and size to readahead.
        let readahead_end_offset = min(
            stream_end_offset,
            start_offset + (end_offset - start_offset) * 2,
        );
        let readahead_batch_max_bytes = block_size_align(batch_max_bytes * 2);

        let dataset = self
            .stream
            .fetch(
                start_offset,
                readahead_end_offset,
                readahead_batch_max_bytes,
            )
            .await?;
        let mut remote_blocks = match dataset {
            FetchDataset::Full(blocks) => blocks,
            FetchDataset::Overflow(blocks) => blocks,
            _ => {
                error!("fetch dataset is not full");
                return Err(EsError::new(
                    ErrorCode::UNEXPECTED,
                    "fetch dataset is not full",
                ));
            }
        };
        let mut remaining_block_index = 0;
        for block in remote_blocks.iter() {
            let records = block.get_records(start_offset, end_offset, batch_max_bytes);
            if records.is_empty() {
                break;
            }
            if records.last().unwrap().end_offset() == block.end_offset() {
                remaining_block_index += 1;
            }
            blocks.push(RecordsBlock::new(records));
        }
        remote_blocks.drain(0..remaining_block_index);
        if !remote_blocks.is_empty() {
            self.block_cache.insert(self.stream_id, remote_blocks);
        }
        Ok(FetchDataset::Full(blocks))
    }

    fn background_readahead(&self, readahead: Readahead) {
        let stream_id = self.stream_id;
        let stream = self.stream.clone();
        let block_cache = self.block_cache.clone();
        tokio_uring::spawn(async move {
            Self::background_readahead0(readahead, stream_id, stream, block_cache).await;
        });
    }

    async fn background_readahead0(
        readahead: Readahead,
        stream_id: u64,
        stream: Rc<S>,
        block_cache: Rc<BlockCache>,
    ) {
        let start_offset = readahead.start_offset;
        let confirm_offset = stream.confirm_offset();
        if start_offset >= confirm_offset {
            return;
        }
        let end_offset = readahead.end_offset.unwrap_or(confirm_offset);
        let size = readahead.size_hint;
        debug!("stream{stream_id} background readahead([{start_offset},{end_offset}), {size})");
        let rst = stream.fetch(start_offset, end_offset, size).await;
        match rst {
            Ok(dataset) => {
                let blocks = match dataset {
                    FetchDataset::Full(blocks) => blocks,
                    FetchDataset::Overflow(blocks) => blocks,
                    _ => {
                        error!("stream{stream_id} readahead([{start_offset},{end_offset}), {size}) failed, not full dataset");
                        return;
                    }
                };
                block_cache.insert(stream_id, blocks);
            }
            Err(e) => {
                warn!(
                    "stream{stream_id} readahead([{start_offset},{end_offset}), {size}) failed: {}",
                    e
                );
            }
        }
    }
}

impl<S> Stream for CacheStream<S>
where
    S: Stream + 'static,
{
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

    async fn append(&self, record_batch: RecordBatch) -> Result<u64, EsError> {
        self.stream.append(record_batch).await
    }

    async fn fetch(
        &self,
        start_offset: u64,
        end_offset: u64,
        batch_max_bytes: u32,
    ) -> Result<FetchDataset, EsError> {
        self.fetch0(start_offset, end_offset, batch_max_bytes).await
    }

    async fn trim(&self, new_start_offset: u64) -> Result<(), EsError> {
        self.stream.trim(new_start_offset).await
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Instant};

    use bytes::BytesMut;
    use mockall::predicate::eq;

    use crate::stream::{records_block::BlockRecord, MockStream};

    use super::*;

    #[test]
    fn test_fetch() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async move {
            let mut mock_stream = MockStream::default();

            mock_stream.expect_start_offset().returning(|| 10);
            mock_stream.expect_confirm_offset().returning(|| 18);

            mock_stream
                .expect_fetch()
                .times(1)
                .with(eq(14), eq(16), eq(1048576))
                .returning(|_, _, _| {
                    let records_block = RecordsBlock::new(vec![
                        BlockRecord {
                            start_offset: 14,
                            end_offset_delta: 2,
                            data: vec![BytesMut::zeroed(2).freeze()],
                        },
                        BlockRecord {
                            start_offset: 16,
                            end_offset_delta: 2,
                            data: vec![BytesMut::zeroed(1).freeze()],
                        },
                    ]);
                    Ok(FetchDataset::Full(vec![records_block]))
                });
            mock_stream
                .expect_fetch()
                .returning(|_, _, _| Err(EsError::unexpected("test mock error")));

            let mock_stream = Rc::new(mock_stream);

            let hot_cache = HotCache::new(4096);
            hot_cache.insert(1, 10, 1, vec![BytesMut::zeroed(1).freeze()]);
            hot_cache.insert(1, 11, 2, vec![BytesMut::zeroed(1).freeze()]);
            let hot_cache = Rc::new(hot_cache);

            let block_cache = BlockCache::new(4096);
            let records_block = RecordsBlock::new(vec![
                BlockRecord {
                    start_offset: 11,
                    end_offset_delta: 2,
                    data: vec![BytesMut::zeroed(1).freeze()],
                },
                BlockRecord {
                    start_offset: 13,
                    end_offset_delta: 1,
                    data: vec![BytesMut::zeroed(1).freeze()],
                },
            ]);
            block_cache.insert(1, vec![records_block]);
            let block_cache = Rc::new(block_cache);

            let cache_stream = CacheStream::new(
                1,
                mock_stream.clone(),
                hot_cache.clone(),
                block_cache.clone(),
            );

            // fetch from hot cache + block cache + under stream.
            let dataset = cache_stream.fetch(11, 15, 100).await.unwrap();
            match dataset {
                FetchDataset::Full(blocks) => {
                    assert_eq!(blocks.len(), 3);
                    assert_eq!(blocks[0].size(), 1);
                    assert_eq!(blocks[0].start_offset(), 11);
                    assert_eq!(blocks[0].end_offset(), 13);
                    assert_eq!(blocks[1].size(), 1);
                    assert_eq!(blocks[1].start_offset(), 13);
                    assert_eq!(blocks[1].end_offset(), 14);
                    assert_eq!(blocks[2].size(), 2);
                    assert_eq!(blocks[2].start_offset(), 14);
                    assert_eq!(blocks[2].end_offset(), 16);
                }
                _ => panic!("not full dataset"),
            }

            // fetch out of bound.
            assert_eq!(
                ErrorCode::OFFSET_OUT_OF_RANGE_BOUNDS,
                cache_stream.fetch(11, 19, 100).await.unwrap_err().code
            );
        });

        Ok(())
    }

    #[test]
    fn test_readahead() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async move {
            let mut mock_stream = MockStream::default();

            mock_stream.expect_start_offset().returning(|| 10);
            mock_stream.expect_confirm_offset().returning(|| 18);

            mock_stream
                .expect_fetch()
                .times(1)
                .with(eq(14), eq(18), eq(5))
                .returning(|_, _, _| {
                    let records_block = RecordsBlock::new(vec![BlockRecord {
                        start_offset: 14,
                        end_offset_delta: 2,
                        data: vec![BytesMut::zeroed(10).freeze()],
                    }]);
                    Ok(FetchDataset::Overflow(vec![records_block]))
                });
            let mock_stream = Rc::new(mock_stream);

            let block_cache = Rc::new(BlockCache::new(4096));

            CacheStream::<MockStream>::background_readahead0(
                Readahead {
                    start_offset: 14,
                    end_offset: None,
                    size_hint: 5,
                    timestamp: Instant::now(),
                },
                1,
                mock_stream.clone(),
                block_cache.clone(),
            )
            .await;

            let (block, _) = block_cache.get_block(1, 14, 18, 1000);
            assert_eq!(block.size(), 10);
            assert_eq!(block.start_offset(), 14);
            assert_eq!(block.end_offset(), 16);
        });
        Ok(())
    }
}
