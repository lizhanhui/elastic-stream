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
                    if let FetchDataset::Full(remote_blocks) = dataset {
                        block_cache.insert(stream_id, remote_blocks);
                    } else {
                        error!("stream{stream_id} readahead([{start_offset},{end_offset}), {size}) failed, not full dataset");
                    }
                }
                Err(e) => {
                    warn!("stream{stream_id} readahead([{start_offset},{end_offset}), {size}) failed: {}", e);
                }
            }
        });
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
