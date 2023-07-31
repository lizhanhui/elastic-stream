#![allow(dead_code)]
use bytes::Bytes;
use lru::LruCache;
use std::cell::RefCell;
use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::ops::Bound;
use std::rc::Rc;
use std::time::Instant;

use super::records_block::{BlockRecord, RecordsBlock};

pub(crate) struct HotCache {
    max_cache_size: u64,
    occupied_size: RefCell<u64>,
    map: RefCell<HashMap<Rc<CacheKey>, Rc<BlockRecord>>>,
    fifo_deque: RefCell<VecDeque<Rc<CacheKey>>>,
}

impl HotCache {
    pub(crate) fn new(max_cache_size: u64) -> Self {
        if max_cache_size < 4 * 1024 {
            panic!("max_cache_size must be greater than 4KB");
        }
        let cache_entry_size = 4 * 1024;
        Self {
            max_cache_size,
            occupied_size: RefCell::new(0),
            map: RefCell::new(HashMap::with_capacity(
                (max_cache_size / cache_entry_size) as usize,
            )),
            fifo_deque: RefCell::new(VecDeque::with_capacity(
                (max_cache_size / cache_entry_size) as usize,
            )),
        }
    }

    pub(crate) fn insert(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset_delta: u32,
        data: Vec<Bytes>,
    ) {
        let mut occupied_size = self.occupied_size.borrow_mut();
        let mut fifo_deque = self.fifo_deque.borrow_mut();
        let mut map = self.map.borrow_mut();
        while *occupied_size >= self.max_cache_size {
            if let Some(cache_key) = fifo_deque.pop_front() {
                if let Some(cache_value) = map.remove(&cache_key) {
                    *occupied_size -= cache_value.size() as u64;
                }
            } else {
                break;
            }
        }
        let cache_key = Rc::new(CacheKey::new(stream_id, start_offset));
        let cache_value = Rc::new(BlockRecord {
            start_offset,
            end_offset_delta,
            data,
        });
        *occupied_size += cache_value.size() as u64;
        fifo_deque.push_back(cache_key.clone());
        map.insert(cache_key, cache_value);
    }

    pub(crate) fn get_block(
        &self,
        stream_id: u64,
        mut start_offset: u64,
        end_offset: u64,
        mut size_hint: u32,
    ) -> RecordsBlock {
        let mut records = vec![];
        let map = self.map.borrow();
        loop {
            if start_offset >= end_offset || size_hint == 0 {
                break;
            }
            let cache_key = Rc::new(CacheKey::new(stream_id, start_offset));
            let record = map.get(&cache_key).cloned();
            if let Some(record) = record {
                let record = record.as_ref().clone();
                start_offset = record.end_offset();
                size_hint -= min(record.size(), size_hint);
                records.push(record);
            } else {
                break;
            }
        }
        if records.is_empty() {
            RecordsBlock::empty_block(start_offset)
        } else {
            RecordsBlock::new(records)
        }
    }
}

impl fmt::Debug for HotCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RecordBatchCache[occupied/total={}/{} cache_count={}]",
            self.occupied_size.borrow(),
            self.max_cache_size,
            self.fifo_deque.borrow().len()
        )
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CacheKey {
    stream_id: u64,
    base_offset: u64,
}

impl CacheKey {
    pub(crate) fn new(stream_id: u64, base_offset: u64) -> Self {
        Self {
            stream_id,
            base_offset,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RecordsBlockKey {
    stream_id: u64,
    start_offset: u64,
}

impl RecordsBlockKey {
    fn new(stream_id: u64, start_offset: u64) -> Self {
        Self {
            stream_id,
            start_offset,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Readahead {
    pub(crate) start_offset: u64,
    pub(crate) end_offset: Option<u64>,
    pub(crate) size_hint: u32,
    pub(crate) timestamp: Instant,
}

type StreamId = u64;
type StartOffset = u64;
type RecordsBlockLen = u32;

const BLOCK_SIZE: u32 = 1024 * 1024;
const READAHEAD_SIZE_INCREASE_ELAPSE_MS: u128 = 100;
const READAHEAD_SIZE_DECREASE_ELAPSE_MS: u128 = 300;
const MAX_READAHEAD_SIZE: u32 = 1024 * 1024 * 128;

pub(crate) struct BlockCache {
    cache_map: RefCell<HashMap<StreamId, StreamCache>>,
    reading_lru: RefCell<LruCache<RecordsBlockKey, RecordsBlockLen>>,
    completed_lru: RefCell<LruCache<RecordsBlockKey, RecordsBlockLen>>,
    max_cache_size: u64,
    cache_size: RefCell<u64>,
}

impl BlockCache {
    pub(crate) fn new(max_cache_size: u64) -> Self {
        Self {
            cache_map: RefCell::new(HashMap::new()),
            reading_lru: RefCell::new(LruCache::unbounded()),
            completed_lru: RefCell::new(LruCache::unbounded()),
            max_cache_size,
            cache_size: RefCell::new(0),
        }
    }

    /// Insert blocks to cache and attach readahead info to the first block.
    /// The readahead info generate rule:
    /// 1. The readahead.start_offset = insert blocks' end_offset.
    /// 2. readahead.size:
    /// - If the stream cache is evicted, it means cache is overhead, and the reading cache is evicted,
    ///     then set readahead.size = blocks' size / 2 to decrease cache pressure.
    /// - Else, the readahead.size = blocks' size.
    pub(crate) fn insert(&self, stream_id: u64, blocks: Vec<RecordsBlock>) {
        let blocks: Vec<RecordsBlock> = blocks.into_iter().filter(|b| !b.is_empty()).collect();
        if blocks.is_empty() {
            return;
        }

        // reserve cache size.
        let blocks_size = blocks.iter().map(|b| b.size()).sum::<u32>() as u64;
        self.evict(blocks_size);
        *self.cache_size.borrow_mut() += blocks_size;

        // get stream cache.
        let mut cache_map = self.cache_map.borrow_mut();
        let stream_cache = if let Some(stream_cache) = cache_map.get_mut(&stream_id) {
            stream_cache
        } else {
            let range_cache = StreamCache::new();
            cache_map.insert(stream_id, range_cache);
            cache_map.get_mut(&stream_id).unwrap()
        };

        let mut readahead = Self::gen_readahead(&blocks, stream_cache);

        let blocks: Vec<RecordsBlock> = blocks
            .into_iter()
            .filter(|b| {
                let (given_block_kick_out, kicked_out_size) =
                    kick_out_covered_block(stream_cache, b);
                *self.cache_size.borrow_mut() -= kicked_out_size;
                !given_block_kick_out
            })
            .collect();

        let mut reading_lru = self.reading_lru.borrow_mut();
        for block in blocks.iter().rev() {
            reading_lru.push(
                RecordsBlockKey::new(stream_id, block.start_offset()),
                block.size(),
            );
        }
        for block in blocks {
            if let Some((old_block, None)) = stream_cache
                .cache_map
                .insert(block.start_offset(), (block, readahead))
            {
                *self.cache_size.borrow_mut() -= old_block.size() as u64;
            }
            readahead = None;
        }
    }

    fn gen_readahead(
        blocks: &Vec<RecordsBlock>,
        stream_cache: &mut StreamCache,
    ) -> Option<Readahead> {
        if blocks.is_empty() {
            return None;
        }
        let blocks_end_offset = blocks.last().unwrap().end_offset();
        let blocks_size = blocks.iter().map(|b| b.size()).sum::<u32>();
        let readahead_size = if stream_cache.evict_mark {
            stream_cache.evict_mark = false;
            block_size_align(blocks_size / 2)
        } else {
            block_size_align(blocks_size)
        };
        Some(Readahead {
            start_offset: blocks_end_offset,
            end_offset: None,
            size_hint: readahead_size,
            timestamp: Instant::now(),
        })
    }

    /// Get RecordsBlock from cache that try fullfil the request,
    /// and return the readahead info:
    /// - if the block is the last block of the stream
    /// - or the read blocks contain a readahead info.
    pub(crate) fn get_block(
        &self,
        stream_id: u64,
        mut start_offset: u64,
        end_offset: u64,
        mut size_hint: u32,
    ) -> (RecordsBlock, Option<Readahead>) {
        let mut final_records = vec![];
        let mut reading_cache = self.cache_map.borrow_mut();
        let mut final_readahead = None;
        let mut expect_next_block_start_offset = None;
        if let Some(stream_cache) = reading_cache.get_mut(&stream_id) {
            let mut cursor = stream_cache
                .cache_map
                .upper_bound(Bound::Included(&start_offset));
            let mut readahead_mark_erase = None;
            loop {
                if size_hint == 0 || start_offset >= end_offset {
                    break;
                }
                if let Some((block_start_offset, (block, readahead))) = cursor.key_value() {
                    if final_readahead.is_none() {
                        if let Some(readahead) = readahead.as_ref() {
                            final_readahead = Some(re_gen_readahead(readahead));
                            readahead_mark_erase = Some(*block_start_offset);
                        }
                    }
                    let block_end_offset = block.end_offset();
                    // if block isn't contain start offset, then break.
                    if block_end_offset <= start_offset || block.start_offset() > start_offset {
                        break;
                    }
                    let records = block.get_records(start_offset, end_offset, size_hint);
                    let records_size = records.iter().map(|r| r.size()).sum();
                    let records_end_offset = records.last().unwrap().end_offset();
                    size_hint -= min(records_size, size_hint);
                    start_offset = records_end_offset;
                    final_records.extend(records);

                    let records_block_key = RecordsBlockKey::new(stream_id, block.start_offset());
                    let block_size = block.size();
                    if records_end_offset == block_end_offset {
                        // block read complete, the add it to completed_lru.
                        // the block in completed_lru will be evicted first when cache is full.
                        self.reading_lru.borrow_mut().pop(&records_block_key);
                        self.completed_lru
                            .borrow_mut()
                            .push(records_block_key, block_size);
                    } else {
                        self.reading_lru
                            .borrow_mut()
                            .push(records_block_key, block_size);
                    }
                    expect_next_block_start_offset = Some(block.end_offset());
                    cursor.move_next();
                } else {
                    break;
                }
            }

            if let Some(block_start_offset) = readahead_mark_erase {
                if let Some(value) = stream_cache.cache_map.get_mut(&block_start_offset) {
                    value.1 = None;
                }
            }

            // if next block isn't in cache, then add it to readahead.
            if final_readahead.is_none() {
                if let Some(expect_next_block_start_offset) = expect_next_block_start_offset {
                    if !stream_cache
                        .cache_map
                        .contains_key(&expect_next_block_start_offset)
                    {
                        final_readahead = Some(Readahead {
                            start_offset: expect_next_block_start_offset,
                            end_offset: None,
                            size_hint: BLOCK_SIZE,
                            timestamp: Instant::now(),
                        });
                    }
                }
            }
        }
        if final_records.is_empty() {
            return (RecordsBlock::empty_block(start_offset), None);
        }
        (RecordsBlock::new(final_records), final_readahead)
    }

    fn evict(&self, mut reserve_size: u64) {
        reserve_size = min(reserve_size, self.max_cache_size);
        let mut cache_size = self.cache_size.borrow_mut();
        if *cache_size + reserve_size <= self.max_cache_size {
            return;
        }
        let mut cache_map = self.cache_map.borrow_mut();
        for mut lru in [
            self.completed_lru.borrow_mut(),
            self.reading_lru.borrow_mut(),
        ] {
            loop {
                if *cache_size + reserve_size <= self.max_cache_size {
                    return;
                }
                if let Some((key, _)) = lru.pop_lru() {
                    if let Some(stream_map) = cache_map.get_mut(&key.stream_id) {
                        if let Some((block, _)) = stream_map.cache_map.remove(&key.start_offset) {
                            *cache_size -= block.size() as u64;
                            stream_map.evict_mark = true;
                        }
                        if stream_map.cache_map.is_empty() {
                            cache_map.remove(&key.stream_id);
                        }
                    }
                } else {
                    break;
                };
            }
        }
    }
}

struct StreamCache {
    cache_map: BTreeMap<StartOffset, (RecordsBlock, Option<Readahead>)>,
    evict_mark: bool,
}

impl StreamCache {
    fn new() -> Self {
        Self {
            cache_map: BTreeMap::new(),
            evict_mark: false,
        }
    }
}

fn re_gen_readahead(readahead: &Readahead) -> Readahead {
    let mut size_hint = block_size_align(readahead.size_hint);
    let elapsed = readahead.timestamp.elapsed().as_millis();
    if elapsed <= READAHEAD_SIZE_INCREASE_ELAPSE_MS {
        size_hint = min(size_hint * 2, MAX_READAHEAD_SIZE);
    } else if elapsed >= READAHEAD_SIZE_DECREASE_ELAPSE_MS {
        size_hint = max(size_hint / 2, BLOCK_SIZE);
    }
    Readahead {
        start_offset: readahead.start_offset,
        end_offset: readahead.end_offset,
        size_hint,
        timestamp: Instant::now(),
    }
}

pub(crate) fn block_size_align(size: u32) -> u32 {
    let align = BLOCK_SIZE;
    if let Some(size) = size.checked_add(align - 1) {
        size / align * align
    } else {
        u32::MAX / align * align
    }
}

/// Kick out the blocks in cache which are covered by the given block.
/// return:
/// - whether the block is covered by cache
/// - the total size of kicked out blocks
fn kick_out_covered_block(stream_cache: &mut StreamCache, block: &RecordsBlock) -> (bool, u64) {
    let block_start_offset = block.start_offset();
    let block_end_offset = block.end_offset();
    let mut cache_block_cursor = stream_cache
        .cache_map
        .upper_bound(Bound::Included(&block_start_offset));
    let mut cache_block_wait_remove = vec![];
    let mut given_block_covered = false;
    while let Some((cache_block_start_offset, (cache_block, _))) = cache_block_cursor.key_value() {
        let cache_block_end_offset = cache_block.end_offset();
        if block_start_offset <= *cache_block_start_offset
            && cache_block_end_offset <= block_end_offset
        {
            // the given block covers the cache block.
            cache_block_wait_remove.push(*cache_block_start_offset);
        } else if *cache_block_start_offset <= block_start_offset
            && block_end_offset <= cache_block_end_offset
        {
            // the cache block covers the given block.
            given_block_covered = true;
            break;
        } else if block_end_offset < *cache_block_start_offset {
            // the given block is before the cache block.
            break;
        }
        cache_block_cursor.move_next();
    }
    let mut kicked_out_size = 0;
    for cache_block_start_offset in cache_block_wait_remove {
        if let Some((cache_block, _)) = stream_cache.cache_map.remove(&cache_block_start_offset) {
            kicked_out_size += cache_block.size() as u64
        }
    }
    (given_block_covered, kicked_out_size)
}

#[cfg(test)]
mod test {
    use crate::stream::{
        cache::HotCache,
        records_block::{BlockRecord, RecordsBlock},
    };
    use bytes::{Bytes, BytesMut};
    use std::error::Error;

    use super::{kick_out_covered_block, BlockCache, StreamCache};

    #[test]
    fn test_hot_cache_insert_get() -> Result<(), Box<dyn Error>> {
        let cache = HotCache::new(4096);
        let stream_id = 1;
        let count = 10;
        for i in 0..100 {
            let base_offset = i * 10;
            let data = vec![Bytes::from(format!("data{}", i))];
            cache.insert(stream_id, base_offset, count, data.clone());
            let cache_value =
                cache.get_block(stream_id, base_offset, base_offset + count as u64, 1024);
            let records = cache_value.records;
            assert_eq!(1, records.len());
            assert_eq!(count, records[0].end_offset_delta);
            assert_eq!(data, records[0].data);
        }
        for i in 0..100 {
            let base_offset = i * 10;
            let cache_value =
                cache.get_block(stream_id, base_offset, base_offset + count as u64, 1024);
            let records = cache_value.records;
            assert_eq!(1, records.len());
            assert_eq!(count, records[0].end_offset_delta);
            assert_eq!(vec![Bytes::from(format!("data{}", i))], records[0].data);
        }
        Ok(())
    }

    #[test]
    fn test_hot_cache_evict() -> Result<(), Box<dyn Error>> {
        let cache = HotCache::new(4096);
        cache.insert(1, 0, 1, vec![Bytes::from(vec![0; 4096])]);
        assert_eq!(4096, cache.get_block(1, 0, 1, 4096).size());
        // expect evict the last record
        cache.insert(1, 1, 2, vec![Bytes::from(vec![0; 1024])]);
        assert_eq!(0, cache.get_block(1, 0, 1, 4096).size());
        assert_eq!(1024, cache.get_block(1, 1, 2, 4096).size());

        cache.insert(1, 3, 3, vec![Bytes::from(vec![0; 1024])]);
        assert_eq!(2048, cache.get_block(1, 1, 5, 4096).size());
        Ok(())
    }

    #[test]
    fn test_block_cache_insert_get() -> Result<(), Box<dyn Error>> {
        let cache = BlockCache::new(10 * 1024 * 1024);
        // insert 10 1M blocks to make cache full
        let mut blocks = Vec::new();
        for i in 0..10 {
            let block = new_block(20 * i);
            blocks.push(block);
        }
        cache.insert(0, blocks);

        // cross get all blocks
        for i in 0..9 {
            let start = i * 20 + 11; // start in N block's second record
            let end = (i + 1) * 20 + 1; // end in N + 1 block's first record
            let (record_block, readahead) = cache.get_block(0, start, end, 1024 * 1024);
            if i == 0 || i == 8 {
                assert!(readahead.is_some());
            } else {
                assert!(readahead.is_none());
            }

            assert_eq!(2, record_block.records.len());
            assert_eq!(i * 20 + 10, record_block.start_offset());
            assert_eq!(i * 20 + 30, record_block.end_offset());
            assert_eq!(1024 * 1024, record_block.size());
        }

        // cross get 0..2 block with size limit 1024.
        let (record_block, readahead) = cache.get_block(0, 11, 21, 1024);
        assert_eq!(1, record_block.records.len());
        assert_eq!(10, record_block.start_offset());
        assert_eq!(20, record_block.end_offset());
        assert!(readahead.is_none()); // the readahead mark is clean in last read.

        Ok(())
    }

    #[test]
    fn test_block_cache_evict() -> Result<(), Box<dyn Error>> {
        let cache = BlockCache::new(5 * 1024 * 1024);

        cache.insert(0, vec![new_block(0), new_block(20)]);

        // read complete (stream 0, block 0), expect it will recycle before block 1
        assert_eq!(512 * 1024, cache.get_block(0, 10, 20, 1024 * 1024).0.size());

        cache.insert(
            1,
            vec![new_block(0), new_block(20), new_block(40), new_block(60)],
        );

        // (stream 0 , block 0) is evicted.
        assert_eq!(0, cache.get_block(0, 10, 20, 1024 * 1024).0.size());

        // partial read (stream 1, block 1), expect keep it live longer.
        assert_eq!(512 * 1024, cache.get_block(0, 20, 30, 1024 * 1024).0.size());

        cache.insert(0, vec![new_block(40)]);

        // (stream 1, block 3) is evicted. the block in vec tail will recycle first.
        assert_eq!(0, cache.get_block(1, 60, 70, 1024 * 1024).0.size());

        Ok(())
    }

    #[test]
    fn test_block_cache_readahead() -> Result<(), Box<dyn Error>> {
        let cache = BlockCache::new(5 * 1024 * 1024);

        cache.insert(0, vec![new_block(0), new_block(20)]);

        // read the last insert blocks' first block,
        // expect return readahead with start_offset = blocks_end_offset and size = blocks_size * 2.
        let (_, readahead) = cache.get_block(0, 10, 20, 1024 * 1024);
        assert!(readahead.is_some());
        let readahead = readahead.unwrap();
        assert_eq!(40, readahead.start_offset);
        assert!(readahead.end_offset.is_none());
        assert_eq!(4 * 1024 * 1024, readahead.size_hint);

        // retry get, expect readahead mark is cleaned.
        assert!(cache.get_block(0, 10, 20, 1024 * 1024).1.is_none());

        // read the last insert blocks' last block,
        // expect return readahead with start_offset = last_block_end_offset and size = BLOCK_SIZE.
        let (_, readahead) = cache.get_block(0, 20, 30, 1024 * 1024);
        assert!(readahead.is_some());
        let readahead = readahead.unwrap();
        assert_eq!(40, readahead.start_offset);
        assert!(readahead.end_offset.is_none());
        assert_eq!(1024 * 1024, readahead.size_hint);

        Ok(())
    }

    #[test]
    fn test_kick_out_covered_block() {
        let mut stream_cache = StreamCache::new();
        stream_cache.cache_map.insert(
            100,
            (
                RecordsBlock::new(vec![BlockRecord::new(
                    100,
                    100,
                    vec![BytesMut::zeroed(1024).freeze()],
                )]),
                None,
            ),
        );
        {
            // given block is covered by current cache.
            // cache: [100, 200)
            // given_block: [160, 180)
            let given_block = RecordsBlock::new(vec![BlockRecord::new(
                160,
                20,
                vec![BytesMut::zeroed(128).freeze()],
            )]);
            let (given_block_kick_out, kick_out_size) =
                kick_out_covered_block(&mut stream_cache, &given_block);
            assert!(given_block_kick_out);
            assert_eq!(0, kick_out_size);
        }

        {
            // given block only overlap the cache block.
            // cache: [100, 200)
            // given_block: [160, 220)
            let given_block = RecordsBlock::new(vec![BlockRecord::new(
                160,
                60,
                vec![BytesMut::zeroed(128).freeze()],
            )]);
            let (given_block_kick_out, kick_out_size) =
                kick_out_covered_block(&mut stream_cache, &given_block);
            assert!(!given_block_kick_out);
            assert_eq!(0, kick_out_size);
        }

        {
            // given block cover the cache block.
            // cache: [100, 200)
            // given_block: [100, 220)
            let given_block = RecordsBlock::new(vec![BlockRecord::new(
                100,
                120,
                vec![BytesMut::zeroed(128).freeze()],
            )]);
            let (given_block_kick_out, kick_out_size) =
                kick_out_covered_block(&mut stream_cache, &given_block);
            assert!(!given_block_kick_out);
            assert_eq!(1024, kick_out_size);
            assert_eq!(0, stream_cache.cache_map.len());
        }
    }

    #[test]
    fn test_get_overlap() {
        let block_cache = BlockCache::new(4096);
        for i in 0..2 {
            block_cache.insert(
                0,
                vec![RecordsBlock::new(vec![
                    BlockRecord::new(i * 10, 10, vec![BytesMut::zeroed(128).freeze()]),
                    BlockRecord::new(i * 10 + 10, 10, vec![BytesMut::zeroed(128).freeze()]),
                ])],
            )
        }
        let (records_block, _) = block_cache.get_block(0, 10, 30, 4096);
        assert_eq!(10, records_block.start_offset());
        assert_eq!(30, records_block.end_offset());
        assert_eq!(128 * 2, records_block.size());
    }

    fn new_block(start_offset: u64) -> RecordsBlock {
        let mut records = Vec::new();
        for i in 0..2 {
            let bytes = BytesMut::zeroed(524288).freeze();
            records.push(BlockRecord {
                start_offset: i * 10 + start_offset,
                end_offset_delta: 10,
                data: vec![bytes],
            })
        }
        RecordsBlock::new(records)
    }
}
