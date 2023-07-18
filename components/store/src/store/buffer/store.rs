use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

use futures::future::join_all;
use local_sync::oneshot;
use log::trace;
use model::range::RangeMetadata;

use crate::{
    error::{AppendError, FetchError, StoreError},
    option::{ReadOptions, WriteOptions},
    AppendRecordRequest, AppendResult, FetchResult, Store,
};

use super::{stream::StreamBuffer, BufferedRequest};

#[derive(Debug)]
pub(crate) struct StoreBuffer {
    buffers: HashMap<u64, StreamBuffer>,
}

impl StoreBuffer {
    pub(crate) fn new() -> Self {
        Self {
            buffers: HashMap::new(),
        }
    }

    fn stream_buffer(&mut self, stream_id: u64) -> &mut StreamBuffer {
        self.buffers
            .entry(stream_id)
            .or_insert(StreamBuffer::new(stream_id))
    }

    pub(crate) fn fast_forward(&mut self, request: &AppendRecordRequest) -> bool {
        self.stream_buffer(request.stream_id as u64)
            .fast_forward(request)
    }

    pub(crate) fn buffer(&mut self, req: BufferedRequest) -> Result<(), BufferedRequest> {
        self.stream_buffer(req.request.stream_id as u64).buffer(req)
    }

    pub(crate) fn create_range(&mut self, stream_id: u64, index: u32, offset: u64) {
        self.stream_buffer(stream_id).create_range(index, offset);
    }

    pub(crate) fn seal_range(&mut self, stream_id: u64, index: u32) {
        self.stream_buffer(stream_id).seal_range(index);
    }

    pub(crate) fn drain(&mut self, stream_id: u64, range: u32) -> Option<Vec<BufferedRequest>> {
        self.stream_buffer(stream_id).drain(range)
    }
}

/// Primary design goal of `BufferedStore` is to sort out append requests of a stream range.
///
/// `BufferedStore` is a wrapper of `Store` trait, which buffers append requests of a stream range till they become continuous.
/// Once the requests become continuous, `BufferedStore` will flush them to the underlying `Store` in one batch.
///
/// Once a stream range is sealed, `BufferedStore` will drop all append requests that are not yet continuous.
#[derive(Debug, Clone)]
pub struct BufferedStore<S> {
    store: S,
    buffer: Rc<RefCell<StoreBuffer>>,
}

impl<S> BufferedStore<S>
where
    S: Store,
{
    pub fn new(store: S) -> Self {
        trace!("BufferedStore::new");
        Self {
            store,
            buffer: Rc::new(RefCell::new(StoreBuffer::new())),
        }
    }
}

impl<S> Store for BufferedStore<S>
where
    S: Store,
{
    /// Append a new record into store.
    ///
    /// * `options` - Write options, specifying how the record is written to persistent medium.
    /// * `record` - Data record to append.
    async fn append(
        &self,
        options: &WriteOptions,
        request: AppendRecordRequest,
    ) -> Result<AppendResult, AppendError> {
        trace!(
            "BufferedStore#append stream-id={}, range-index={}, offset={}",
            request.stream_id,
            request.range_index,
            request.offset
        );
        if self.buffer.borrow_mut().fast_forward(&request) {
            let stream_id = request.stream_id as u64;
            let range = request.range_index as u32;
            let fut = self.store.append(options, request);

            let mut futures = vec![fut];
            let mut senders = vec![];
            if let Some(requests) = self.buffer.borrow_mut().drain(stream_id, range) {
                for req in requests {
                    let fut = self.store.append(options, req.request);
                    futures.push(fut);
                    senders.push(req.tx);
                }
            }
            let mut res = join_all(futures).await;
            let ret = res.remove(0);
            res.into_iter().zip(senders).for_each(|(res, tx)| {
                let _ = tx.send(res);
            });
            ret
        } else {
            let (tx, rx) = oneshot::channel();
            let item = BufferedRequest { request, tx };
            let res = self.buffer.borrow_mut().buffer(item);
            match res {
                Ok(_) => rx.await.map_err(|_e| AppendError::ChannelRecv)?,
                Err(_e) => {
                    // The only reason we cannot buffer an request is that target range does not exist.
                    Err(AppendError::RangeNotFound)
                }
            }
        }
    }

    /// Retrieve a single existing record at the given stream and offset.
    /// * `options` - Read options, specifying target stream and offset.
    async fn fetch(&self, options: ReadOptions) -> Result<FetchResult, FetchError> {
        self.store.fetch(options).await
    }

    /// List all stream ranges in the store
    ///
    /// if `filter` returns true, the range is kept in the final result vector; dropped otherwise.
    async fn list<F>(&self, filter: F) -> Result<Vec<RangeMetadata>, StoreError>
    where
        F: Fn(&RangeMetadata) -> bool + 'static,
    {
        self.store.list(filter).await
    }

    /// List all ranges pertaining to the specified stream in the store
    ///
    /// if `filter` returns true, the range is kept in the final result vector; dropped otherwise.
    async fn list_by_stream<F>(
        &self,
        stream_id: i64,
        filter: F,
    ) -> Result<Vec<RangeMetadata>, StoreError>
    where
        F: Fn(&RangeMetadata) -> bool + 'static,
    {
        self.store.list_by_stream(stream_id, filter).await
    }

    /// Seal stream range in metadata column family after cross check with placement driver.
    async fn seal(&self, range: RangeMetadata) -> Result<(), StoreError> {
        self.buffer
            .borrow_mut()
            .seal_range(range.stream_id() as u64, range.index() as u32);
        self.store.seal(range).await
    }

    /// Create a stream range in metadata.
    async fn create(&self, range: RangeMetadata) -> Result<(), StoreError> {
        self.buffer.borrow_mut().create_range(
            range.stream_id() as u64,
            range.index() as u32,
            range.start(),
        );
        self.store.create(range).await
    }

    /// Max record offset in the store of the specified stream.
    fn max_record_offset(&self, stream_id: i64, range: u32) -> Result<Option<u64>, StoreError> {
        self.store.max_record_offset(stream_id, range)
    }

    fn id(&self) -> i32 {
        self.store.id()
    }

    fn config(&self) -> Arc<config::Configuration> {
        self.store.config()
    }
}
