use crate::stream::replication_range::RangeAppendContext;
use crate::stream::replication_range::ReplicationRange;

use super::cache::HotCache;
use super::FetchDataset;
use super::Stream;
use client::client::Client;
use itertools::Itertools;
use local_sync::{mpsc, oneshot};
use log::{error, info, trace, warn};
use model::error::EsError;
use model::RecordBatch;
use protocol::rpc::header::ErrorCode;
use std::cell::OnceCell;
use std::cell::RefCell;
use std::cmp::min;
use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::rc::{Rc, Weak};
use std::time::Instant;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};

pub(crate) struct ReplicationStream<R, C>
where
    R: ReplicationRange<C> + 'static,
    C: Client + 'static,
{
    log_ident: String,
    weak_self: RefCell<Weak<Self>>,
    id: u64,
    epoch: u64,
    ranges: RefCell<BTreeMap<u64, Rc<R>>>,
    client: Weak<C>,
    next_offset: RefCell<u64>,
    last_range: RefCell<Option<Rc<R>>>,
    // stream start offset.
    start_offset: RefCell<u64>,
    /// #append send StreamAppendRequest to tx.
    append_requests_tx: mpsc::unbounded::Tx<StreamAppendRequest>,
    /// send by range ack / delay retry to trigger append task loop next round.
    append_tasks_tx: mpsc::unbounded::Tx<()>,
    // send when stream close.
    shutdown_signal_tx: broadcast::Sender<()>,
    // stream closed mark.
    closed: Rc<RefCell<bool>>,
    cache: Rc<HotCache>,
}

impl<R, C> ReplicationStream<R, C>
where
    R: ReplicationRange<C> + 'static,
    C: Client + 'static,
{
    pub(crate) fn new(id: u64, epoch: u64, client: Weak<C>, cache: Rc<HotCache>) -> Rc<Self> {
        let (append_requests_tx, append_requests_rx) = mpsc::unbounded::channel();
        let (append_tasks_tx, append_tasks_rx) = mpsc::unbounded::channel();
        let (shutdown_signal_tx, shutdown_signal_rx) = broadcast::channel(1);
        let this = Rc::new(Self {
            log_ident: format!("Stream[{id}] "),
            weak_self: RefCell::new(Weak::new()),
            id,
            epoch,
            ranges: RefCell::new(BTreeMap::new()),
            client,
            next_offset: RefCell::new(0),
            last_range: RefCell::new(None),
            start_offset: RefCell::new(0),
            append_requests_tx,
            append_tasks_tx,
            shutdown_signal_tx,
            closed: Rc::new(RefCell::new(false)),
            cache,
        });

        *(this.weak_self.borrow_mut()) = Rc::downgrade(&this);

        let weak_this = this.weak_self.borrow().clone();
        let closed = this.closed.clone();
        tokio_uring::spawn(async move {
            Self::append_task(
                weak_this,
                append_requests_rx,
                append_tasks_rx,
                shutdown_signal_rx,
                closed,
            )
            .await
        });

        this
    }

    async fn new_range(&self, range_index: u32, start_offset: u64) -> Result<Rc<R>, EsError> {
        if let Some(client) = self.client.upgrade() {
            let range_metadata =
                R::create(client, self.id, self.epoch, range_index, start_offset).await?;
            let weak_this = self.weak_self.borrow().clone();
            let range = R::new(
                range_metadata,
                true,
                Box::new(move || {
                    if let Some(stream) = weak_this.upgrade() {
                        stream.try_ack();
                    }
                }),
                self.client.clone(),
                self.cache.clone(),
            );
            info!("{}Create new range: {:?}", self.log_ident, range.metadata());
            self.ranges.borrow_mut().insert(start_offset, range.clone());
            *self.last_range.borrow_mut() = Some(range.clone());
            Ok(range)
        } else {
            Err(EsError::new(
                ErrorCode::UNEXPECTED,
                "new range fail, client is drop",
            ))
        }
    }

    pub(crate) fn try_ack(&self) {
        self.trigger_append_task();
    }

    pub(crate) fn trigger_append_task(&self) {
        let _ = self.append_tasks_tx.send(());
    }

    async fn append_task(
        stream: Weak<Self>,
        mut append_requests_rx: mpsc::unbounded::Rx<StreamAppendRequest>,
        mut append_tasks_rx: mpsc::unbounded::Rx<()>,
        mut shutdown_signal_rx: broadcast::Receiver<()>,
        closed: Rc<RefCell<bool>>,
    ) {
        let stream_option = stream.upgrade();
        if stream_option.is_none() {
            warn!("Stream is already released, then directly exit append task");
            return;
        }
        let stream = stream_option.expect("stream id cannot be none");
        let log_ident = &stream.log_ident;
        let mut inflight: BTreeMap<u64, Rc<StreamAppendRequest>> = BTreeMap::new();
        let mut next_append_start_offset: u64 = 0;
        let mut append_error = None;

        loop {
            tokio::select! {
                Some(append_request) = append_requests_rx.recv() => {
                    if append_error.is_some() {
                        append_request.fail(append_error.clone().take().unwrap());
                        continue;
                    }
                    inflight.insert(append_request.base_offset(), Rc::new(append_request));
                }
                Some(_) = append_tasks_rx.recv() => {
                    // usually send by range ack / delay retry
                }
                _ = shutdown_signal_rx.recv() => {
                    let inflight_count = inflight.len();
                    info!("{}Receive shutdown signal, then quick fail {inflight_count} inflight requests with AlreadyClosed err.", log_ident);
                    for (_, append_request) in inflight.iter() {
                        append_request.fail(EsError::new(ErrorCode::STREAM_ALREADY_CLOSED, "stream is closed"));
                    }
                    break;
                }
            }
            if *closed.borrow() {
                let inflight_count = inflight.len();
                info!("{}Detect closed mark, then quick fail {inflight_count} inflight requests with AlreadyClosed err.", log_ident);
                for (_, append_request) in inflight.iter() {
                    append_request.fail(EsError::new(
                        ErrorCode::STREAM_ALREADY_CLOSED,
                        "stream is closed",
                    ));
                }
                break;
            }
            let append_rst = Self::append_task0(
                &stream,
                log_ident,
                &mut inflight,
                &mut next_append_start_offset,
            )
            .await;
            if let Err(e) = append_rst {
                append_error = Some(e.clone());
                for (_, append_request) in inflight.iter() {
                    append_request.fail(e.clone());
                }
            }
        }
    }

    async fn append_task0(
        stream: &Rc<Self>,
        log_ident: &str,
        inflight: &mut BTreeMap<u64, Rc<StreamAppendRequest>>,
        next_append_start_offset: &mut u64,
    ) -> Result<(), EsError> {
        // 1. get writable range.
        let (last_writable_range, rewind_back_next_append_offset) =
            Self::get_writable_range(stream).await?;
        if let Some(rewind) = rewind_back_next_append_offset {
            *next_append_start_offset = rewind;
        }

        if inflight.is_empty() {
            // if no inflight request, then just return.
            return Ok(());
        }

        let range_index = last_writable_range.metadata().index();
        // 2. ack success append request, and remove them from inflight.
        let confirm_offset = last_writable_range.confirm_offset();
        let mut ack_count = 0;
        for (base_offset, append_request) in inflight.iter() {
            if *base_offset < confirm_offset {
                // if base offset is less than confirm offset, it means append request is already success.
                append_request.success();
                ack_count += 1;
                trace!("{}Ack append request with base_offset={base_offset}, confirm_offset={confirm_offset}", log_ident);
            }
        }
        for _ in 0..ack_count {
            inflight.pop_first();
        }

        // 3. try append request which base_offset >= next_append_start_offset.
        let mut cursor = inflight.lower_bound(Included(next_append_start_offset));
        while let Some((base_offset, append_request)) = cursor.key_value() {
            last_writable_range.append(
                &append_request.record_batch,
                RangeAppendContext::new(*base_offset),
            );
            trace!(
                "{}Try append record[{base_offset}] to range[{range_index}]",
                log_ident
            );
            *next_append_start_offset = base_offset + append_request.count() as u64;
            cursor.move_next();
        }
        Ok(())
    }

    async fn get_writable_range(stream: &Rc<Self>) -> Result<(Rc<R>, Option<u64>), EsError> {
        let log_ident = &stream.log_ident;
        let mut next_append_start_offset = None;
        loop {
            let last_range = stream.last_range.borrow().as_ref().cloned();
            let last_writable_range = match last_range {
                Some(last_range) => {
                    let range_index = last_range.metadata().index() as u32;
                    if !last_range.is_writable() {
                        info!("{}The last range[{range_index}] is not writable, try create a new range.", log_ident);
                        // if last range is not writable, try to seal it and create a new range and retry append in next round.
                        match last_range.seal().await {
                            Ok(end_offset) => {
                                info!("{}Seal not writable last range[{range_index}] with end_offset={end_offset}.", log_ident);
                                // rewind back next append start offset and try append to new writable range in next round.
                                next_append_start_offset = Some(end_offset);
                                if let Err(e) = stream.new_range(range_index + 1, end_offset).await
                                {
                                    if e.code == ErrorCode::EXPIRED_STREAM_EPOCH {
                                        return Err(e);
                                    }
                                    error!(
                                        "{}Try create a new range fail, retry later, err[{e}]",
                                        log_ident
                                    );
                                    // delay retry to avoid busy loop
                                    sleep(Duration::from_millis(1000)).await;
                                }
                            }
                            Err(e) => {
                                if e.code == ErrorCode::EXPIRED_STREAM_EPOCH {
                                    return Err(e);
                                }
                                // delay retry to avoid busy loop
                                sleep(Duration::from_millis(1000)).await;
                            }
                        }
                        continue;
                    }
                    last_range
                }
                None => {
                    info!(
                        "{}The stream don't have any range, then try new a range.",
                        log_ident
                    );
                    if let Err(e) = stream.new_range(0, 0).await {
                        error!(
                            "{}New a range from absent fail, retry later, err[{e}]",
                            log_ident
                        );
                        // TODO: check fenced EsError
                        // delay retry to avoid busy loop
                        sleep(Duration::from_millis(1000)).await;
                    }
                    continue;
                }
            };
            return Ok((last_writable_range, next_append_start_offset));
        }
    }

    fn get_client(&self) -> Result<Rc<C>, EsError> {
        self.client.upgrade().ok_or(EsError::new(
            ErrorCode::UNEXPECTED,
            "range get client fail, client is dropped",
        ))
    }
}

impl<R, C> Stream for ReplicationStream<R, C>
where
    R: ReplicationRange<C> + 'static,
    C: Client + 'static,
{
    async fn open(&self) -> Result<(), EsError> {
        info!("{}Opening...", self.log_ident);
        let client = self.get_client()?;
        // 1. fence the stream with new epoch.
        let _ = client
            .update_stream(self.id, None, None, Some(self.epoch))
            .await?;
        // 2. load all ranges
        client
            .list_ranges(model::ListRangeCriteria::new(None, Some(self.id)))
            .await
            .map_err(|e| {
                error!(
                    "{}Failed to list ranges from placement-driver: {e}",
                    self.log_ident
                );
                e
            })?
            .into_iter()
            // skip old empty range when two range has the same start offset
            .sorted_by(|a, b| Ord::cmp(&a.index(), &b.index()))
            .for_each(|range| {
                let this = self.weak_self.borrow().clone();
                self.ranges.borrow_mut().insert(
                    range.start(),
                    R::new(
                        range,
                        false,
                        Box::new(move || {
                            if let Some(stream) = this.upgrade() {
                                stream.try_ack();
                            }
                        }),
                        self.client.clone(),
                        self.cache.clone(),
                    ),
                );
            });
        // 3. seal the last range
        let last_range = self
            .ranges
            .borrow_mut()
            .last_entry()
            .map(|e| e.get().clone());
        if let Some(last_range) = last_range {
            *(self.last_range.borrow_mut()) = Option::Some(last_range.clone());
            let range_index = last_range.metadata().index();
            match last_range.seal().await {
                Ok(confirm_offset) => {
                    // 4. set stream next offset to the exclusive end of the last range
                    *self.next_offset.borrow_mut() = confirm_offset;
                }
                Err(e) => {
                    error!("{}Failed to seal range[{range_index}], {e}", self.log_ident);
                    return Err(e);
                }
            }
        }
        let start_offset = self
            .ranges
            .borrow()
            .first_key_value()
            .map(|(k, _)| *k)
            .unwrap_or(0);
        *self.start_offset.borrow_mut() = start_offset;
        let range_count = self.ranges.borrow().len();
        let start_offset = self.start_offset();
        let next_offset = self.next_offset();
        info!("{}Opened with range_count={range_count} start_offset={start_offset} next_offset={next_offset}", self.log_ident);
        Ok(())
    }

    /// Close the stream.
    /// 1. send stop signal to append task.
    /// 2. await append task to stop.
    /// 3. close all ranges.
    async fn close(&self) {
        info!("{}Closing...", self.log_ident);
        *self.closed.borrow_mut() = true;
        let _ = self.shutdown_signal_tx.send(());
        // TODO: await append task to stop.
        let last_range = self.last_range.borrow().as_ref().cloned();
        if let Some(range) = last_range {
            let _ = range.seal().await;
        }
        info!("{}Closed...", self.log_ident);
    }

    /// Get stream start offset.
    /// The self.start_offset is the source of truth, cause of #trim only remove deleted ranges.
    fn start_offset(&self) -> u64 {
        *self.start_offset.borrow()
    }

    fn confirm_offset(&self) -> u64 {
        self.last_range
            .borrow()
            .as_ref()
            .map_or(0, |r| r.confirm_offset())
    }

    /// next record offset to be appended.
    fn next_offset(&self) -> u64 {
        *self.next_offset.borrow()
    }

    async fn append(&self, record_batch: RecordBatch) -> Result<u64, EsError> {
        let start_timestamp = Instant::now();
        if *self.closed.borrow() {
            warn!("{}Keep append to a closed stream.", self.log_ident);
            return Err(EsError::new(
                ErrorCode::STREAM_ALREADY_CLOSED,
                "stream is closed",
            ));
        }
        let base_offset = *self.next_offset.borrow();
        let count = record_batch.last_offset_delta();
        *self.next_offset.borrow_mut() = base_offset + count as u64;

        let (append_tx, append_rx) = oneshot::channel::<Result<(), EsError>>();
        // trigger background append task to handle the append request.
        if self
            .append_requests_tx
            .send(StreamAppendRequest::new(
                base_offset,
                record_batch,
                append_tx,
            ))
            .is_err()
        {
            warn!("{}Send to append request channel fail.", self.log_ident);
            return Err(EsError::new(
                ErrorCode::STREAM_ALREADY_CLOSED,
                "stream is closed",
            ));
        }
        // await append result.
        match append_rx.await {
            Ok(result) => {
                trace!(
                    "{}Append new record with base_offset={base_offset} count={count} cost {}us",
                    self.log_ident,
                    start_timestamp.elapsed().as_micros()
                );
                result.map(|_| base_offset)
            }
            Err(_) => Err(EsError::new(
                ErrorCode::STREAM_ALREADY_CLOSED,
                "stream is closed",
            )),
        }
    }

    async fn fetch(
        &self,
        start_offset: u64,
        end_offset: u64,
        batch_max_bytes: u32,
    ) -> Result<FetchDataset, EsError> {
        trace!(
            "{}Fetch [{start_offset}, {end_offset}) with batch_max_bytes={batch_max_bytes}",
            self.log_ident
        );
        if start_offset == end_offset {
            return Ok(FetchDataset::Partial(vec![]));
        }
        let last_range = match self.last_range.borrow().as_ref() {
            Some(range) => range.clone(),
            None => {
                return Err(EsError::new(
                    ErrorCode::OFFSET_OUT_OF_RANGE_BOUNDS,
                    "fetch out of range",
                ))
            }
        };
        // Fetch range is out of stream range.
        if last_range.confirm_offset() < end_offset {
            return Err(EsError::new(
                ErrorCode::OFFSET_OUT_OF_RANGE_BOUNDS,
                "fetch out of range",
            ));
        }
        // Fast path: if fetch range is in the last range, then fetch from it.
        if last_range.start_offset() <= start_offset {
            return last_range
                .fetch(start_offset, end_offset, batch_max_bytes)
                .await;
        }
        // Slow path
        // 1. Find the *first* range which match the start_offset.
        // 2. Fetch the range.
        // 3. The invoker should loop invoke fetch util the Dataset fullfil the need.
        let range = self
            .ranges
            .borrow()
            .upper_bound(Included(&start_offset))
            .value()
            .cloned();
        if let Some(range) = range {
            if range.start_offset() > start_offset {
                return Err(EsError::new(
                    ErrorCode::OFFSET_OUT_OF_RANGE_BOUNDS,
                    "fetch out of range",
                ));
            }
            let dataset = match range
                .fetch(
                    start_offset,
                    min(end_offset, range.confirm_offset()),
                    batch_max_bytes,
                )
                .await?
            {
                // Cause of only fetch one range in a time, so the dataset is partial
                FetchDataset::Full(blocks) => FetchDataset::Partial(blocks),
                FetchDataset::Mixin(blocks, objects) => FetchDataset::Mixin(blocks, objects),
                _ => {
                    error!(
                        "{}Fetch dataset should not be Partial or Overflow",
                        self.log_ident
                    );
                    return Err(EsError::new(
                        ErrorCode::UNEXPECTED,
                        "fetch dataset should not be Partial or Overflow",
                    ));
                }
            };
            Ok(dataset)
        } else {
            Err(EsError::new(
                ErrorCode::OFFSET_OUT_OF_RANGE_BOUNDS,
                "fetch out of range",
            ))
        }
    }

    async fn trim(&self, new_start_offset: u64) -> Result<(), EsError> {
        *self.start_offset.borrow_mut() = new_start_offset;
        {
            // Remove deleted ranges from ranges besides the last range.
            // Cause of we need the last range to create new range.
            let mut ranges = self.ranges.borrow_mut();
            if !ranges.is_empty() {
                let last_range_start_offset = *(ranges.last_entry().unwrap().key());
                ranges.retain(|range_start_offset, range| {
                    *range_start_offset == last_range_start_offset
                        || range
                            .metadata()
                            .end()
                            .map(|end_offset| end_offset > new_start_offset)
                            .unwrap_or(true)
                });
            }
        }
        self.get_client()?
            .trim_stream(self.id, self.epoch, new_start_offset)
            .await
    }

    async fn delete(&self) -> Result<(), EsError> {
        self.ranges.borrow_mut().clear();
        let _ = self.last_range.borrow_mut().take();
        self.get_client()?.delete_stream(self.id, self.epoch).await
    }
}

struct StreamAppendRequest {
    base_offset: u64,
    record_batch: RecordBatch,
    append_tx: RefCell<OnceCell<oneshot::Sender<Result<(), EsError>>>>,
}

impl StreamAppendRequest {
    pub fn new(
        base_offset: u64,
        record_batch: RecordBatch,
        append_tx: oneshot::Sender<Result<(), EsError>>,
    ) -> Self {
        let append_tx_cell = OnceCell::new();
        let _ = append_tx_cell.set(append_tx);
        Self {
            base_offset,
            record_batch,
            append_tx: RefCell::new(append_tx_cell),
        }
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn count(&self) -> u32 {
        self.record_batch.last_offset_delta() as u32
    }

    pub fn success(&self) {
        if let Some(append_tx) = self.append_tx.borrow_mut().take() {
            let _ = append_tx.send(Ok(()));
        }
    }

    pub fn fail(&self, err: EsError) {
        if let Some(append_tx) = self.append_tx.borrow_mut().take() {
            let _ = append_tx.send(Err(err));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use bytes::BytesMut;
    use client::client::MockClient;
    use model::{range::RangeMetadata, stream::StreamMetadata};

    use crate::stream::{
        records_block::RecordsBlock,
        replication_range::{
            record_batch_to_bytes, vec_bytes_to_bytes, AckCallback, MockReplicationRange,
        },
    };

    use super::*;

    #[test]
    fn test_open() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async move {
            let mut client = MockClient::new();
            client
                .expect_update_stream()
                .returning(|_, _, _, _| Ok(StreamMetadata::default()));
            client.expect_list_ranges().returning(|_| {
                Ok(vec![
                    RangeMetadata::new(0, 0, 0, 0, Some(100)),
                    RangeMetadata::new(0, 1, 1, 100, None),
                ])
            });
            let client = Rc::new(client);
            let range_new_context = MockReplicationRange::new_context();
            range_new_context.expect().returning(|m, _, _, _, _| {
                if m.index() == 0 {
                    let mut range0: MockReplicationRange<MockClient> =
                        MockReplicationRange::default();
                    range0.expect_metadata().times(0).return_const(m);
                    range0.expect_seal().times(0);
                    Rc::new(range0)
                } else if m.index() == 1 {
                    let mut range1: MockReplicationRange<MockClient> =
                        MockReplicationRange::default();
                    range1.expect_metadata().times(1).return_const(m);
                    range1.expect_seal().times(2).returning(|| Ok(200));
                    range1.expect_confirm_offset().times(1).returning(|| 200);
                    Rc::new(range1)
                } else {
                    panic!("unexpected range index")
                }
            });
            let stream: Rc<ReplicationStream<MockReplicationRange<MockClient>, MockClient>> =
                ReplicationStream::new(0, 1, Rc::downgrade(&client), Rc::new(HotCache::new(4096)));
            stream.open().await.unwrap();
            assert_eq!(0, stream.start_offset());
            assert_eq!(200, stream.confirm_offset());
            stream.close().await;
        });
        Ok(())
    }

    #[test]
    fn test_append() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async move {
            let mut client = MockClient::new();
            client
                .expect_update_stream()
                .returning(|_, _, _, _| Ok(StreamMetadata::default()));
            client
                .expect_list_ranges()
                .returning(|_| Ok(vec![RangeMetadata::new(0, 0, 0, 0, Some(100))]));
            let client = Rc::new(client);
            let stream: Rc<ReplicationStream<MemoryReplicationRange, MockClient>> =
                ReplicationStream::new(0, 1, Rc::downgrade(&client), Rc::new(HotCache::new(4096)));
            stream.open().await.unwrap();
            assert_eq!(1, stream.ranges.borrow().len());
            let offset = stream.append(new_record(1)).await.unwrap();
            assert_eq!(100, offset);
            let offset = stream.append(new_record(1)).await.unwrap();
            assert_eq!(101, offset);
            assert_eq!(2, stream.ranges.borrow().len());
            {
                // fail the last range.
                stream
                    .last_range
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .writable
                    .replace(false);
            }
            let offset = stream.append(new_record(1)).await.unwrap();
            assert_eq!(102, offset);
            assert_eq!(3, stream.ranges.borrow().len());
            assert_eq!(103, stream.confirm_offset());
            assert_eq!(103, stream.next_offset());

            {
                // fail the last range and fence the stream.
                stream
                    .last_range
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .writable
                    .replace(false);
                unsafe { FENCED = true };
            }
            assert_eq!(
                ErrorCode::EXPIRED_STREAM_EPOCH,
                stream.append(new_record(1)).await.unwrap_err().code
            );
            unsafe { FENCED = false };
            assert_eq!(
                ErrorCode::EXPIRED_STREAM_EPOCH,
                stream.append(new_record(1)).await.unwrap_err().code
            );
        });
        Ok(())
    }

    #[test]
    fn test_fetch() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async move {
            let mut client = MockClient::new();
            client
                .expect_update_stream()
                .returning(|_, _, _, _| Ok(StreamMetadata::default()));
            client.expect_list_ranges().returning(|_| Ok(vec![]));
            let client = Rc::new(client);
            let stream: Rc<ReplicationStream<MemoryReplicationRange, MockClient>> =
                ReplicationStream::new(0, 1, Rc::downgrade(&client), Rc::new(HotCache::new(4096)));
            stream.open().await.unwrap();
            let _ = stream.append(new_record(1)).await.unwrap();
            let _ = stream.append(new_record(1)).await.unwrap();
            {
                // fail the last range.
                stream
                    .last_range
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .writable
                    .replace(false);
            }

            // slow path
            let _ = stream.append(new_record(1)).await.unwrap();
            let rst = stream.fetch(0, 1, 1024).await.unwrap();
            let blocks = get_records_blocks(rst);
            assert_eq!(1, blocks.len());
            assert_eq!(0, blocks[0].start_offset());
            assert_eq!(1, blocks[0].end_offset());
            let rst = stream.fetch(0, 2, 1024).await.unwrap();
            let blocks = get_records_blocks(rst);
            assert_eq!(1, blocks.len());
            assert_eq!(0, blocks[0].start_offset());
            assert_eq!(2, blocks[0].end_offset());
            // only return first range
            let rst = stream.fetch(0, 3, 1024).await.unwrap();
            let blocks = get_records_blocks(rst);
            assert_eq!(1, blocks.len());
            assert_eq!(0, blocks[0].start_offset());
            assert_eq!(2, blocks[0].end_offset());

            // fast path
            let rst = stream.fetch(2, 3, 1024).await.unwrap();
            let blocks = get_records_blocks(rst);
            assert_eq!(1, blocks.len());
            assert_eq!(2, blocks[0].start_offset());
            assert_eq!(3, blocks[0].end_offset());

            // out of bound
            if let Err(e) = stream.fetch(0, 4, 1024).await {
                assert_eq!(ErrorCode::OFFSET_OUT_OF_RANGE_BOUNDS, e.code);
            } else {
                panic!("fetch out of bound should fail")
            }

            if let Err(e) = stream.fetch(4, 10, 1024).await {
                assert_eq!(ErrorCode::OFFSET_OUT_OF_RANGE_BOUNDS, e.code);
            } else {
                panic!("fetch out of bound should fail")
            }
        });
        Ok(())
    }

    #[test]
    fn test_trim() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async move {
            let mut client = MockClient::new();
            client
                .expect_update_stream()
                .returning(|_, _, _, _| Ok(StreamMetadata::default()));
            client.expect_list_ranges().returning(|_| {
                Ok(vec![
                    RangeMetadata::new(0, 0, 0, 0, Some(100)),
                    RangeMetadata::new(0, 1, 0, 100, Some(200)),
                ])
            });
            client.expect_trim_stream().returning(|_, _, _| Ok(()));

            let client = Rc::new(client);
            let stream: Rc<ReplicationStream<MemoryReplicationRange, MockClient>> =
                ReplicationStream::new(0, 1, Rc::downgrade(&client), Rc::new(HotCache::new(4096)));
            stream.open().await.unwrap();
            assert_eq!(2, stream.ranges.borrow().len());
            assert_eq!(0, stream.start_offset());
            stream.trim(150).await.unwrap();
            assert_eq!(1, stream.ranges.borrow().len());
            assert_eq!(150, stream.start_offset());
        });
        Ok(())
    }

    #[test]
    fn test_delete() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async move {
            let mut client = MockClient::new();
            client
                .expect_update_stream()
                .returning(|_, _, _, _| Ok(StreamMetadata::default()));
            client.expect_list_ranges().returning(|_| {
                Ok(vec![
                    RangeMetadata::new(0, 0, 0, 0, Some(100)),
                    RangeMetadata::new(0, 1, 0, 100, Some(200)),
                ])
            });
            client.expect_delete_stream().returning(|_, _| Ok(()));

            let client = Rc::new(client);
            let stream: Rc<ReplicationStream<MemoryReplicationRange, MockClient>> =
                ReplicationStream::new(0, 1, Rc::downgrade(&client), Rc::new(HotCache::new(4096)));
            stream.open().await.unwrap();
            stream.delete().await.unwrap();
            assert_eq!(0, stream.ranges.borrow().len());
            assert!(stream.last_range.borrow().is_none());
        });
        Ok(())
    }

    fn get_records_blocks(dataset: FetchDataset) -> Vec<RecordsBlock> {
        match dataset {
            FetchDataset::Full(blocks) => blocks,
            FetchDataset::Partial(blocks) => blocks,
            FetchDataset::Mixin(_, _) => todo!(),
            FetchDataset::Overflow(_) => todo!(),
        }
    }

    fn new_record(count: u32) -> RecordBatch {
        RecordBatch::new_builder()
            .with_stream_id(0)
            .with_range_index(0)
            .with_base_offset(0)
            .with_last_offset_delta(count as i32)
            .with_payload(BytesMut::zeroed(1).freeze())
            .build()
            .unwrap()
    }

    struct MemoryReplicationRange {
        metadata: RangeMetadata,
        records: HotCache,
        next_offset: RefCell<u64>,
        confirm_offset: RefCell<u64>,
        writable: RefCell<bool>,
        ack: AckCallback,
    }

    static mut FENCED: bool = false;

    impl ReplicationRange<MockClient> for MemoryReplicationRange {
        async fn create(
            _client: Rc<MockClient>,
            stream_id: u64,
            epoch: u64,
            index: u32,
            start_offset: u64,
        ) -> Result<RangeMetadata, EsError> {
            if unsafe { FENCED } {
                Err(EsError::new(
                    ErrorCode::EXPIRED_STREAM_EPOCH,
                    "test mock error",
                ))
            } else {
                Ok(RangeMetadata::new(
                    stream_id,
                    index as i32,
                    epoch,
                    start_offset,
                    None,
                ))
            }
        }

        fn new(
            metadata: RangeMetadata,
            open_for_write: bool,
            ack_callback: AckCallback,
            _client: Weak<MockClient>,
            _cache: Rc<HotCache>,
        ) -> Rc<Self> {
            let confirm_offset = metadata.end().unwrap_or(metadata.start());
            Rc::new(MemoryReplicationRange {
                metadata,
                records: HotCache::new(1024 * 1024),
                next_offset: RefCell::new(confirm_offset),
                confirm_offset: RefCell::new(confirm_offset),
                writable: RefCell::new(open_for_write),
                ack: ack_callback,
            })
        }

        fn metadata(&self) -> &RangeMetadata {
            &self.metadata
        }

        fn append(&self, record_batch: &RecordBatch, context: RangeAppendContext) {
            let base_offset = context.base_offset;
            let last_offset_delta = record_batch.last_offset_delta() as u32;
            *self.next_offset.borrow_mut() =
                context.base_offset + record_batch.last_offset_delta() as u64;
            let flat_record_batch_bytes = record_batch_to_bytes(
                record_batch,
                &context,
                self.metadata().stream_id(),
                self.metadata().index() as u32,
            );
            self.records.insert(
                self.metadata.stream_id(),
                base_offset,
                last_offset_delta,
                vec![vec_bytes_to_bytes(&flat_record_batch_bytes)],
            );
            *self.next_offset.borrow_mut() =
                context.base_offset + record_batch.last_offset_delta() as u64;
            *self.confirm_offset.borrow_mut() =
                context.base_offset + record_batch.last_offset_delta() as u64;
            (self.ack)();
        }

        async fn fetch(
            &self,
            start_offset: u64,
            end_offset: u64,
            batch_max_bytes: u32,
        ) -> Result<FetchDataset, EsError> {
            Ok(FetchDataset::Full(vec![self.records.get_block(
                self.metadata.stream_id(),
                start_offset,
                end_offset,
                batch_max_bytes,
            )]))
        }

        fn try_ack(&self) {
            todo!()
        }

        async fn seal(&self) -> Result<u64, EsError> {
            self.writable.replace(false);
            Ok(self.confirm_offset())
        }

        fn is_sealed(&self) -> bool {
            todo!()
        }

        fn is_writable(&self) -> bool {
            *self.writable.borrow()
        }

        fn start_offset(&self) -> u64 {
            self.metadata.start()
        }

        fn confirm_offset(&self) -> u64 {
            *self.confirm_offset.borrow()
        }
    }
}
