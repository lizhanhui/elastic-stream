use std::{
    cell::RefCell,
    cmp::Ordering,
    rc::{Rc, Weak},
    time::Instant,
};

use bytes::{Bytes, BytesMut};
use client::client::Client;
use itertools::Itertools;
use log::{debug, error, info, warn};

use model::record::{flat_record::FlatRecordBatch, RecordBatch};
use model::{error::EsError, range::RangeMetadata};

use tokio::sync::broadcast;

use super::{
    cache::HotCache, metrics::METRICS, records_block::RecordsBlock,
    replication_replica::ReplicationReplica, FetchDataset,
};

use protocol::rpc::header::{ErrorCode, SealKind};

#[cfg(test)]
use mockall::automock;

pub(crate) type AckCallback = Box<dyn Fn()>;

#[cfg_attr(test, automock)]
pub(crate) trait ReplicationRange<C>
where
    C: Client + 'static,
{
    async fn create(
        client: Rc<C>,
        stream_id: i64,
        epoch: u64,
        index: i32,
        start_offset: u64,
    ) -> Result<RangeMetadata, EsError>;

    fn new(
        metadata: RangeMetadata,
        open_for_write: bool,
        ack_callback: AckCallback,
        client: Weak<C>,
        cache: Rc<HotCache>,
    ) -> Rc<Self>;

    fn metadata(&self) -> &RangeMetadata;

    fn append(&self, record_batch: &RecordBatch, context: RangeAppendContext);

    async fn fetch(
        &self,
        start_offset: u64,
        end_offset: u64,
        batch_max_bytes: u32,
    ) -> Result<FetchDataset, EsError>;

    fn try_ack(&self);

    async fn seal(&self) -> Result<u64, EsError>;

    fn is_sealed(&self) -> bool;

    fn is_writable(&self) -> bool;

    fn start_offset(&self) -> u64;

    fn confirm_offset(&self) -> u64;
}

const CORRUPTED_FLAG: u32 = 1 << 0;
const SEALING_FLAG: u32 = 1 << 1;
const SEALED_FLAG: u32 = 1 << 2;

pub(crate) struct DefaultReplicationRange<Replica, C>
where
    Replica: ReplicationReplica<C> + 'static,
    C: Client + 'static,
{
    log_ident: String,

    weak_self: Weak<Self>,

    metadata: RangeMetadata,

    client: Weak<C>,

    cache: Rc<HotCache>,

    replicas: Rc<Vec<Rc<Replica>>>,

    /// Exclusive confirm offset.
    confirm_offset: RefCell<u64>,
    next_offset: RefCell<u64>,
    /// If range is created by current stream, then open_for_write is true.
    open_for_write: bool,
    /// Range status.
    status: RefCell<u32>,
    seal_task_tx: Rc<broadcast::Sender<Result<u64, Rc<EsError>>>>,

    sticky_read_index: RefCell<u64>,

    ack_callback: Box<dyn Fn() + 'static>,
}

impl<Replica, C> DefaultReplicationRange<Replica, C>
where
    Replica: ReplicationReplica<C> + 'static,
    C: Client + 'static,
{
    fn calculate_confirm_offset(&self) -> Result<u64, EsError> {
        Self::calculate_confirm_offset0(&self.replicas, &self.metadata)
    }

    fn calculate_confirm_offset0(
        replicas: &Rc<Vec<Rc<Replica>>>,
        metadata: &RangeMetadata,
    ) -> Result<u64, EsError> {
        if replicas.is_empty() {
            return Err(EsError::new(
                ErrorCode::REPLICA_NOT_ENOUGH,
                "cal confirm offset fail, replicas is empty",
            ));
        }

        // Example1: replicas confirmOffset = [1, 2, 3]
        // - when replica_count=3 and ack_count = 1, then result confirm offset = 3.
        // - when replica_count=3 and ack_count = 2, then result confirm offset = 2.
        // - when replica_count=3 and ack_count = 3, then result confirm offset = 1.
        // Example2: replicas confirmOffset = [1, corrupted, 3]
        // - when replica_count=3 and ack_count = 1, then result confirm offset = 3.
        // - when replica_count=3 and ack_count = 2, then result confirm offset = 1.
        // - when replica_count=3 and ack_count = 3, then result is EsError.
        let confirm_offset_index = metadata.ack_count() - 1;
        replicas
            .iter()
            .filter(|r| !r.corrupted())
            .map(|r| r.confirm_offset())
            .sorted()
            .rev() // Descending order
            .nth(confirm_offset_index as usize)
            .ok_or(EsError::new(
                ErrorCode::REPLICA_NOT_ENOUGH,
                "cal confirm offset fail, replicas is not enough",
            ))
    }

    async fn placement_driver_seal(&self, end_offset: u64) -> Result<(), EsError> {
        let client = self.get_client()?;
        let mut metadata = self.metadata.clone();
        metadata.set_end(end_offset);
        match client
            .seal(None, SealKind::PLACEMENT_DRIVER, metadata)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                error!(
                    "{}Request pd seal with end_offset[{end_offset}] fail, err: {e}",
                    self.log_ident
                );
                Err(e)
            }
        }
    }

    async fn replicas_seal(
        log_ident: &String,
        replicas: Rc<Vec<Rc<Replica>>>,
        replica_count: u8,
        ack_count: u8,
        end_offset: Option<u64>,
    ) -> Result<u64, EsError> {
        let end_offsets = Rc::new(RefCell::new(Vec::<u64>::new()));
        let mut seal_tasks = vec![];
        let replicas = replicas.clone();
        for replica in replicas.iter() {
            let end_offsets = end_offsets.clone();
            let replica = replica.clone();
            seal_tasks.push(monoio::spawn(async move {
                if let Ok(replica_end_offset) = replica.seal(end_offset).await {
                    (*end_offsets).borrow_mut().push(replica_end_offset);
                }
            }));
        }
        for task in seal_tasks {
            let _ = task.await;
        }
        let end_offsets = end_offsets.take();
        replicas_seal0(&end_offsets, replica_count, ack_count, log_ident)
    }

    fn mark_sealed(&self) {
        *self.status.borrow_mut() |= SEALED_FLAG;
        self.erase_sealing();
    }

    fn is_sealing(&self) -> bool {
        *self.status.borrow() & SEALING_FLAG != 0
    }

    fn mark_sealing(&self) {
        *self.status.borrow_mut() |= SEALING_FLAG;
    }

    fn erase_sealing(&self) {
        *self.status.borrow_mut() &= !SEALING_FLAG;
    }

    fn mark_corrupted(&self) {
        *self.status.borrow_mut() |= CORRUPTED_FLAG;
    }

    fn get_client(&self) -> Result<Rc<C>, EsError> {
        self.client.upgrade().ok_or(EsError::new(
            ErrorCode::UNEXPECTED,
            "range get client fail, client is dropped",
        ))
    }
}

fn replicas_seal0(
    end_offsets: &Vec<u64>,
    replica_count: u8,
    ack_count: u8,
    log_ident: &String,
) -> Result<u64, EsError> {
    // Example1: replicas confirmOffset = [1, 2, 3]
    // - when replica_count=3 and ack_count = 1, must seal 3 replica success, the result end offset = 3.
    // - when replica_count=3 and ack_count = 2, must seal 2 replica success, the result end offset = 2.
    // - when replica_count=3 and ack_count = 3, must seal 1 replica success, the result end offset = 1.
    // Example2: replicas confirmOffset = [1, corrupted, 3]
    // - when replica_count=3 and ack_count = 1, must seal 3 replica success, the result is seal fail Err.
    // - when replica_count=3 and ack_count = 2, must seal 2 replica success, the result end offset = 3.
    // - when replica_count=3 and ack_count = 3, must seal 1 replica success, the result end offset = 1.
    // assume the corrupted replica with the largest end offset.
    let end_offset = end_offsets
        .iter()
        .sorted()
        .nth((replica_count - ack_count) as usize)
        .copied()
        .ok_or(EsError::new(
            ErrorCode::REPLICA_NOT_ENOUGH,
            "seal fail, replica is not enough",
        ));
    info!(
        "{}Replicas seal with end_offsets={end_offsets:?} and final end_offset={end_offset:?}",
        log_ident
    );
    end_offset
}

impl<Replica, C> ReplicationRange<C> for DefaultReplicationRange<Replica, C>
where
    Replica: ReplicationReplica<C> + 'static,
    C: Client + 'static,
{
    async fn create(
        client: Rc<C>,
        stream_id: i64,
        epoch: u64,
        index: i32,
        start_offset: u64,
    ) -> Result<RangeMetadata, EsError> {
        // 1. request placement driver to create range and get the range metadata.
        let mut metadata = RangeMetadata::new(stream_id, index, epoch, start_offset, None);
        metadata = client.create_range(metadata).await?;
        // 2. request range server to create range replica.
        let mut create_replica_tasks = vec![];
        for server in metadata.replica().iter() {
            let address = server.advertise_address.clone();
            let metadata = metadata.clone();
            let client = client.clone();
            create_replica_tasks.push(monoio::spawn(async move {
                client
                    .create_range_replica(&address, metadata)
                    .await
                    .map_err(|e| {
                        error!("Create range[{stream_id}#{index}] to range server[{address}] failed, err: {e}");
                        e
                    })
            }));
        }
        for task in create_replica_tasks {
            // if success replica is less than ack count, the stream append task will create new range triggered by append error.
            let _ = task.await;
        }
        // 3. return metadata
        Ok(metadata)
    }

    fn new(
        metadata: RangeMetadata,
        open_for_write: bool,
        ack_callback: Box<dyn Fn()>,
        client: Weak<C>,
        cache: Rc<HotCache>,
    ) -> Rc<Self> {
        let confirm_offset = metadata.end().unwrap_or_else(|| metadata.start());
        let status = if metadata.end().is_some() {
            SEALED_FLAG
        } else {
            0
        };

        let (seal_task_tx, _) = broadcast::channel::<Result<u64, Rc<EsError>>>(1);

        let log_ident = format!("Range[{}#{}] ", metadata.stream_id(), metadata.index());
        let mut this = Rc::new(Self {
            log_ident,
            weak_self: Weak::new(),
            metadata,
            open_for_write,
            client,
            cache,
            replicas: Rc::new(vec![]),
            confirm_offset: RefCell::new(confirm_offset),
            next_offset: RefCell::new(confirm_offset),
            status: RefCell::new(status),
            seal_task_tx: Rc::new(seal_task_tx),
            sticky_read_index: RefCell::new(0),
            ack_callback,
        });

        let mut replicators = Vec::with_capacity(this.metadata.replica().len());
        let weak_this = Rc::downgrade(&this);
        for replica_server in this.metadata.replica().iter() {
            let weak_this = weak_this.clone();
            let client = this.client.clone();
            replicators.push(Rc::new(Replica::new(
                this.metadata.clone(),
                replica_server.clone(),
                Box::new(move || {
                    if let Some(range) = weak_this.upgrade() {
                        range.try_ack();
                    }
                }),
                client,
            )));
        }
        // #Safety: the weak_self/replicators only changed(init) in range new.
        unsafe {
            Rc::get_mut_unchecked(&mut this).weak_self = Rc::downgrade(&this);
            Rc::get_mut_unchecked(&mut this).replicas = Rc::new(replicators);
        }
        info!(
            "Load range with metadata: {:?} open_for_write={open_for_write}",
            this.metadata
        );
        this
    }

    fn metadata(&self) -> &RangeMetadata {
        &self.metadata
    }

    fn append(&self, record_batch: &RecordBatch, context: RangeAppendContext) {
        let base_offset = context.base_offset;
        let last_offset_delta = record_batch.last_offset_delta() as u32;
        let next_offset = *self.next_offset.borrow();
        if next_offset != base_offset {
            error!(
                "{}Range append record batch with invalid base offset, expect: {}, actual: {}",
                self.log_ident, next_offset, base_offset
            );
            panic!("Range append record batch with invalid base offset");
        }
        *self.next_offset.borrow_mut() = context.base_offset + last_offset_delta as u64;
        let flat_record_batch_bytes = record_batch_to_bytes(
            record_batch,
            &context,
            self.metadata().stream_id() as u64,
            self.metadata().index() as u32,
        );
        self.cache.insert(
            self.metadata.stream_id() as u64,
            base_offset,
            last_offset_delta,
            // deep copy record batch bytes cause of replication directly use the bytes passed from frontend which
            // will be reused in future appends.
            vec![vec_bytes_to_bytes(&flat_record_batch_bytes)],
        );
        for replica in (*self.replicas).iter() {
            replica.append(
                flat_record_batch_bytes.clone(),
                base_offset,
                base_offset + last_offset_delta as u64,
            );
        }
    }

    async fn fetch(
        &self,
        start_offset: u64,
        end_offset: u64,
        batch_max_bytes: u32,
    ) -> Result<FetchDataset, EsError> {
        let now: Instant = Instant::now();
        let mut read_times = 0;
        let replicas_len = self.replicas.len();
        let mut last_read_err = None;
        loop {
            read_times += 1;
            if read_times > replicas_len {
                break;
            }
            let read_index = *self.sticky_read_index.borrow() % replicas_len as u64;
            let replica = &self.replicas[read_index as usize];

            let result = replica
                .fetch(start_offset, end_offset, batch_max_bytes)
                .await;
            let fetch_result = match result {
                Ok(rs) => rs,
                Err(e) => {
                    warn!("{}Fetch [{start_offset}, {end_offset}) with batch_max_bytes={batch_max_bytes} fail, err: {e}", self.log_ident);
                    last_read_err = Some(Err(e));
                    *self.sticky_read_index.borrow_mut() += 1;
                    continue;
                }
            };
            METRICS.with(|m| m.record_fetch_stream(now.elapsed().as_micros() as u64));
            // range server local records
            let local_records = fetch_result.payload.unwrap_or_default();
            let blocks = if !local_records.is_empty() {
                RecordsBlock::parse(local_records, 1024 * 1024, false)
                    .map_err(|e| {
                        error!(
                            "{}Fetch [{}, {}) decode fail, err: {}",
                            self.log_ident, start_offset, end_offset, e
                        );
                        EsError::new(ErrorCode::RECORDS_PARSE_ERROR, "parse records fail")
                    })
                    .map_err(|e| {
                        *self.sticky_read_index.borrow_mut() += 1;
                        e
                    })?
            } else {
                vec![RecordsBlock::empty_block(end_offset)]
            };
            return if let Some(object) = fetch_result.object_metadata_list {
                Ok(FetchDataset::Mixin(blocks, object))
            } else {
                Ok(FetchDataset::Full(blocks))
            };
        }
        last_read_err.unwrap_or_else(|| {
            Err(EsError::new(
                ErrorCode::ALL_REPLICAS_FETCH_FAILED,
                "all replicas fetch fail",
            ))
        })
    }

    /// update range confirm offset and invoke stream#try_ack.
    fn try_ack(&self) {
        if !self.is_writable() {
            return;
        }
        match self.calculate_confirm_offset() {
            Ok(new_confirm_offset) => {
                let mut confirm_offset = self.confirm_offset.borrow_mut();
                match new_confirm_offset.cmp(&confirm_offset) {
                    Ordering::Greater => {
                        *confirm_offset = new_confirm_offset;
                    }
                    _ => {
                        // the new_confirm_offset may be less than current offset, in under replicated scenario.
                        // replicas confirm offsets = [3, 2, 1], then confirm offset is 2
                        // replicas confirm offsets = [3, corrupted, 1], then confirm offset is 1
                        return;
                    }
                }
                (self.ack_callback)();
            }
            Err(err) => {
                warn!(
                    "{}Calculate confirm offset fail, current confirm_offset=[{}], err: {err}",
                    self.log_ident,
                    self.confirm_offset()
                );
                self.mark_corrupted();
                (self.ack_callback)();
            }
        }
    }

    async fn seal(&self) -> Result<u64, EsError> {
        if self.is_sealed() {
            // if range is already sealed, return confirm offset.
            return Ok(*(self.confirm_offset.borrow()));
        }
        if self.is_sealing() {
            // if range is sealing, wait for seal task to complete.
            self.seal_task_tx
                .subscribe()
                .recv()
                .await
                .map(|rst| {
                    rst.map_err(|e| {
                        warn!("{} The range seal fail, {}", self.log_ident, e);
                        EsError::new(e.code, &e.message)
                    })
                })
                .map_err(|_| {
                    error!("{}Seal task channel closed", self.log_ident);
                    EsError::new(ErrorCode::UNEXPECTED, "seal task channel closed")
                })?
        } else {
            self.mark_sealing();
            if self.open_for_write {
                // the range is open for write, it's ok to directly use memory confirm offset as range end offset.
                let end_offset = self.confirm_offset();
                // 1. call placement driver to seal range
                match self.placement_driver_seal(end_offset).await {
                    Ok(_) => {
                        info!("{}The range is created by current stream, then directly seal with memory confirm_offset=[{}]", self.log_ident, end_offset);
                        self.mark_sealed();
                        let _ = self.seal_task_tx.send(Ok(end_offset));
                        // 2. spawn task to async seal range replicas
                        let replicas = self.replicas.clone();
                        let replica_count = self.metadata.replica_count();
                        let ack_count = self.metadata.ack_count();
                        let log_ident = self.log_ident.clone();
                        let range = self.weak_self.upgrade();
                        monoio::spawn(async move {
                            if Self::replicas_seal(
                                &log_ident,
                                replicas,
                                replica_count,
                                ack_count,
                                Some(end_offset),
                            )
                            .await
                            .is_err()
                            {
                                debug!(
                                    "Failed to seal range-server after sealing placement-driver"
                                );
                            }
                            // keep range alive until seal task complete.
                            drop(range);
                        });
                        Ok(end_offset)
                    }
                    Err(e) => {
                        error!("{}Request pd seal fail, err: {e}", self.log_ident);
                        self.erase_sealing();
                        Err(e)
                    }
                }
            } else {
                // the range is created by old stream, it need to calculate end offset from replicas.
                let replicas = self.replicas.clone();
                // 1. seal range replicas and calculate end offset.
                match Self::replicas_seal(
                    &self.log_ident,
                    replicas,
                    self.metadata.replica_count(),
                    self.metadata.ack_count(),
                    None,
                )
                .await
                {
                    Ok(end_offset) => {
                        // 2. call placement driver to seal range.
                        info!("{}The range is created by other stream, then seal replicas to calculate end_offset=[{}]", self.log_ident, end_offset);
                        match self.placement_driver_seal(end_offset).await {
                            Ok(_) => {
                                self.mark_sealed();
                                *self.confirm_offset.borrow_mut() = end_offset;
                                let _ = self.seal_task_tx.send(Ok(end_offset));
                                Ok(end_offset)
                            }
                            Err(e) => {
                                error!("{}Request pd seal fail, err: {e}", self.log_ident);
                                self.erase_sealing();
                                Err(e)
                            }
                        }
                    }
                    Err(e) => {
                        self.erase_sealing();
                        Err(e)
                    }
                }
            }
        }
    }

    fn is_sealed(&self) -> bool {
        *self.status.borrow() & SEALED_FLAG != 0
    }

    fn is_writable(&self) -> bool {
        *self.status.borrow() == 0 && self.open_for_write
    }

    fn start_offset(&self) -> u64 {
        self.metadata.start()
    }

    fn confirm_offset(&self) -> u64 {
        *(self.confirm_offset.borrow())
    }
}

pub struct RangeAppendContext {
    pub(crate) base_offset: u64,
}

impl RangeAppendContext {
    pub fn new(base_offset: u64) -> Self {
        Self { base_offset }
    }
}

pub(crate) fn vec_bytes_to_bytes(vec_bytes: &Vec<Bytes>) -> Bytes {
    let mut size = 0;
    for bytes in vec_bytes.iter() {
        size += bytes.len();
    }
    let mut bytes_mut = BytesMut::with_capacity(size);
    for bytes in vec_bytes {
        bytes_mut.extend_from_slice(&bytes[..]);
    }
    bytes_mut.freeze()
}

pub(crate) fn record_batch_to_bytes(
    record_batch: &RecordBatch,
    context: &RangeAppendContext,
    stream_id: u64,
    range_index: u32,
) -> Vec<Bytes> {
    let base_offset = context.base_offset;
    let mut record_batch_builder = RecordBatch::new_builder()
        .with_stream_id(stream_id as i64)
        // use current range index.
        .with_range_index(range_index as i32)
        .with_flags(record_batch.flags())
        // use base_offset from context.
        .with_base_offset(base_offset as i64)
        .with_last_offset_delta(record_batch.last_offset_delta() as i32)
        .with_base_timestamp(record_batch.base_timestamp())
        .with_payload(record_batch.payload());
    if let Some(properties) = record_batch.properties() {
        for kv in properties.iter() {
            record_batch_builder =
                record_batch_builder.with_property(kv.key.clone(), kv.value.clone());
        }
    }
    let record_batch = record_batch_builder.build().expect("valid record batch");
    let flat_record_batch: FlatRecordBatch = Into::into(record_batch);
    let (flat_record_batch_bytes, _) = flat_record_batch.encode();
    flat_record_batch_bytes
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use client::client::MockClient;
    use model::{range_server::RangeServer, response::fetch::FetchResultSet};
    use protocol::rpc::header::RangeServerState;

    use crate::stream::replication_replica::MockReplicationReplica;

    use super::*;

    use lazy_static::lazy_static;
    use std::sync::{Mutex, MutexGuard};

    lazy_static! {
        static ref MTX: Mutex<()> = Mutex::new(());
    }

    fn get_lock(m: &'static Mutex<()>) -> MutexGuard<'static, ()> {
        match m.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    type Range = DefaultReplicationRange<MockReplicationReplica<MockClient>, MockClient>;

    #[test]
    pub fn test_calculate_confirm_offset() -> Result<(), Box<dyn Error>> {
        let metadata = RangeMetadata::new_range(0, 1, 2, 0, None, 3, 2);
        let replicas = vec![
            new_replica(1, false),
            new_replica(2, false),
            new_replica(3, false),
        ];
        let replicas = Rc::new(replicas.into_iter().map(Rc::new).collect());
        assert_eq!(
            2,
            DefaultReplicationRange::<MockReplicationReplica<MockClient>, MockClient>::calculate_confirm_offset0(&replicas, &metadata).unwrap()
        );

        let replicas = vec![
            new_replica(1, false),
            new_replica(2, true),
            new_replica(3, false),
        ];
        let replicas = Rc::new(replicas.into_iter().map(Rc::new).collect());
        assert_eq!(
            1,
            DefaultReplicationRange::<MockReplicationReplica<MockClient>, MockClient>::calculate_confirm_offset0(&replicas, &metadata).unwrap()
        );

        let replicas = vec![
            new_replica(1, true),
            new_replica(2, true),
            new_replica(3, false),
        ];
        let replicas = Rc::new(replicas.into_iter().map(Rc::new).collect());
        assert_eq!(ErrorCode::REPLICA_NOT_ENOUGH, DefaultReplicationRange::<MockReplicationReplica<MockClient>, MockClient>::calculate_confirm_offset0(&replicas, &metadata).unwrap_err().code);

        Ok(())
    }

    #[test]
    fn test_try_ack() {
        let metadata = RangeMetadata::new_range(0, 1, 2, 233, None, 1, 1);
        let ack_callback_invoke_times = Rc::new(RefCell::new(0));
        let mut range = new_range(
            metadata,
            true,
            ack_callback_invoke_times.clone(),
            Rc::new(MockClient::default()),
        );
        {
            let replica = get_replica(&mut range, 0);
            replica.expect_corrupted().times(1).returning(|| false);
            replica
                .expect_confirm_offset()
                .times(1)
                .returning(|| 250_u64);
        }
        assert_eq!(233, range.start_offset());
        assert_eq!(233, range.confirm_offset());
        assert!(range.is_writable());
        assert_eq!(0, *ack_callback_invoke_times.borrow());
        range.try_ack();
        assert_eq!(250, range.confirm_offset());
        assert_eq!(1, *ack_callback_invoke_times.borrow());

        // confirm offset rewind back
        {
            let replica = get_replica(&mut range, 0);
            replica.expect_corrupted().times(1).returning(|| false);
            replica
                .expect_confirm_offset()
                .times(1)
                .returning(|| 240_u64);
        }
        range.try_ack();
        assert_eq!(250, range.confirm_offset());
        assert_eq!(1, *ack_callback_invoke_times.borrow());

        // corrupted
        {
            let replica = get_replica(&mut range, 0);
            replica.expect_corrupted().times(1).returning(|| true);
        }
        assert!(range.is_writable());
        range.try_ack();
        assert_eq!(250, range.confirm_offset());
        assert_eq!(2, *ack_callback_invoke_times.borrow());
        assert!(!range.is_writable());
    }

    #[test]
    fn test_append() {
        let metadata = RangeMetadata::new_range(0, 1, 2, 233, None, 1, 1);
        let ack_callback_invoke_times = Rc::new(RefCell::new(0));
        let mut range = new_range(
            metadata,
            true,
            ack_callback_invoke_times.clone(),
            Rc::new(MockClient::default()),
        );

        {
            let replica = get_replica(&mut range, 0);
            replica.expect_append().times(1).return_const(());
        }

        let record_batch = new_record(233, 10);
        range.append(&record_batch, RangeAppendContext::new(233));
        assert_eq!(243, *range.next_offset.borrow());
        {
            let replica = get_replica(&mut range, 0);
            replica.checkpoint();
        }
    }

    #[monoio::test]
    async fn test_fetch() -> Result<(), Box<dyn Error>> {
        let metadata = RangeMetadata::new_range(0, 1, 2, 233, None, 2, 2);
        let ack_callback_invoke_times = Rc::new(RefCell::new(0));
        let mut range = new_range(
            metadata,
            true,
            ack_callback_invoke_times,
            Rc::new(MockClient::default()),
        );

        assert_eq!(0, *range.sticky_read_index.borrow());

        // replica0 fetch fail, replica fetch success
        {
            let replica0 = get_replica(&mut range, 0);
            replica0
                .expect_fetch()
                .times(1)
                .returning(|_, _, _| Err(EsError::unexpected("test mock error")));
            let replica1 = get_replica(&mut range, 1);
            replica1.expect_fetch().times(1).returning(|_, _, _| {
                let record = new_record(233, 10);
                let payload = record_batch_to_bytes(&record, &RangeAppendContext::new(233), 0, 1);
                let rst = FetchResultSet {
                    throttle: None,
                    object_metadata_list: None,
                    payload: Some(vec_bytes_to_bytes(&payload)),
                };
                Ok(rst)
            });
        }

        let dataset = range.fetch(233, 240, 1234).await.unwrap();
        assert_eq!(1, *range.sticky_read_index.borrow());
        if let FetchDataset::Full(blocks) = dataset {
            assert_eq!(1, blocks.len());
            assert_eq!(233, blocks[0].start_offset());
            assert_eq!(243, blocks[0].end_offset());
        } else {
            panic!("fetch dataset is not full");
        }

        // replica1 return corrupted data
        {
            let replica1 = get_replica(&mut range, 1);
            replica1.expect_fetch().times(1).returning(|_, _, _| {
                let rst = FetchResultSet {
                    throttle: None,
                    object_metadata_list: None,
                    payload: Some(BytesMut::zeroed(10).freeze()),
                };
                Ok(rst)
            });
        }
        let rst = range.fetch(233, 240, 1234).await;
        assert_eq!(2, *range.sticky_read_index.borrow());
        assert_eq!(ErrorCode::RECORDS_PARSE_ERROR, rst.unwrap_err().code);
        Ok(())
    }

    #[monoio::test]
    async fn test_seal_with_range_created_by_current_stream() -> Result<(), Box<dyn Error>> {
        let metadata = RangeMetadata::new_range(0, 1, 2, 233, None, 1, 1);
        let ack_callback_invoke_times = Rc::new(RefCell::new(0));
        let mut client = MockClient::default();
        client
            .expect_seal()
            .times(1)
            .returning(|_target, kind, meta| match kind {
                SealKind::PLACEMENT_DRIVER => {
                    assert_eq!(240, meta.end().unwrap());
                    Ok(meta)
                }
                _ => panic!("unexpected seal kind: {:?}", kind),
            });
        let client = Rc::new(client);
        let mut range = new_range(metadata, true, ack_callback_invoke_times, client.clone());
        {
            let replica = get_replica(&mut range, 0);
            replica
                .expect_seal()
                .returning(|_| Err(EsError::unexpected("test mock error")));
        }
        *range.confirm_offset.borrow_mut() = 240;
        assert_eq!(240, range.seal().await.unwrap());
        assert!(range.is_sealed());
        assert!(!range.is_writable());
        // repeated seal
        assert_eq!(240, range.seal().await.unwrap());
        Ok(())
    }

    #[monoio::test]
    async fn test_seal_with_range_created_by_old_stream() -> Result<(), Box<dyn Error>> {
        let metadata = RangeMetadata::new_range(0, 1, 2, 233, None, 1, 1);
        let ack_callback_invoke_times = Rc::new(RefCell::new(0));
        let mut client = MockClient::default();
        client
            .expect_seal()
            .times(1)
            .returning(|_target, kind, meta| match kind {
                SealKind::PLACEMENT_DRIVER => {
                    assert_eq!(240, meta.end().unwrap());
                    Ok(meta)
                }
                _ => panic!("unexpected seal kind: {:?}", kind),
            });
        let client = Rc::new(client);
        let mut range = new_range(metadata, false, ack_callback_invoke_times, client.clone());
        {
            let replica = get_replica(&mut range, 0);
            replica
                .expect_seal()
                .times(1)
                .returning(|_| Err(EsError::unexpected("test mock error")));
        }
        let rst = range.seal().await;
        assert_eq!(ErrorCode::REPLICA_NOT_ENOUGH, rst.unwrap_err().code);
        assert!(!range.is_sealed());

        {
            let replica = get_replica(&mut range, 0);
            replica.expect_seal().times(1).returning(|_| Ok(240));
        }
        assert_eq!(240, range.seal().await.unwrap());
        assert!(range.is_sealed());
        assert!(!range.is_writable());
        Ok(())
    }

    #[test]
    fn test_replica_seal0() {
        // Example1: replicas confirmOffset = [1, 2, 3]
        // - when replica_count=3 and ack_count = 1, must seal 3 replica success, the result end offset = 3.
        // - when replica_count=3 and ack_count = 2, must seal 2 replica success, the result end offset = 2.
        // - when replica_count=3 and ack_count = 3, must seal 1 replica success, the result end offset = 1.
        // Example2: replicas confirmOffset = [1, corrupted, 3]
        // - when replica_count=3 and ack_count = 1, must seal 3 replica success, the result is seal fail Err.
        // - when replica_count=3 and ack_count = 2, must seal 2 replica success, the result end offset = 3.
        // - when replica_count=3 and ack_count = 3, must seal 1 replica success, the result end offset = 1.
        // assume the corrupted replica with the largest end offset.
        let test_ident = "test_ident".to_string();
        assert_eq!(
            3,
            replicas_seal0(&vec![1, 2, 3], 3, 1, &test_ident).unwrap()
        );
        assert_eq!(
            2,
            replicas_seal0(&vec![1, 2, 3], 3, 2, &test_ident).unwrap()
        );
        assert_eq!(
            1,
            replicas_seal0(&vec![1, 2, 3], 3, 3, &test_ident).unwrap()
        );
        assert_eq!(
            ErrorCode::REPLICA_NOT_ENOUGH,
            replicas_seal0(&vec![1, 3], 3, 1, &test_ident)
                .unwrap_err()
                .code
        );
        assert_eq!(3, replicas_seal0(&vec![1, 3], 3, 2, &test_ident).unwrap());
        assert_eq!(1, replicas_seal0(&vec![1, 3], 3, 3, &test_ident).unwrap());
    }

    fn new_range(
        mut metadata: RangeMetadata,
        open_for_write: bool,
        ack_callback_invoke_times: Rc<RefCell<i32>>,
        client: Rc<MockClient>,
    ) -> Rc<Range> {
        // static method invoke must protected by synchronization, see https://docs.rs/mockall/latest/mockall/#static-methods
        let _m = get_lock(&MTX);
        let new_context = MockReplicationReplica::new_context();
        new_context
            .expect()
            .returning(|_, _, _, _| MockReplicationReplica::<MockClient>::default());
        for i in 0..metadata.replica_count() {
            metadata.replica_mut().push(RangeServer::new(
                233 + i as i32,
                "addr",
                RangeServerState::RANGE_SERVER_STATE_READ_WRITE,
            ));
        }
        DefaultReplicationRange::<MockReplicationReplica<MockClient>, MockClient>::new(
            metadata,
            open_for_write,
            Box::new(move || {
                *ack_callback_invoke_times.borrow_mut() += 1;
            }),
            Rc::downgrade(&client),
            Rc::new(HotCache::new(1024 * 1024)),
        )
    }

    fn get_replica(range: &mut Rc<Range>, index: u32) -> &mut MockReplicationReplica<MockClient> {
        unsafe {
            let range = Rc::get_mut_unchecked(range);
            let replicas = Rc::get_mut_unchecked(&mut range.replicas);
            let replica = Rc::get_mut_unchecked(&mut replicas[index as usize]);
            replica
        }
    }

    fn new_replica(confirm_offset: u64, corrupted: bool) -> MockReplicationReplica<MockClient> {
        let mut replica = MockReplicationReplica::default();
        replica.expect_corrupted().return_const(corrupted);
        replica.expect_confirm_offset().return_const(confirm_offset);
        replica
    }

    fn new_record(base_offset: u64, count: u32) -> RecordBatch {
        RecordBatch::new_builder()
            .with_stream_id(0)
            .with_range_index(0)
            .with_base_offset(base_offset as i64)
            .with_last_offset_delta(count as i32)
            .with_payload(BytesMut::zeroed(1).freeze())
            .build()
            .unwrap()
    }
}
