use client::DefaultClient;
use config::{Configuration, ObjectStorageConfig};
use log::{debug, info};
use model::object::ObjectMetadata;
use monoio::time::sleep;
use opendal::services::{Fs, S3};
use opendal::Operator;
use pd_client::pd_client::DefaultPlacementDriverClient;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
};
use std::{env, thread};
use store::Store;
use tokio::sync::{mpsc, oneshot};

use crate::object_manager::DefaultObjectManager;
use crate::range_accumulator::{DefaultRangeAccumulator, RangeAccumulator};
use crate::range_fetcher::{DefaultRangeFetcher, RangeFetcher};
use crate::range_offload::RangeOffload;
use crate::{
    shutdown_chan, ObjectManager, ObjectStorage, OffloadProgressListener, OwnerEvent, ShutdownRx,
    ShutdownTx,
};
use crate::{Owner, RangeKey};

#[derive(Clone)]
pub struct AsyncObjectStorage {
    tx: mpsc::UnboundedSender<Task>,
}

impl AsyncObjectStorage {
    pub fn new<S>(config: &Configuration, store: S) -> Self
    where
        S: Store + Send + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let config = config.clone();
        let _ = thread::Builder::new()
            .name("ObjectStorage".to_owned())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let range_fetcher = DefaultRangeFetcher::new(Rc::new(store));
                    let client = Rc::new(DefaultClient::new(Arc::new(config.clone())));
                    let client = Rc::new(DefaultPlacementDriverClient::new(client));
                    let object_manager = DefaultObjectManager::new(
                        &config.object_storage.cluster,
                        client,
                        config.server.server_id,
                    );
                    let object_storage = DefaultObjectStorage::new(
                        &config.object_storage,
                        range_fetcher,
                        object_manager,
                    );
                    while let Some(task) = rx.recv().await {
                        match task {
                            Task::NewCommit(stream_id, range_index, record_size) => {
                                object_storage.new_commit(stream_id, range_index, record_size);
                            }
                            Task::GetObjects(task) => {
                                let object_storage = object_storage.clone();
                                monoio::spawn(async move {
                                    let rst = object_storage
                                        .get_objects(
                                            task.stream_id,
                                            task.range_index,
                                            task.start_offset,
                                            task.end_offset,
                                            task.size_hint,
                                        )
                                        .await;
                                    let _ = task.tx.send(rst);
                                });
                            }
                            Task::GetOffloadingRange(tx) => {
                                let object_storage = object_storage.clone();
                                monoio::spawn(async move {
                                    let rst = object_storage.get_offloading_range().await;
                                    let _ = tx.send(rst);
                                });
                            }
                            Task::Close(_) => {
                                let object_storage = object_storage.clone();
                                object_storage.close().await;
                                break;
                            }
                            Task::Watch(tx) => {
                                let object_storage = object_storage.clone();
                                monoio::spawn(async move {
                                    let rst = object_storage.watch_offload_progress().await;
                                    let _ = tx.send(rst);
                                });
                            }
                        }
                    }
                });
            });
        Self { tx }
    }
}

impl ObjectStorage for AsyncObjectStorage {
    fn new_commit(&self, stream_id: u64, range_index: u32, record_size: u32) {
        let _ = self
            .tx
            .send(Task::NewCommit(stream_id, range_index, record_size));
    }

    async fn get_objects(
        &self,
        stream_id: u64,
        range_index: u32,
        start_offset: u64,
        end_offset: u64,
        size_hint: u32,
    ) -> (Vec<ObjectMetadata>, bool) {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Task::GetObjects(GetObjectsTask {
            stream_id,
            range_index,
            start_offset,
            end_offset,
            size_hint,
            tx,
        }));
        rx.await.unwrap()
    }

    async fn get_offloading_range(&self) -> Vec<RangeKey> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Task::GetOffloadingRange(tx));
        rx.await.unwrap()
    }

    async fn close(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Task::Close(tx));
        let _ = rx.await;
    }

    async fn watch_offload_progress(&self) -> OffloadProgressListener {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Task::Watch(tx));
        rx.await.unwrap()
    }
}

enum Task {
    NewCommit(u64, u32, u32),
    GetObjects(GetObjectsTask),
    GetOffloadingRange(oneshot::Sender<Vec<RangeKey>>),
    Close(oneshot::Sender<()>),
    Watch(oneshot::Sender<OffloadProgressListener>),
}

struct GetObjectsTask {
    stream_id: u64,
    range_index: u32,
    start_offset: u64,
    end_offset: u64,
    size_hint: u32,
    tx: oneshot::Sender<(Vec<ObjectMetadata>, bool)>,
}

pub struct DefaultObjectStorage<F: RangeFetcher, M: ObjectManager> {
    config: ObjectStorageConfig,
    ranges: Rc<RefCell<HashMap<RangeKey, Rc<DefaultRangeAccumulator>>>>,
    part_full_ranges: RefCell<HashSet<RangeKey>>,
    cache_size: RefCell<i64>,
    op: Option<Operator>,
    object_manager: Rc<M>,
    range_fetcher: Rc<F>,
    shutdown_tx: ShutdownTx,
    shutdown_rx: RefCell<Option<ShutdownRx>>,
}

impl<F, M> DefaultObjectStorage<F, M>
where
    F: RangeFetcher + 'static,
    M: ObjectManager + 'static,
{
    fn add_range(&self, stream_id: u64, range_index: u32, owner: &Owner) {
        let op = if let Some(op) = self.op.as_ref() {
            op.clone()
        } else {
            return;
        };
        debug!("add range {stream_id}#{range_index} offload owner {owner:?}");
        let range = RangeKey::new(stream_id, range_index);
        let range_offload = Rc::new(RangeOffload::new(
            stream_id,
            range_index,
            owner.epoch,
            op,
            self.object_manager.clone(),
            &self.config,
        ));

        let shutdown_rx = if let Some(shutdown_rx) = self.shutdown_rx.borrow().as_ref() {
            shutdown_rx.clone()
        } else {
            return;
        };
        self.ranges.borrow_mut().insert(
            range,
            Rc::new(DefaultRangeAccumulator::new(
                range,
                owner.start_offset,
                self.range_fetcher.clone(),
                &self.config,
                range_offload,
                shutdown_rx,
            )),
        );
    }

    fn remove_range(&self, stream_id: u64, range_index: u32) {
        if self.op.is_none() {
            return;
        }
        debug!("remove range {stream_id}#{range_index} offload owner");
        let range = RangeKey::new(stream_id, range_index);
        let _ = self.ranges.borrow_mut().remove(&range);
    }
}

impl<F, M> ObjectStorage for DefaultObjectStorage<F, M>
where
    F: RangeFetcher + 'static,
    M: ObjectManager + 'static,
{
    fn new_commit(&self, stream_id: u64, range_index: u32, record_size: u32) {
        if self.op.is_none() {
            return;
        }
        let range_key = RangeKey::new(stream_id, range_index);
        let range = self.ranges.borrow().get(&range_key).cloned();
        if let Some(range) = range {
            let (size_change, is_part_full) = range.accumulate(record_size);
            let mut cache_size = self.cache_size.borrow_mut();
            *cache_size += i64::from(size_change);
            if (*cache_size as u64) < self.config.max_cache_size {
                if is_part_full {
                    // if range accumulate size is large than a part, then add it to part_full_ranges.
                    // when cache_size is large than max_cache_size, we will offload ranges in part_full_ranges
                    // util cache_size under cache_low_watermark.
                    self.part_full_ranges.borrow_mut().insert(range_key);
                }
            } else {
                if is_part_full {
                    *cache_size += i64::from(range.try_offload_part());
                }
                // try offload ranges in part_full_ranges util cache_size under cache_low_watermark.
                let mut part_full_ranges = self.part_full_ranges.borrow_mut();
                let part_full_ranges_length = part_full_ranges.len();
                if part_full_ranges_length > 0 {
                    let mut remove_keys = Vec::with_capacity(part_full_ranges_length);
                    for range_key in &*part_full_ranges {
                        if let Some(range) = self.ranges.borrow().get(range_key) {
                            *cache_size += i64::from(range.try_offload_part());
                            remove_keys.push(*range_key);
                            if *cache_size < self.config.cache_low_watermark as i64 {
                                break;
                            }
                        }
                    }
                    for range_key in remove_keys {
                        part_full_ranges.remove(&range_key);
                    }
                }
            }
        }
    }

    async fn get_objects(
        &self,
        stream_id: u64,
        range_index: u32,
        start_offset: u64,
        end_offset: u64,
        size_hint: u32,
    ) -> (Vec<ObjectMetadata>, bool) {
        self.object_manager
            .get_objects(stream_id, range_index, start_offset, end_offset, size_hint)
    }

    async fn get_offloading_range(&self) -> Vec<RangeKey> {
        self.object_manager.get_offloading_range()
    }

    async fn close(&self) {
        info!("object storage closing");
        drop(self.shutdown_rx.borrow_mut().take());
        self.shutdown_tx.shutdown().await;
        info!("object storage closed");
    }

    async fn watch_offload_progress(&self) -> OffloadProgressListener {
        self.object_manager.watch_offload_progress()
    }
}

impl<F, M> DefaultObjectStorage<F, M>
where
    F: RangeFetcher + 'static,
    M: ObjectManager + 'static,
{
    ///
    /// # Panics
    /// * Failed to build [`opendal::Operator`]
    pub fn new(config: &ObjectStorageConfig, range_fetcher: F, object_manager: M) -> Rc<Self> {
        let op = if config.endpoint.starts_with("fs://") {
            let mut builder = Fs::default();
            builder.root("/tmp/");
            Some(Operator::new(builder).unwrap().finish())
        } else if !config.endpoint.is_empty() {
            let mut s3_builder = S3::default();
            s3_builder.root("/");
            s3_builder.bucket(&config.bucket);
            s3_builder.region(&config.region);
            s3_builder.endpoint(&config.endpoint);
            s3_builder.access_key_id(
                &env::var("ES_OBJ_ACCESS_KEY_ID")
                    .map_err(|_| "ES_OBJ_ACCESS_KEY_ID cannot find in env")
                    .unwrap(),
            );
            s3_builder.secret_access_key(
                &env::var("ES_OBJ_SECRET_ACCESS_KEY")
                    .map_err(|_| "ES_OBJ_SECRET_ACCESS_KEY cannot find in env")
                    .unwrap(),
            );
            Some(Operator::new(s3_builder).unwrap().finish())
        } else {
            None
        };

        let force_flush_secs = config.force_flush_secs;
        let (shutdown_tx, shutdown_rx) = shutdown_chan();
        let this = Rc::new(DefaultObjectStorage {
            config: config.clone(),
            ranges: Rc::new(RefCell::new(HashMap::new())),
            part_full_ranges: RefCell::new(HashSet::new()),
            cache_size: RefCell::new(0),
            op,
            object_manager: Rc::new(object_manager),
            range_fetcher: Rc::new(range_fetcher),
            shutdown_tx,
            shutdown_rx: RefCell::new(Some(shutdown_rx.clone())),
        });
        Self::listen_owner_change(this.clone(), shutdown_rx.clone());
        Self::run_force_flush_task(
            this.ranges.clone(),
            Duration::from_secs(force_flush_secs),
            shutdown_rx,
        );
        this
    }

    fn listen_owner_change(object_storage: Rc<Self>, shutdown_rx: ShutdownRx) {
        let mut rx = object_storage.object_manager.owner_watcher();
        monoio::spawn(async move {
            let mut notify_shutdown_rx = shutdown_rx.subscribe();
            loop {
                tokio::select! {
                    _ = notify_shutdown_rx.recv() => {
                        break;
                    },
                    owner_event = rx.recv() => {
                        if let Some(owner_event) = owner_event {
                            Self::handle_owner_event(&object_storage, owner_event);
                        } else {
                            break;
                        }
                    }
                }
            }
        });
    }

    fn handle_owner_event(object_storage: &Rc<Self>, owner_event: OwnerEvent) {
        let stream_id = owner_event.range_key.stream_id;
        let range_index = owner_event.range_key.range_index;
        if let Some(owner) = owner_event.owner {
            object_storage.add_range(stream_id, range_index, &owner);
        } else {
            object_storage.remove_range(stream_id, range_index);
        }
    }

    pub fn run_force_flush_task(
        ranges: Rc<RefCell<HashMap<RangeKey, Rc<DefaultRangeAccumulator>>>>,
        max_duration: Duration,
        shutdown_rx: ShutdownRx,
    ) {
        monoio::spawn(async move {
            let mut notify_shutdown_rx = shutdown_rx.subscribe();
            loop {
                monoio::select! {
                    _ = notify_shutdown_rx.recv() => {
                        break;
                    }
                    _ = sleep(Duration::from_secs(1)) => {
                        ranges.borrow().iter().for_each(|(_, range)| {
                            range.try_flush(max_duration);
                        });
                    }
                }
            }
            info!("object storage force flush task shutdown");
        });
    }
}
