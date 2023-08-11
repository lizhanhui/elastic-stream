use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    rc::Rc,
};

use crate::{ObjectManager, OffloadProgress, OffloadProgressListener, Owner, OwnerEvent, RangeKey};
use bytes::Bytes;
use model::{
    error::EsError,
    object::ObjectMetadata,
    resource::{EventType, Resource, ResourceEvent},
};
use pd_client::PlacementDriverClient;
use protocol::rpc::header::ResourceType;
use tokio::sync::mpsc::{self, unbounded_channel, Receiver, UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ObjectKey {
    epoch: u16,
    start_offset: u64,
}

impl From<&ObjectMetadata> for ObjectKey {
    fn from(value: &ObjectMetadata) -> Self {
        Self {
            start_offset: value.start_offset,
            epoch: value.epoch,
        }
    }
}

#[derive(Debug)]
struct Object {
    end_offset_delta: u32,
    data_len: u32,
    sparse_index: Bytes,
}

impl From<&ObjectMetadata> for Object {
    fn from(value: &ObjectMetadata) -> Self {
        Self {
            end_offset_delta: value.end_offset_delta,
            data_len: value.data_len,
            sparse_index: value.sparse_index.clone(),
        }
    }
}

fn gen_object_metadata(key: &ObjectKey, object: &Object) -> ObjectMetadata {
    ObjectMetadata {
        stream_id: 0,
        range_index: 0,
        epoch: key.epoch,
        start_offset: key.start_offset,
        end_offset_delta: object.end_offset_delta,
        data_len: object.data_len,
        sparse_index: object.sparse_index.clone(),
        key: None,
    }
}

#[derive(Debug, Default)]
struct Objects(BTreeMap<ObjectKey, Object>);

impl Objects {
    /// Get a list of objects that
    /// * continuous (`object[i].start_offset` + `object[i].end_offset_delta` == `object[i+1].start_offset`)
    /// * start from `start_offset` (`object[0].start_offset` <= `start_offset`, if `object[0]` exists)
    /// * cover the request range (`start_offset`, `end_offset`)
    ///   or exceed the size hint (`object[1].data_len` + `object[2].data_len` + ... >= `size_hint`)
    ///
    /// Return a list of objects and whether the objects cover the request range or exceed the size hint.
    /// If `end_offset` is None, the range is not limited by `end_offset`.
    /// If `size_hint` is None, the range is not limited by size.
    /// If `end_offset` <= `start_offset`, return the first object that covers `start_offset`, if exists.
    ///
    /// Note: the returned objects' `stream_id` and `range_index` are not set.
    fn get_continuous_objects(
        &self,
        start_offset: u64,
        end_offset: Option<u64>,
        size_hint: Option<u32>,
    ) -> (Vec<ObjectMetadata>, bool) {
        let mut current_offset = start_offset;
        let mut objects = vec![];
        let mut size = 0;

        let epoch_min = self.0.keys().next().map_or(u16::MAX, |k| k.epoch);
        let epoch_max = self.0.keys().last().map_or(u16::MIN, |k| k.epoch);

        let mut epoch = epoch_min;
        // find the first object that covers `start_offset`
        while epoch <= epoch_max {
            if let Some((key, object)) = self
                .0
                .range(
                    ObjectKey {
                        epoch,
                        start_offset: u64::MIN,
                    }..=ObjectKey {
                        epoch,
                        start_offset: current_offset,
                    },
                )
                .last()
            {
                let end_offset = key.start_offset + u64::from(object.end_offset_delta);
                // check whether the object covers `start_offset`
                if key.start_offset <= current_offset && end_offset > current_offset {
                    current_offset = end_offset;
                    objects.push(gen_object_metadata(key, object));
                    break;
                }
            }
            epoch += 1;
        }

        // find continuous objects after the first object
        while epoch <= epoch_max
            && current_offset < end_offset.unwrap_or(u64::MAX)
            && size < size_hint.unwrap_or(u32::MAX)
        {
            if let Some((key, object)) = self.0.get_key_value(&ObjectKey {
                epoch,
                start_offset: current_offset,
            }) {
                current_offset = key.start_offset + u64::from(object.end_offset_delta);
                objects.push(gen_object_metadata(key, object));
                size += object.data_len;
            } else {
                epoch += 1;
            }
        }

        let cover_all = current_offset >= end_offset.unwrap_or(u64::MAX)
            || size >= size_hint.unwrap_or(u32::MAX);
        (objects, cover_all)
    }
}

#[derive(Debug, Default)]
struct ManagedObjects {
    objects: Objects,

    /// This server is the owner of the range or not
    owner: bool,

    /// The epoch of the current owner
    epoch: u16,

    /// The start offset of the range
    start_offset: u64,

    offloaded: bool,
}

impl ManagedObjects {
    fn offload_offset(&self) -> u64 {
        // TODO: cache the offload offset
        self.objects
            .get_continuous_objects(self.start_offset, None, None)
            .0
            .last()
            .map_or(self.start_offset, ObjectMetadata::end_offset)
    }

    fn owner(&self) -> Option<Owner> {
        if self.owner {
            Some(Owner {
                start_offset: self.offload_offset(),
                epoch: self.epoch,
            })
        } else {
            None
        }
    }
}

#[derive(Debug, Default)]
struct Metadata {
    /// Ranges held by this server.
    managed: HashMap<RangeKey, ManagedObjects>,

    /// Ranges not held by this server.
    other: HashMap<RangeKey, Objects>,

    /// Senders of owner change event.
    /// Owner changes in [`offloading`] will be sent to these senders.
    owner_event_senders: Vec<UnboundedSender<OwnerEvent>>,

    /// Listeners of range offload progress.
    offload_progress_listeners: Vec<mpsc::UnboundedSender<OffloadProgress>>,
}

impl Metadata {
    /// Save object metadata.
    /// return offload offset.
    fn add_object(&mut self, object: &ObjectMetadata) -> Option<u64> {
        let key = RangeKey::new(object.stream_id, object.range_index);
        match self.managed.get_mut(&key) {
            Some(managed) => {
                managed.objects.0.insert(object.into(), object.into());
                return Some(managed.offload_offset());
            }
            None => {
                // range not held by this server
                self.other
                    .entry(key)
                    .or_default()
                    .0
                    .insert(object.into(), object.into());
            }
        }
        None
    }

    fn get_objects(
        &self,
        key: &RangeKey,
        start_offset: u64,
        end_offset: u64,
        size_hint: u32,
    ) -> (Vec<ObjectMetadata>, bool) {
        self.managed
            .get(key)
            .map(|o| &o.objects)
            .or(self.other.get(key))
            .map_or((vec![], false), |objects| {
                let (mut objects, cover_all) =
                    objects.get_continuous_objects(start_offset, Some(end_offset), Some(size_hint));
                for o in &mut objects {
                    o.stream_id = key.stream_id;
                    o.range_index = key.range_index;
                }
                (objects, cover_all)
            })
    }

    fn reset(&mut self) {
        self.managed.clear();
        self.other.clear();
    }
}

pub struct DefaultObjectManager<C>
where
    C: PlacementDriverClient + 'static,
{
    cluster: String,
    token: CancellationToken,
    pd_client: Rc<C>,
    metadata: Rc<RefCell<Metadata>>,
}

impl<C> DefaultObjectManager<C>
where
    C: PlacementDriverClient + 'static,
{
    pub fn new(cluster: &str, pd_client: Rc<C>, server_id: i32) -> Self {
        let token = CancellationToken::new();
        let metadata = Rc::new(RefCell::new(Metadata::default()));
        let rx = pd_client.list_and_watch_resource(&[ResourceType::RANGE, ResourceType::OBJECT]);

        let t = token.clone();
        let m = metadata.clone();
        monoio::spawn(async move {
            Self::list_and_watch(t, rx, server_id, m).await;
        });

        Self {
            cluster: cluster.to_owned(),
            token,
            pd_client,
            metadata,
        }
    }

    async fn list_and_watch(
        token: CancellationToken,
        mut rx: Receiver<ResourceEvent>,
        server_id: i32,
        metadata: Rc<RefCell<Metadata>>,
    ) {
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    return;
                }
                Some(event) = rx.recv() => {
                    let metadata = metadata.clone();
                    match event.event_type {
                        EventType::Listed  | EventType::Added=> {
                            Self::handle_added_resource(&event.resource, server_id, &metadata);
                        }
                        EventType::Modified => {
                            Self::handle_modified_resource(&event.resource, &metadata);
                        }
                        EventType::Deleted => {
                            Self::handle_deleted_resource(&event.resource, &metadata);
                        }
                        EventType::Reset => {
                            metadata.borrow_mut().reset();
                        }
                        // TODO: handle list finished
                        EventType::None | EventType::ListFinished => (),
                    }
                }
            }
        }
    }

    fn handle_added_resource(
        resource: &Resource,
        server_id: i32,
        metadata: &Rc<RefCell<Metadata>>,
    ) {
        match resource {
            Resource::Range(range) => {
                let mut metadata = metadata.borrow_mut();
                let stream_id = range.stream_id() as u64;
                let range_index = range.index() as u32;
                let key = RangeKey::new(stream_id, range_index);
                if range.held_by(server_id) {
                    if let Some(owner) = range.offload_owner() {
                        let objects = metadata.managed.entry(key).or_default();
                        objects.owner = owner.server_id == server_id;
                        objects.epoch = owner.epoch as u16;
                        objects.start_offset = range.start();

                        // send owner change event
                        let owner = objects.owner();
                        metadata.owner_event_senders.retain(|sender| {
                            sender
                                .send(OwnerEvent {
                                    range_key: key,
                                    owner,
                                })
                                .is_ok()
                        });
                    } else {
                        // Skip if the owner is none, which should not happen.
                        log::warn!("range {key:?} has no owner");
                    }
                } else {
                    _ = metadata.other.entry(key).or_default();
                }
            }
            Resource::Object(object) => {
                let stream_id = object.stream_id;
                let range_index = object.range_index;
                let mut metadata = metadata.borrow_mut();
                // When listing objects, objects do not come by time order.
                if let Some(offload_offset) = metadata.add_object(object) {
                    metadata.offload_progress_listeners.retain(|listener| {
                        listener
                            .send(vec![((stream_id, range_index), offload_offset)])
                            .is_ok()
                    });
                }
            }
            _ => (),
        }
    }

    fn handle_modified_resource(_resource: &Resource, _metadata: &Rc<RefCell<Metadata>>) {
        // TODO: handle modified resource
    }

    fn handle_deleted_resource(_resource: &Resource, _metadata: &Rc<RefCell<Metadata>>) {
        // TODO: handle deleted resource
    }
}

impl<C> ObjectManager for DefaultObjectManager<C>
where
    C: PlacementDriverClient + 'static,
{
    fn owner_watcher(&self) -> UnboundedReceiver<OwnerEvent> {
        let (tx, rx) = unbounded_channel();
        let mut metadata = self.metadata.borrow_mut();
        for (key, offloading) in &mut metadata.managed {
            _ = tx.send(OwnerEvent {
                range_key: *key,
                owner: offloading.owner(),
            });
        }
        metadata.owner_event_senders.push(tx);
        rx
    }

    async fn commit_object(&self, object_metadata: ObjectMetadata) -> Result<(), EsError> {
        self.pd_client.commit_object(object_metadata).await
    }

    fn get_objects(
        &self,
        stream_id: u64,
        range_index: u32,
        start_offset: u64,
        end_offset: u64,
        size_hint: u32,
    ) -> (Vec<ObjectMetadata>, bool) {
        let (mut objects, cover_all) = self.metadata.borrow().get_objects(
            &RangeKey::new(stream_id, range_index),
            start_offset,
            end_offset,
            size_hint,
        );
        for m in &mut objects {
            m.gen_object_key(&self.cluster);
        }
        (objects, cover_all)
    }

    fn get_offloading_range(&self) -> Vec<RangeKey> {
        self.metadata
            .borrow()
            .managed
            .iter()
            .filter(|o| !o.1.offloaded)
            .map(|o| o.0)
            .copied()
            .collect::<Vec<_>>()
    }

    fn watch_offload_progress(&self) -> OffloadProgressListener {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut metadata = self.metadata.borrow_mut();

        let offload_progress = metadata
            .managed
            .iter()
            .map(|(range_key, objects)| {
                (
                    (range_key.stream_id, range_key.range_index),
                    objects.offload_offset(),
                )
            })
            .collect();
        let _ = tx.send(offload_progress);
        metadata.offload_progress_listeners.push(tx);
        rx
    }
}

impl<C> Drop for DefaultObjectManager<C>
where
    C: PlacementDriverClient + 'static,
{
    fn drop(&mut self) {
        self.token.cancel();
    }
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use super::*;
    use model::{object::ObjectMetadata, range::RangeMetadata};
    use protocol::rpc::header::{OffloadOwnerT, RangeServerT, RangeT};
    use tokio::sync::mpsc;

    fn new_object_with_epoch(
        epoch: u16,
        start_offset: u64,
        end_offset: u64,
        size: u32,
    ) -> ObjectMetadata {
        let mut obj = ObjectMetadata::new(0, 0, epoch, start_offset);
        obj.end_offset_delta = u32::try_from(end_offset - start_offset).unwrap();
        obj.data_len = size;
        obj
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_objects_get_continuous_objects() {
        struct Args {
            start_offset: u64,
            end_offset: Option<u64>,
            size_hint: Option<u32>,
        }
        struct Want {
            objects: Vec<ObjectMetadata>,
            cover_all: bool,
        }
        struct Test {
            name: String,
            fields: Vec<ObjectMetadata>,
            args: Args,
            want: Want,
        }
        let tests = vec![
            Test {
                name: "base case".to_string(),
                fields: vec![
                    new_object_with_epoch(0, 0, 100, 1),
                    new_object_with_epoch(0, 100, 200, 1),
                    new_object_with_epoch(0, 200, 300, 1),
                    new_object_with_epoch(0, 300, 400, 1),
                ],
                args: Args {
                    start_offset: 150,
                    end_offset: None,
                    size_hint: None,
                },
                want: Want {
                    objects: vec![
                        new_object_with_epoch(0, 100, 200, 1),
                        new_object_with_epoch(0, 200, 300, 1),
                        new_object_with_epoch(0, 300, 400, 1),
                    ],
                    cover_all: false,
                },
            },
            Test {
                name: "base case 02".to_string(),
                fields: vec![
                    new_object_with_epoch(0, 0, 100, 1),
                    new_object_with_epoch(0, 100, 200, 1),
                    new_object_with_epoch(0, 200, 300, 1),
                    new_object_with_epoch(0, 300, 400, 1),
                ],
                args: Args {
                    start_offset: 0,
                    end_offset: Some(600),
                    size_hint: Some(1),
                },
                want: Want {
                    objects: vec![
                        new_object_with_epoch(0, 0, 100, 1),
                        new_object_with_epoch(0, 100, 200, 1),
                    ],
                    cover_all: true,
                },
            },
            Test {
                name: "limit by end offset".to_string(),
                fields: vec![
                    new_object_with_epoch(0, 0, 100, 1),
                    new_object_with_epoch(0, 100, 200, 1),
                    new_object_with_epoch(0, 200, 300, 1),
                    new_object_with_epoch(0, 300, 400, 1),
                ],
                args: Args {
                    start_offset: 150,
                    end_offset: Some(250),
                    size_hint: None,
                },
                want: Want {
                    objects: vec![
                        new_object_with_epoch(0, 100, 200, 1),
                        new_object_with_epoch(0, 200, 300, 1),
                    ],
                    cover_all: true,
                },
            },
            Test {
                name: "limit by size hint".to_string(),
                fields: vec![
                    new_object_with_epoch(0, 0, 100, 1),
                    new_object_with_epoch(0, 100, 200, 1),
                    new_object_with_epoch(0, 200, 300, 1),
                    new_object_with_epoch(0, 300, 400, 1),
                ],
                args: Args {
                    start_offset: 150,
                    end_offset: None,
                    size_hint: Some(1), // not include the first object
                },
                want: Want {
                    objects: vec![
                        new_object_with_epoch(0, 100, 200, 1),
                        new_object_with_epoch(0, 200, 300, 1),
                    ],
                    cover_all: true,
                },
            },
            Test {
                name: "limit by end offset and size hint".to_string(),
                fields: vec![
                    new_object_with_epoch(0, 0, 100, 1),
                    new_object_with_epoch(0, 100, 200, 1),
                    new_object_with_epoch(0, 200, 300, 1),
                    new_object_with_epoch(0, 300, 400, 1),
                ],
                args: Args {
                    start_offset: 150,
                    end_offset: Some(350),
                    size_hint: Some(2),
                },
                want: Want {
                    objects: vec![
                        new_object_with_epoch(0, 100, 200, 1),
                        new_object_with_epoch(0, 200, 300, 1),
                        new_object_with_epoch(0, 300, 400, 1),
                    ],
                    cover_all: true,
                },
            },
            Test {
                name: "mismatch in older epoch".to_string(),
                fields: vec![
                    new_object_with_epoch(0, 0, 100, 1),
                    new_object_with_epoch(1, 100, 140, 1),
                    new_object_with_epoch(2, 100, 200, 1),
                    new_object_with_epoch(2, 200, 300, 1),
                    new_object_with_epoch(2, 300, 400, 1),
                ],
                args: Args {
                    start_offset: 150,
                    end_offset: Some(350),
                    size_hint: Some(2),
                },
                want: Want {
                    objects: vec![
                        new_object_with_epoch(2, 100, 200, 1),
                        new_object_with_epoch(2, 200, 300, 1),
                        new_object_with_epoch(2, 300, 400, 1),
                    ],
                    cover_all: true,
                },
            },
            Test {
                name: "mismatch in newer epoch".to_string(),
                fields: vec![
                    new_object_with_epoch(0, 0, 100, 1),
                    new_object_with_epoch(0, 100, 200, 1),
                    new_object_with_epoch(0, 200, 300, 1),
                    new_object_with_epoch(1, 310, 400, 1),
                ],
                args: Args {
                    start_offset: 150,
                    end_offset: Some(350),
                    size_hint: Some(2),
                },
                want: Want {
                    objects: vec![
                        new_object_with_epoch(0, 100, 200, 1),
                        new_object_with_epoch(0, 200, 300, 1),
                    ],
                    cover_all: false,
                },
            },
            Test {
                name: "objects in different epoch".to_string(),
                fields: vec![
                    new_object_with_epoch(0, 0, 100, 1),
                    new_object_with_epoch(1, 100, 200, 1),
                    new_object_with_epoch(2, 200, 300, 1),
                    new_object_with_epoch(4, 300, 400, 1),
                ],
                args: Args {
                    start_offset: 150,
                    end_offset: Some(350),
                    size_hint: Some(2),
                },
                want: Want {
                    objects: vec![
                        new_object_with_epoch(1, 100, 200, 1),
                        new_object_with_epoch(2, 200, 300, 1),
                        new_object_with_epoch(4, 300, 400, 1),
                    ],
                    cover_all: true,
                },
            },
            Test {
                name: "duplicate objects in different epoch".to_string(),
                fields: vec![
                    new_object_with_epoch(0, 0, 100, 1),
                    new_object_with_epoch(1, 0, 100, 1),
                    new_object_with_epoch(1, 100, 200, 1),
                    new_object_with_epoch(2, 0, 100, 1),
                    new_object_with_epoch(2, 100, 200, 1),
                    new_object_with_epoch(2, 200, 300, 1),
                    new_object_with_epoch(3, 0, 100, 1),
                    new_object_with_epoch(3, 100, 200, 1),
                    new_object_with_epoch(3, 200, 300, 1),
                    new_object_with_epoch(3, 300, 400, 1),
                ],
                args: Args {
                    start_offset: 150,
                    end_offset: Some(350),
                    size_hint: Some(2),
                },
                want: Want {
                    objects: vec![
                        new_object_with_epoch(1, 100, 200, 1),
                        new_object_with_epoch(2, 200, 300, 1),
                        new_object_with_epoch(3, 300, 400, 1),
                    ],
                    cover_all: true,
                },
            },
        ];

        for test in tests {
            let mut objects = Objects::default();
            // prepare
            for o in test.fields {
                objects.0.insert((&o).into(), (&o).into());
            }
            // test
            let (objects, cover_all) = objects.get_continuous_objects(
                test.args.start_offset,
                test.args.end_offset,
                test.args.size_hint,
            );
            // check
            assert_eq!(test.want.objects, objects, "{}", test.name);
            assert_eq!(test.want.cover_all, cover_all, "{}", test.name);
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_default_object_manager() {
        tokio_uring::start(async {
            const CHANNEL_SIZE: usize = 16;
            let (tx, rx) = mpsc::channel(CHANNEL_SIZE);

            // mock pd client
            let mut mock_pd_client = pd_client::MockPlacementDriverClient::new();
            mock_pd_client
                .expect_list_and_watch_resource()
                .times(1)
                .return_once(|_| rx);
            mock_pd_client
                .expect_commit_object()
                .withf(|object| object.start_offset == 300)
                .times(1)
                .returning(|_| Ok(()));
            let object_manager = DefaultObjectManager::<pd_client::MockPlacementDriverClient>::new(
                "testcluster",
                Rc::new(mock_pd_client),
                42,
            );

            // a listed range
            let mut range_t = RangeT::default();
            range_t.stream_id = 1;
            range_t.index = 2;
            range_t.start = 100;
            range_t.end = 500;
            let mut range_server_t = RangeServerT::default();
            range_server_t.server_id = 42;
            range_t.servers = Some(vec![range_server_t]);
            let mut offload_owner_t = OffloadOwnerT::default();
            offload_owner_t.server_id = 42;
            offload_owner_t.epoch = 3;
            range_t.offload_owner = Some(Box::new(offload_owner_t));
            tx.send(ResourceEvent {
                event_type: EventType::Listed,
                resource: Resource::Range(RangeMetadata::from(&range_t)),
            })
            .await
            .unwrap();

            // two listed objects
            let mut object = new_object_with_epoch(1, 100, 200, 1);
            object.stream_id = 1;
            object.range_index = 2;
            let mut object1 = object.clone();
            object1.gen_object_key("testcluster");
            tx.send(ResourceEvent {
                event_type: EventType::Listed,
                resource: Resource::Object(object),
            })
            .await
            .unwrap();
            let mut object = new_object_with_epoch(1, 300, 400, 1);
            object.stream_id = 1;
            object.range_index = 2;
            tx.send(ResourceEvent {
                event_type: EventType::Listed,
                resource: Resource::Object(object),
            })
            .await
            .unwrap();

            // a added object
            let mut object = new_object_with_epoch(2, 200, 300, 1);
            object.stream_id = 1;
            object.range_index = 2;
            let mut object2 = object.clone();
            object2.gen_object_key("testcluster");
            tx.send(ResourceEvent {
                event_type: EventType::Added,
                resource: Resource::Object(object),
            })
            .await
            .unwrap();

            // wait for all events processed
            loop {
                if tx.capacity() == CHANNEL_SIZE {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // test `owner_watcher`
            let mut owner_watcher = object_manager.owner_watcher();
            assert_eq!(
                Some(OwnerEvent {
                    range_key: RangeKey::new(1, 2),
                    owner: Some(Owner {
                        epoch: 3,
                        start_offset: 300
                    })
                }),
                owner_watcher.recv().await
            );

            // test `commit_object`
            assert!(object_manager
                .commit_object(ObjectMetadata::new(1, 2, 3, 300))
                .await
                .is_ok());

            // test `get_objects`
            let (object_list, cover_all) = object_manager.get_objects(1, 2, 150, 250, 1);
            assert_eq!(vec![object1, object2], object_list);
            assert!(cover_all);

            // test `get_offloading_range`
            assert_eq!(
                vec![RangeKey::new(1, 2)],
                object_manager.get_offloading_range()
            );

            // test `watch_offload_progress`
            let mut object_rx = object_manager.watch_offload_progress();
            let events = object_rx.recv().await.unwrap();
            assert_eq!(vec![((1, 2), 300)], events);
            let mut object = new_object_with_epoch(2, 300, 400, 1);
            object.stream_id = 1;
            object.range_index = 2;
            tx.send(ResourceEvent {
                event_type: EventType::Listed,
                resource: Resource::Object(object),
            })
            .await
            .unwrap();
            let events = object_rx.recv().await.unwrap();
            assert_eq!(vec![((1, 2), 400)], events);
        });
    }

    #[test]
    fn test_metadata() {
        // TODO: test `list_and_watch`
    }
}
