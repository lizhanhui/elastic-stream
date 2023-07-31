use std::{
    cell::RefCell,
    cmp::min,
    collections::{BTreeMap, HashMap},
    rc::Rc,
};

use crate::{ObjectManager, Owner, RangeKey};
use bytes::Bytes;
use model::{
    error::EsError,
    object::{gen_object_key, ObjectMetadata},
    resource::{EventType, Resource, ResourceEvent},
};
use pd_client::PlacementDriverClient;
use protocol::rpc::header::ResourceType;
use tokio::sync::mpsc::Receiver;
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
    /// * continuous (object_i.start_offset + object_i.end_offset_delta == object_i+1.start_offset)
    /// * start from `start_offset` (object_0.start_offset <= start_offset, if object_0 exists)
    /// * cover the request range (start_offset, end_offset)
    ///   or exceed the size hint (object_1.data_len + object_2.data_len + ... >= size_hint)
    ///
    /// Return a list of objects and whether the objects cover the request range.
    /// If end_offset is None, the range is not limited by end_offset.
    /// If size_hint is None, the range is not limited by size.
    /// If end_offset <= start_offset, return the first object that covers `start_offset`, if exists.
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

        let epoch_min = self.0.keys().next().map(|k| k.epoch).unwrap_or(u16::MAX);
        let epoch_max = self.0.keys().last().map(|k| k.epoch).unwrap_or(u16::MIN);

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
                let end_offset = key.start_offset + object.end_offset_delta as u64;
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
                current_offset = key.start_offset + object.end_offset_delta as u64;
                objects.push(gen_object_metadata(key, object));
                size += object.data_len;
            } else {
                epoch += 1;
            }
        }

        (objects, current_offset >= end_offset.unwrap_or(u64::MAX))
    }
}

#[derive(Debug, Default)]
struct OffloadingObjects {
    objects: Objects,

    /// This server is the owner of the range or not
    owner: bool,

    /// The epoch of the current owner
    epoch: u16,

    /// The start offset of the range
    start_offset: u64,
}

impl OffloadingObjects {
    fn offload_offset(&self) -> u64 {
        // TODO: cache the offload offset
        self.objects
            .get_continuous_objects(self.start_offset, None, None)
            .0
            .last()
            .map(|object| object.end_offset())
            .unwrap_or(self.start_offset)
    }
}

#[derive(Debug, Default)]
struct Metadata {
    /// Ranges held by this server and not offloaded yet
    offloading: HashMap<RangeKey, OffloadingObjects>,

    /// Ranges held by this server and already offloaded
    /// or
    /// Ranges not held by this server
    other: HashMap<RangeKey, Objects>,
}

impl Metadata {
    /// Save object metadata.
    /// If `need_flush` is true, flush the offload offset of the range.
    fn add_object(&mut self, object: &ObjectMetadata, need_flush: bool) {
        let key = RangeKey::new(object.stream_id, object.range_index);
        match self.offloading.get_mut(&key) {
            Some(offloading) => {
                offloading.objects.0.insert(object.into(), object.into());

                // TODO: flush offload offset
                let _ = need_flush;
                if 0 == 1 {
                    self.archive_objects(&key);
                }
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
    }

    /// Move objects from `offloading` to `other`.
    fn archive_objects(&mut self, key: &RangeKey) {
        if let Some(offloading) = self.offloading.remove(key) {
            self.other
                .entry(*key)
                .or_default()
                .0
                .extend(offloading.objects.0);
        }
    }

    fn get_objects(
        &self,
        key: &RangeKey,
        start_offset: u64,
        end_offset: u64,
        size_hint: u32,
    ) -> (Vec<ObjectMetadata>, bool) {
        self.offloading
            .get(key)
            .map(|o| &o.objects)
            .or(self.other.get(key))
            .map(|objects| {
                let (mut objects, cover_all) =
                    objects.get_continuous_objects(start_offset, Some(end_offset), Some(size_hint));
                objects.iter_mut().for_each(|o| {
                    o.stream_id = key.stream_id;
                    o.range_index = key.range_index;
                });
                (objects, cover_all)
            })
            .unwrap_or((vec![], false))
    }
}

pub struct DefaultObjectManager<C>
where
    C: PlacementDriverClient + 'static,
{
    token: CancellationToken,
    pd_client: Rc<C>,
    metadata: Rc<RefCell<Metadata>>,
}

impl<C> DefaultObjectManager<C>
where
    C: PlacementDriverClient + 'static,
{
    pub fn new(pd_client: Rc<C>, server_id: i32) -> Self {
        let token = CancellationToken::new();
        let metadata = Rc::new(RefCell::new(Metadata::default()));
        let rx = pd_client.list_and_watch_resource(&[ResourceType::RANGE, ResourceType::OBJECT]);

        let t = token.clone();
        let m = metadata.clone();
        tokio_uring::spawn(async move {
            Self::list_and_watch(t, rx, server_id, m).await;
        });

        Self {
            token,
            pd_client,
            metadata,
        }
    }

    async fn list_and_watch(
        token: CancellationToken,
        mut rx: Receiver<Result<ResourceEvent, EsError>>,
        server_id: i32,
        metadata: Rc<RefCell<Metadata>>,
    ) {
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    return;
                }
                Some(event) = rx.recv() => {
                    match event {
                        Ok(event) => {
                            let metadata = metadata.clone();
                            match event.event_type {
                                EventType::LISTED  => {
                                    Self::handle_added_resource(&event.resource, server_id, metadata, true);
                                }
                                EventType::ADDED => {
                                    Self::handle_added_resource(&event.resource, server_id, metadata, false);
                                }
                                EventType::MODIFIED => {
                                    Self::handle_modified_resource(&event.resource, metadata);
                                }
                                EventType::DELETED => {
                                    Self::handle_deleted_resource(&event.resource, metadata);
                                }
                                EventType::NONE => (),
                            }
                        }
                        Err(e) => {
                            // TODO: handle error
                            log::error!("receive resource event error: {:?}", e);
                            return;
                        }
                    }
                }
            }
        }
    }

    fn handle_added_resource(
        resource: &Resource,
        server_id: i32,
        metadata: Rc<RefCell<Metadata>>,
        listed: bool,
    ) {
        match resource {
            Resource::Range(range) => {
                let mut metadata = metadata.borrow_mut();
                let key = RangeKey::new(range.stream_id() as u64, range.index() as u32);
                if range.held_by(server_id) {
                    let objects = metadata.offloading.entry(key).or_default();
                    objects.owner = range.offload_owner().server_id == server_id;
                    objects.epoch = range.offload_owner().epoch as u16;
                    objects.start_offset = range.start();
                } else {
                    _ = metadata.other.entry(key).or_default();
                }
            }
            Resource::Object(object) => {
                let mut metadata = metadata.borrow_mut();
                // When listing objects, objects do not come by time order.
                metadata.add_object(object, !listed);
            }
            _ => (),
        }
    }

    fn handle_modified_resource(_resource: &Resource, _metadata: Rc<RefCell<Metadata>>) {
        // TODO: handle modified resource
    }

    fn handle_deleted_resource(_resource: &Resource, _metadata: Rc<RefCell<Metadata>>) {
        // TODO: handle deleted resource
    }
}

impl<C> ObjectManager for DefaultObjectManager<C>
where
    C: PlacementDriverClient + 'static,
{
    fn is_owner(&self, stream_id: u64, range_index: u32) -> Option<Owner> {
        self.metadata
            .borrow()
            .offloading
            .get(&RangeKey::new(stream_id, range_index))
            .and_then(|o| match o.owner {
                true => Some(Owner {
                    epoch: o.epoch,
                    start_offset: o.offload_offset(),
                }),
                false => None,
            })
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
        self.metadata.borrow().get_objects(
            &RangeKey::new(stream_id, range_index),
            start_offset,
            end_offset,
            size_hint,
        )
    }

    fn get_offloading_range(&self) -> Vec<RangeKey> {
        self.metadata
            .borrow()
            .offloading
            .keys()
            .cloned()
            .collect::<Vec<_>>()
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

pub struct MemoryObjectManager {
    cluster: String,
    map: RefCell<HashMap<RangeKey, Vec<ObjectMetadata>>>,
}

impl MemoryObjectManager {
    pub fn new(cluster: &str) -> Self {
        Self {
            cluster: cluster.to_string(),
            map: RefCell::new(HashMap::new()),
        }
    }
}

impl ObjectManager for MemoryObjectManager {
    fn is_owner(&self, _stream_id: u64, _range_index: u32) -> Option<Owner> {
        Some(Owner {
            epoch: 0,
            start_offset: 0,
        })
    }

    async fn commit_object(&self, object_metadata: ObjectMetadata) -> Result<(), EsError> {
        let key = RangeKey::new(object_metadata.stream_id, object_metadata.range_index);
        let mut map = self.map.borrow_mut();
        let metas = if let Some(metas) = map.get_mut(&key) {
            metas
        } else {
            let metas = vec![];
            map.insert(key, metas);
            map.get_mut(&key).unwrap()
        };
        metas.push(object_metadata);
        Ok(())
    }

    fn get_objects(
        &self,
        stream_id: u64,
        range_index: u32,
        start_offset: u64,
        end_offset: u64,
        size_hint: u32,
    ) -> (Vec<ObjectMetadata>, bool) {
        let key = RangeKey::new(stream_id, range_index);
        if let Some(metas) = self.map.borrow().get(&key) {
            let objects = metas
                .iter()
                .filter(|meta| {
                    meta.start_offset < end_offset
                        && (meta.end_offset_delta as u64 + meta.start_offset) >= start_offset
                })
                .map(|meta| {
                    let mut meta = meta.clone();
                    let key = gen_object_key(
                        &self.cluster,
                        stream_id,
                        range_index,
                        meta.epoch,
                        meta.start_offset,
                    );
                    meta.key = Some(key);
                    meta
                })
                .collect();

            let cover_all = is_cover_all(start_offset, end_offset, size_hint, &objects);
            (objects, cover_all)
        } else {
            (vec![], false)
        }
    }

    fn get_offloading_range(&self) -> Vec<RangeKey> {
        // TODO: replay meta event, get offloading range.
        vec![]
    }
}

/// Check whether objects cover the request range.
/// Note: expect objects is sorted by start_offset and overlap the request range.
fn is_cover_all(
    mut start_offset: u64,
    end_offset: u64,
    mut size_hint: u32,
    objects: &Vec<ObjectMetadata>,
) -> bool {
    for object in objects {
        let object_end_offset = object.start_offset + object.end_offset_delta as u64;
        if object.start_offset <= start_offset && start_offset < object_end_offset {
            if start_offset == object.start_offset {
                size_hint -= min(object.data_len, size_hint);
            }
            start_offset = object_end_offset;
            if start_offset >= end_offset || size_hint == 0 {
                return true;
            }
        } else {
            return false;
        }
    }
    false
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use super::*;
    use model::{object::ObjectMetadata, range::RangeMetadata};
    use protocol::rpc::header::{OffloadOwnerT, RangeServerT, RangeT};
    use tokio::sync::mpsc;

    #[test]
    fn test_is_cover_all() {
        // sequence objects
        let objects = vec![
            new_object(100, 150, 10),
            new_object(150, 200, 100),
            new_object(200, 300, 100),
        ];
        // range match
        assert!(is_cover_all(100, 300, 100, &objects));
        assert!(is_cover_all(110, 250, 100, &objects));

        // size match
        assert!(is_cover_all(100, 1000, 210, &objects));
        assert!(is_cover_all(110, 1000, 200, &objects));
        // size not match
        assert!(!is_cover_all(110, 1000, 210, &objects));

        // hollow objects
        let objects = vec![new_object(100, 150, 10), new_object(200, 300, 100)];
        assert!(!is_cover_all(100, 300, 100, &objects));
    }

    fn new_object(start_offset: u64, end_offset: u64, size: u32) -> ObjectMetadata {
        new_object_with_epoch(0, start_offset, end_offset, size)
    }

    fn new_object_with_epoch(
        epoch: u16,
        start_offset: u64,
        end_offset: u64,
        size: u32,
    ) -> ObjectMetadata {
        let mut obj = ObjectMetadata::new(0, 0, epoch, start_offset);
        obj.end_offset_delta = (end_offset - start_offset) as u32;
        obj.data_len = size;
        obj
    }

    #[test]
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
                    cover_all: false,
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
            range_t.offload_owner = Box::new(offload_owner_t);
            tx.send(Ok(ResourceEvent {
                event_type: EventType::LISTED,
                resource: Resource::Range(RangeMetadata::from(&range_t)),
            }))
            .await
            .unwrap();

            // two listed objects
            let mut object = new_object_with_epoch(1, 100, 200, 1);
            object.stream_id = 1;
            object.range_index = 2;
            let object1 = object.clone();
            tx.send(Ok(ResourceEvent {
                event_type: EventType::LISTED,
                resource: Resource::Object(object),
            }))
            .await
            .unwrap();
            let mut object = new_object_with_epoch(1, 300, 400, 1);
            object.stream_id = 1;
            object.range_index = 2;
            tx.send(Ok(ResourceEvent {
                event_type: EventType::LISTED,
                resource: Resource::Object(object),
            }))
            .await
            .unwrap();

            // a added object
            let mut object = new_object_with_epoch(2, 200, 300, 1);
            object.stream_id = 1;
            object.range_index = 2;
            let object2 = object.clone();
            tx.send(Ok(ResourceEvent {
                event_type: EventType::ADDED,
                resource: Resource::Object(object),
            }))
            .await
            .unwrap();

            // wait for all events processed
            loop {
                if tx.capacity() == CHANNEL_SIZE {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // test `is_owner`
            assert_eq!(
                Some(Owner {
                    epoch: 3,
                    start_offset: 300
                }),
                object_manager.is_owner(1, 2),
            );

            // test `commit_object`
            assert!(object_manager
                .commit_object(ObjectMetadata::new(1, 2, 3, 300))
                .await
                .is_ok());

            // test `get_objects`
            let (objects, cover_all) = object_manager.get_objects(1, 2, 150, 250, 1);
            assert_eq!(vec![object1, object2], objects);
            assert!(cover_all);

            // test `get_offloading_range`
            assert_eq!(
                vec![RangeKey::new(1, 2)],
                object_manager.get_offloading_range()
            );
        });
    }

    #[test]
    fn test_metadata() {
        // TODO: test `list_and_watch`
    }
}
