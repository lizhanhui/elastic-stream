use super::{MetadataEventRx, MetadataManager, ResourceEventObserver, ResourceEventRx};
use local_sync::oneshot;
use log::info;
use model::{
    error::EsError,
    range::{Range, RangeEvent, RangeIndex, RangeMetadata, StreamId},
    resource::{EventType, Resource, ResourceEvent},
    stream::StreamMetadata,
};
use std::{
    cell::RefCell,
    cmp::max,
    collections::{BTreeMap, HashMap},
    rc::{Rc, Weak},
};
use tokio::sync::mpsc;
type Listener = mpsc::UnboundedSender<Vec<RangeEvent>>;

pub(crate) struct DefaultMetadataManager {
    server_id: i32,
    metadata_rx: RefCell<Option<ResourceEventRx>>,
    object_rx: RefCell<Option<object_storage::OffloadProgressListener>>,
    stream_map: Rc<RefCell<HashMap<StreamId, Stream>>>,
    listeners: Vec<Listener>,
    observers: Rc<RefCell<Vec<Weak<dyn ResourceEventObserver>>>>,
}

impl DefaultMetadataManager {
    pub(crate) fn new(
        metadata_rx: ResourceEventRx,
        object_rx: Option<object_storage::OffloadProgressListener>,
        server_id: i32,
    ) -> Self {
        let stream_map = Rc::new(RefCell::new(HashMap::new()));
        Self {
            server_id,
            metadata_rx: RefCell::new(Some(metadata_rx)),
            object_rx: RefCell::new(object_rx),
            stream_map: stream_map.clone(),
            listeners: Vec::new(),
            observers: Rc::new(RefCell::new(vec![])),
        }
    }

    async fn start0(
        server_id: i32,
        metadata_rx: ResourceEventRx,
        object_rx: object_storage::OffloadProgressListener,
        stream_map: Rc<RefCell<HashMap<u64, Stream>>>,
        listeners: Vec<Listener>,
        observers: Rc<RefCell<Vec<Weak<dyn ResourceEventObserver>>>>,
    ) {
        Self::listen_metadata_events(
            server_id,
            metadata_rx,
            stream_map.clone(),
            listeners.clone(),
            observers,
        )
        .await;
        Self::listen_object_events(object_rx, stream_map, listeners);
    }

    async fn listen_metadata_events(
        server_id: i32,
        mut metadata_rx: ResourceEventRx,
        stream_map: Rc<RefCell<HashMap<u64, Stream>>>,
        listeners: Vec<Listener>,
        observers: Rc<RefCell<Vec<Weak<dyn ResourceEventObserver>>>>,
    ) {
        let (list_done_tx, list_done_rx) = oneshot::channel();
        let mut list_done_tx = Some(list_done_tx);
        let mut listeners = listeners.clone();
        tokio_uring::spawn(async move {
            let mut list_done_buffer_events = Vec::new();
            let mut list_done = false;
            loop {
                match metadata_rx.recv().await {
                    Some(event) => {
                        {
                            for observer in observers.borrow().iter() {
                                if let Some(observer) = observer.upgrade() {
                                    observer.on_resource_event(&event);
                                }
                            }
                        }

                        match event.event_type {
                            EventType::ListFinished => {
                                if let Some(tx) = list_done_tx.take() {
                                    let _ = tx.send(());
                                }
                                list_done = true;
                                if list_done {
                                    if !notify_listeners(
                                        list_done_buffer_events.clone(),
                                        &mut listeners,
                                    ) {
                                        break;
                                    } else {
                                        list_done_buffer_events.clear();
                                    }
                                } else {
                                    continue;
                                }
                            }
                            EventType::Reset => {
                                todo!("reset pd list&watch support");
                            }
                            _ => {}
                        }
                        let mut incremental_range_events =
                            Self::handle_event(server_id, event, &stream_map);
                        if !list_done {
                            list_done_buffer_events.append(&mut incremental_range_events);
                        } else if !incremental_range_events.is_empty()
                            && !notify_listeners(incremental_range_events, &mut listeners)
                        {
                            break;
                        }
                    }
                    None => {
                        info!("tx are dropped, quit metadata watch");
                        break;
                    }
                }
            }
        });
        let _ = list_done_rx.await;
        info!("Complete fetching metadata from PD");
    }

    #[inline]
    fn handle_event(
        server_id: i32,
        event: ResourceEvent,
        stream_map: &Rc<RefCell<HashMap<u64, Stream>>>,
    ) -> Vec<RangeEvent> {
        let mut stream_map = stream_map.borrow_mut();
        match event.resource {
            Resource::Stream(stream_metadata) => {
                let stream_id = stream_metadata.stream_id;
                match event.event_type {
                    EventType::Listed | EventType::Added | EventType::Modified => {
                        if let Some(stream) = stream_map.get_mut(&stream_id) {
                            return stream.update_stream(stream_metadata);
                        } else {
                            stream_map
                                .insert(stream_id, Stream::new(stream_id, Some(stream_metadata)));
                        }
                    }
                    EventType::Deleted => {
                        return stream_map
                            .remove(&stream_id)
                            .map(|s| s.gen_destroy_events())
                            .unwrap_or_default();
                    }
                    _ => {}
                }
            }
            Resource::Range(range) => {
                if !range.held_by(server_id) {
                    return Vec::default();
                }
                let stream_id = range.stream_id();
                let stream = if let Some(stream) = stream_map.get_mut(&stream_id) {
                    stream
                } else {
                    stream_map.insert(stream_id, Stream::new(range.stream_id(), None));
                    stream_map.get_mut(&stream_id).unwrap()
                };
                match event.event_type {
                    EventType::Listed | EventType::Added | EventType::Modified => {
                        return vec![stream.add(range)];
                    }
                    EventType::Deleted => {
                        return vec![stream.del(&range)];
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        Vec::new()
    }

    fn listen_object_events(
        mut object_rx: object_storage::OffloadProgressListener,
        stream_map: Rc<RefCell<HashMap<u64, Stream>>>,
        mut listeners: Vec<Listener>,
    ) {
        tokio_uring::spawn(async move {
            loop {
                match object_rx.recv().await {
                    Some(offload_progress) => {
                        let events: Vec<RangeEvent> = offload_progress
                            .into_iter()
                            .map(|((stream_id, range_index), offloaded_offset)| {
                                let mut stream_map = stream_map.borrow_mut();
                                let stream = if let Some(stream) = stream_map.get_mut(&stream_id) {
                                    stream
                                } else {
                                    stream_map.insert(stream_id, Stream::new(stream_id, None));
                                    stream_map.get_mut(&stream_id).unwrap()
                                };
                                stream.update_offloaded_offset(range_index, offloaded_offset)
                            })
                            .collect();
                        if !events.is_empty() {
                            notify_listeners(events, &mut listeners);
                        }
                    }
                    None => {
                        info!("tx are dropped, quit object watch");
                        break;
                    }
                }
            }
        });
    }
}

fn notify_listeners(events: Vec<RangeEvent>, listeners: &mut Vec<Listener>) -> bool {
    let mut has_closed = false;
    for tx in listeners.iter() {
        if tx.send(events.clone()).is_err() {
            has_closed = true;
        }
    }
    if has_closed {
        listeners.retain(|tx| !tx.is_closed());
        if listeners.is_empty() {
            info!("all listeners are closed");
            return false;
        }
    }
    true
}

impl MetadataManager for DefaultMetadataManager {
    async fn start(&self) {
        let metadata_rx = self.metadata_rx.borrow_mut().take().unwrap();
        let object_rx = self.object_rx.borrow_mut().take().unwrap();
        Self::start0(
            self.server_id,
            metadata_rx,
            object_rx,
            self.stream_map.clone(),
            self.listeners.clone(),
            Rc::clone(&self.observers),
        )
        .await;
    }

    fn watch(&mut self) -> Result<MetadataEventRx, EsError> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.listeners.push(tx);
        Ok(rx)
    }

    fn add_observer(&mut self, observer: Weak<dyn ResourceEventObserver>) {
        self.observers.borrow_mut().push(observer);
    }
}

type DeletableOffset = u64;

struct Stream {
    stream_id: StreamId,
    metadata: Option<StreamMetadata>,
    ranges: BTreeMap<RangeIndex, (RangeMetadata, DeletableOffset)>,
    range_offloaded_offsets: BTreeMap<RangeIndex, u64>,
}

impl Stream {
    fn new(stream_id: StreamId, metadata: Option<StreamMetadata>) -> Self {
        Self {
            stream_id,
            metadata,
            ranges: BTreeMap::new(),
            range_offloaded_offsets: BTreeMap::new(),
        }
    }

    fn add(&mut self, range: RangeMetadata) -> RangeEvent {
        let range_index = range.index() as u32;
        let new_start_offset = max(self.new_start_offset(range_index), range.start());
        let range_key = (self.stream_id, range_index);
        let range_end_offset = range.end();
        self.ranges.insert(range_index, (range, new_start_offset));
        Self::gen_event(
            range_key,
            range_end_offset.as_ref(),
            self.range_offloaded_offsets.get(&range_index),
            new_start_offset,
        )
    }

    fn del(&mut self, range: &RangeMetadata) -> RangeEvent {
        let range_index = range.index() as u32;
        let event = RangeEvent::Del((range.stream_id(), range_index));
        self.ranges.remove(&range_index);
        self.range_offloaded_offsets.remove(&range_index);
        event
    }

    fn update_stream(&mut self, metadata: StreamMetadata) -> Vec<RangeEvent> {
        self.metadata = Some(metadata);
        let mut events = Vec::new();
        let range_indexes: Vec<u32> = self.ranges.keys().copied().collect();
        for range_index in range_indexes {
            let range_key = (self.stream_id, range_index);
            let new_start_offset = self.new_start_offset(range_index);
            let (range, last_start_offset) = self.ranges.get_mut(&range_index).unwrap();
            if new_start_offset != *last_start_offset {
                *last_start_offset = new_start_offset;
                events.push(Self::gen_event(
                    range_key,
                    range.end().as_ref(),
                    self.range_offloaded_offsets.get(&range_index),
                    new_start_offset,
                ));
            }
        }
        events
    }

    fn update_offloaded_offset(
        &mut self,
        range_index: RangeIndex,
        offloaded_offset: u64,
    ) -> RangeEvent {
        self.range_offloaded_offsets
            .insert(range_index, offloaded_offset);
        let new_start_offset = self.new_start_offset(range_index);
        let mut range_end_offset = None;
        if let Some((range, last_start_offset)) = self.ranges.get_mut(&range_index) {
            *last_start_offset = new_start_offset;
            range_end_offset = range.end();
        }
        Self::gen_event(
            (self.stream_id, range_index),
            range_end_offset.as_ref(),
            self.range_offloaded_offsets.get(&range_index),
            new_start_offset,
        )
    }

    fn gen_destroy_events(&self) -> Vec<RangeEvent> {
        let mut events = Vec::with_capacity(self.ranges.len());
        for (range, _) in self.ranges.values() {
            let range_key = (range.stream_id(), range.index() as u32);
            events.push(RangeEvent::Del(range_key));
        }
        events
    }

    fn new_start_offset(&self, range_index: RangeIndex) -> u64 {
        max(
            self.stream_start_offset(),
            max(
                self.ranges
                    .get(&range_index)
                    .map(|r| r.0.start())
                    .unwrap_or_default(),
                *(self.range_offloaded_offsets.get(&range_index).unwrap_or(&0)),
            ),
        )
    }

    fn stream_start_offset(&self) -> u64 {
        self.metadata.as_ref().map(|m| m.start_offset).unwrap_or(0)
    }

    fn gen_event(
        range: Range,
        range_end_offset: Option<&u64>,
        offloaded_offset: Option<&u64>,
        new_start_offset: u64,
    ) -> RangeEvent {
        if offloaded_offset
            .zip(range_end_offset)
            .map(|(a, b)| *a >= *b)
            .unwrap_or(false)
        {
            RangeEvent::Offloaded(range)
        } else {
            RangeEvent::OffsetMove(range, new_start_offset)
        }
    }
}

#[cfg(test)]
mod tests {
    use model::RangeServer;
    use protocol::rpc::header::RangeServerState;
    use tokio::sync::mpsc::error::TryRecvError;

    use super::*;

    #[test]
    fn test_all() -> Result<(), EsError> {
        tokio_uring::start(async move {
            let (m_tx, m_rx) = mpsc::unbounded_channel();
            let (o_tx, o_rx) = mpsc::unbounded_channel();
            let mut manager = DefaultMetadataManager::new(m_rx, Some(o_rx), 233);
            let mut events_rx = manager.watch().unwrap();
            let _ = m_tx.send(ResourceEvent {
                event_type: EventType::Listed,
                resource: stream(1, 100),
            });
            let _ = m_tx.send(ResourceEvent {
                event_type: EventType::Listed,
                resource: range(1, 1, 10, Some(150), 233),
            });
            let _ = m_tx.send(ResourceEvent {
                event_type: EventType::Listed,
                resource: range(1, 2, 150, None, 233),
            });
            let _ = m_tx.send(ResourceEvent {
                event_type: EventType::ListFinished,
                resource: Resource::None,
            });
            assert_eq!(TryRecvError::Empty, events_rx.try_recv().unwrap_err());
            manager.start().await;
            let events = events_rx.try_recv().unwrap();
            assert_eq!(
                vec![
                    RangeEvent::OffsetMove((1, 1), 100),
                    RangeEvent::OffsetMove((1, 2), 150)
                ],
                events
            );

            o_tx.send(vec![((1, 2), 200)]).unwrap();
            let events = events_rx.recv().await.unwrap();
            assert_eq!(vec![RangeEvent::OffsetMove((1, 2), 200)], events);
        });
        Ok(())
    }

    #[test]
    fn test_stream() {
        let mut stream = Stream::new(1, None);
        assert_eq!(
            RangeEvent::OffsetMove((1, 1), 10),
            stream.add(RangeMetadata::new(1, 1, 0, 10, Some(50)))
        );

        assert_eq!(
            RangeEvent::OffsetMove((1, 1), 20),
            stream.update_offloaded_offset(1, 20),
        );
        assert!(stream
            .update_stream(StreamMetadata {
                stream_id: 1,
                start_offset: 15,
                ..Default::default()
            })
            .is_empty());
        assert_eq!(
            vec![RangeEvent::OffsetMove((1, 1), 40)],
            stream.update_stream(StreamMetadata {
                stream_id: 1,
                start_offset: 40,
                ..Default::default()
            }),
        );
        assert_eq!(
            RangeEvent::Offloaded((1, 1)),
            stream.update_offloaded_offset(1, 50),
        );

        assert_eq!(
            RangeEvent::Del((1, 1)),
            stream.del(&RangeMetadata::new(1, 1, 0, 10, Some(50))),
        );
    }

    fn stream(stream_id: u64, start_offset: u64) -> Resource {
        Resource::Stream(StreamMetadata {
            stream_id,
            start_offset,
            ..Default::default()
        })
    }

    fn range(
        stream_id: u64,
        range_index: u32,
        start_offset: u64,
        end_offset: Option<u64>,
        server_id: i32,
    ) -> Resource {
        let mut range =
            RangeMetadata::new(stream_id, range_index as i32, 0, start_offset, end_offset);
        range.replica_mut().push(RangeServer::new(
            server_id,
            "",
            RangeServerState::RANGE_SERVER_STATE_READ_WRITE,
        ));
        Resource::Range(range)
    }
}
