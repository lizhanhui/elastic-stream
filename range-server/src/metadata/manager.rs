use std::{
    cell::RefCell,
    cmp::max,
    collections::{BTreeMap, HashMap},
    rc::Rc,
};

use log::info;
use model::{
    error::EsError,
    range::{RangeIndex, RangeLifecycleEvent, RangeMetadata, StreamId},
    resource::{EventType, Resource, ResourceEvent},
    stream::StreamMetadata,
};
use tokio::sync::{mpsc, oneshot};

use super::MetadataManager;

type Listener = mpsc::UnboundedSender<Vec<RangeLifecycleEvent>>;

pub(crate) struct DefaultMetadataManager {
    server_id: i32,
    rx: RefCell<Option<mpsc::UnboundedReceiver<ResourceEvent>>>,
    stream_map: Rc<RefCell<HashMap<StreamId, Stream>>>,
    listeners: Vec<Listener>,
}

impl DefaultMetadataManager {
    pub(crate) fn new(rx: mpsc::UnboundedReceiver<ResourceEvent>, server_id: i32) -> Self {
        let stream_map = Rc::new(RefCell::new(HashMap::new()));
        Self {
            server_id,
            rx: RefCell::new(Some(rx)),
            stream_map: stream_map.clone(),
            listeners: Vec::new(),
        }
    }

    async fn start0(
        server_id: i32,
        mut rx: mpsc::UnboundedReceiver<ResourceEvent>,
        stream_map: Rc<RefCell<HashMap<u64, Stream>>>,
        listeners: Vec<Listener>,
    ) {
        let (list_done_tx, list_done_rx) = oneshot::channel();
        let mut list_done_tx = Some(list_done_tx);
        let mut listeners = listeners.clone();
        tokio_uring::spawn(async move {
            let mut list_done = false;
            loop {
                match rx.recv().await {
                    Some(event) => {
                        match event.event_type {
                            EventType::ListFinished => {
                                if let Some(tx) = list_done_tx.take() {
                                    let _ = tx.send(());
                                }
                                list_done = true;
                                if !notify_listeners(
                                    gen_all_range_events(&stream_map),
                                    &mut listeners,
                                ) {
                                    break;
                                } else {
                                    continue;
                                }
                            }
                            EventType::Reset => {
                                todo!("reset pd list&watch support");
                            }
                            _ => {}
                        }
                        let incremental_range_events =
                            Self::handle_event(server_id, event, &stream_map);
                        if list_done && !notify_listeners(incremental_range_events, &mut listeners)
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
    }

    #[inline]
    fn handle_event(
        server_id: i32,
        event: ResourceEvent,
        stream_map: &Rc<RefCell<HashMap<u64, Stream>>>,
    ) -> Vec<RangeLifecycleEvent> {
        let mut stream_map = stream_map.borrow_mut();
        match event.resource {
            Resource::Stream(stream_metadata) => {
                let stream_id = stream_metadata.stream_id.unwrap_or(0);
                match event.event_type {
                    EventType::Listed | EventType::Added | EventType::Modified => {
                        if let Some(stream) = stream_map.get_mut(&stream_id) {
                            stream.metadata = Some(stream_metadata);
                            return stream.gen_range_events(true);
                        } else {
                            stream_map.insert(stream_id, Stream::new(Some(stream_metadata)));
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
                let stream_id = range.stream_id() as u64;
                let stream = if let Some(stream) = stream_map.get_mut(&stream_id) {
                    stream
                } else {
                    stream_map.insert(stream_id, Stream::new(None));
                    stream_map.get_mut(&stream_id).unwrap()
                };
                match event.event_type {
                    EventType::Listed | EventType::Added | EventType::Modified => {
                        return vec![stream.add(range)];
                    }
                    EventType::Deleted => {
                        stream.del(&range);
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        Vec::new()
    }
}

fn notify_listeners(events: Vec<RangeLifecycleEvent>, listeners: &mut Vec<Listener>) -> bool {
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

fn gen_all_range_events(
    stream_map: &RefCell<HashMap<StreamId, Stream>>,
) -> Vec<RangeLifecycleEvent> {
    let stream_map = stream_map.borrow();
    let mut events = Vec::with_capacity(stream_map.len());
    for stream in stream_map.values() {
        events.extend(stream.gen_range_events(false));
    }
    events
}

impl MetadataManager for DefaultMetadataManager {
    async fn start(&self) {
        let rx = self.rx.borrow_mut().take().unwrap();
        Self::start0(
            self.server_id,
            rx,
            self.stream_map.clone(),
            self.listeners.clone(),
        )
        .await;
    }

    fn watch(&mut self) -> Result<mpsc::UnboundedReceiver<Vec<RangeLifecycleEvent>>, EsError> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.listeners.push(tx);
        Ok(rx)
    }
}

struct Stream {
    metadata: Option<StreamMetadata>,
    ranges: BTreeMap<RangeIndex, RangeMetadata>,
}

impl Stream {
    fn new(metadata: Option<StreamMetadata>) -> Self {
        Self {
            metadata,
            ranges: BTreeMap::new(),
        }
    }

    fn add(&mut self, range: RangeMetadata) -> RangeLifecycleEvent {
        let event = RangeLifecycleEvent::OffsetMove(
            (range.stream_id() as u64, range.index() as u32),
            max(self.stream_start_offset(), range.start()),
        );
        self.ranges.insert(range.index() as u32, range);
        event
    }

    fn del(&mut self, range: &RangeMetadata) {
        self.ranges.remove(&(range.index() as u32));
    }

    fn gen_range_events(&self, incremental: bool) -> Vec<RangeLifecycleEvent> {
        let start_offset = self.stream_start_offset();
        let mut events = Vec::new();
        for range in self.ranges.values() {
            let range_start_offset = range.start();
            let range_key = (range.stream_id() as u64, range.index() as u32);
            if range_start_offset >= start_offset {
                if incremental {
                    break;
                } else {
                    events.push(RangeLifecycleEvent::OffsetMove(
                        range_key,
                        range_start_offset,
                    ));
                    continue;
                }
            }
            if let Some(range_end_offset) = range.end() {
                if range_end_offset <= start_offset {
                    events.push(RangeLifecycleEvent::Del(range_key));
                    continue;
                }
            }
            events.push(RangeLifecycleEvent::OffsetMove(range_key, start_offset));
        }
        events
    }

    fn gen_destroy_events(&self) -> Vec<RangeLifecycleEvent> {
        let mut events = Vec::with_capacity(self.ranges.len());
        for range in self.ranges.values() {
            let range_key = (range.stream_id() as u64, range.index() as u32);
            events.push(RangeLifecycleEvent::Del(range_key));
        }
        events
    }

    fn stream_start_offset(&self) -> u64 {
        self.metadata.as_ref().map(|m| m.start_offset).unwrap_or(0)
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
            let (tx, rx) = mpsc::unbounded_channel();
            let mut manager = DefaultMetadataManager::new(rx, 233);
            let mut events_rx = manager.watch().unwrap();
            let _ = tx.send(ResourceEvent {
                event_type: EventType::Listed,
                resource: stream(1, 100),
            });
            let _ = tx.send(ResourceEvent {
                event_type: EventType::Listed,
                resource: range(1, 1, 10, Some(150), 233),
            });
            let _ = tx.send(ResourceEvent {
                event_type: EventType::Listed,
                resource: range(1, 2, 150, None, 233),
            });
            let _ = tx.send(ResourceEvent {
                event_type: EventType::ListFinished,
                resource: Resource::NONE,
            });
            assert_eq!(TryRecvError::Empty, events_rx.try_recv().unwrap_err());
            manager.start().await;
            let events = events_rx.try_recv().unwrap();
            assert_eq!(
                vec![
                    RangeLifecycleEvent::OffsetMove((1, 1), 100),
                    RangeLifecycleEvent::OffsetMove((1, 2), 150)
                ],
                events
            )
        });
        Ok(())
    }

    #[test]
    fn test_stream_gen_range_events() {
        let mut stream = Stream::new(Some(StreamMetadata {
            stream_id: Some(1),
            start_offset: 100,
            ..Default::default()
        }));
        stream.add(RangeMetadata::new(1, 1, 0, 10, Some(20)));
        stream.add(RangeMetadata::new(1, 2, 0, 20, Some(150)));
        stream.add(RangeMetadata::new(1, 3, 0, 150, None));
        assert_eq!(
            vec![
                RangeLifecycleEvent::Del((1, 1)),
                RangeLifecycleEvent::OffsetMove((1, 2), 100),
                RangeLifecycleEvent::OffsetMove((1, 3), 150),
            ],
            stream.gen_range_events(false)
        );
        assert_eq!(
            vec![
                RangeLifecycleEvent::Del((1, 1)),
                RangeLifecycleEvent::OffsetMove((1, 2), 100),
            ],
            stream.gen_range_events(true)
        );
    }

    fn stream(stream_id: u64, start_offset: u64) -> Resource {
        Resource::Stream(StreamMetadata {
            stream_id: Some(stream_id),
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
        let mut range = RangeMetadata::new(
            stream_id as i64,
            range_index as i32,
            0,
            start_offset,
            end_offset,
        );
        range.replica_mut().push(RangeServer::new(
            server_id,
            "",
            RangeServerState::RANGE_SERVER_STATE_READ_WRITE,
        ));
        Resource::Range(range)
    }
}
