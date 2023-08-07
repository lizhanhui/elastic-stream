use std::{cell::RefCell, collections::HashMap, rc::Rc};

use log::info;
use model::{
    range::RangeMetadata,
    resource::{EventType, Resource, ResourceEvent},
    stream::StreamMetadata,
};
use tokio::sync::{mpsc, oneshot};

use super::MetadataManager;

type RangeIndex = (u64, u32);

pub(crate) struct DefaultMetadataManager {
    server_id: i32,
    rx: RefCell<Option<mpsc::UnboundedReceiver<ResourceEvent>>>,
    stream_metadata_map: Rc<RefCell<HashMap<u64, StreamMetadata>>>,
    range_metadata_map: Rc<RefCell<HashMap<RangeIndex, RangeMetadata>>>,
}

impl DefaultMetadataManager {
    pub(crate) fn new(rx: mpsc::UnboundedReceiver<ResourceEvent>, server_id: i32) -> Self {
        let range_metadata_map = Rc::new(RefCell::new(HashMap::new()));
        let stream_metadata_map = Rc::new(RefCell::new(HashMap::new()));
        Self {
            server_id,
            rx: RefCell::new(Some(rx)),
            stream_metadata_map: stream_metadata_map.clone(),
            range_metadata_map: range_metadata_map.clone(),
        }
    }

    async fn watch(
        server_id: i32,
        mut rx: mpsc::UnboundedReceiver<ResourceEvent>,
        stream_metadata_map: Rc<RefCell<HashMap<u64, StreamMetadata>>>,
        range_metadata_map: Rc<RefCell<HashMap<RangeIndex, RangeMetadata>>>,
    ) {
        let (list_done_tx, list_done_rx) = oneshot::channel();
        let mut list_done_tx = Some(list_done_tx);
        tokio_uring::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(event) => {
                        match event.event_type {
                            EventType::ListFinished => {
                                if let Some(tx) = list_done_tx.take() {
                                    let _ = tx.send(());
                                }
                                continue;
                            }
                            EventType::Reset => {
                                todo!("reset pd list&watch support");
                            }
                            _ => {}
                        }
                        Self::handle_event(
                            server_id,
                            event,
                            &stream_metadata_map,
                            &range_metadata_map,
                        );
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
        stream_metadata_map: &Rc<RefCell<HashMap<u64, StreamMetadata>>>,
        range_metadata_map: &Rc<RefCell<HashMap<RangeIndex, RangeMetadata>>>,
    ) {
        match event.resource {
            Resource::Stream(stream) => {
                let stream_id = stream.stream_id.unwrap_or(0);
                match event.event_type {
                    EventType::Listed | EventType::Added | EventType::Modified => {
                        stream_metadata_map.borrow_mut().insert(stream_id, stream);
                    }
                    EventType::Deleted => {
                        stream_metadata_map.borrow_mut().remove(&stream_id);
                    }
                    _ => {}
                }
            }
            Resource::Range(range) => {
                if !range.held_by(server_id) {
                    return;
                }
                match event.event_type {
                    EventType::Listed | EventType::Added | EventType::Modified => {
                        range_metadata_map
                            .borrow_mut()
                            .insert(range_key(&range), range);
                    }
                    EventType::Deleted => {
                        range_metadata_map.borrow_mut().remove(&range_key(&range));
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
}

impl MetadataManager for DefaultMetadataManager {
    async fn start(&self) {
        let rx = self.rx.borrow_mut().take().unwrap();
        Self::watch(
            self.server_id,
            rx,
            self.stream_metadata_map.clone(),
            self.range_metadata_map.clone(),
        )
        .await;
    }
}

#[inline]
fn range_key(range: &RangeMetadata) -> RangeIndex {
    (range.stream_id() as u64, range.index() as u32)
}
