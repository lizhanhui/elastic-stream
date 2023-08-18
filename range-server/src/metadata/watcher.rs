use log::warn;
use model::{error::EsError, resource::ResourceEvent};
use pd_client::PlacementDriverClient;
use protocol::rpc::header::ResourceType;
use tokio::sync::mpsc;

use super::{MetadataWatcher, ResourceEventRx};

#[derive(Debug, Default)]
pub(crate) struct DefaultMetadataWatcher {
    listeners: Vec<mpsc::UnboundedSender<ResourceEvent>>,
    started: bool,
}

impl DefaultMetadataWatcher {
    pub(crate) fn new() -> Self {
        Self {
            listeners: Vec::new(),
            started: false,
        }
    }
}

impl MetadataWatcher for DefaultMetadataWatcher {
    fn start<P>(&mut self, pd_client: Box<P>)
    where
        P: PlacementDriverClient + 'static,
    {
        self.started = true;
        let mut rx = pd_client.list_and_watch_resource(&[
            ResourceType::RESOURCE_STREAM,
            ResourceType::RESOURCE_RANGE,
        ]);
        let mut listeners = self.listeners.clone();
        tokio_uring::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(event) => {
                        let mut has_closed = false;
                        for tx in listeners.iter() {
                            if tx.send(event.clone()).is_err() {
                                has_closed = true;
                            }
                        }
                        if has_closed {
                            listeners.retain(|tx| !tx.is_closed());
                            if listeners.is_empty() {
                                break;
                            }
                        }
                    }
                    _ => {
                        warn!(
                            "RangeMetadataWatcher failed to receive resource event: channel closed"
                        );
                        break;
                    }
                }
            }
        });
    }

    fn watch(&mut self) -> Result<ResourceEventRx, EsError> {
        if self.started {
            return Err(EsError::unexpected("watcher already started"));
        }
        let (tx, rx) = mpsc::unbounded_channel();
        self.listeners.push(tx);
        Ok(rx)
    }
}
