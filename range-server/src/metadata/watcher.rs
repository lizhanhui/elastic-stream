use log::warn;
use model::{error::EsError, resource::ResourceEvent};
use pd_client::PlacementDriverClient;
use protocol::rpc::header::ResourceType;
use tokio::sync::mpsc;

use super::{MetadataWatcher, ResourceEventRx};

#[derive(Debug, Default)]
pub(crate) struct DefaultMetadataWatcher<P> {
    listeners: Vec<mpsc::UnboundedSender<ResourceEvent>>,
    pd_client: P,
}

impl<P> DefaultMetadataWatcher<P> {
    pub(crate) fn new(pd_client: P) -> Self {
        Self {
            listeners: Vec::new(),
            pd_client,
        }
    }
}

impl<P> MetadataWatcher for DefaultMetadataWatcher<P>
where
    P: PlacementDriverClient,
{
    async fn start(&mut self) {
        let mut rx = self.pd_client.list_and_watch_resource(&[
            ResourceType::RESOURCE_STREAM,
            ResourceType::RESOURCE_RANGE,
            ResourceType::RESOURCE_OBJECT,
        ]);
        loop {
            match rx.recv().await {
                Some(event) => {
                    let mut has_closed = false;
                    for tx in self.listeners.iter() {
                        if tx.send(event.clone()).is_err() {
                            has_closed = true;
                        }
                    }
                    if has_closed {
                        self.listeners.retain(|tx| !tx.is_closed());
                        if self.listeners.is_empty() {
                            break;
                        }
                    }
                }
                _ => {
                    warn!("RangeMetadataWatcher failed to receive resource event: channel closed");
                    break;
                }
            }
        }
    }

    fn watch(&mut self) -> Result<ResourceEventRx, EsError> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.listeners.push(tx);
        Ok(rx)
    }
}
