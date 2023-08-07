use std::{cell::RefCell, rc::Rc};

use log::warn;
use model::{error::EsError, resource::ResourceEvent};
use pd_client::PlacementDriverClient;
use protocol::rpc::header::ResourceType;
use tokio::sync::mpsc;

use super::MetadataWatcher;

pub(crate) struct DefaultMetadataWatcher {
    listeners: Vec<mpsc::UnboundedSender<ResourceEvent>>,
    started: RefCell<bool>,
}

impl DefaultMetadataWatcher {
    pub(crate) fn new() -> Self {
        Self {
            listeners: Vec::new(),
            started: RefCell::new(false),
        }
    }
}

impl MetadataWatcher for DefaultMetadataWatcher {
    fn start<P>(&self, pd_client: Rc<P>)
    where
        P: PlacementDriverClient + 'static,
    {
        *self.started.borrow_mut() = true;
        let mut rx =
            pd_client.list_and_watch_resource(&[ResourceType::STREAM, ResourceType::RANGE]);
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

    fn watch(&mut self) -> Result<mpsc::UnboundedReceiver<ResourceEvent>, EsError> {
        if *self.started.borrow() {
            return Err(EsError::unexpected("watcher already started"));
        }
        let (tx, rx) = mpsc::unbounded_channel();
        self.listeners.push(tx);
        Ok(rx)
    }
}
