pub(crate) mod manager;
pub(crate) mod watcher;

use std::rc::Rc;

use model::{error::EsError, resource::ResourceEvent};
use pd_client::PlacementDriverClient;
use tokio::sync::mpsc;

#[cfg(any(test, feature = "mock"))]
use mockall::automock;

#[cfg_attr(test, automock)]
pub(crate) trait MetadataWatcher {
    fn watch(&mut self) -> Result<mpsc::UnboundedReceiver<ResourceEvent>, EsError>;

    fn start<P: PlacementDriverClient + 'static>(&self, pd_client: Rc<P>);
}

#[cfg_attr(test, automock)]
pub(crate) trait MetadataManager {
    async fn start(&self);
}
