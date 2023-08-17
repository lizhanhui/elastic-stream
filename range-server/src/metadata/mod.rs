pub(crate) mod manager;
pub(crate) mod watcher;

use std::rc::Weak;

use model::{error::EsError, range::RangeEvent, resource::ResourceEvent};
use pd_client::PlacementDriverClient;
use tokio::sync::mpsc;

#[cfg(any(test, feature = "mock"))]
use mockall::automock;

pub type MetadataListener = mpsc::UnboundedReceiver<ResourceEvent>;

/// Watch metadata changes through `PlacementDriverClient` and dispatch these changes to `MetadataManager` of each
/// `Worker`.
///
/// Primary worker aggregates a `MetadataWatcher` instance and a valid `PlacementDriverClient`.
#[cfg_attr(test, automock)]
pub(crate) trait MetadataWatcher {
    fn start<P: PlacementDriverClient + 'static>(&mut self, pd_client: Box<P>);

    fn watch(&mut self) -> Result<MetadataListener, EsError>;
}

pub type MetadataEventRx = mpsc::UnboundedReceiver<Vec<RangeEvent>>;

/// Each worker has a `MetadataManager` and it caches all metadata that is relevant to current range server.
///
/// It also aggregates a list of metadata observers. On receiving an metadata event, it decodes and transforms
/// the event, then iterates each observer to notify.
///
/// `MetadataManager` follows [Observer Pattern](https://en.wikipedia.org/wiki/Observer_pattern).
#[cfg_attr(test, automock)]
pub(crate) trait MetadataManager {
    async fn start(&self);

    /// Watch range lifecycle event
    /// - firstly ,the listener will receive all range lifecycle events in a random order.
    /// - then, the listener will receive incremental event when range lifecycle event happens.
    /// Note: the watch must happen before #start.
    fn watch(&mut self) -> Result<MetadataEventRx, EsError>;

    /// Add an observer that is interested in metadata change events.
    ///
    /// # Arguments
    /// * `observer` - Weak reference to the observer
    fn add_observer(&mut self, observer: Weak<dyn ResourceObserver>);
}

/// Contract of metadata change observer.
///
/// Components that are interested in metadata and its changes need to implement this trait and register itself
/// into `MetadataManager`.
///
pub(crate) trait ResourceObserver {
    /// This method feeds metadata and changes to components that are interested
    fn on_resource_event(&self, event: &ResourceEvent);
}
