use std::{rc::Rc, time::Duration};

use client::client::Client;
use model::{
    error::EsError,
    object::ObjectMetadata,
    resource::{EventType, ResourceEvent},
};
use monoio::time;
use protocol::rpc::header::{ErrorCode, ResourceType};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::sync::CancellationToken;

use crate::PlacementDriverClient;

const LIST_PAGE_SIZE: i32 = 1024;
const WATCH_TIMEOUT: Duration = Duration::from_secs(5 * 60);

enum ListAndWatchError {
    Es(EsError),
    Cancelled,
}

pub struct DefaultPlacementDriverClient<C>
where
    C: Client + 'static,
{
    token: CancellationToken,
    client: Rc<C>,
}

impl<C> DefaultPlacementDriverClient<C>
where
    C: Client + 'static,
{
    pub fn new(client: Rc<C>) -> Self {
        let token = CancellationToken::new();
        Self { token, client }
    }
}

impl<C> PlacementDriverClient for DefaultPlacementDriverClient<C>
where
    C: Client + 'static,
{
    fn list_and_watch_resource(&self, types: &[ResourceType]) -> Receiver<ResourceEvent> {
        let types = types
            .iter()
            .copied()
            .filter(|&t| t.0 >= ResourceType::ENUM_MIN)
            .filter(|&t| t.0 <= ResourceType::ENUM_MAX)
            .filter(|&t| t != ResourceType::UNKNOWN)
            .collect::<Vec<_>>();
        assert!(!types.is_empty(), "resource types must not be empty");

        let (tx, rx) = channel(LIST_PAGE_SIZE as usize); // one page size is enough

        let client = self.client.clone();
        let token = self.token.child_token();
        monoio::spawn(async move {
            loop {
                let version = match Self::list_resource(&token, &client, &types, &tx).await {
                    Ok(v) => v,
                    Err(ListAndWatchError::Cancelled) => {
                        return;
                    }
                    Err(ListAndWatchError::Es(_)) => {
                        if tx.send(ResourceEvent::reset()).await.is_err() {
                            // receiver dropped, stop to list and watch resource
                            return;
                        }
                        time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                if tx.send(ResourceEvent::list_finished()).await.is_err() {
                    // receiver dropped, stop to list and watch resource
                    return;
                }

                match Self::watch_resource(&token, &client, &types, version, &tx).await {
                    ListAndWatchError::Cancelled => {
                        return;
                    }
                    ListAndWatchError::Es(_) => {
                        if tx.send(ResourceEvent::reset()).await.is_err() {
                            // receiver dropped, stop to list and watch resource
                            return;
                        }
                        time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
            }
        });

        rx
    }

    async fn commit_object(&self, metadata: ObjectMetadata) -> Result<(), EsError> {
        self.client.commit_object(metadata.clone()).await
    }
}

impl<C> DefaultPlacementDriverClient<C>
where
    C: Client + 'static,
{
    /// List all resources of the given types.
    /// It returns the version of the resources if the list operation succeeds.
    async fn list_resource(
        token: &CancellationToken,
        client: &Rc<C>,
        types: &[ResourceType],
        tx: &Sender<ResourceEvent>,
    ) -> Result<i64, ListAndWatchError> {
        let mut continuation = None;
        loop {
            match client
                .list_resource(types, LIST_PAGE_SIZE, &continuation)
                .await
            {
                Ok(result) => {
                    log::trace!("list resource success. result: {:?}", result);
                    continuation = result.continuation;
                    for resource in result.resources {
                        let event = ResourceEvent {
                            resource,
                            event_type: EventType::Listed,
                        };
                        monoio::select! {
                            _ = token.cancelled() => {
                                return Err(ListAndWatchError::Cancelled);
                            }
                            sent = tx.send(event) => {
                                if sent.is_err() {
                                    log::debug!("receiver dropped, stop to list resource. types: {:?}, continuation: {:?}", types, continuation);
                                    return Err(ListAndWatchError::Cancelled);
                                }
                            }
                        }
                    }
                    if continuation.is_none() {
                        log::trace!("no more resources to list. types: {:?}", types);
                        return Ok(result.version);
                    }
                }
                Err(e) => {
                    // TODO: handle error
                    log::error!(
                        "list resource failed. types: {:?}, continuation: {:?}, err: {:?}",
                        types,
                        continuation,
                        e
                    );
                    return Err(ListAndWatchError::Es(e));
                }
            }
        }
    }

    /// Watch the changes of the resources.
    async fn watch_resource(
        token: &CancellationToken,
        client: &Rc<C>,
        types: &[ResourceType],
        start_version: i64,
        tx: &Sender<ResourceEvent>,
    ) -> ListAndWatchError {
        let mut version = start_version;
        loop {
            match client.watch_resource(types, version, WATCH_TIMEOUT).await {
                Ok(result) => {
                    log::trace!("watch resource success. result: {result:?}");
                    version = result.version;
                    for event in result.events {
                        monoio::select! {
                            _ = token.cancelled() => {
                                return ListAndWatchError::Cancelled;
                            }
                            sent = tx.send(event) => {
                                if sent.is_err() {
                                    log::debug!("receiver dropped, stop to watch resource. types: {types:?}, version: {version:?}");
                                    return ListAndWatchError::Cancelled;
                                }
                            }
                        }
                    }
                }
                #[allow(clippy::single_match_else)]
                Err(e) => match e.code {
                    ErrorCode::RPC_TIMEOUT => continue,
                    _ => {
                        // TODO: handle error
                        log::error!(
                            "watch resource failed. types: {types:?}, version: {version:?}, err: {e:?}"
                        );
                        return ListAndWatchError::Es(e);
                    }
                },
            }
        }
    }
}

impl<C> Drop for DefaultPlacementDriverClient<C>
where
    C: Client + 'static,
{
    fn drop(&mut self) {
        self.token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::DefaultPlacementDriverClient;
    use crate::PlacementDriverClient;
    use client::DefaultClient;
    use mock_server::run_listener;
    use model::resource::Resource;
    use protocol::rpc::header::ResourceType;
    use std::{error::Error, rc::Rc, sync::Arc};

    #[monoio::test]
    async fn test_list_and_watch_resource() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        let port = run_listener().await;
        let config = config::Configuration {
            placement_driver: format!("127.0.0.1:{port}"),
            ..Default::default()
        };
        let client = DefaultClient::new(Arc::new(config));
        let pd_client = DefaultPlacementDriverClient::new(Rc::new(client));

        let mut receiver = pd_client.list_and_watch_resource(&[
            ResourceType::RANGE_SERVER,
            ResourceType::STREAM,
            ResourceType::RANGE,
            ResourceType::OBJECT,
        ]);
        let mut events = Vec::new();
        for _ in 0..9 {
            let event = receiver.recv().await.unwrap();
            events.push(event);
        }

        assert_eq!(model::resource::EventType::Listed, events[0].event_type);
        assert!(matches!(events[0].resource, Resource::RangeServer(_)));
        assert_eq!(model::resource::EventType::Listed, events[1].event_type);
        assert!(matches!(events[1].resource, Resource::Stream(_)));
        assert_eq!(model::resource::EventType::Listed, events[2].event_type);
        assert!(matches!(events[2].resource, Resource::Range(_)));
        assert_eq!(model::resource::EventType::Listed, events[3].event_type);
        assert!(matches!(events[3].resource, Resource::Object(_)));

        assert_eq!(
            model::resource::EventType::ListFinished,
            events[4].event_type
        );

        assert_eq!(model::resource::EventType::Added, events[5].event_type);
        assert!(matches!(events[5].resource, Resource::RangeServer(_)));
        assert_eq!(model::resource::EventType::Modified, events[6].event_type);
        assert!(matches!(events[6].resource, Resource::Stream(_)));
        assert_eq!(model::resource::EventType::Deleted, events[7].event_type);
        assert!(matches!(events[7].resource, Resource::Range(_)));
        assert_eq!(model::resource::EventType::Added, events[8].event_type);
        assert!(matches!(events[8].resource, Resource::Object(_)));
        Ok(())
    }
}
