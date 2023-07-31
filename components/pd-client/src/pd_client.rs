use std::{rc::Rc, time::Duration};

use client::client::Client;
use model::{
    error::EsError,
    object::ObjectMetadata,
    resource::{EventType, ResourceEvent},
};
use protocol::rpc::header::{ErrorCode, ResourceType};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::sync::CancellationToken;

use crate::PlacementDriverClient;

const LIST_PAGE_SIZE: i32 = 1024;
const WATCH_TIMEOUT: Duration = Duration::from_secs(5 * 60);

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
    fn list_and_watch_resource(
        &self,
        types: &[ResourceType],
    ) -> Receiver<Result<ResourceEvent, EsError>> {
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
        tokio_uring::spawn(async move {
            let Some(version) = Self::list_resource(&token, &client, &types, &tx).await else {
                return;
            };
            _ = Self::watch_resource(&token, &client, &types, version, &tx).await;
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
    async fn list_resource(
        token: &CancellationToken,
        client: &Rc<C>,
        types: &[ResourceType],
        tx: &Sender<Result<ResourceEvent, EsError>>,
    ) -> Option<i64> {
        let mut continuation = None;
        let mut version;
        loop {
            match client
                .list_resource(types, LIST_PAGE_SIZE, &continuation)
                .await
            {
                Ok(result) => {
                    log::trace!("list resource success. result: {:?}", result);
                    continuation = result.continuation;
                    version = Some(result.version);
                    for resource in result.resources {
                        let event = ResourceEvent {
                            resource,
                            event_type: EventType::LISTED,
                        };
                        tokio::select! {
                            _ = token.cancelled() => {
                                return None;
                            }
                            sent = tx.send(Ok(event)) => {
                                if sent.is_err() {
                                    log::debug!("receiver dropped, stop to list resource. types: {:?}, continuation: {:?}", types, continuation);
                                    return None;
                                }
                            }
                        }
                    }
                    if continuation.is_none() {
                        log::trace!("no more resources to list. types: {:?}", types);
                        break;
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
                    return None;
                }
            }
        }
        version
    }

    async fn watch_resource(
        token: &CancellationToken,
        client: &Rc<C>,
        types: &[ResourceType],
        start_version: i64,
        tx: &Sender<Result<ResourceEvent, EsError>>,
    ) -> Result<(), EsError> {
        let mut version = start_version;
        loop {
            match client.watch_resource(types, version, WATCH_TIMEOUT).await {
                Ok(result) => {
                    log::trace!("watch resource success. result: {:?}", result);
                    version = result.version;
                    for event in result.events {
                        tokio::select! {
                            _ = token.cancelled() => {
                                return Ok(());
                            }
                            sent = tx.send(Ok(event)) => {
                                if sent.is_err() {
                                    log::debug!("receiver dropped, stop to watch resource. types: {:?}, version: {:?}", types, version);
                                    return Ok(());
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
                            "watch resource failed. types: {:?}, version: {:?}, err: {:?}",
                            types,
                            version,
                            e
                        );
                        return Err(EsError::unexpected("todo"));
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
    use std::{error::Error, rc::Rc, sync::Arc};

    use client::DefaultClient;
    use mock_server::run_listener;
    use model::resource::Resource;
    use protocol::rpc::header::ResourceType;
    use tokio::sync::broadcast;

    use crate::PlacementDriverClient;

    use super::DefaultPlacementDriverClient;

    #[test]
    fn test_list_and_watch_resource() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        tokio_uring::start(async {
            let port = run_listener().await;
            let config = config::Configuration {
                placement_driver: format!("127.0.0.1:{port}"),
                ..Default::default()
            };
            let (tx, _rx) = broadcast::channel(1);
            let client = DefaultClient::new(Arc::new(config), tx);
            let pd_client = DefaultPlacementDriverClient::new(Rc::new(client));

            let mut receiver = pd_client.list_and_watch_resource(&[
                ResourceType::RANGE_SERVER,
                ResourceType::STREAM,
                ResourceType::RANGE,
                ResourceType::OBJECT,
            ]);
            let mut events = Vec::new();
            for _ in 0..8 {
                let event = receiver.recv().await.unwrap().unwrap();
                events.push(event);
            }

            assert_eq!(model::resource::EventType::LISTED, events[0].event_type);
            assert!(matches!(events[0].resource, Resource::RangeServer(_)));
            assert_eq!(model::resource::EventType::LISTED, events[1].event_type);
            assert!(matches!(events[1].resource, Resource::Stream(_)));
            assert_eq!(model::resource::EventType::LISTED, events[2].event_type);
            assert!(matches!(events[2].resource, Resource::Range(_)));
            assert_eq!(model::resource::EventType::LISTED, events[3].event_type);
            assert!(matches!(events[3].resource, Resource::Object(_)));

            assert_eq!(model::resource::EventType::ADDED, events[4].event_type);
            assert!(matches!(events[4].resource, Resource::RangeServer(_)));
            assert_eq!(model::resource::EventType::MODIFIED, events[5].event_type);
            assert!(matches!(events[5].resource, Resource::Stream(_)));
            assert_eq!(model::resource::EventType::DELETED, events[6].event_type);
            assert!(matches!(events[6].resource, Resource::Range(_)));
            assert_eq!(model::resource::EventType::ADDED, events[7].event_type);
            assert!(matches!(events[7].resource, Resource::Object(_)));
            Ok(())
        })
    }
}
