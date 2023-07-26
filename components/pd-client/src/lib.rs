#![feature(async_fn_in_trait)]

pub mod pd_client;

use client::error::ClientError;
use model::resource::ResourceEvent;
use protocol::rpc::header::ResourceType;
use tokio::sync::mpsc::Receiver;

pub trait PlacementDriverClient {
    /// List all resources of the given types, then watch the changes of them.
    ///
    /// # Arguments
    /// * `types` - The types of resources to list and watch. If empty, panic.
    ///
    /// # Returns
    /// * `Receiver` - The receiver of resource events.
    /// Firstly, the receiver will receive multiple [`ResourceEvent`]s with  [`LISTED`], indicating the resources that are already there before the watch starts.
    /// Then, the receiver will receive multiple [`ResourceEvent`]s with [`ADDED`], [`MODIFIED`] or [`DELETED`], indicating the changes of the resources.
    /// Once the returned receiver is dropped, the operation will be cancelled and related resources will be released.
    ///
    /// [`LISTED`]: model::resource::EventType::LISTED
    /// [`ADDED`]: model::resource::EventType::ADDED
    /// [`MODIFIED`]: model::resource::EventType::MODIFIED
    /// [`DELETED`]: model::resource::EventType::DELETED
    ///
    fn list_and_watch_resource(
        &self,
        types: &[ResourceType],
    ) -> Receiver<Result<ResourceEvent, ClientError>>;
}
