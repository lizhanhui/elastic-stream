#![feature(async_fn_in_trait)]
#![warn(clippy::pedantic)]

pub mod pd_client;

use model::{error::EsError, object::ObjectMetadata, resource::ResourceEvent};
use protocol::rpc::header::ResourceType;
use tokio::sync::mpsc::Receiver;

#[cfg(any(test, feature = "mock"))]
use mockall::automock;

#[cfg_attr(any(test, feature = "mock"), automock)]
pub trait PlacementDriverClient {
    /// List all resources of the given types, then watch the changes of them.
    ///
    /// # Arguments
    /// * `types` - The types of resources to list and watch. If empty, panic.
    ///
    /// # Returns
    /// * `Receiver` - The receiver of resource events.
    /// Firstly, the receiver will receive multiple [`ResourceEvent`]s with [`Listed`], indicating the resources that are already there before the watch starts.
    /// NOTE: The [`Listed`] resources are in the same order as the given types, but the order of the resources in the same type is NOT guaranteed.
    /// Then, the receiver will receive one [`ResourceEvent`] with [`ListFinished`], indicating that all resources have been listed.
    /// After that, the receiver will receive multiple [`ResourceEvent`]s with [`Added`], [`Modified`] or [`Deleted`], indicating the changes of the resources.
    /// If an error occurs during list or watch (for example, compacted), the receiver will receive one [`ResourceEvent`] with [`Reset`], then all resources will be re-listed and re-watched.
    /// Once the returned receiver is dropped, the operation will be cancelled and related resources will be released.
    ///
    /// [`Listed`]: model::resource::EventType::Listed
    /// [`ListFinished`]: model::resource::EventType::ListFinished
    /// [`Added`]: model::resource::EventType::Added
    /// [`Modified`]: model::resource::EventType::Modified
    /// [`Deleted`]: model::resource::EventType::Deleted
    /// [`Reset`]: model::resource::EventType::Reset
    ///
    fn list_and_watch_resource(&self, types: &[ResourceType]) -> Receiver<ResourceEvent>;

    async fn commit_object(&self, metadata: ObjectMetadata) -> Result<(), EsError>;
}
