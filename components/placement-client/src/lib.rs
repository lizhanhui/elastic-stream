#![feature(try_find)]

pub mod client;
pub mod error;
pub mod notifier;

pub use crate::client::placement_client::PlacementClient;
pub use crate::client::placement_client_builder::PlacementClientBuilder;
pub use crate::client::response::Response;
