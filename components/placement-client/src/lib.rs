//! Data node uses placement clients to talk to placement managers.
//!
//! As a result, placement clients shall comply with `thread-per-core` threading model. Further,
//! placement clients reuse the same `tokio-uring` network library stack to initiate requests and
//! reap completed responses.
//!  
//! For applications that need to talk to `PlacementManager` and `DataNode`, please use crate `front-end-sdk`.

#![feature(try_find)]

pub mod client;
pub mod error;
pub mod notifier;

pub use crate::client::config::ClientConfig;
pub use crate::client::placement_client::PlacementClient;
pub use crate::client::placement_client_builder::PlacementClientBuilder;
pub use crate::client::response::Response;
