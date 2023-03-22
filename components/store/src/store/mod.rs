pub(crate) mod append_result;
pub(crate) mod fetch_result;
mod elastic_store;

pub use elastic_store::ElasticStore;
pub use append_result::AppendResult;
pub use fetch_result::FetchResult;
