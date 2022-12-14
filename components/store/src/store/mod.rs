use std::path::Path;

use futures::Future;

use crate::{
    api::{AsyncStore, PutResult},
    error::StoreError,
};

#[derive(Debug, Clone, PartialEq)]
pub struct StorePath {
    path: String,

    /// Target size of total files under the path, in byte.
    target_size: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoreOptions {
    create_if_missing: bool,
    store_path: StorePath,
}

impl StoreOptions {
    pub fn new(store_path: &StorePath) -> Self {
        Self {
            create_if_missing: true,
            store_path: store_path.clone(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ElasticStore {
    options: StoreOptions,
}

impl ElasticStore {
    pub fn new(options: &StoreOptions) -> Option<Self> {
        if !options.create_if_missing && !Path::new(&options.store_path.path).exists() {
            return None;
        }

        Some(Self {
            options: options.clone(),
        })
    }

    pub async fn open(&mut self) -> Result<(), StoreError> {
        Ok(())
    }
}

impl AsyncStore for ElasticStore {
    type PutFuture<'a> = impl Future<Output = Result<PutResult, StoreError>>
    where
    Self: 'a;

    fn put(&mut self, buf: &[u8]) -> Self::PutFuture<'_> {
        async move { Ok(PutResult {}) }
    }
}

#[cfg(test)]
mod tests {

    use uuid::Uuid;

    use super::*;

    #[monoio::test]
    async fn test_store_options_new() {
        let store_path = StorePath {
            path: "/data".to_owned(),
            target_size: 1024 * 1024,
        };

        let options = StoreOptions::new(&store_path);

        assert_eq!(true, options.create_if_missing);
    }

    #[test]
    fn test_elastic_store_new_with_non_existing_path() {
        let uuid = Uuid::new_v4();
        dbg!(uuid);
        let store_path = StorePath {
            path: format!("/data/{}", uuid.hyphenated().to_string()),
            target_size: 1024 * 1024,
        };

        let mut options = StoreOptions::new(&store_path);
        options.create_if_missing = false;
        assert_eq!(None, ElasticStore::new(&options));
    }
}
