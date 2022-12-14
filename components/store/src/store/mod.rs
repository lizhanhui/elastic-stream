use std::{fs, path::Path};

use futures::Future;
use monoio::fs::{File, OpenOptions};
use slog::{error, info, warn, Logger};

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
    destroy_on_exit: bool,
}

impl StoreOptions {
    pub fn new(store_path: &StorePath) -> Self {
        Self {
            create_if_missing: true,
            store_path: store_path.clone(),
            destroy_on_exit: false,
        }
    }
}

struct WriteCursor {
    write: u64,
    commit: u64,
}

impl WriteCursor {
    fn new() -> Self {
        Self {
            write: 0,
            commit: 0,
        }
    }
}

pub struct ElasticStore {
    options: StoreOptions,
    logger: Logger,
    current: Option<File>,
    cursor: WriteCursor,
}

impl ElasticStore {
    pub fn new(options: &StoreOptions, logger: &Logger) -> Result<Self, StoreError> {
        if !options.create_if_missing && !Path::new(&options.store_path.path).exists() {
            error!(
                logger,
                "Specified store path[`{}`] does not exist.", options.store_path.path
            );
            return Err(StoreError::NotFound(
                "Specified store path does not exist".to_owned(),
            ));
        }

        Ok(Self {
            options: options.clone(),
            logger: logger.clone(),
            current: None,
            cursor: WriteCursor::new(),
        })
    }

    pub async fn open(&mut self) -> Result<(), StoreError> {
        debug_assert!(self.current.is_none());

        let path = format!("{}/1", self.options.store_path.path);

        let path = Path::new(&path);
        {
            let dir = match path.parent() {
                Some(p) => p,
                None => {
                    return Err(StoreError::InvalidPath(
                        self.options.store_path.path.clone(),
                    ))
                }
            };

            if !dir.exists() {
                info!(self.logger, "Create directory: {}", dir.display());
                fs::create_dir_all(dir).map_err(|e| StoreError::Other(e))?
            }
        }

        let f = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)
            .await?;
        self.current = Some(f);
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

impl Drop for ElasticStore {
    fn drop(&mut self) {
        if self.options.destroy_on_exit {
            warn!(
                self.logger,
                "Start to destroy ElasticStore[`{}`]", self.options.store_path.path
            );
        }
    }
}

#[cfg(test)]
mod tests {

    use uuid::Uuid;

    use super::*;

    use slog::{o, Drain, Logger};

    fn get_logger() -> Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let log = slog::Logger::root(drain, o!());
        log
    }

    #[monoio::test]
    async fn test_store_options_new() {
        let store_path = StorePath {
            path: "/data/store".to_owned(),
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
        let logger = get_logger();

        let store = ElasticStore::new(&options, &logger);
        assert_eq!(true, store.is_err());
    }

    #[monoio::test]
    async fn test_elastic_store_open() -> Result<(), StoreError> {
        let store_path = StorePath {
            path: "/data/store".to_owned(),
            target_size: 1024 * 1024,
        };

        let mut options = StoreOptions::new(&store_path);
        options.destroy_on_exit = true;
        let logger = get_logger();

        let mut store = ElasticStore::new(&options, &logger)?;

        store.open().await?;
        assert_eq!(true, store.current.is_some());
        Ok(())
    }
}
