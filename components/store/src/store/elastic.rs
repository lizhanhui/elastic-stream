use std::{cell::RefCell, fs, path::Path, rc::Rc};

use futures::Future;
use monoio::fs::{File, OpenOptions};
use slog::{debug, error, info, warn, Logger};

use crate::{
    api::{AsyncStore, PutResult, WriteCursor},
    error::StoreError,
};

use super::option::StoreOptions;

pub struct ElasticStore {
    options: StoreOptions,
    logger: Logger,
    segments: Vec<Rc<RefCell<File>>>,
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
            segments: vec![],
            cursor: WriteCursor::new(),
        })
    }

    pub async fn open(&mut self) -> Result<(), StoreError> {
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
        self.segments.push(Rc::new(RefCell::new(f)));
        Ok(())
    }

    /// Return the last segment of the store journal.
    fn last_segment(&self) -> Option<Rc<RefCell<File>>> {
        match self.segments.last() {
            Some(f) => Some(Rc::clone(f)),
            None => None,
        }
    }

    pub fn destroy(&mut self) -> Result<(), StoreError> {
        self.segments.iter().for_each(|s| {
            debug!(self.logger, "Delete file[`{}`]", "a");
        });
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
            self.destroy().unwrap_or_else(|e| {
                warn!(
                    self.logger,
                    "Failed to destroy ElasticStore[{}]. Cause: {:?}",
                    self.options.store_path.path,
                    e
                );
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use slog::{o, Drain, Logger};
    use uuid::Uuid;

    use crate::store::option::StorePath;

    fn get_logger() -> Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let log = slog::Logger::root(drain, o!());
        log
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
        assert_eq!(false, store.segments.is_empty());
        Ok(())
    }

    #[monoio::test]
    async fn test_elastic_store_last_segment() -> Result<(), StoreError> {
        let store_path = StorePath {
            path: "/data/store".to_owned(),
            target_size: 1024 * 1024,
        };

        let mut options = StoreOptions::new(&store_path);
        options.destroy_on_exit = true;
        let logger = get_logger();

        let mut store = ElasticStore::new(&options, &logger)?;

        store.open().await?;
        assert_eq!(true, store.last_segment().is_some());
        Ok(())
    }
}
