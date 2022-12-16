use std::{cell::RefCell, fs, path::Path, rc::Rc};

use async_trait::async_trait;
use monoio::{buf::IoBuf, fs::OpenOptions};
use slog::{debug, error, info, warn, Logger};

use crate::{
    api::{AsyncStore, PutResult, WriteCursor},
    error::StoreError,
};

use super::{option::StoreOptions, segment::JournalSegment};

pub struct ElasticStore {
    options: StoreOptions,
    logger: Logger,
    segments: Vec<Rc<RefCell<JournalSegment>>>,
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
        let file_name = format!("{}/1", self.options.store_path.path);

        let path = Path::new(&file_name);
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
                fs::create_dir_all(dir).map_err(|e| StoreError::IO(e))?
            }
        }

        let f = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)
            .await?;
        let segment = JournalSegment { file: f, file_name };
        self.segments.push(Rc::new(RefCell::new(segment)));
        Ok(())
    }

    /// Return the last segment of the store journal.
    fn last_segment(&self) -> Option<Rc<RefCell<JournalSegment>>> {
        match self.segments.last() {
            Some(f) => Some(Rc::clone(f)),
            None => None,
        }
    }

    /// Delete all journal segment files.
    pub fn destroy(&mut self) -> Result<(), StoreError> {
        self.segments.iter().for_each(|s| {
            let file_name = &s.borrow().file_name;
            debug!(self.logger, "Delete file[`{}`]", file_name);
            fs::remove_file(Path::new(file_name)).unwrap_or_else(|e| {
                warn!(
                    self.logger,
                    "Failed to delete file[`{}`]. Cause: {:?}", file_name, e
                );
            });
        });

        Ok(())
    }
}

#[async_trait(?Send)]
impl AsyncStore for ElasticStore {
    async fn put<T>(&mut self, buf: T) -> Result<PutResult, StoreError>
    where
        T: IoBuf,
    {
        let segment = self.last_segment();

        let segment = match segment {
            Some(ref segment) => segment,
            None => {
                error!(self.logger, "No writable journal segment is available");
                // A ready failure future
                return Err(StoreError::DiskFull(
                    "No writeable segment is available".to_owned(),
                ));
            }
        };
        let file = &segment.borrow().file;

        let (res, _buf) = file.write_all_at(buf, self.cursor.write).await;
        match res {
            Ok(_) => Ok(PutResult {}),
            Err(e) => {
                error!(
                    self.logger,
                    "Failed to append data to journal segment. {:?}", e
                );
                Err(StoreError::IO(e))
            }
        }
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
            path: format!(
                "{}/{}",
                std::env::temp_dir().as_path().display(),
                uuid.hyphenated().to_string()
            ),
            target_size: 1024 * 1024,
        };

        let mut options = StoreOptions::new(&store_path);
        options.create_if_missing = false;
        let logger = get_logger();

        let store = ElasticStore::new(&options, &logger);
        assert_eq!(true, store.is_err());
    }

    async fn new_elastic_store() -> Result<ElasticStore, StoreError> {
        let store_path = StorePath {
            path: format!("{}/store", std::env::temp_dir().as_path().display()),
            target_size: 1024 * 1024,
        };

        let mut options = StoreOptions::new(&store_path);
        options.destroy_on_exit = true;
        let logger = get_logger();

        Ok(ElasticStore::new(&options, &logger)?)
    }

    #[monoio::test]
    async fn test_elastic_store_open() -> Result<(), StoreError> {
        let mut store = new_elastic_store().await.unwrap();
        store.open().await?;
        assert_eq!(false, store.segments.is_empty());
        Ok(())
    }

    #[monoio::test]
    async fn test_elastic_store_last_segment() -> Result<(), StoreError> {
        let mut store = new_elastic_store().await.unwrap();
        store.open().await?;
        assert_eq!(true, store.last_segment().is_some());
        Ok(())
    }

    #[monoio::test]
    async fn test_elastic_store_put() -> Result<(), StoreError> {
        let mut store = new_elastic_store().await.unwrap();
        store.open().await?;
        let _res = store.put("Test").await.unwrap();
        Ok(())
    }
}
