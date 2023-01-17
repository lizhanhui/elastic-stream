//! An io-uring based implemention of the `Store` trait.

use std::{cell::UnsafeCell, fs, os::fd::AsRawFd, path::Path, rc::Rc, time::Duration};

use futures::Future;
use local_sync::{
    mpsc::bounded::{self, Tx},
    oneshot,
};
use monoio::{fs::OpenOptions, time::sleep};
use nix::unistd::Whence;
use slog::{debug, error, info, trace, warn, Logger};

use crate::{
    error::PutError,
    ops::{put::PutResult, AppendRecordRequest},
};
use crate::{error::StoreError, ops::Command};

use super::{
    cursor::Cursor,
    ops::{Get, Put, Scan},
    option::StoreOptions,
    segment::JournalSegment,
    ReadOptions, Record, Store, WriteOptions,
};

const STATS_INTERVAL: u64 = 10;

/// io-uring based implementation of `Store` trait.
///
/// `ElasticStore` is designed to be `!Send` as it follows `thread-per-core` paradigm.
pub struct ElasticStore {
    options: StoreOptions,
    logger: Logger,
    segments: UnsafeCell<Vec<Rc<JournalSegment>>>,
    cursor: UnsafeCell<Cursor>,
    tx: Rc<Tx<Command>>,
}

impl ElasticStore {
    /// Create a new `ElasticStore`
    pub fn new(options: &StoreOptions, logger: &Logger) -> Result<Rc<Self>, StoreError> {
        if !options.create_if_missing && !Path::new(&options.store_path.path).exists() {
            error!(
                logger,
                "Specified store path[`{}`] does not exist.", options.store_path.path
            );
            return Err(StoreError::NotFound(
                "Specified store path does not exist".to_owned(),
            ));
        }

        let (tx, mut rx) = bounded::channel(options.command_queue_depth);
        let store = Rc::new(Self {
            options: options.clone(),
            logger: logger.clone(),
            segments: UnsafeCell::new(vec![]),
            cursor: UnsafeCell::new(Cursor::new()),
            tx: Rc::new(tx),
        });

        let cloned = Rc::clone(&store);
        monoio::spawn(async move {
            let store = cloned;
            loop {
                match rx.recv().await {
                    Some(command) => {
                        let _store = Rc::clone(&store);
                        monoio::spawn(async move { _store.process(command).await });
                    }
                    None => {
                        break;
                    }
                }
            }
        });

        let stats_store = Rc::clone(&store);
        monoio::spawn(async move {
            let store = stats_store;
            loop {
                let prev = store.cursor_committed();
                sleep(Duration::from_secs(STATS_INTERVAL)).await;
                let delta = store.cursor_committed() - prev;
                info!(
                    store.logger,
                    "On average, {} bytes are appended to store",
                    delta / STATS_INTERVAL
                );
            }
        });

        Ok(store)
    }

    pub async fn open(&self) -> Result<(), StoreError> {
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

        let fd = f.as_raw_fd();
        match nix::unistd::lseek(fd, 0, Whence::SeekEnd) {
            Ok(offset) => {
                debug!(
                    self.logger,
                    "Previously appended file:`{}` to {}", file_name, offset
                );
                self.cursor_alloc(offset as u64);
                self.cursor_commit(0, offset as u64);
            }

            Err(errno) => {
                error!(self.logger, "Failed to seek to file end. errno: {}", errno);
            }
        };

        let segment = JournalSegment { file: f, file_name };
        let segments = unsafe { &mut *self.segments.get() };
        segments.push(Rc::new(segment));
        Ok(())
    }

    /// Return the last segment of the store journal.
    fn last_segment(&self) -> Option<Rc<JournalSegment>> {
        let segments = unsafe { &mut *self.segments.get() };
        match segments.last() {
            Some(f) => Some(Rc::clone(f)),
            None => None,
        }
    }

    async fn process(&self, command: Command) {
        match command {
            Command::Append(req) => {
                match self.last_segment() {
                    Some(segment) => {
                        let buf = req.buf;
                        // Allocate buffer to append data
                        let len = buf.len() as u64;
                        let pos = self.cursor_alloc(len);

                        let (res, _buf) = segment.file.write_all_at(buf, pos).await;

                        let result = match res {
                            Ok(_) => {
                                trace!(
                                    self.logger,
                                    "Appended {} bytes to {}[{}, {}]",
                                    len,
                                    segment.file_name,
                                    pos,
                                    pos + len
                                );
                                // Assume we have written `len` bytes.
                                if !self.cursor_commit(pos, len) {
                                    // TODO put [pos, pos + len] to the binary search tree
                                }
                                Ok(PutResult {})
                            }
                            Err(e) => Err(StoreError::IO(e)),
                        };

                        match req.sender.send(result) {
                            Ok(_) => {}
                            Err(_e) => {}
                        };
                    }
                    None => {
                        let err = Err(StoreError::DiskFull(
                            "No writable segment available".to_owned(),
                        ));
                        match req.sender.send(err) {
                            Ok(_) => {}
                            Err(e) => {
                                error!(self.logger, "Failed to send result. Cause: {:?}", e);
                            }
                        }
                    }
                }
            }
        }
    }

    fn cursor_committed(&self) -> u64 {
        let cursor = unsafe { &mut *self.cursor.get() };
        cursor.committed()
    }

    fn cursor_alloc(&self, len: u64) -> u64 {
        let cursor = unsafe { &mut *self.cursor.get() };
        cursor.alloc(len)
    }

    fn cursor_commit(&self, pos: u64, len: u64) -> bool {
        let cursor = unsafe { &mut *self.cursor.get() };
        cursor.commit(pos, len)
    }

    /// Delete all journal segment files.
    pub fn destroy(&self) -> Result<(), StoreError> {
        let segments = unsafe { &*self.segments.get() };
        segments.iter().for_each(|s| {
            let file_name = &s.file_name;
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

    fn submission_queue(&self) -> Rc<Tx<Command>> {
        Rc::clone(&self.tx)
    }
}

impl Store for ElasticStore {
    type PutOp = impl Future<Output = Result<PutResult, PutError>>;

    fn put(&self, _options: WriteOptions, record: Record) -> Put<Self::PutOp> {
        let sq = self.submission_queue();

        let (tx, rx) = oneshot::channel();

        let append_request = AppendRecordRequest {
            sender: tx,
            buf: record.buffer,
        };

        let command = Command::Append(append_request);

        let log = self.logger.clone();
        let fut = async move {
            sq.send(command).await.map_err(|e| {
                error!(log, "Failed to pass AppendCommand to store layer {:?}", e);
                PutError::SubmissionQueue
            })?;
            rx.await
                .map_err(|e| PutError::ChannelRecv)?
                .map_err(|e| PutError::Internal)
        };

        Put { inner: fut }
    }

    fn get(&self, _options: ReadOptions) -> Get {
        todo!()
    }

    fn scan(&self, _options: ReadOptions) -> Scan {
        todo!()
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

    use bytes::Bytes;
    use local_sync::oneshot;
    use slog::{o, Drain, Logger};
    use uuid::Uuid;

    use crate::option::StorePath;

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

    async fn new_elastic_store() -> Result<Rc<ElasticStore>, StoreError> {
        let store_path = StorePath {
            path: format!("{}/store", std::env::temp_dir().as_path().display()),
            target_size: 1024 * 1024,
        };

        let mut options = StoreOptions::new(&store_path);
        options.destroy_on_exit = true;
        let logger = get_logger();

        Ok(ElasticStore::new(&options, &logger)?)
    }

    #[monoio::test(enable_timer = true)]
    async fn test_elastic_store_open() -> Result<(), StoreError> {
        let store = new_elastic_store().await.unwrap();
        store.open().await?;
        let segments = unsafe { &*store.segments.get() };
        assert_eq!(false, segments.is_empty());
        Ok(())
    }

    #[monoio::test(enable_timer = true)]
    async fn test_elastic_store_last_segment() -> Result<(), StoreError> {
        let store = new_elastic_store().await.unwrap();
        store.open().await?;
        assert_eq!(true, store.last_segment().is_some());
        Ok(())
    }

    #[monoio::test(enable_timer = true)]
    async fn test_elastic_store_append() -> Result<(), StoreError> {
        let store = new_elastic_store().await.unwrap();
        store.open().await?;

        let tx = store.submission_queue();

        let (cb_tx, cb_rx) = oneshot::channel();
        let append_request = AppendRecordRequest {
            buf: Bytes::from("abcd"),
            sender: cb_tx,
        };

        tx.send(Command::Append(append_request)).await.unwrap();

        let res = cb_rx.await;
        assert_eq!(true, res.is_ok());
        Ok(())
    }
}
