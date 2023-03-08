use std::{
    ops::Deref,
    rc::Rc,
    sync::Arc,
    thread::{sleep, Builder, JoinHandle},
};

use crate::error::StoreError;
use crossbeam::channel::{self, Receiver, Select, Sender, TryRecvError};
use slog::{error, info, Logger};

use super::{indexer::Indexer, MinOffset};

pub(crate) struct IndexDriver {
    log: Logger,
    tx: Sender<IndexCommand>,
    indexer: Arc<Indexer>,
    handles: Vec<JoinHandle<()>>,
}

pub(crate) enum IndexCommand {
    Index {
        stream_id: i64,
        offset: u64,
        wal_offset: u64,
        len: u32,
        hash: u64,
    },
}

impl IndexDriver {
    pub(crate) fn new(
        log: Logger,
        path: &str,
        min_offset: Rc<dyn MinOffset>,
        shutdown: Receiver<()>,
    ) -> Result<Self, StoreError> {
        let (tx, rx) = channel::unbounded();
        let indexer = Arc::new(Indexer::new(log.clone(), path, min_offset)?);
        let runner = IndexDriverRunner::new(log.clone(), rx, shutdown, Arc::clone(&indexer));
        let handle = Builder::new()
            .name("IndexDriver".to_owned())
            .spawn(move || {
                runner.run();
            })
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        info!(log, "IndexDriver thread started");
        Ok(Self {
            log,
            tx,
            indexer,
            handles: vec![handle],
        })
    }

    pub(crate) fn index(&self, stream_id: i64, offset: u64, wal_offset: u64, len: u32, hash: u64) {
        if let Err(e) = self.tx.send(IndexCommand::Index {
            stream_id,
            offset,
            wal_offset,
            len,
            hash,
        }) {
            error!(self.log, "Failed to send index entry to internal indexer");
        }
    }

    pub(crate) fn join(&mut self) {
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

impl Drop for IndexDriver {
    fn drop(&mut self) {
        self.join();
    }
}

impl Deref for IndexDriver {
    type Target = Arc<Indexer>;

    fn deref(&self) -> &Self::Target {
        &self.indexer
    }
}

struct IndexDriverRunner {
    log: Logger,
    rx: Receiver<IndexCommand>,
    shutdown_rx: Receiver<()>,
    indexer: Arc<Indexer>,
}

impl IndexDriverRunner {
    fn new(
        log: Logger,
        rx: Receiver<IndexCommand>,
        shutdown_rx: Receiver<()>,
        indexer: Arc<Indexer>,
    ) -> Self {
        Self {
            log,
            rx,
            shutdown_rx,
            indexer,
        }
    }

    fn run(&self) {
        let mut selector = Select::new();
        selector.recv(&self.rx);
        selector.recv(&self.shutdown_rx);
        loop {
            let index = selector.ready();
            if 0 == index {
                match self.rx.try_recv() {
                    Ok(index_command) => match index_command {
                        IndexCommand::Index {
                            stream_id,
                            offset,
                            wal_offset,
                            len,
                            hash,
                        } => {
                            while let Err(e) =
                                self.indexer.index(stream_id, offset, wal_offset, len, hash)
                            {
                                error!(self.log, "Failed to index: stream_id={}, offset={}, wal_offset={}, len={}, hash={}, cause: {}", 
                                stream_id, offset, wal_offset, len, hash, e);
                                sleep(std::time::Duration::from_millis(100));
                            }
                        }
                    },
                    Err(TryRecvError::Empty) => {
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => {
                        info!(self.log, "IndexChannel disconnected");
                        break;
                    }
                }
            } else if 1 == index {
                info!(self.log, "Got a command to quit IndexDriverRunner thread");
                break;
            }
        }
        info!(self.log, "IndexDriverRunner thread completed");
    }
}

#[cfg(test)]
mod tests {
    use crate::index::MinOffset;
    use crossbeam::channel;
    use std::{env, error::Error, rc::Rc};
    use uuid::Uuid;

    struct TestMinOffset {}

    impl MinOffset for TestMinOffset {
        fn min_offset(&self) -> u64 {
            0
        }
    }

    #[test]
    fn test_index_driver() -> Result<(), Box<dyn Error>> {
        let log = util::terminal_logger();
        let db_path = util::create_random_path()?;
        let _dir_guard = util::DirectoryRemovalGuard::new(log.clone(), db_path.as_path());
        let min_offset = Rc::new(TestMinOffset {});
        let (tx, rx) = channel::bounded(1);
        let index_driver =
            super::IndexDriver::new(log, db_path.as_os_str().to_str().unwrap(), min_offset, rx)?;
        assert_eq!(0, index_driver.get_wal_checkpoint()?);
        let _ = tx.send(())?;
        Ok(())
    }
}
