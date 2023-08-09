use std::{
    ops::Deref,
    sync::Arc,
    thread::{sleep, Builder, JoinHandle},
};

use crate::{
    error::StoreError,
    index::{cleaner::LogCleaner, LocalRangeManager},
};
use config::Configuration;
use crossbeam::channel::{self, Receiver, Select, Sender, TryRecvError};
use log::{error, info, warn};
use model::{
    error::EsError,
    range::{RangeLifecycleEvent, RangeMetadata},
};
use tokio::sync::{mpsc, oneshot};

use super::{indexer::DefaultIndexer, record_handle::RecordHandle, Indexer, MinOffset};

pub(crate) struct IndexDriver {
    tx: Sender<IndexCommand>,
    // Issue a shutdown signal to the driver thread.
    shutdown_tx: Sender<()>,
    indexer: Arc<DefaultIndexer>,
    handles: Vec<JoinHandle<()>>,
}

pub(crate) enum IndexCommand {
    Index {
        stream_id: i64,
        range: u32,
        offset: u64,
        handle: RecordHandle,
    },
    /// Used to retrieve a batch of record handles from a given offset.
    ScanRecord {
        stream_id: i64,
        range: u32,
        offset: u64,
        max_offset: u64,
        max_bytes: u32,
        observer: oneshot::Sender<Result<Option<Vec<RecordHandle>>, StoreError>>,
    },

    ListRange {
        tx: mpsc::UnboundedSender<RangeMetadata>,
    },

    ListRangeByStream {
        stream_id: i64,
        tx: mpsc::UnboundedSender<RangeMetadata>,
    },

    SealRange {
        range: RangeMetadata,
        tx: oneshot::Sender<Result<(), StoreError>>,
    },

    CreateRange {
        range: RangeMetadata,
        tx: oneshot::Sender<Result<(), StoreError>>,
    },
    RangeEvent {
        events: Vec<RangeLifecycleEvent>,
        tx: oneshot::Sender<u64>,
    },
}

impl IndexDriver {
    pub(crate) fn new(
        config: &Arc<Configuration>,
        min_offset: Arc<dyn MinOffset>,
        flush_threshold: usize,
    ) -> Result<Self, StoreError> {
        let (tx, rx) = channel::unbounded();
        let (shutdown_tx, shutdown_rx) = channel::bounded(1);
        let indexer = Arc::new(DefaultIndexer::new(config, min_offset, flush_threshold)?);
        let runner = IndexDriverRunner::new(rx, shutdown_rx, Arc::clone(&indexer));
        // Always bind indexer thread to processor-0, which runs miscellaneous tasks.
        let core = core_affinity::CoreId { id: 0 };
        let handle = Builder::new()
            .name("IndexDriver".to_owned())
            .spawn(move || {
                if !core_affinity::set_for_current(core) {
                    warn!("Failed to set core affinity for indexer thread");
                }
                runner.run();
            })
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        info!("IndexDriver thread started");
        Ok(Self {
            tx,
            shutdown_tx,
            indexer,
            handles: vec![handle],
        })
    }

    pub(crate) fn index(&self, stream_id: i64, range: u32, offset: u64, handle: RecordHandle) {
        if let Err(_e) = self.tx.send(IndexCommand::Index {
            stream_id,
            range,
            offset,
            handle,
        }) {
            error!("Failed to send index entry to internal indexer");
        }
    }

    pub(crate) fn scan_record_handles(
        &self,
        stream_id: i64,
        range: u32,
        offset: u64,
        max_offset: u64,
        max_bytes: u32,
        observer: oneshot::Sender<Result<Option<Vec<RecordHandle>>, StoreError>>,
    ) {
        if let Err(_e) = self.tx.send(IndexCommand::ScanRecord {
            stream_id,
            range,
            offset,
            max_offset,
            max_bytes,
            observer,
        }) {
            error!("Failed to send scan record handles command to internal indexer");
        }
    }

    pub(crate) fn list_ranges(&self, tx: mpsc::UnboundedSender<RangeMetadata>) {
        if let Err(_e) = self.tx.send(IndexCommand::ListRange { tx }) {
            error!("Failed to send list range command");
        }
    }

    pub(crate) fn list_ranges_by_stream(
        &self,
        stream_id: i64,
        tx: mpsc::UnboundedSender<RangeMetadata>,
    ) {
        if let Err(_e) = self
            .tx
            .send(IndexCommand::ListRangeByStream { stream_id, tx })
        {
            error!("Failed to send list range by stream command");
        }
    }

    pub(crate) fn create_range(
        &self,
        range: RangeMetadata,
        tx: oneshot::Sender<Result<(), StoreError>>,
    ) {
        if let Err(e) = self.tx.send(IndexCommand::CreateRange { range, tx }) {
            error!("Failed to submit create range command");
            if let IndexCommand::CreateRange { tx, .. } = e.0 {
                let _ = tx.send(Err(StoreError::Internal(
                    "Submit create range failed".to_owned(),
                )));
            }
        }
    }

    pub(crate) fn seal_range(
        &self,
        range: RangeMetadata,
        tx: oneshot::Sender<Result<(), StoreError>>,
    ) {
        if let Err(e) = self.tx.send(IndexCommand::SealRange { range, tx }) {
            error!("Failed to submit create range command");
            if let IndexCommand::SealRange { tx, .. } = e.0 {
                let _ = tx.send(Err(StoreError::Internal(
                    "Submit seal range failed".to_owned(),
                )));
            }
        }
    }

    pub(crate) fn shutdown_indexer(&self) {
        if let Err(e) = self.indexer.flush(true) {
            error!("Failed to flush primary index. Cause: {:?}", e);
        }

        if self.shutdown_tx.send(()).is_err() {
            error!("Failed to send shutdown signal to indexer");
        }
    }

    pub(crate) fn join(&mut self) {
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }

    pub(crate) async fn handle_range_event(
        &self,
        events: Vec<RangeLifecycleEvent>,
    ) -> Result<u64, EsError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(IndexCommand::RangeEvent { events, tx })
            .map_err(|e| {
                EsError::unexpected("Cannot send command to index driver").set_source(e)
            })?;
        rx.await.map_err(|e| {
            EsError::unexpected("handle_range_event cannot get response from rx").set_source(e)
        })
    }
}

impl Drop for IndexDriver {
    fn drop(&mut self) {
        self.join();
    }
}

impl Deref for IndexDriver {
    type Target = Arc<DefaultIndexer>;

    fn deref(&self) -> &Self::Target {
        &self.indexer
    }
}

struct IndexDriverRunner {
    rx: Receiver<IndexCommand>,
    shutdown_rx: Receiver<()>,
    indexer: Arc<DefaultIndexer>,
}

impl IndexDriverRunner {
    fn new(
        rx: Receiver<IndexCommand>,
        shutdown_rx: Receiver<()>,
        indexer: Arc<DefaultIndexer>,
    ) -> Self {
        Self {
            rx,
            shutdown_rx,
            indexer,
        }
    }

    fn run(&self) {
        let mut selector = Select::new();
        selector.recv(&self.rx);
        selector.recv(&self.shutdown_rx);
        let mut log_cleaner = LogCleaner::new(self.indexer.clone());
        loop {
            let index = selector.ready();
            if 0 == index {
                match self.rx.try_recv() {
                    Ok(index_command) => match index_command {
                        IndexCommand::Index {
                            stream_id,
                            range,
                            offset,
                            handle,
                        } => {
                            let physical_offset = handle.wal_offset;
                            while let Err(e) = self.indexer.index(stream_id, range, offset, &handle)
                            {
                                error!("Failed to index: stream_id={}, offset={}, record_handle={:?}, cause: {}",
                                stream_id, offset, handle, e);
                                sleep(std::time::Duration::from_millis(100));
                            }
                            log_cleaner.handle_new_index(
                                (stream_id as u64, range),
                                offset,
                                physical_offset,
                            );
                        }
                        IndexCommand::ScanRecord {
                            stream_id,
                            range,
                            offset,
                            max_offset,
                            max_bytes,
                            observer,
                        } => {
                            observer
                                .send(
                                    self.indexer
                                        .scan_record_handles_left_shift(
                                            stream_id, range, offset, max_offset, max_bytes,
                                        )
                                        .map(|indexes_opt| {
                                            indexes_opt.map(|indexes| {
                                                indexes
                                                    .into_iter()
                                                    .map(|(_, handle)| handle)
                                                    .collect()
                                            })
                                        }),
                                )
                                .unwrap_or_else(|_e| {
                                    error!(
                                        "Failed to send scan result of {}/{} to observer.",
                                        stream_id, offset
                                    );
                                });
                        }

                        IndexCommand::ListRange { tx } => {
                            self.indexer.list(tx);
                        }

                        IndexCommand::ListRangeByStream { stream_id, tx } => {
                            self.indexer.list_by_stream(stream_id, tx);
                        }

                        IndexCommand::CreateRange { range, tx } => {
                            log_cleaner.handle_new_range(
                                (range.stream_id() as u64, range.index() as u32),
                                range.start(),
                            );
                            match self.indexer.add(range.stream_id(), &range) {
                                Ok(()) => {
                                    let _ = tx.send(Ok(()));
                                }
                                Err(e) => {
                                    error!("Failed to add stream range: {}", range);
                                    let _ = tx.send(Err(e));
                                }
                            }
                        }

                        IndexCommand::SealRange { range, tx } => {
                            match self.indexer.seal(range.stream_id(), &range) {
                                Ok(()) => {
                                    let _ = tx.send(Ok(()));
                                }
                                Err(e) => {
                                    error!("Failed to seal range: {}", range);
                                    let _ = tx.send(Err(e));
                                }
                            }
                        }
                        IndexCommand::RangeEvent { events, tx } => {
                            let phy_offset = log_cleaner.handle_range_event(events);
                            let _ = tx.send(phy_offset);
                        }
                    },
                    Err(TryRecvError::Empty) => {
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => {
                        info!("IndexChannel disconnected");
                        break;
                    }
                }
            } else if 1 == index {
                info!("Got a command to quit IndexDriverRunner thread");
                break;
            }
        }
        info!("IndexDriverRunner thread completed");
    }
}

#[cfg(test)]
mod tests {
    use config::Configuration;

    use crate::index::{Indexer, MinOffset};
    use std::{error::Error, sync::Arc};

    struct TestMinOffset {}

    impl MinOffset for TestMinOffset {
        fn min_offset(&self) -> u64 {
            0
        }
    }

    #[test]
    fn test_index_driver() -> Result<(), Box<dyn Error>> {
        let db_path = tempfile::tempdir()?;
        let min_offset = Arc::new(TestMinOffset {});
        let mut configuration = Configuration::default();
        configuration
            .store
            .path
            .set_base(db_path.path().as_os_str().to_str().unwrap());
        let config = Arc::new(configuration);
        let index_driver = super::IndexDriver::new(&config, min_offset, 128)?;
        assert_eq!(0, index_driver.get_wal_checkpoint()?);

        index_driver.shutdown_indexer();
        Ok(())
    }
}
