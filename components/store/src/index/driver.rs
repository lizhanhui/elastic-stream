use std::{
    ops::Deref,
    sync::Arc,
    thread::{sleep, Builder, JoinHandle},
};

use crossbeam::channel::{self, Receiver, Select, Sender, TryRecvError};
use log::{error, info, warn};
use tokio::sync::{mpsc, oneshot};

use config::Configuration;
use model::{range::RangeMetadata, resource::EventType};

use crate::index::record::Record;
use crate::{
    error::StoreError,
    index::LocalRangeManager,
    watermark::{DefaultWatermarkManager, Watermark, WatermarkManager},
};

use super::{indexer::DefaultIndexer, Indexer};

pub(crate) struct IndexDriver {
    tx: Sender<IndexCommand>,
    // Issue a shutdown signal to the driver thread.
    shutdown_tx: Sender<()>,
    indexer: Arc<DefaultIndexer>,
    handles: Vec<JoinHandle<()>>,
}

pub(crate) enum IndexCommand {
    Index {
        record: Record,
    },
    /// Used to retrieve a batch of record handles from a given offset.
    ScanRecord {
        stream_id: u64,
        range: u32,
        offset: u64,
        max_offset: u64,
        max_bytes: u32,
        observer: oneshot::Sender<Result<Option<Vec<Record>>, StoreError>>,
    },

    ListRange {
        tx: mpsc::UnboundedSender<RangeMetadata>,
    },

    ListRangeByStream {
        stream_id: u64,
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
        event_type: EventType,
        metadata: RangeMetadata,
    },

    StreamTrim {
        stream_id: u64,

        /// Minimum offset
        offset: u64,
    },

    DataOffload {
        stream_id: u64,
        range: u32,
        offset: u64,
        delta: u32,
    },
}

impl IndexDriver {
    pub(crate) fn new(
        config: &Arc<Configuration>,
        watermark: Arc<dyn Watermark + Send + Sync>,
        flush_threshold: usize,
    ) -> Result<Self, StoreError> {
        let (tx, rx) = channel::unbounded();
        let (shutdown_tx, shutdown_rx) = channel::bounded(1);
        let watermark_ = Arc::clone(&watermark);
        let indexer = Arc::new(DefaultIndexer::new(config, watermark, flush_threshold)?);
        let indexer_ = Arc::clone(&indexer);
        // Always bind indexer thread to processor-0, which runs miscellaneous tasks.
        let core = core_affinity::CoreId { id: 0 };
        let handle = Builder::new()
            .name("IndexDriver".to_owned())
            .spawn(move || {
                if !core_affinity::set_for_current(core) {
                    warn!("Failed to set core affinity for indexer thread");
                }
                let mut watermark_manager = DefaultWatermarkManager::new(Arc::clone(&indexer_));
                watermark_manager.add_watermark(watermark_);
                let mut runner =
                    IndexDriverRunner::new(rx, shutdown_rx, indexer_, watermark_manager);
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

    pub(crate) fn index(&self, record: Record) {
        if let Err(_e) = self.tx.send(IndexCommand::Index { record }) {
            error!("Failed to send index entry to internal indexer");
        }
    }

    pub(crate) fn scan_records(
        &self,
        stream_id: u64,
        range: u32,
        offset: u64,
        max_offset: u64,
        max_bytes: u32,
        observer: oneshot::Sender<Result<Option<Vec<Record>>, StoreError>>,
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
        stream_id: u64,
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

    pub(crate) fn trim_stream(&self, cmd: IndexCommand) {
        let _ = self.tx.send(cmd);
    }

    pub(crate) fn offload_range_data(&self, cmd: IndexCommand) {
        let _ = self.tx.send(cmd);
    }

    pub(crate) fn process_range_event(&self, cmd: IndexCommand) {
        let _ = self.tx.send(cmd);
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

struct IndexDriverRunner<M> {
    rx: Receiver<IndexCommand>,
    shutdown_rx: Receiver<()>,
    indexer: Arc<DefaultIndexer>,

    /// Manage watermarks of store.
    watermark_manager: M,
}

impl<M> IndexDriverRunner<M>
where
    M: WatermarkManager,
{
    fn new(
        rx: Receiver<IndexCommand>,
        shutdown_rx: Receiver<()>,
        indexer: Arc<DefaultIndexer>,
        watermark_manager: M,
    ) -> Self {
        Self {
            rx,
            shutdown_rx,
            indexer,
            watermark_manager,
        }
    }

    /// Process create range event directly from frontend SDK RPC call.
    ///
    /// We have two sources of metadata events: frontend SDK RPC call and PD watch.
    /// Create-range need to be idempotent;
    /// Seal range is kind of complex: if frontend SDK kicks off an active seal operation, the seal would be eventual
    ///  and identical to the incoming one from watch; however, if the seal operation is passive, namely, it picks up
    ///  a stream whose previous writer crashed out, semantics of the seal operation to range server is carrying two
    ///  atomic purposes: making the range immutable and report back its max continuous offset. After the frontend SDK
    /// receives a quorum of such responses, it seals the PD cluster with the final range end offset. The eventual end
    /// offset would be available to range server via the PD client watch later. Now the range servers may three kinds
    /// of cases to handle according to its actual end-offset and eventual end-offset: '>', '=', '<'.  For the last
    /// case, the range server needs to copy data from its replication peers.
    fn run(&mut self) {
        let mut selector = Select::new();
        selector.recv(&self.rx);
        selector.recv(&self.shutdown_rx);
        loop {
            let index = selector.ready();
            if 0 == index {
                match self.rx.try_recv() {
                    Ok(command) => match command {
                        IndexCommand::Index { record } => {
                            while let Err(e) = self.indexer.index(&record) {
                                error!("Failed to index: stream_id={}, range={}, offset={}, record_handle={:?}, cause: {}",
                                record.index.stream_id, record.index.range, record.index.offset, record.handle, e);
                                sleep(std::time::Duration::from_millis(100));
                            }
                            self.watermark_manager.on_index(
                                record.index.stream_id,
                                record.index.range,
                                record.index.offset,
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
                            let res = self
                                .indexer
                                .scan_record_left_shift(
                                    stream_id, range, offset, max_offset, max_bytes,
                                )
                                .map(|indexes_opt| {
                                    indexes_opt.map(|indexes| indexes.into_iter().collect())
                                });
                            observer.send(res).unwrap_or_else(|_e| {
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

                        IndexCommand::DataOffload {
                            stream_id,
                            range,
                            offset,
                            delta,
                        } => {
                            self.watermark_manager
                                .on_data_offload(stream_id, range, offset, delta);
                        }

                        IndexCommand::RangeEvent {
                            event_type,
                            metadata,
                        } => match event_type {
                            EventType::Added | EventType::Listed => {
                                match u32::try_from(metadata.index()) {
                                    Ok(index) => {
                                        self.watermark_manager.add_range(
                                            metadata.stream_id(),
                                            index,
                                            metadata.start(),
                                            metadata.end(),
                                        );
                                    }
                                    Err(e) => {
                                        error!("Got an invalid range: {}", e);
                                    }
                                }
                            }

                            EventType::Modified => match u32::try_from(metadata.index()) {
                                Ok(range) => {
                                    self.watermark_manager.trim_range(
                                        metadata.stream_id(),
                                        range,
                                        metadata.start(),
                                    );
                                }
                                Err(e) => {
                                    warn!("Got an invalid range metadata: {}", e);
                                }
                            },

                            EventType::Deleted => {
                                self.watermark_manager
                                    .delete_range(metadata.stream_id(), metadata.index() as u32);
                            }

                            _ => {}
                        },

                        IndexCommand::StreamTrim { stream_id, offset } => {
                            self.watermark_manager.trim_stream(stream_id, offset);
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
    use std::{error::Error, sync::Arc};

    use config::Configuration;

    use crate::{index::Indexer, watermark::Watermark};

    struct TestWatermark {}

    impl Watermark for TestWatermark {
        fn min(&self) -> u64 {
            0
        }

        fn offload(&self) -> u64 {
            0
        }

        fn set_min(&self, _value: u64) {
            todo!()
        }

        fn set_offload(&self, _value: u64) {
            todo!()
        }
    }

    #[test]
    fn test_index_driver() -> Result<(), Box<dyn Error>> {
        let db_path = tempfile::tempdir()?;
        let min_offset = Arc::new(TestWatermark {});
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
