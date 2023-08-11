use std::{
    collections::{hash_map::Entry, HashMap},
    rc::Rc,
    time::Duration,
};

use log::{error, info, warn};
use model::{
    error::EsError, object::ObjectMetadata, range::RangeMetadata, replica::RangeProgress,
    stream::StreamMetadata, Batch,
};
use object_storage::ObjectStorage;
use store::{error::AppendError, Store};

use crate::{
    error::ServiceError,
    metadata::{MetadataManager, RangeEventListener},
};

use super::{fetcher::PlacementFetcher, range::Range, stream::Stream, RangeManager};

pub(crate) struct DefaultRangeManager<S, F, O, M> {
    streams: HashMap<i64, Stream>,

    fetcher: F,

    store: Rc<S>,

    object_storage: Rc<O>,

    #[allow(dead_code)]
    metadata_manager: M,
}

impl<S, F, O, M> DefaultRangeManager<S, F, O, M>
where
    S: Store + 'static,
    F: PlacementFetcher,
    O: ObjectStorage,
    M: MetadataManager,
{
    pub(crate) fn new(
        fetcher: F,
        store: Rc<S>,
        object_storage: Rc<O>,
        metadata_manager: M,
    ) -> Self {
        Self {
            streams: HashMap::new(),
            fetcher,
            store,
            object_storage,
            metadata_manager,
        }
    }

    /// Bootstrap all stream ranges that are assigned to current range server.
    ///
    /// # Panic
    /// If failed to access store to acquire max offset of the stream with mutable range.
    async fn bootstrap(&mut self) -> Result<(), EsError> {
        let range_event_rx = self.metadata_manager.watch()?;
        self.metadata_manager.start().await;
        let ranges = self
            .fetcher
            .bootstrap(self.store.config().server.server_id as u32)
            .await
            .map_err(|e| EsError::unexpected("pd fetcher bootstrap fail").set_source(e))?;
        self.start_notify_range_event_task(range_event_rx);

        for range in ranges {
            let committed = self
                .store
                .get_range_end_offset(range.stream_id(), range.index() as u32)
                .expect("Failed to acquire end offset of the range");
            let range_index = range.index();
            let entry = self.streams.entry(range.stream_id());
            match entry {
                Entry::Occupied(mut occupied) => {
                    occupied.get_mut().create_range(range);
                    if let Some(offset) = committed {
                        occupied.get_mut().reset_commit(range_index, offset);
                    }
                }
                Entry::Vacant(vacant) => {
                    let metadata = self.fetcher.describe_stream(range.stream_id() as u64).await.expect(
                        "Failed to fetch stream metadata from placement driver during bootstrap",
                    );
                    let mut stream = Stream::new(metadata);
                    stream.create_range(range);
                    if let Some(offset) = committed {
                        stream.reset_commit(range_index, offset);
                    }
                    vacant.insert(stream);
                }
            }
        }
        Ok(())
    }

    fn start_notify_range_event_task(&self, mut range_event_rx: RangeEventListener) {
        let store = self.store.clone();
        tokio_uring::spawn(async move {
            while let Some(events) = range_event_rx.recv().await {
                store.handle_range_event(events).await;
            }
        });
    }

    fn get_range(&self, stream_id: u64, range_index: u32) -> Option<&Range> {
        if let Some(stream) = self.streams.get(&(stream_id as i64)) {
            stream.get_range(range_index as i32)
        } else {
            None
        }
    }

    /// Get a stream by id.
    ///
    /// # Arguments
    /// `stream_id` - The id of the stream.
    ///
    /// # Returns
    /// The stream if it exists, otherwise `None`.
    fn get_stream(&mut self, stream_id: i64) -> Option<&mut Stream> {
        self.streams.get_mut(&stream_id)
    }

    fn get_range_mut(&mut self, stream_id: i64, index: i32) -> Option<&mut Range> {
        if let Some(stream) = self.get_stream(stream_id) {
            stream.get_range_mut(index)
        } else {
            None
        }
    }
}

impl<S, F, O, M> RangeManager for DefaultRangeManager<S, F, O, M>
where
    S: Store + 'static,
    F: PlacementFetcher,
    O: ObjectStorage,
    M: MetadataManager,
{
    async fn start(&mut self) -> Result<(), EsError> {
        self.bootstrap().await?;
        Ok(())
    }

    /// Create a new range for the specified stream.
    fn create_range(&mut self, range: RangeMetadata) -> Result<(), ServiceError> {
        info!("Create range={:?}", range);

        match self.streams.entry(range.stream_id()) {
            Entry::Occupied(mut occupied) => {
                occupied.get_mut().create_range(range);
            }
            Entry::Vacant(vacant) => {
                let metadata = StreamMetadata {
                    stream_id: Some(range.stream_id() as u64),
                    replica: 0,
                    ack_count: 0,
                    retention_period: Duration::from_secs(1),
                    start_offset: 0,
                    epoch: 0,
                };
                let mut stream = Stream::new(metadata);
                stream.create_range(range);
                vacant.insert(stream);
            }
        }
        Ok(())
    }

    fn commit(
        &mut self,
        stream_id: i64,
        range_index: i32,
        offset: u64,
        last_offset_delta: u32,
        bytes_len: u32,
    ) -> Result<(), ServiceError> {
        if let Some(range) = self.get_range_mut(stream_id, range_index) {
            range.commit(offset + last_offset_delta as u64)?;
            self.object_storage
                .new_commit(stream_id as u64, range_index as u32, bytes_len);
            Ok(())
        } else {
            error!("Commit fail, range[{stream_id}#{range_index}] is not found");
            Err(ServiceError::NotFound(format!(
                "range[{stream_id}#{range_index}]"
            )))
        }
    }

    fn seal(&mut self, range: &mut RangeMetadata) -> Result<(), ServiceError> {
        if let Some(stream) = self.streams.get_mut(&range.stream_id()) {
            if !stream.has_range(range.index()) {
                stream.create_range(range.clone());
            }
            stream.seal(range)
        } else {
            info!(
                "Stream[id={}] is not found, fetch stream metadata from placement driver",
                range.stream_id()
            );
            let stream_metadata = StreamMetadata {
                stream_id: Some(range.stream_id() as u64),
                replica: 0,
                ack_count: 0,
                retention_period: Duration::from_secs(1),
                start_offset: 0,
                epoch: 0,
            };
            let mut stream = Stream::new(stream_metadata);
            stream.create_range(range.clone());
            // Seal the range
            stream.seal(range)?;
            self.streams.insert(range.stream_id(), stream);
            Ok(())
        }
    }

    fn check_barrier<R>(
        &mut self,
        stream_id: i64,
        range_index: i32,
        req: &R,
    ) -> Result<(), AppendError>
    where
        R: Batch + Ord + 'static,
    {
        if let Some(range) = self.get_range_mut(stream_id, range_index) {
            if let Some(window) = range.window_mut() {
                // Check write barrier to ensure that the incoming requests arrive in order.
                // Some ServiceError is returned if the request is out of order.
                window.check_barrier(req)?;
            } else {
                warn!(
                    "Try append to a sealed range[{}#{}]",
                    stream_id, range_index
                );
                return Err(AppendError::RangeSealed);
            }
        } else {
            warn!(
                "Target stream/range is not found. stream-id={}, range-index={}",
                stream_id, range_index
            );
            return Err(AppendError::RangeNotFound);
        }
        Ok(())
    }

    fn has_range(&self, stream_id: u64, index: u32) -> bool {
        self.get_range(stream_id, index).is_some()
    }

    async fn get_objects(
        &self,
        stream_id: u64,
        range_index: u32,
        start_offset: u64,
        end_offset: u64,
        size_hint: u32,
    ) -> (Vec<ObjectMetadata>, bool) {
        self.object_storage
            .get_objects(stream_id, range_index, start_offset, end_offset, size_hint)
            .await
    }

    async fn get_range_progress(&self) -> Vec<RangeProgress> {
        let mut progress = Vec::new();
        let offloading_range = self.object_storage.get_offloading_range().await;
        for range_key in offloading_range.iter() {
            let stream_id = range_key.stream_id;
            let range_index = range_key.range_index;
            if let Some(range) = self.get_range(stream_id, range_index) {
                if let Some(committed) = range.committed() {
                    progress.push(RangeProgress {
                        stream_id,
                        range_index,
                        confirm_offset: committed,
                    });
                }
            }
        }
        progress
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    #[test]
    fn test_new() -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}
