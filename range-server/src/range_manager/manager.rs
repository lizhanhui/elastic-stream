use super::{range::Range, stream::Stream, RangeManager};
use crate::error::ServiceError;
use log::{error, info, warn};
use model::{
    object::ObjectMetadata,
    range::RangeMetadata,
    replica::RangeProgress,
    resource::{EventType, Resource, ResourceEvent, ResourceEventObserver},
    stream::StreamMetadata,
    Batch,
};
use object_storage::ObjectStorage;
use std::{
    cell::UnsafeCell,
    collections::{hash_map::Entry, HashMap},
    rc::Rc,
    time::Duration,
};
use store::{
    error::{AppendError, FetchError},
    option::{ReadOptions, WriteOptions},
    AppendRecordRequest, AppendResult, FetchResult, Store,
};

/// Manager of ranges, both metadata of control plane and data append/fetch of data plane.
///
/// TODO: `ObjectStorage` should be an implementation layer of `Store`.
pub(crate) struct DefaultRangeManager<S, O> {
    streams: UnsafeCell<HashMap<u64, Stream>>,

    store: Rc<S>,

    object_storage: O,
}

impl<S, O> DefaultRangeManager<S, O> {
    pub(crate) fn new(store: Rc<S>, object_storage: O) -> Self {
        Self {
            streams: UnsafeCell::new(HashMap::new()),
            store,
            object_storage,
        }
    }

    #[inline]
    fn streams(&self) -> &HashMap<u64, Stream> {
        unsafe { &*self.streams.get() }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn streams_mut(&self) -> &mut HashMap<u64, Stream> {
        unsafe { &mut *self.streams.get() }
    }

    fn get_range(&self, stream_id: u64, range_index: u32) -> Option<&Range> {
        if let Some(stream) = self.streams().get(&(stream_id)) {
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
    fn get_stream(&self, stream_id: u64) -> Option<&mut Stream> {
        self.streams_mut().get_mut(&stream_id)
    }

    fn get_range_mut(&self, stream_id: u64, index: i32) -> Option<&mut Range> {
        if let Some(stream) = self.get_stream(stream_id) {
            stream.get_range_mut(index)
        } else {
            None
        }
    }

    fn on_range_event(&self, ty: EventType, metadata: &RangeMetadata) {
        match ty {
            EventType::None | EventType::Reset | EventType::ListFinished => {}
            EventType::Added | EventType::Modified | EventType::Listed => {
                let streams = self.streams_mut();
                let stream = streams.entry(metadata.stream_id()).or_insert_with(|| {
                    let stream_metadata_holder = StreamMetadata {
                        stream_id: metadata.stream_id(),
                        ..Default::default()
                    };
                    Stream::new(stream_metadata_holder)
                });
                match stream.get_range_mut(metadata.index()) {
                    Some(range) => {
                        range.metadata = metadata.clone();
                    }
                    None => {
                        stream.create_range(metadata.clone());
                    }
                }
            }
            EventType::Deleted => {
                let streams = self.streams_mut();
                if let Some(stream) = streams.get_mut(&metadata.stream_id()) {
                    stream.remove_range(metadata);
                }
            }
        }
    }

    fn on_stream_event(&self, ty: EventType, metadata: &StreamMetadata) {
        match ty {
            EventType::None | EventType::Reset | EventType::ListFinished => {}
            EventType::Added | EventType::Modified | EventType::Listed => {
                let streams = self.streams_mut();

                match streams.entry(metadata.stream_id) {
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().update_metadata(metadata.clone());
                    }
                    Entry::Vacant(entry) => {
                        let stream = Stream::new(metadata.clone());
                        entry.insert(stream);
                    }
                }
            }
            EventType::Deleted => {
                self.streams_mut().remove(&metadata.stream_id);
            }
        }
    }
}

impl<S, O> RangeManager for DefaultRangeManager<S, O>
where
    S: Store,
    O: ObjectStorage,
{
    async fn start(&self) {
        self.store.start();
    }

    /// Create a new range for the specified stream.
    fn create_range(&self, range: RangeMetadata) -> Result<(), ServiceError> {
        info!("Create range={:?}", range);

        match self.streams_mut().entry(range.stream_id()) {
            Entry::Occupied(mut occupied) => {
                occupied.get_mut().create_range(range);
            }
            Entry::Vacant(vacant) => {
                let metadata = StreamMetadata {
                    stream_id: range.stream_id(),
                    replica: 0,
                    ack_count: 0,
                    retention_period: Duration::from_secs(1),
                    start_offset: 0,
                    epoch: 0,
                    deleted: false,
                };
                let mut stream = Stream::new(metadata);
                stream.create_range(range);
                vacant.insert(stream);
            }
        }
        Ok(())
    }

    fn commit(
        &self,
        stream_id: u64,
        range_index: i32,
        offset: u64,
        last_offset_delta: u32,
        _bytes_len: u32,
    ) -> Result<(), ServiceError> {
        if let Some(range) = self.get_range_mut(stream_id, range_index) {
            range.commit(offset + last_offset_delta as u64)?;
            // self.object_storage
            //     .new_commit(stream_id as u64, range_index as u32, bytes_len);
            Ok(())
        } else {
            error!("Commit fail, range[{stream_id}#{range_index}] is not found");
            Err(ServiceError::NotFound(format!(
                "range[{stream_id}#{range_index}]"
            )))
        }
    }

    fn seal(&self, range: &mut RangeMetadata) -> Result<(), ServiceError> {
        if let Some(stream) = self.streams_mut().get_mut(&range.stream_id()) {
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
                stream_id: range.stream_id(),
                replica: 0,
                ack_count: 0,
                retention_period: Duration::from_secs(1),
                start_offset: 0,
                epoch: 0,
                deleted: false,
            };
            let mut stream = Stream::new(stream_metadata);
            stream.create_range(range.clone());
            // Seal the range
            stream.seal(range)?;
            self.streams_mut().insert(range.stream_id(), stream);
            Ok(())
        }
    }

    fn check_barrier<R>(&self, stream_id: u64, range_index: i32, req: &R) -> Result<(), AppendError>
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

    async fn append(
        &self,
        options: &WriteOptions,
        request: AppendRecordRequest,
    ) -> Result<AppendResult, AppendError> {
        self.store.append(options, request).await
    }

    async fn fetch(&self, options: ReadOptions) -> Result<FetchResult, FetchError> {
        self.store.fetch(options).await
    }
}

impl<S, O> ResourceEventObserver for DefaultRangeManager<S, O> {
    fn on_resource_event(&self, event: &ResourceEvent) {
        match &event.resource {
            Resource::Range(range) => {
                self.on_range_event(event.event_type, range);
            }
            Resource::Stream(stream) => {
                self.on_stream_event(event.event_type, stream);
            }
            _ => {}
        }
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
