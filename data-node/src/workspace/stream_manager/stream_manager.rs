use std::collections::HashMap;

use futures::io::Window;
use model::{
    range::{Range, StreamRange},
    stream::Stream,
};
use slog::{error, trace, Logger};

use crate::{error::ServiceError, workspace::append_window::AppendWindow};

use super::fetcher::Fetcher;

pub(crate) struct StreamManager {
    log: Logger,
    streams: HashMap<i64, Stream>,
    windows: HashMap<i64, AppendWindow>,
    fetcher: Fetcher,
}

impl StreamManager {
    pub(crate) fn new(log: Logger, fetcher: Fetcher) -> Self {
        Self {
            log,
            streams: HashMap::new(),
            windows: HashMap::new(),
            fetcher,
        }
    }

    pub(crate) async fn start(&mut self) -> Result<(), ServiceError> {
        self.fetcher.start();
        let mut bootstrap = false;
        if let Fetcher::PlacementClient { .. } = &self.fetcher {
            bootstrap = true;
        }

        if bootstrap {
            self.bootstrap().await?;
        }
        Ok(())
    }

    /// Bootstrap all stream ranges that are assigned to current data node.
    async fn bootstrap(&mut self) -> Result<(), ServiceError> {
        let ranges = self.fetcher.bootstrap(&self.log).await?;

        for range in ranges {
            let stream = self
                .streams
                .entry(range.stream_id())
                .or_insert(Stream::with_id(range.stream_id()));
            stream.push(range);
        }

        self.streams
            .iter_mut()
            .for_each(|(_, stream)| stream.sort());

        Ok(())
    }

    pub(crate) async fn create_stream_if_missing(
        &mut self,
        stream_id: i64,
    ) -> Result<(), ServiceError> {
        // If, though unlikely, the stream is firstly assigned to it.
        // TODO: https://doc.rust-lang.org/std/intrinsics/fn.unlikely.html
        if !self.streams.contains_key(&stream_id) {
            trace!(
                self.log,
                "About to fetch ranges for stream[id={}]",
                stream_id
            );
            let ranges = self.fetcher.fetch(stream_id, &self.log).await?;

            debug_assert!(
                !ranges.is_empty(),
                "PlacementManager should not respond with empty range list"
            );
            let range = ranges
                .last()
                .expect("Stream range list must have at least one range");
            debug_assert!(
                !range.is_sealed(),
                "The last range of a stream should always be mutable"
            );
            let start = range.start();
            trace!(
                self.log,
                "Mutable range of stream[id={}] is: [{}, -1)",
                stream_id,
                start
            );

            // TODO: verify current node is actually a leader or follower of the last mutable range.

            let window = AppendWindow::new(start);

            self.windows.insert(stream_id, window);

            let stream = Stream::new(stream_id, ranges);

            self.streams.insert(stream_id, stream);
            trace!(self.log, "Create Stream[id={}]", stream_id);

            return Ok(());
        }
        Ok(())
    }

    pub(crate) async fn ensure_mutable(&mut self, stream_id: i64) -> Result<(), ServiceError> {
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            if stream.is_mut() {
                return Ok(());
            }
        }

        let ranges = self.fetcher.fetch(stream_id, &self.log).await?;
        if let Some(range) = ranges.last() {
            if range.is_sealed() {
                return Err(ServiceError::AlreadySealed);
            }
        }
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.refresh(ranges);
        }

        Ok(())
    }

    /// Allocate a record slot for the specified stream.
    ///
    /// If, though unlikely, a mutable range is not available, fetch it from placement manager.
    pub(crate) async fn alloc_record_slot(&mut self, stream_id: i64) -> Result<u64, ServiceError> {
        self.create_stream_if_missing(stream_id).await?;
        self.ensure_mutable(stream_id).await?;

        if let Some(window) = self.windows.get_mut(&stream_id) {
            let slot = window.alloc_slot();
            return Ok(slot);
        }

        unreachable!("Should have an `AppendWindow` for stream[id={}]", stream_id);
    }

    pub(crate) fn alloc_record_batch_slots(
        &mut self,
        stream_id: i64,
        batch_size: usize,
    ) -> Result<u64, ServiceError> {
        if let Some(window) = self.windows.get_mut(&stream_id) {
            let start_slot = window.alloc_batch_slots(batch_size);
            return Ok(start_slot);
        }
        // There is not an append window available, the segments of the stream should have been sealed.
        Err(ServiceError::AlreadySealed)
    }

    pub(crate) fn ack(&mut self, stream_id: i64, offset: u64) -> Result<(), ServiceError> {
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            if !stream.is_mut() {
                return Err(ServiceError::AlreadySealed);
            }
        }

        if let Some(window) = self.windows.get_mut(&stream_id) {
            window.ack(offset);
        }
        Ok(())
    }

    pub(crate) fn seal(&mut self, stream_id: i64, range_index: i32) -> Result<u64, ServiceError> {
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            if !stream.is_mut() {
                return Err(ServiceError::AlreadySealed);
            }
        }

        let committed = match self.windows.remove(&stream_id) {
            Some(window) => window.commit,
            None => {
                error!(self.log, "Expected `AppendWindow` is missing");
                return Err(ServiceError::Seal);
            }
        };

        if let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.seal(committed, range_index);
            Ok(committed)
        } else {
            Err(ServiceError::Seal)
        }
    }

    pub(crate) async fn describe_range(
        &mut self,
        stream_id: i64,
        range_id: i32,
    ) -> Result<StreamRange, ServiceError> {
        self.create_stream_if_missing(stream_id).await?;

        if let Some(stream) = self.streams.get(&stream_id) {
            if let Some(mut range) = stream.range(range_id) {
                if let None = range.end() {
                    if let Some(window) = self.windows.get(&stream_id) {
                        range.set_limit(window.commit);
                    }
                }
                return Ok(range);
            } else {
                return Err(ServiceError::NotFound(format!("Range[index={}]", range_id)));
            }
        }
        return Err(ServiceError::NotFound(format!("Stream[id={}]", stream_id)));
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use model::range::StreamRange;
    use tokio::sync::mpsc;

    use crate::workspace::stream_manager::{fetcher::Fetcher, StreamManager};
    const TOTAL: i32 = 16;

    async fn create_fetcher() -> Fetcher {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let fetcher = Fetcher::Channel { sender: tx };

        tokio_uring::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(task) => {
                        let stream_id = task.stream_id;
                        let ranges = (0..TOTAL)
                            .map(|i| {
                                if i < TOTAL - 1 {
                                    StreamRange::new(
                                        stream_id,
                                        i,
                                        (i * 100) as u64,
                                        ((i + 1) * 100) as u64,
                                        Some(((i + 1) * 100) as u64),
                                    )
                                } else {
                                    StreamRange::new(stream_id, i, (i * 100) as u64, 0, None)
                                }
                            })
                            .collect::<Vec<_>>();
                        if let Err(e) = task.tx.send(Ok(ranges)) {
                            panic!("Failed to transfer mocked ranges");
                        }
                    }
                    None => {
                        break;
                    }
                }
            }
        });

        fetcher
    }

    #[test]
    fn test_seal() -> Result<(), Box<dyn Error>> {
        let logger = test_util::terminal_logger();
        tokio_uring::start(async {
            let fetcher = create_fetcher().await;
            let stream_id = 1;
            let mut stream_manager = StreamManager::new(logger, fetcher);
            let offset = stream_manager.alloc_record_slot(stream_id).await.unwrap();
            stream_manager.ack(stream_id, offset)?;
            let seal_offset = stream_manager.seal(stream_id, TOTAL - 1).unwrap();
            assert_eq!(offset + 1, seal_offset);
            Ok(())
        })
    }

    #[test]
    fn test_describe_range() -> Result<(), Box<dyn Error>> {
        let logger = test_util::terminal_logger();
        tokio_uring::start(async {
            let fetcher = create_fetcher().await;
            let stream_id = 1;
            let mut stream_manager = StreamManager::new(logger, fetcher);
            let offset = stream_manager.alloc_record_slot(stream_id).await.unwrap();
            stream_manager.ack(stream_id, offset)?;
            let range = stream_manager.describe_range(stream_id, TOTAL - 1).await?;
            assert_eq!(offset + 1, range.limit());
            Ok(())
        })
    }
}
