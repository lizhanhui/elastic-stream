use std::collections::HashMap;

use model::{
    range::{Range, StreamRange},
    stream::Stream,
};
use slog::{error, trace, Logger};
use tokio::sync::oneshot;

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

    pub(crate) fn start(&mut self) {
        self.fetcher.start();
    }

    async fn create_stream_if_missing(&mut self, stream_id: i64) -> Result<(), ServiceError> {
        // If, though unlikely, the stream is firstly assigned to it.
        // TODO: https://doc.rust-lang.org/std/intrinsics/fn.unlikely.html
        if !self.streams.contains_key(&stream_id) {
            trace!(
                self.log,
                "About to fetch ranges for stream[id={}]",
                stream_id
            );
            let ranges = self.fetcher.fetch(stream_id).await?;

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

    async fn ensure_mutable(&mut self, stream_id: i64) -> Result<(), ServiceError> {
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            if stream.is_mut() {
                return Ok(());
            }
        }

        let ranges = self.fetcher.fetch(stream_id).await?;
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
    pub(crate) async fn alloc_record_slot(
        &mut self,
        stream_id: i64,
        tx: oneshot::Sender<()>,
    ) -> Result<u64, ServiceError> {
        self.create_stream_if_missing(stream_id).await?;
        self.ensure_mutable(stream_id).await?;

        if let Some(window) = self.windows.get_mut(&stream_id) {
            let slot = window.alloc_slot(tx);
            return Ok(slot);
        }

        unreachable!("Should have an `AppendWindow` for stream[id={}]", stream_id);
    }

    pub(crate) async fn ack(&mut self, stream_id: i64, offset: u64) -> Result<(), ServiceError> {
        self.ensure_mutable(stream_id).await?;
        if let Some(window) = self.windows.get_mut(&stream_id) {
            window.ack(offset);
        }
        Ok(())
    }

    /// TODO: Consider current
    pub(crate) async fn seal(&mut self, stream_id: i64) -> Result<u64, ServiceError> {
        self.ensure_mutable(stream_id).await?;
        let committed = match self.windows.remove(&stream_id) {
            Some(window) => window.commit,
            None => {
                error!(self.log, "Expected `AppendWindow` is missing");
                return Err(ServiceError::Seal);
            }
        };

        if let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.seal(committed);
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
    use tokio::sync::{mpsc, oneshot};

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
            let (tx, _rx) = oneshot::channel();
            let offset = stream_manager
                .alloc_record_slot(stream_id, tx)
                .await
                .unwrap();
            stream_manager.ack(stream_id, offset).await?;
            let seal_offset = stream_manager.seal(stream_id).await.unwrap();
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
            let (tx, rx) = oneshot::channel();
            let offset = stream_manager
                .alloc_record_slot(stream_id, tx)
                .await
                .unwrap();
            stream_manager.ack(stream_id, offset).await?;
            let range = stream_manager.describe_range(stream_id, TOTAL - 1).await?;
            assert_eq!(offset + 1, range.limit());
            Ok(())
        })
    }
}
