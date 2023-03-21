use std::collections::HashMap;

use model::{
    range::{Range, StreamRange},
    stream::Stream,
};
use slog::{error, trace, warn, Logger};
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

    async fn create_stream_if_missing(&mut self, stream_id: i64) -> Result<(), ServiceError> {
        // If, though unlikely, the stream is firstly assigned to it.
        // TODO: https://doc.rust-lang.org/std/intrinsics/fn.unlikely.html
        if !self.streams.contains_key(&stream_id) {
            trace!(
                self.log,
                "About to fetch ranges for stream[id={}]",
                stream_id
            );
            if let Some(ranges) = self.fetcher.fetch(stream_id).await? {
                debug_assert!(
                    !ranges.is_empty(),
                    "PlacementManager should not respond with empty range list"
                );
                let range = ranges
                    .last()
                    .expect("Stream range list must have at least one range");
                debug_assert!(
                    !range.sealed(),
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
            } else {
                warn!(self.log, "Failed to list range from placement manager");
                return Err(ServiceError::Internal(format!(
                    "ListRange for stream[id={}] failed",
                    stream_id
                )));
            }
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

        if let Some(window) = self.windows.get_mut(&stream_id) {
            let slot = window.alloc_slot(tx);
            return Ok(slot);
        }

        unreachable!("Should have an `AppendWindow` for stream[id={}]", stream_id);
    }

    /// TODO: Consider current
    pub(crate) fn seal(&mut self, stream_id: i64) -> Result<u64, ServiceError> {
        let committed = match self.windows.remove(&stream_id) {
            Some(window) => window.commit,
            None => {
                error!(self.log, "Expected `AppendWindow` is missing");
                return Err(ServiceError::Seal);
            }
        };

        if let Some(stream) = self.streams.get(&stream_id) {
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

    

}