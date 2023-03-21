use std::collections::HashMap;

use model::stream::Stream;
use slog::{error, Logger};

use crate::{error::ServiceError, workspace::append_window::AppendWindow};

pub(crate) struct StreamManager {
    log: Logger,
    streams: HashMap<i64, Stream>,
    windows: HashMap<i64, AppendWindow>,
}

impl StreamManager {
    /// Allocate a record slot for the specified stream.
    ///
    /// If, though unlikely, a mutable range is not available, fetch it from placement manager.
    pub(crate) async fn alloc_record_slot(&self, stream_id: i64) -> Result<u64, ServiceError> {
        // TODO: https://doc.rust-lang.org/std/intrinsics/fn.unlikely.html
        todo!()
    }

    /// TODO: Consider current
    pub(crate) fn seal(&mut self, stream_id: i64) -> Result<u64, ServiceError> {
        let committed = match self.windows.remove(&stream_id) {
            Some(window) => window.committed,
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
}
