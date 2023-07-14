use std::collections::HashMap;

use log::{debug, info, warn};

use crate::AppendRecordRequest;

use super::{range::RangeBuffer, BufferedRequest};

/// A stream may have one or more ranges being appended.
///
/// It is true that only the last range is conceptually mutable within a stream. But, under the chasing-write replication algorithm,
/// immutable ranges may have incomplete data the moment they got sealed. As as result, there is a procedure to complement these incomplete ranges. And this is also the reason why StreamBuffer has to work with multiple concurrently working range buffers.
#[derive(Debug)]
pub(crate) struct StreamBuffer {
    stream_id: u64,
    buffers: HashMap<u32, RangeBuffer>,
}

impl StreamBuffer {
    pub(crate) fn new(stream_id: u64) -> Self {
        Self {
            stream_id,
            buffers: HashMap::new(),
        }
    }

    pub(crate) fn fast_forward(&mut self, req: &AppendRecordRequest) -> bool {
        if let Some(buffer) = self.buffers.get_mut(&(req.range_index as u32)) {
            buffer.fast_forward(req)
        } else {
            false
        }
    }

    pub(crate) fn buffer(&mut self, request: BufferedRequest) -> Result<(), BufferedRequest> {
        if let Some(buffer) = self.buffers.get_mut(&(request.request.range_index as u32)) {
            buffer.buffer(request);
            Ok(())
        } else {
            Err(request)
        }
    }

    pub(crate) fn create_range(&mut self, index: u32, offset: u64) {
        let buffer = self
            .buffers
            .entry(index)
            .or_insert(RangeBuffer::new(offset));
        debug!(
            "Created RangeBuffer[stream-id={}, index={}, offset={}]",
            self.stream_id, index, offset
        );
        debug_assert_eq!(buffer.next(), offset);
    }

    pub(crate) fn seal_range(&mut self, index: u32) {
        if let Some(buffer) = self.buffers.remove(&index) {
            let dropped = buffer.buffered_requests();
            if dropped > 0 {
                warn!(
                    "RangeBuffer[stream-id={}, index={}] dropped {} buffered append requests",
                    self.stream_id, index, dropped
                );
            } else {
                info!(
                    "RangeBuffer[stream-id={}, index={}] dropped",
                    self.stream_id, index
                );
            }
        }
    }

    pub(crate) fn drain(&mut self, range: u32) -> Option<Vec<BufferedRequest>> {
        if let Some(buffer) = self.buffers.get_mut(&range) {
            buffer.drain()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StreamBuffer;
    use crate::{store::buffer::BufferedRequest, AppendRecordRequest};
    use bytes::Bytes;
    use std::error::Error;

    #[test]
    fn test_new() -> Result<(), Box<dyn Error>> {
        let buffer = StreamBuffer::new(1);
        assert_eq!(buffer.stream_id, 1);
        assert!(buffer.buffers.is_empty());
        Ok(())
    }

    #[test]
    fn test_create_range() -> Result<(), Box<dyn Error>> {
        let mut buffer = StreamBuffer::new(1);
        buffer.create_range(1, 0);
        assert_eq!(buffer.buffers.len(), 1);
        assert!(buffer.buffers.contains_key(&1));

        // Verify create range is reentrant.
        buffer.create_range(1, 0);
        assert_eq!(buffer.buffers.len(), 1);
        assert!(buffer.buffers.contains_key(&1));
        Ok(())
    }

    #[test]
    fn test_fast_forward() -> Result<(), Box<dyn Error>> {
        let mut buffer = StreamBuffer::new(1);
        buffer.create_range(1, 0);

        // Fast forward to the next offset.
        let request = AppendRecordRequest {
            stream_id: 1,
            range_index: 1,
            offset: 0,
            len: 8,
            buffer: Bytes::new(),
        };
        assert!(buffer.fast_forward(&request));

        let request = AppendRecordRequest {
            stream_id: 1,
            range_index: 2, // Non-existent range.
            offset: 16,
            len: 8,
            buffer: Bytes::new(),
        };
        assert!(!buffer.fast_forward(&request));
        Ok(())
    }

    #[test]
    fn test_buffer() -> Result<(), Box<dyn Error>> {
        let mut buffer = StreamBuffer::new(1);
        let request = AppendRecordRequest {
            stream_id: 1,
            range_index: 1,
            offset: 16,
            len: 8,
            buffer: Bytes::new(),
        };
        let (tx, _rx) = local_sync::oneshot::channel();
        let item = BufferedRequest { request, tx };
        let res = buffer.buffer(item);
        assert!(res.is_err());

        buffer.create_range(1, 0);
        let request = AppendRecordRequest {
            stream_id: 1,
            range_index: 1,
            offset: 16,
            len: 8,
            buffer: Bytes::new(),
        };
        let (tx, _rx) = local_sync::oneshot::channel();
        let item = BufferedRequest { request, tx };
        assert_eq!(buffer.buffer(item), Ok(()));
        Ok(())
    }

    #[test]
    fn test_seal() -> Result<(), Box<dyn Error>> {
        let mut buffer = StreamBuffer::new(1);
        buffer.create_range(1, 0);
        buffer.seal_range(1);
        assert!(buffer.buffers.is_empty());
        Ok(())
    }

    #[test]
    fn test_drain() -> Result<(), Box<dyn Error>> {
        let mut buffer = StreamBuffer::new(1);
        buffer.create_range(1, 0);
        let request = AppendRecordRequest {
            stream_id: 1,
            range_index: 1,
            offset: 1,
            len: 8,
            buffer: Bytes::new(),
        };
        let (tx, _rx) = local_sync::oneshot::channel();
        let item = BufferedRequest { request, tx };
        buffer
            .buffer(item)
            .expect("Append requests should be buffered");
        assert_eq!(buffer.drain(1), None);

        let request = AppendRecordRequest {
            stream_id: 1,
            range_index: 1,
            offset: 0,
            len: 1,
            buffer: Bytes::new(),
        };
        assert!(buffer.fast_forward(&request));
        let buffered = buffer.drain(1);
        assert_eq!(buffered.unwrap().len(), 1);
        Ok(())
    }
}
