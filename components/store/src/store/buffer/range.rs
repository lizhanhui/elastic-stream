use std::{cmp::Ordering, collections::BinaryHeap};

use crate::AppendRecordRequest;

use super::BufferedRequest;

#[derive(Debug)]
pub(crate) struct RangeBuffer {
    /// Next continuous offset
    next: u64,

    buffered: BinaryHeap<BufferedRequest>,
}

impl RangeBuffer {
    pub(crate) fn new(next: u64) -> Self {
        Self {
            next,
            buffered: BinaryHeap::new(),
        }
    }

    pub(crate) fn next(&self) -> u64 {
        self.next
    }

    pub(crate) fn fast_forward(&mut self, request: &AppendRecordRequest) -> bool {
        match self.next.cmp(&request.offset) {
            Ordering::Less => false,
            Ordering::Equal => {
                self.next += request.len as u64;
                true
            }
            Ordering::Greater => true,
        }
    }

    pub(crate) fn drain(&mut self) -> Option<Vec<BufferedRequest>> {
        if self.buffered.is_empty() {
            return None;
        }

        if let Some(item) = self.buffered.peek() {
            if item.request.offset != self.next {
                return None;
            }
        }

        let mut res = vec![];
        while let Some(item) = self.buffered.peek() {
            if item.request.offset != self.next {
                break;
            }

            if let Some(item) = self.buffered.pop() {
                self.next += item.request.len as u64;
                res.push(item);
            }
        }
        Some(res)
    }

    pub(crate) fn buffer(&mut self, req: BufferedRequest) {
        debug_assert!(
            req.request.offset != self.next,
            "Should NOT buffer an append request if it's already continuous"
        );
        self.buffered.push(req);
    }

    pub(crate) fn buffered_requests(&self) -> usize {
        self.buffered.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::{store::buffer::BufferedRequest, AppendRecordRequest};

    use super::RangeBuffer;
    use bytes::Bytes;
    use std::error::Error;

    #[test]
    fn test_new() -> Result<(), Box<dyn Error>> {
        let buffer = super::RangeBuffer::new(1);
        assert_eq!(0, buffer.buffered.len());
        assert_eq!(1, buffer.next);
        Ok(())
    }

    #[test]
    fn test_fast_forward() -> Result<(), Box<dyn Error>> {
        let mut buffer = RangeBuffer::new(16);
        let request = AppendRecordRequest {
            stream_id: 1,
            range_index: 1,
            offset: 16,
            len: 8,
            buffer: Bytes::new(),
        };
        assert!(buffer.fast_forward(&request));
        assert_eq!(24, buffer.next);
        Ok(())
    }

    #[test]
    fn test_buffer() -> Result<(), Box<dyn Error>> {
        let mut buffer = RangeBuffer::new(16);
        let request = AppendRecordRequest {
            stream_id: 1,
            range_index: 1,
            offset: 17,
            len: 8,
            buffer: Bytes::new(),
        };
        let (tx, _rx) = local_sync::oneshot::channel();
        let item = BufferedRequest { request, tx };
        buffer.buffer(item);
        Ok(())
    }

    #[test]
    fn test_drain() -> Result<(), Box<dyn Error>> {
        let mut buffer = RangeBuffer::new(16);

        for i in 0..2 {
            let request = AppendRecordRequest {
                stream_id: 1,
                range_index: 1,
                offset: 17 + 8 * i,
                len: 8,
                buffer: Bytes::new(),
            };
            assert!(!buffer.fast_forward(&request));
            let (tx, _rx) = local_sync::oneshot::channel();
            let item = BufferedRequest { request, tx };
            buffer.buffer(item);
        }

        let request = AppendRecordRequest {
            stream_id: 1,
            range_index: 1,
            offset: 16,
            len: 1,
            buffer: Bytes::new(),
        };
        assert!(buffer.fast_forward(&request));

        let res = buffer.drain().unwrap();
        assert_eq!(33, buffer.next);
        assert_eq!(2, res.len());
        assert_eq!(17, res[0].request.offset);
        Ok(())
    }
}
