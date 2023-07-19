//! Buffer and sort incoming append requests.
//!
//! Admittedly, we can support concurrent, out-of-order append requests of a range as long as we acknowledge them
//! orderly. Considering this also brings about much complexity to the recovery and complicating crash safety
//! implementation, we decide to simplify the problem: append requests are buffered and sorted before dispatching
//! to IO engine.
//!
//! Definitely it is a trade-off between {response time, write merge, IOPS} and implementation simplicity.

use crate::{error::AppendError, AppendRecordRequest, AppendResult};
use local_sync::oneshot;
use std::cmp::Ordering;
pub(crate) mod range;
pub mod store;
pub(crate) mod stream;

#[derive(Debug)]
pub(crate) struct BufferedRequest {
    pub(crate) request: AppendRecordRequest,
    pub(crate) tx: oneshot::Sender<Result<AppendResult, AppendError>>,
}

impl PartialEq for BufferedRequest {
    fn eq(&self, other: &Self) -> bool {
        self.request == other.request
    }
}

impl PartialOrd for BufferedRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for BufferedRequest {}

impl Ord for BufferedRequest {
    fn cmp(&self, other: &Self) -> Ordering {
        self.request.cmp(&other.request)
    }
}

#[cfg(test)]
mod tests {
    use local_sync::oneshot;

    #[test]
    fn test_ordering() {
        use super::BufferedRequest;
        use crate::AppendRecordRequest;
        use bytes::Bytes;

        let req1 = AppendRecordRequest {
            stream_id: 1,
            range_index: 1,
            offset: 16,
            len: 8,
            buffer: Bytes::new(),
        };
        let (tx, _rx) = oneshot::channel();
        let item1 = BufferedRequest { request: req1, tx };

        let req2 = AppendRecordRequest {
            stream_id: 1,
            range_index: 1,
            offset: 24,
            len: 8,
            buffer: Bytes::new(),
        };
        let (tx, _rx) = oneshot::channel();
        let item2 = BufferedRequest { request: req2, tx };

        assert!(item1 > item2);
    }
}
