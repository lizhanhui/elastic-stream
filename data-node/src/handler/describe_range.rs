use codec::frame::Frame;

use protocol::rpc::header::{DescribeRangesRequest, ErrorCode};
use slog::{warn, Logger};
use std::rc::Rc;
use store::ElasticStore;

use super::util::root_as_rpc_request;

#[derive(Debug)]
pub(crate) struct DescribeRange<'a> {
    /// Logger
    logger: Logger,

    /// The describe request already parsed by flatbuffers
    describe_request: DescribeRangesRequest<'a>,
}

impl<'a> DescribeRange<'a> {
    pub(crate) fn parse_frame(logger: Logger, request: &Frame) -> Result<DescribeRange, ErrorCode> {
        let request_buf = match request.header {
            Some(ref buf) => buf,
            None => {
                warn!(
                    logger,
                    "DescribeRangesRequest[stream-id={}] received without payload",
                    request.stream_id
                );
                return Err(ErrorCode::BAD_REQUEST);
            }
        };

        let describe_request = match root_as_rpc_request::<DescribeRangesRequest>(request_buf) {
            Ok(request) => request,
            Err(e) => {
                warn!(
                    logger,
                    "DescribeRangesRequest[stream-id={}] received with invalid payload. Cause: {:?}",
                    request.stream_id,
                    e
                );
                return Err(ErrorCode::BAD_REQUEST);
            }
        };

        Ok(DescribeRange {
            logger,
            describe_request,
        })
    }

    pub(crate) async fn apply(&self, store: Rc<ElasticStore>, response: &mut Frame) {}
}
