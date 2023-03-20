use std::rc::Rc;

use codec::frame::Frame;
use protocol::rpc::header::{ErrorCode, SealRangesRequest};
use slog::{warn, Logger};
use store::ElasticStore;

use super::util::root_as_rpc_request;

#[derive(Debug)]
pub(crate) struct SealRanges<'a> {
    log: Logger,
    request: SealRangesRequest<'a>,
}

impl<'a> SealRanges<'a> {
    pub(crate) fn from_frame(log: Logger, frame: &'a Frame) -> Result<Self, ErrorCode> {
        let res = frame
            .header
            .as_ref()
            .map(|buf| root_as_rpc_request::<SealRangesRequest>(buf))
            .ok_or(ErrorCode::BAD_REQUEST)?
            .map_err(|_e| {
                warn!(
                    log,
                    "Received an invalid seal range request[stream-id={}]", frame.stream_id
                );
                ErrorCode::BAD_REQUEST
            })?;

        Ok(Self { log, request: res })
    }

    pub(crate) async fn apply(&self, store: Rc<ElasticStore>, response: &mut Frame) {}
}
