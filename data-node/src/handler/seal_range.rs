use std::{cell::RefCell, rc::Rc};

use codec::frame::Frame;
use protocol::rpc::header::{ErrorCode, SealRangesRequest};
use slog::{warn, Logger};
use store::ElasticStore;

use crate::workspace::stream_manager::StreamManager;

use super::util::root_as_rpc_request;

#[derive(Debug)]
pub(crate) struct SealRange<'a> {
    log: Logger,
    request: SealRangesRequest<'a>,
}

impl<'a> SealRange<'a> {
    pub(crate) fn parse_frame(log: Logger, frame: &'a Frame) -> Result<Self, ErrorCode> {
        let request = frame
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

        Ok(Self { log, request })
    }

    pub(crate) async fn apply(
        &self,
        _store: Rc<ElasticStore>,
        stream_manager: Rc<RefCell<StreamManager>>,
        _response: &mut Frame,
    ) {
    }
}
