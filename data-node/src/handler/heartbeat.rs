use std::rc::Rc;

use codec::frame::{Frame, OperationCode};
use protocol::rpc::header::{ErrorCode, HeartbeatRequest};
use slog::Logger;
use store::ElasticStore;

use super::util::root_as_rpc_request;

#[derive(Debug)]
pub(crate) struct Heartbeat<'a> {
    log: Logger,
    request: HeartbeatRequest<'a>,
}

impl<'a> Heartbeat<'a> {
    pub(crate) fn parse_frame(log: Logger, frame: &'a Frame) -> Result<Self, ErrorCode> {
        let request = frame
            .header
            .as_ref()
            .map(|buf| root_as_rpc_request::<HeartbeatRequest>(buf))
            .ok_or(ErrorCode::BAD_REQUEST)?
            .map_err(|e| ErrorCode::BAD_REQUEST)?;

        Ok(Self { log, request })
    }

    pub(crate) async fn apply(&self, store: Rc<ElasticStore>, response: &mut Frame) {
    }
}
