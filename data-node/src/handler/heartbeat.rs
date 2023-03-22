use std::{cell::RefCell, rc::Rc};

use codec::frame::Frame;
use protocol::rpc::header::{ErrorCode, HeartbeatRequest};
use slog::Logger;
use store::ElasticStore;

use crate::workspace::stream_manager::StreamManager;

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
            .map_err(|_e| ErrorCode::BAD_REQUEST)?;

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
