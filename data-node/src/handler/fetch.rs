use bytes::Bytes;
use codec::frame::Frame;

use chrono::prelude::*;
use flatbuffers::FlatBufferBuilder;
use futures::future::join_all;
use protocol::rpc::header::{
    AppendRequest, AppendResponseArgs, AppendResultArgs, ErrorCode, FetchRequest,
};
use slog::{warn, Logger};
use std::rc::Rc;
use store::{
    error::AppendError,
    ops::append::AppendResult,
    option::{ReadOptions, WriteOptions},
    AppendRecordRequest, ElasticStore, Store,
};

use super::util::{
    finish_response_builder, root_as_rpc_request, system_error_frame_bytes, MIN_BUFFER_SIZE,
};

#[derive(Debug)]
pub(crate) struct Fetch<'a> {
    /// Logger
    logger: Logger,

    /// The append request already parsed by flatbuffers
    fetch_request: FetchRequest<'a>,
}

impl<'a> Fetch<'a> {
    pub(crate) fn parse_frame(logger: Logger, request: &Frame) -> Result<Fetch, ErrorCode> {
        let request_buf = match request.header {
            Some(ref buf) => buf,
            None => {
                warn!(
                    logger,
                    "FetchRequest[stream-id={}] received without payload", request.stream_id
                );
                return Err(ErrorCode::INVALID_REQUEST);
            }
        };

        let fetch_request = match root_as_rpc_request::<FetchRequest>(request_buf) {
            Ok(request) => request,
            Err(e) => {
                warn!(
                    logger,
                    "FetchRequest[stream-id={}] received with invalid payload. Cause: {:?}",
                    request.stream_id,
                    e
                );
                return Err(ErrorCode::INVALID_REQUEST);
            }
        };

        Ok(Fetch {
            logger,
            fetch_request,
        })
    }

    /// Apply the fetch requests to the store
    pub(crate) async fn apply(&self, store: Rc<ElasticStore>, response: &mut Frame) {
        let store_requests = self.build_store_requests();
        let futures: Vec<_> = store_requests
            .iter()
            .map(|fetch_option| store.fetch(fetch_option))
            .collect();
    }

    fn build_store_requests(&self) -> Vec<ReadOptions> {
        self.fetch_request
            .fetch_requests()
            .iter()
            .flatten()
            .map(|req| ReadOptions {
                stream_id: req.stream_id(),
                offset: req.fetch_offset(),
                max_wait_ms: self.fetch_request.max_wait_ms(),
                max_bytes: req.batch_max_bytes(),
            })
            .collect()
    }
}
