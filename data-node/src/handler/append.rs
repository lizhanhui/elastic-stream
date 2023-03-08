use bytes::Bytes;
use codec::frame::Frame;

use chrono::prelude::*;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use futures::future::join_all;
use protocol::rpc::header::{AppendRequest, AppendResponseArgs, AppendResultArgs, ErrorCode};
use slog::{warn, Logger};
use std::rc::Rc;
use store::{
    error::AppendError, ops::append::AppendResult, option::WriteOptions, AppendRecordRequest,
    ElasticStore, Store,
};

use super::util::{
    finish_response_builder, root_as_rpc_request, system_error_frame_bytes, MIN_BUFFER_SIZE,
};

#[derive(Debug)]
pub(crate) struct Append<'a> {
    /// Logger
    logger: Logger,

    /// The append request already parsed by flatbuffers
    append_request: AppendRequest<'a>,

    /// The payload may contains multiple record batches,
    /// the length of each batch is stored in append_request
    payload: &'a Bytes,
}

impl<'a> Append<'a> {
    pub(crate) fn parse_frame(logger: Logger, request: &Frame) -> Result<Append, ErrorCode> {
        let request_buf = match request.header {
            Some(ref buf) => buf,
            None => {
                warn!(
                    logger,
                    "AppendRequest[stream-id={}] received without payload", request.stream_id
                );
                return Err(ErrorCode::INVALID_REQUEST);
            }
        };

        let append_request = match root_as_rpc_request::<AppendRequest>(request_buf) {
            Ok(request) => request,
            Err(e) => {
                warn!(
                    logger,
                    "AppendRequest[stream-id={}] received with invalid payload. Cause: {:?}",
                    request.stream_id,
                    e
                );
                return Err(ErrorCode::INVALID_REQUEST);
            }
        };

        let payload = match request.payload {
            Some(ref buf) => buf,
            None => {
                warn!(
                    logger,
                    "AppendRequest[stream-id={}] received without payload", request.stream_id
                );
                return Err(ErrorCode::INVALID_REQUEST);
            }
        };

        Ok(Append {
            logger,
            append_request,
            payload,
        })
    }

    /// Process message publishment request
    ///
    /// On receiving a message publishment request, it wraps the incoming request to a `Record`.
    /// The record is then moved to `Store::put`, where persistence, replication and other auxillary
    /// operations are properly performed.
    ///
    /// Once the underlying operations are completed, the `Store#put` API shall asynchronously return
    /// `Result<PutResult, PutError>`. The result will be rendered into the `response`.
    ///
    /// `response` - Mutable response frame reference, into which required business data are filled.
    ///
    pub(crate) async fn apply(&self, store: Rc<ElasticStore>, response: &mut Frame) {
        let to_store_requests = match self.build_store_requests() {
            Ok(requests) => requests,
            Err(err_code) => {
                // The request frame is invalid, return a system error frame directly
                response.flag_system_err();
                response.header = Some(system_error_frame_bytes(err_code, "Invalid request"));
                return;
            }
        };

        let futures: Vec<_> = to_store_requests
            .iter()
            .map(|req| {
                let options = WriteOptions::default();
                store.append(options, req.clone())
            })
            .collect();

        let res_from_store: Vec<Result<AppendResult, AppendError>> = join_all(futures).await;

        let mut builder = FlatBufferBuilder::with_capacity(MIN_BUFFER_SIZE);
        let append_results: Vec<_> = res_from_store
            .iter()
            .map(|res| {
                match res {
                    Ok(result) => {
                        let args = AppendResultArgs {
                            stream_id: result.stream_id,
                            // TODO: fill the write request index
                            request_index: 0,
                            base_offset: result.offset,
                            stream_append_time_ms: Utc::now().timestamp(),
                            error_code: ErrorCode::NONE,
                            error_message: None,
                        };
                        protocol::rpc::header::AppendResult::create(&mut builder, &args)
                    }
                    Err(e) => {
                        warn!(self.logger, "Append failed: {:?}", e);
                        let (err_code, error_message) = self.convert_append_error(e);

                        let mut error_message_fb = None;
                        if let Some(error_message) = error_message {
                            error_message_fb = Some(builder.create_string(error_message.as_str()));
                        }

                        let args = AppendResultArgs {
                            stream_id: 0,
                            request_index: 0,
                            base_offset: 0,
                            stream_append_time_ms: 0,
                            error_code: err_code,
                            error_message: error_message_fb,
                        };
                        protocol::rpc::header::AppendResult::create(&mut builder, &args)
                    }
                }
            })
            .collect();

        let append_results_fb = builder.create_vector(&append_results);

        let res_args = AppendResponseArgs {
            throttle_time_ms: 0,
            append_responses: Some(append_results_fb),
            error_code: ErrorCode::NONE,
            error_message: None,
        };

        let res_offset = protocol::rpc::header::AppendResponse::create(&mut builder, &res_args);
        let res_header = finish_response_builder(&mut builder, res_offset);
        response.header = Some(res_header);

        ()
    }

    fn build_store_requests(&self) -> Result<Vec<AppendRecordRequest>, ErrorCode> {
        let mut payload = self.payload.clone();
        let mut err_code = ErrorCode::NONE;
        // Iterate over the append requests and append each record batch
        let to_store_requests: Vec<_> = self
            .append_request
            .append_requests()
            .iter()
            .flatten()
            .map_while(|record_batch| {
                let stream_id = record_batch.stream_id();
                let request_index = record_batch.request_index();
                let batch_len = record_batch.batch_length();

                // TODO: Check if the stream exists and
                // the current data node owns the newly writable range of the stream

                // TODO: Set the offset and modify the payload
                let offset = 0 as i64;

                // Split the current batch payload from the whole payload
                if payload.len() < batch_len as usize {
                    err_code = ErrorCode::INVALID_REQUEST;
                    return None;
                }
                let payload_b = payload.split_to(batch_len as usize);

                let to_store = AppendRecordRequest {
                    stream_id,
                    offset: offset,
                    buffer: payload_b,
                };
                Some(to_store)
            })
            .collect();
        if err_code != ErrorCode::NONE {
            return Err(err_code);
        }
        Ok(to_store_requests)
    }

    fn convert_append_error(&self, err: &AppendError) -> (ErrorCode, Option<String>) {
        match err {
            AppendError::SubmissionQueue => (
                ErrorCode::STORAGE_NOT_AVAILABLE,
                Some(AppendError::SubmissionQueue.to_string()),
            ),
            AppendError::ChannelRecv => (
                ErrorCode::STORAGE_NOT_AVAILABLE,
                Some(AppendError::SubmissionQueue.to_string()),
            ),
            AppendError::System(_) => (
                ErrorCode::UNKNOWN_STORAGE_ERROR,
                Some(AppendError::SubmissionQueue.to_string()),
            ),
            AppendError::BadRequest => (
                ErrorCode::INVALID_REQUEST,
                Some(AppendError::SubmissionQueue.to_string()),
            ),
            AppendError::Internal => (
                ErrorCode::UNKNOWN_STORAGE_ERROR,
                Some(AppendError::SubmissionQueue.to_string()),
            ),
        }
    }
}
