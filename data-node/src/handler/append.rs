use bytes::Bytes;
use codec::frame::Frame;

use chrono::prelude::*;
use flatbuffers::FlatBufferBuilder;
use futures::future::join_all;
use protocol::rpc::header::{
    AppendRequest, AppendResponseArgs, AppendResultArgs, ErrorCode, StatusArgs,
};
use slog::{warn, Logger};
use std::{cell::RefCell, rc::Rc};
use store::{
    error::AppendError, ops::append::AppendResult, option::WriteOptions, AppendRecordRequest,
    ElasticStore, Store,
};

use crate::workspace::stream_manager::StreamManager;

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
    payload: Bytes,
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
                return Err(ErrorCode::BAD_REQUEST);
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
                return Err(ErrorCode::BAD_REQUEST);
            }
        };

        let payload = match request.payload {
            // For append frame, the payload must be a single buffer
            Some(ref buf) if buf.len() == 1 => buf.first().ok_or(ErrorCode::BAD_REQUEST)?,
            _ => {
                warn!(
                    logger,
                    "AppendRequest[stream-id={}] received without payload", request.stream_id
                );
                return Err(ErrorCode::BAD_REQUEST);
            }
        };

        Ok(Append {
            logger,
            append_request,
            payload: payload.clone(),
        })
    }

    /// Process message publishment request
    ///
    /// On receiving a message publishment request, it wraps the incoming request to a `Record`.
    /// The record is then moved to `Store::append`, where persistence, replication and other auxillary
    /// operations are properly performed.
    ///
    /// Once the underlying operations are completed, the `Store#append` API shall asynchronously return
    /// `Result<AppendResult, AppendError>`. The result will be rendered into the `response`.
    ///
    /// `response` - Mutable response frame reference, into which required business data are filled.
    ///
    pub(crate) async fn apply(
        &self,
        store: Rc<ElasticStore>,
        stream_manager: Rc<RefCell<StreamManager>>,
        response: &mut Frame,
    ) {
        let to_store_requests = match self.build_store_requests(&stream_manager).await {
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
        let no_err_status = protocol::rpc::header::Status::create(
            &mut builder,
            &StatusArgs {
                code: ErrorCode::OK,
                message: None,
                detail: None,
            },
        );
        let append_results: Vec<_> = res_from_store
            .iter()
            .map(|res| {
                match res {
                    Ok(result) => {
                        if let Err(e) = stream_manager
                            .borrow_mut()
                            .ack(result.stream_id, result.offset as u64)
                        {
                            warn!(
                                self.logger,
                                "Failed to ack offset on store completion to stream manager: {:?}",
                                e
                            );
                        }
                        let args = AppendResultArgs {
                            stream_id: result.stream_id,
                            // TODO: fill the write request index
                            request_index: 0,
                            base_offset: result.offset,
                            stream_append_time_ms: Utc::now().timestamp(),
                            status: Some(no_err_status),
                        };
                        protocol::rpc::header::AppendResult::create(&mut builder, &args)
                    }
                    Err(e) => {
                        // TODO: what to do with the offset on failure?
                        warn!(self.logger, "Append failed: {:?}", e);
                        let (err_code, error_message) = self.convert_store_error(e);

                        let mut error_message_fb = None;
                        if let Some(error_message) = error_message {
                            error_message_fb = Some(builder.create_string(error_message.as_str()));
                        }
                        let status = protocol::rpc::header::Status::create(
                            &mut builder,
                            &StatusArgs {
                                code: err_code,
                                message: error_message_fb,
                                detail: None,
                            },
                        );

                        let args = AppendResultArgs {
                            stream_id: 0,
                            request_index: 0,
                            base_offset: 0,
                            stream_append_time_ms: 0,
                            status: Some(status),
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
            status: Some(no_err_status),
        };

        let res_offset = protocol::rpc::header::AppendResponse::create(&mut builder, &res_args);
        let res_header = finish_response_builder(&mut builder, res_offset);
        response.header = Some(res_header);
    }

    async fn build_store_requests(
        &self,
        stream_manager: &Rc<RefCell<StreamManager>>,
    ) -> Result<Vec<AppendRecordRequest>, ErrorCode> {
        let mut payload = self.payload.clone();
        let mut err_code = ErrorCode::OK;
        let mut manager = stream_manager.borrow_mut();

        let mut offsets = vec![];
        for entry in self.append_request.append_requests().iter().flatten() {
            let stream_id = entry.stream_id();
            let offset = manager
                .alloc_record_slot(stream_id)
                .await
                .map_err(|_e| ErrorCode::DN_INTERNAL_SERVER_ERROR)?;
            offsets.push(offset);
        }

        // Iterate over the append requests and append each record batch
        let to_store_requests: Vec<_> = self
            .append_request
            .append_requests()
            .iter()
            .flatten()
            .zip(offsets)
            .map_while(|(record_batch, offset)| {
                let stream_id = record_batch.stream_id();
                let _request_index = record_batch.request_index();
                let batch_len = record_batch.batch_length();

                // TODO: Check if the stream exists and
                // the current data node owns the newly writable range of the stream

                // Split the current batch payload from the whole payload
                if payload.len() < batch_len as usize {
                    err_code = ErrorCode::BAD_REQUEST;
                    return None;
                }
                let payload_b = payload.split_to(batch_len as usize);

                let to_store = AppendRecordRequest {
                    stream_id,
                    offset: offset as i64,
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

    fn convert_store_error(&self, err: &AppendError) -> (ErrorCode, Option<String>) {
        match err {
            AppendError::SubmissionQueue => (
                ErrorCode::PM_NO_AVAILABLE_DN,
                Some(AppendError::SubmissionQueue.to_string()),
            ),
            AppendError::ChannelRecv => (
                ErrorCode::PM_NO_AVAILABLE_DN,
                Some(AppendError::SubmissionQueue.to_string()),
            ),
            AppendError::System(_) => (
                ErrorCode::PM_NO_AVAILABLE_DN,
                Some(AppendError::SubmissionQueue.to_string()),
            ),
            AppendError::BadRequest => (
                ErrorCode::BAD_REQUEST,
                Some(AppendError::SubmissionQueue.to_string()),
            ),
            AppendError::Internal => (
                ErrorCode::PM_NO_AVAILABLE_DN,
                Some(AppendError::SubmissionQueue.to_string()),
            ),
        }
    }
}
