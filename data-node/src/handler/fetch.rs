use bytes::Bytes;
use codec::frame::Frame;

use flatbuffers::FlatBufferBuilder;
use futures::future::join_all;
use protocol::rpc::header::{
    ErrorCode, FetchRequest, FetchResponseArgs, FetchResultArgs, StatusArgs,
};
use slog::{warn, Logger};
use std::{cell::RefCell, rc::Rc};
use store::{error::FetchError, option::ReadOptions, ElasticStore, Store, FetchResult};

use crate::workspace::stream_manager::StreamManager;

use super::util::{finish_response_builder, root_as_rpc_request, MIN_BUFFER_SIZE};

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
                return Err(ErrorCode::BAD_REQUEST);
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
                return Err(ErrorCode::BAD_REQUEST);
            }
        };

        Ok(Fetch {
            logger,
            fetch_request,
        })
    }

    /// Apply the fetch requests to the store
    pub(crate) async fn apply(
        &self,
        store: Rc<ElasticStore>,
        stream_manager: Rc<RefCell<StreamManager>>,
        response: &mut Frame,
    ) {
        let store_requests = self.build_store_requests();
        let futures: Vec<_> = store_requests
            .into_iter()
            .map(|fetch_option| store.fetch(fetch_option))
            .collect();

        let res_from_store: Vec<Result<FetchResult, FetchError>> = join_all(futures).await;

        let mut builder = FlatBufferBuilder::with_capacity(MIN_BUFFER_SIZE);
        let mut payloads = Vec::new();
        let no_err_status = protocol::rpc::header::Status::create(
            &mut builder,
            &StatusArgs {
                code: ErrorCode::OK,
                message: None,
                detail: None,
            },
        );
        let fetch_results: Vec<_> = res_from_store
            .into_iter()
            .map(|res| {
                match res {
                    Ok(fetch_result) => {
                        let fetch_result_args = FetchResultArgs {
                            stream_id: fetch_result.stream_id,
                            batch_length: fetch_result.payload.len() as i32,
                            // TODO: Fill the request index
                            request_index: 0,
                            status: Some(no_err_status),
                        };
                        payloads.push(fetch_result.payload);
                        protocol::rpc::header::FetchResult::create(&mut builder, &fetch_result_args)
                    }
                    Err(e) => {
                        warn!(self.logger, "Failed to fetch from store. Cause: {:?}", e);

                        let (err_code, err_message) = self.convert_store_error(&e);
                        let mut err_message_fb = None;
                        if let Some(err_message) = err_message {
                            err_message_fb = Some(builder.create_string(err_message.as_str()));
                        }
                        let status = protocol::rpc::header::Status::create(
                            &mut builder,
                            &StatusArgs {
                                code: err_code,
                                message: err_message_fb,
                                detail: None,
                            },
                        );
                        let fetch_result_args = FetchResultArgs {
                            stream_id: 0,
                            batch_length: 0,
                            request_index: 0,
                            status: Some(status),
                        };
                        protocol::rpc::header::FetchResult::create(&mut builder, &fetch_result_args)
                    }
                }
            })
            .collect();

        let fetch_results_fb = builder.create_vector(&fetch_results);
        let res_args = FetchResponseArgs {
            throttle_time_ms: 0,
            fetch_responses: Some(fetch_results_fb),
            status: Some(no_err_status),
        };
        let res_offset = protocol::rpc::header::FetchResponse::create(&mut builder, &res_args);
        let res_header = finish_response_builder(&mut builder, res_offset);
        response.header = Some(res_header);

        // Flatten the payloads, since we already identified the sequence of the payloads
        let payloads: Vec<_> = payloads
            .into_iter()
            .flatten()
            // Skip the 8 bytes of the storage prefix
            // TODO: Find a efficient way to avoid copying the payload
            .map(|payload| Bytes::copy_from_slice(&payload[8..]))
            .collect();
        response.payload = Some(payloads);
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

    fn convert_store_error(&self, err: &FetchError) -> (ErrorCode, Option<String>) {
        (ErrorCode::PM_INTERNAL_SERVER_ERROR, Some(err.to_string()))
    }
}
