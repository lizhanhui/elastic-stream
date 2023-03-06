//! Server-side handlers, processors for requests of each kind.
//!
//! See details docs for each operation code

use bytes::{BufMut, Bytes, BytesMut};
use codec::frame::{Frame, OperationCode};
use flatbuffers::{FlatBufferBuilder, Verifiable, WIPOffset};

use protocol::rpc::header::{
    DescribeRangeResult, DescribeRangeResultArgs, DescribeRangesRequest, DescribeRangesResponse,
    DescribeRangesResponseArgs, ErrorCode,
};
use slog::{debug, trace, warn, Logger};
use std::rc::Rc;
use store::{
    ops::append::AppendResult, option::WriteOptions, AppendRecordRequest, ElasticStore, Store,
};

const MIN_BUFFER_SIZE: usize = 64;
const MEDIUM_BUFFER_SIZE: usize = 4 * MIN_BUFFER_SIZE;
const LARGE_BUFFER_SIZE: usize = 16 * MEDIUM_BUFFER_SIZE;

/// Representation of the incoming request.
///
///
pub struct ServerCall {
    /// The incoming request
    pub(crate) request: Frame,

    /// Sender for future response
    ///
    /// Note the receiver part is polled by `ChannelWriter` in a spawned task.
    pub(crate) sender: tokio::sync::mpsc::UnboundedSender<Frame>,

    /// Logger
    pub(crate) logger: Logger,

    /// `Store` to query, persist and replicate records.
    ///
    /// Note this store is `!Send` as it follows thread-per-core pattern.
    pub(crate) store: Rc<ElasticStore>,
}

impl ServerCall {
    /// Serve the incoming request
    ///
    /// Delegate each incoming request to its dedicated `on_xxx` method according to
    /// operation code.
    pub async fn call(&mut self) {
        let mut response = Frame::new(OperationCode::Unknown);
        response.stream_id = self.request.stream_id;

        // Flag it's a response frame, as well as the end of the response.
        // If the response sequence is not ended, please note reset the flag in the subsequent logic.
        response.flag_end_response();

        match self.request.operation_code {
            OperationCode::Unknown => {}
            OperationCode::Ping => {
                response.operation_code = OperationCode::Ping;
                self.on_ping(&mut response).await
            }
            OperationCode::GoAway => {
                response.operation_code = OperationCode::GoAway;
            }
            OperationCode::Append => {
                response.operation_code = OperationCode::Append;
                self.on_publish(&mut response).await;
            }
            OperationCode::Heartbeat => {
                response.operation_code = OperationCode::Heartbeat;
            }
            OperationCode::ListRanges => {
                response.operation_code = OperationCode::Heartbeat;
            }
            OperationCode::Fetch => todo!(),
            OperationCode::SealRanges => todo!(),
            OperationCode::SyncRanges => todo!(),
            OperationCode::DescribeRanges => {
                response.operation_code = OperationCode::DescribeRanges;
                self.on_describe_ranges(&mut response).await;
            }
            OperationCode::CreateStreams => todo!(),
            OperationCode::DeleteStreams => todo!(),
            OperationCode::UpdateStreams => todo!(),
            OperationCode::DescribeStreams => todo!(),
            OperationCode::TrimStreams => todo!(),
            OperationCode::ReportMetrics => todo!(),
        };

        // Send response to channel.
        // Note there is a spawned task, in which channel writer is polling the channel.
        // Once the response is received, it would immediately get written to network.
        match self.sender.send(response) {
            Ok(_) => {
                trace!(
                    self.logger,
                    "Response[stream-id={}] transferred to channel",
                    self.request.stream_id
                );
            }
            Err(e) => {
                warn!(
                    self.logger,
                    "Failed to send response[stream-id={}] to channel. Cause: {:?}",
                    self.request.stream_id,
                    e
                );
            }
        };
    }

    async fn on_describe_streams(&self, response: &mut Frame) {
        todo!()
    }

    async fn on_describe_ranges(&self, response: &mut Frame) {
        let response_builder = &mut FlatBufferBuilder::with_capacity(64);
        let mut args = DescribeRangesResponseArgs {
            throttle_time_ms: 0,
            describe_responses: None,
            error_code: ErrorCode::NONE,
            error_message: None,
        };

        let request_buf = match self.request.header {
            Some(ref buf) => buf,
            None => {
                args.error_code = ErrorCode::INVALID_REQUEST;
                args.error_message = Some(response_builder.create_string("No request header"));
                warn!(
                    self.logger,
                    "DescribeRangesRequest[stream-id={}] received without payload",
                    self.request.stream_id
                );

                // Serialize response to the FlatBuffer
                // The returned value is an offset used to track the location of this serializaed response.
                let response_offset = DescribeRangesResponse::create(response_builder, &args);
                response.header = finish_response_builder(response_builder, response_offset);
                return;
            }
        };

        let describe_requst = match root_as_rpc_request::<DescribeRangesRequest>(request_buf) {
            Ok(request) => request,
            Err(e) => {
                args.error_code = ErrorCode::INVALID_REQUEST;
                args.error_message = Some(response_builder.create_string("Invalid request header"));
                warn!(
                    self.logger,
                    "DescribeRangesRequest[stream-id={}] received with invalid payload. Cause: {:?}",
                    self.request.stream_id,
                    e
                );
                let response_offset = DescribeRangesResponse::create(response_builder, &args);
                response.payload = finish_response_builder(response_builder, response_offset);
                return;
            }
        };

        // TODO: Get the range from store
        let response_offset = DescribeRangesResponse::create(response_builder, &args);
        response.header = finish_response_builder(response_builder, response_offset);
    }

    /// Process Ping request
    ///
    /// Ping-pong mechanism is designed to be a light weight API to probe liveness of data-node.
    /// The Pong response return the same header and payload as the Ping request.
    async fn on_ping(&self, response: &mut Frame) {
        debug!(
            self.logger,
            "PingRequest[stream-id={}] received", self.request.stream_id
        );
        response.header = self.request.header.clone();
        response.payload = self.request.payload.clone();
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
    async fn on_publish(&self, _response: &mut Frame) {
        let options = WriteOptions::default();
        let record = self.build_proof_of_concept_record();
        match self.store.append(options, record).await {
            Ok(_result) => {
                // response.header = self.build_publish_response_header(&result);
            }
            Err(_e) => {}
        };
    }

    /// Build frame header according to `PutResult` with FlatBuffers encoding.
    ///
    /// `_result` - PutResult from underlying `Store`
    // fn build_publish_response_header(&self, _result: &AppendResult) -> Option<Bytes> {
    //     let mut builder = FlatBufferBuilder::with_capacity(256);
    //     let status = Status::create(
    //         &mut builder,
    //         &StatusArgs {
    //             code: Code::OK,
    //             message: None,
    //             nodes: None,
    //         },
    //     );

    //     let topic = builder.create_string("topic");
    //     let metadata = RecordMetadata::create(
    //         &mut builder,
    //         &RecordMetadataArgs {
    //             offset: 0,
    //             partition: 0,
    //             serialized_key_size: 0,
    //             serialized_value_size: 0,
    //             timestamp: 0,
    //             topic: Some(topic),
    //         },
    //     );

    //     let response_header = PublishRecordResponseHeader::create(
    //         &mut builder,
    //         &PublishRecordResponseHeaderArgs {
    //             status: Some(status),
    //             metadata: Some(metadata),
    //         },
    //     );

    //     builder.finish(response_header, None);
    //     let header_data = builder.finished_data();
    //     let mut header = BytesMut::with_capacity(header_data.len());
    //     // TODO: dig if memory copy here can be avoided...say moving finished data from flatbuffer builder to bytes::Bytes
    //     header.extend_from_slice(header_data);
    //     Some(header.into())
    // }

    /// Build proof of concept record.
    ///
    /// TODO:
    /// 1. Check metadata cache to see if there is a writable range for the targeting partition;
    /// 2. If step-1 returns None, query placement manager;
    /// 3. Ensure current data-node is the leader of the writable range;
    /// 4. If
    fn build_proof_of_concept_record(&self) -> AppendRecordRequest {
        let mut buffer = bytes::BytesMut::new();

        if self.request.encode(&mut buffer).is_ok() {
            trace!(self.logger, "PoC: header section");
        }

        if let Some(body) = &self.request.payload {
            buffer.extend_from_slice(body);
        }

        AppendRecordRequest {
            stream_id: 0,
            offset: 0,
            buffer: buffer.freeze(),
        }
    }
}

/// Verifies that a buffer of bytes contains a rpc request and returns it.
fn root_as_rpc_request<'a, R>(buf: &'a [u8]) -> Result<R, flatbuffers::InvalidFlatbuffer>
where
    R: flatbuffers::Follow<'a, Inner = R> + 'a + Verifiable,
{
    flatbuffers::root::<R>(buf)
}

/// Finish the response builder and returns the finished response frame header.
/// Use a generic type to support different response types,
/// and ensure the finished_data is called after the builder.finish().
fn finish_response_builder<R>(
    builder: &mut FlatBufferBuilder,
    response_offset: WIPOffset<R>,
) -> Option<Bytes> {
    builder.finish(response_offset, None);
    Some(Bytes::copy_from_slice(builder.finished_data()))
}
