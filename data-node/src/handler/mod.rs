//! Server-side handlers, processors for requests of each kind.
//!
//! See details docs for each operation code

use async_channel::Sender;
use bytes::{BufMut, BytesMut};
use codec::frame::{Frame, OperationCode};
use slog::{debug, trace, warn, Logger};
use std::rc::Rc;
use store::{elastic::ElasticStore, option::WriteOptions, Record, Store};

/// Representation of the incoming request.
///
///
pub struct ServerCall {
    /// The incoming request
    pub(crate) request: Frame,

    /// Sender for future response
    ///
    /// Note the receiver part is polled by `ChannelWriter` in a spawned task.
    pub(crate) sender: Sender<Frame>,

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

        response.flag_response();

        match self.request.operation_code {
            OperationCode::Unknown => {}
            OperationCode::Ping => {
                response.operation_code = OperationCode::Ping;
                self.on_ping(&mut response).await
            }
            OperationCode::GoAway => {
                response.operation_code = OperationCode::GoAway;
            }
            OperationCode::Publish => {
                response.operation_code = OperationCode::Publish;
                self.on_publish(&mut response).await;
            }
            OperationCode::Heartbeat => {
                response.operation_code = OperationCode::Heartbeat;
            }
            OperationCode::ListRange => {
                response.operation_code = OperationCode::Heartbeat;
            }
        };

        // Send response to channel.
        // Note there is a spawned task, in which channel writer is polling the channel.
        // Once the response is received, it would immediately get written to network.
        match self.sender.send(response).await {
            Ok(_) => {
                debug!(
                    self.logger,
                    "Response[stream-id={}] transferred to channel", self.request.stream_id
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

    /// Process Ping request
    ///
    /// Ping-pong mechanism is designed to be a light weight API to probe liveness of data-node.
    async fn on_ping(&self, response: &mut Frame) {
        debug!(
            self.logger,
            "PingRequest[stream-id={}] received", self.request.stream_id
        );
        let mut header = BytesMut::new();
        let text = format!("stream-id={}, response=true", self.request.stream_id);
        header.put(text.as_bytes());
        response.header = Some(header.freeze());
    }

    /// Process message publishment request
    ///
    /// On receiving a message publishment request, it wraps the incoming request to a `Record`.
    /// The record is then moved to `Store::put`, where persistence, replication and other auxillary
    /// operations are properly performed.
    ///
    /// Once the underlying operations are completed, the `Store#put` API shall asynchronously return
    /// `Result<PutResult, PutError>`. The result will be rendered into the `response`.
    async fn on_publish(&self, response: &mut Frame) {
        let options = WriteOptions::default();
        let record = self.build_proof_of_concept_record();
        match self.store.put(options, record).await {
            Ok(_append_result) => {}
            Err(_e) => {}
        };
    }

    /// Build proof of concept record.
    /// 
    /// TODO:
    /// 1. Check metadata cache to see if there is a writable range for the targeting partition;
    /// 2. If step-1 returns None, query placement manager;
    /// 3. Ensure current data-node is the leader of the writable range;
    /// 4. If 
    fn build_proof_of_concept_record(&self) -> Record {
        let mut buffer = bytes::BytesMut::new();

        if let Ok(_) = self.request.encode(&mut buffer) {
            trace!(self.logger, "PoC: header section");
        }

        if let Some(body) = &self.request.payload {
            buffer.extend_from_slice(body);
        }

        Record {
            buffer: buffer.freeze(),
        }
    }
}
