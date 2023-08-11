//! Server-side handlers, processors for requests of each kind.
//!
//! See details docs for each operation code
mod append;
mod cmd;
mod create_range;
mod fetch;
mod go_away;
mod heartbeat;
mod ping;
mod seal_range;
mod util;
use self::cmd::Command;
use crate::{
    metrics::{APPEND_LATENCY, FETCH_LATENCY},
    range_manager::RangeManager,
};
use bytes::Bytes;
use codec::frame::Frame;
use local_sync::mpsc;
use log::{trace, warn};
use protocol::rpc::header::{StatusT, SystemErrorT};
use std::{cell::UnsafeCell, rc::Rc};
use store::Store;

/// Representation of the incoming request.
///
///
pub(crate) struct ServerCall<S, M> {
    /// The incoming request
    pub(crate) request: Frame,

    /// Sender for future response
    ///
    /// Note the receiver part is polled by `ChannelWriter` in a spawned task.
    pub(crate) sender: mpsc::unbounded::Tx<Frame>,

    /// `Store` to query, persist and replicate records.
    ///
    /// Note this store is `!Send` as it follows thread-per-core pattern.
    pub(crate) store: Rc<S>,

    pub(crate) range_manager: Rc<UnsafeCell<M>>,
}

impl<S, M> ServerCall<S, M>
where
    S: Store,
    M: RangeManager,
{
    /// Serve the incoming request
    ///
    /// Delegate each incoming request to its dedicated `on_xxx` method according to
    /// operation code.
    pub async fn call(&mut self) {
        trace!(
            "Receive a request. stream-id={}, opcode={}",
            self.request.stream_id,
            self.request
                .operation_code
                .variant_name()
                .unwrap_or("INVALID_OPCODE")
        );
        let now = std::time::Instant::now();
        let mut response = Frame::new(self.request.operation_code);
        response.stream_id = self.request.stream_id;

        // Flag it's a response frame, as well as the end of the response.
        // If the response sequence is not ended, please note reset the flag in the subsequent logic.
        response.flag_end_of_response_stream();

        match Command::from_frame(&self.request) {
            Ok(cmd) => {
                // Log the `cmd` object.
                trace!(
                    "Command of frame[stream-id={}]: {}",
                    self.request.stream_id,
                    cmd
                );

                // Delegate the request to its dedicated handler.
                cmd.apply(
                    Rc::clone(&self.store),
                    Rc::clone(&self.range_manager),
                    &mut response,
                )
                .await;

                match cmd {
                    Command::Append(_) => {
                        APPEND_LATENCY.observe(now.elapsed().as_micros() as f64);
                    }
                    Command::Fetch(_) => {
                        FETCH_LATENCY.observe(now.elapsed().as_micros() as f64);
                    }
                    _ => {}
                }
                trace!("Response frame generated. stream-id={}", response.stream_id);
            }
            Err(e) => {
                response.flag_system_err();
                let mut system_error = SystemErrorT::default();
                let mut status = StatusT::default();
                status.code = e;
                system_error.status = Box::new(status);
                let mut builder = flatbuffers::FlatBufferBuilder::new();
                let header = system_error.pack(&mut builder);
                builder.finish(header, None);
                let data = builder.finished_data();
                response.header = Some(Bytes::copy_from_slice(data));
            }
        };

        // Send response to channel.
        // Note there is a spawned task, in which channel writer is polling the channel.
        // Once the response is received, it would immediately get written to network.
        match self.sender.send(response) {
            Ok(_) => {
                trace!(
                    "Response[stream-id={}] transferred to channel",
                    self.request.stream_id
                );
            }
            Err(_e) => {
                warn!(
                    "Failed to send response[stream-id={}] to channel because rx of mpsc channel has been closed",
                    self.request.stream_id
                );
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::UnsafeCell, rc::Rc};

    use codec::frame::Frame;
    use local_sync::mpsc;
    use protocol::rpc::header::{ErrorCode, OperationCode, SystemError};
    use store::MockStore;

    use crate::range_manager::MockRangeManager;

    use super::ServerCall;

    #[monoio::test]
    async fn test_call() {
        let store = MockStore::default();
        let range_manager = MockRangeManager::default();
        let (tx, mut rx) = mpsc::unbounded::channel();

        let request = Frame::new(OperationCode::PING);

        let mut server_call = ServerCall {
            request,
            sender: tx,
            store: Rc::new(store),
            range_manager: Rc::new(UnsafeCell::new(range_manager)),
        };

        server_call.call().await;
        match rx.recv().await {
            Some(resp) => {
                assert_eq!(resp.operation_code, OperationCode::PING);
            }
            None => {
                panic!("Should get a response frame");
            }
        }
    }

    #[monoio::test]
    async fn test_call_when_error() {
        let store = MockStore::default();
        let range_manager = MockRangeManager::default();
        let (tx, mut rx) = mpsc::unbounded::channel();

        let request = Frame::new(OperationCode::CREATE_RANGE);

        let mut server_call = ServerCall {
            request,
            sender: tx,
            store: Rc::new(store),
            range_manager: Rc::new(UnsafeCell::new(range_manager)),
        };

        server_call.call().await;
        match rx.recv().await {
            Some(resp) => {
                assert_eq!(resp.operation_code, OperationCode::CREATE_RANGE);
                assert!(resp.system_error());
                if let Some(ref buf) = resp.header {
                    let sys_error = flatbuffers::root::<SystemError>(buf).unwrap();
                    assert_eq!(sys_error.status().code(), ErrorCode::BAD_REQUEST);
                }
            }
            None => {
                panic!("Should get a response frame");
            }
        }
    }
}
