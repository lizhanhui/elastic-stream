//! Server-side handlers, processors for requests of each kind.
//!
//! See details docs for each operation code
use std::rc::Rc;

use bytes::Bytes;
use local_sync::mpsc;
use log::{trace, warn};

use codec::frame::Frame;
use observation::metrics::range_server::{record_append_operation, record_fetch_operation};
use protocol::rpc::header::{StatusT, SystemErrorT};

use crate::range_manager::RangeManager;

use self::cmd::Command;

mod append;
mod cmd;
mod create_range;
mod fetch;
mod go_away;
mod heartbeat;
mod ping;
mod seal_range;
mod util;

/// Representation of the incoming request.
///
///
pub(crate) struct ServerCall<M> {
    /// The incoming request
    pub(crate) request: Frame,

    /// Sender for future response
    ///
    /// Note the receiver part is polled by `ChannelWriter` in a spawned task.
    pub(crate) sender: mpsc::unbounded::Tx<Frame>,

    pub(crate) range_manager: Rc<M>,
}

impl<M> ServerCall<M>
where
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
                cmd.apply(Rc::clone(&self.range_manager), &mut response)
                    .await;

                match cmd {
                    Command::Append(_) => {
                        record_append_operation(now.elapsed().as_micros() as u64);
                    }
                    Command::Fetch(_) => {
                        record_fetch_operation(now.elapsed().as_micros() as u64);
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
    use std::rc::Rc;

    use local_sync::mpsc;

    use codec::frame::Frame;
    use protocol::rpc::header::{ErrorCode, OperationCode, SystemError};

    use crate::range_manager::MockRangeManager;

    use super::ServerCall;

    #[test]
    fn test_call() {
        let range_manager = MockRangeManager::default();
        let (tx, mut rx) = mpsc::unbounded::channel();

        let request = Frame::new(OperationCode::PING);

        let mut server_call = ServerCall {
            request,
            sender: tx,
            range_manager: Rc::new(range_manager),
        };

        tokio_uring::start(async move {
            server_call.call().await;
            match rx.recv().await {
                Some(resp) => {
                    assert_eq!(resp.operation_code, OperationCode::PING);
                }
                None => {
                    panic!("Should get a response frame");
                }
            }
        });
    }

    #[test]
    fn test_call_when_error() {
        let range_manager = MockRangeManager::default();
        let (tx, mut rx) = mpsc::unbounded::channel();

        let request = Frame::new(OperationCode::CREATE_RANGE);

        let mut server_call = ServerCall {
            request,
            sender: tx,
            range_manager: Rc::new(range_manager),
        };

        tokio_uring::start(async move {
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
        });
    }
}
