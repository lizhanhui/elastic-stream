use std::rc::Rc;

use async_channel::Sender;
use bytes::{BufMut, BytesMut};
use codec::frame::{Frame, OperationCode};
use local_sync::oneshot;
use slog::{debug, error, warn, Logger};
use store::api::{AppendRecordRequest, Command, Store};

pub struct ServerCall {
    pub(crate) request: Frame,
    pub(crate) sender: Sender<Frame>,
    pub(crate) logger: Logger,
    pub(crate) store: Rc<dyn Store>,
}

impl ServerCall {
    pub async fn call(&mut self) {
        match self.request.operation_code {
            OperationCode::Unknown => {}
            OperationCode::Ping => {
                debug!(
                    self.logger,
                    "PingRequest[stream-id={}] received", self.request.stream_id
                );
                let mut header = BytesMut::new();
                let text = format!("stream-id={}, response=true", self.request.stream_id);
                header.put(text.as_bytes());
                let response = Frame {
                    operation_code: OperationCode::Ping,
                    flag: 1u8,
                    stream_id: self.request.stream_id,
                    header_format: codec::frame::HeaderFormat::FlatBuffer,
                    header: Some(header.freeze()),
                    payload: None,
                };
                match self.sender.send(response).await {
                    Ok(_) => {
                        debug!(
                            self.logger,
                            "PingResponse[stream-id={}] transferred to channel",
                            self.request.stream_id
                        );
                    }
                    Err(e) => {
                        warn!(
                            self.logger,
                            "Failed to send ping-response[stream-id={}] to channel. Cause: {:?}",
                            self.request.stream_id,
                            e
                        );
                    }
                };
            }
            OperationCode::GoAway => {}
            OperationCode::Publish => {
                let sq = self.store.submission_queue();

                let (tx, rx) = oneshot::channel();

                let mut buf = BytesMut::new();
                match self.request.encode(&mut buf) {
                    Ok(_) => {}
                    Err(e) => {
                        error!(self.logger, "Failed to encode {:?}", e);
                    }
                };

                let append_request = AppendRecordRequest {
                    sender: tx,
                    buf: buf.freeze(),
                };

                let command = Command::Append(append_request);

                match sq.send(command).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            self.logger,
                            "Failed to pass AppendCommand to store layer {:?}", e
                        );
                    }
                };

                match rx.await {
                    Ok(result) => match result {
                        Ok(_append) => {
                            debug!(self.logger, "Append OK");
                        }
                        Err(e) => {
                            error!(self.logger, "Failed to append {:?}", e);
                        }
                    },
                    Err(e) => {
                        error!(self.logger, "Failed to receive for StoreCommand {:?}", e);
                    }
                }

                todo!()
            }
        }
    }
}
