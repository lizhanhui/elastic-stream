use std::rc::Rc;

use async_channel::Sender;
use bytes::{BufMut, BytesMut};
use codec::frame::{Frame, OperationCode};
use slog::{debug, warn, Logger};
use store::store::elastic::ElasticStore;

pub struct ServerCall {
    pub(crate) request: Frame,
    pub(crate) sender: Sender<Frame>,
    pub(crate) logger: Logger,
    pub(crate) store: Rc<ElasticStore>,
}

impl ServerCall {
    pub async fn call(&mut self) {
        match self.request.operation_code {
            OperationCode::Unknown => {}
            OperationCode::Ping => {
                debug!(
                    self.logger,
                    "Request[stream-id={}] received", self.request.stream_id
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
            OperationCode::GoAway => {}
        }
    }
}
