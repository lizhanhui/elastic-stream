use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::error::PutError;

use super::Put;
use futures::Future;

#[derive(Debug)]
pub struct PutResult {}

impl<Op> Future for Put<Op>
where
    Op: Future<Output = Result<PutResult, PutError>>,
{
    type Output = <Op as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => {
                // Convert res to desired type.
                Poll::Ready(res)
            }
        }

        /*
        match rx.await {
                Ok(result) => match result {
                    Ok(_append) => {
                        debug!(self.logger, "Append OK");
                        let mut header = BytesMut::new();
                        let text = format!("stream-id={}, response=true", self.request.stream_id);
                        header.put(text.as_bytes());
                        let response = Frame {
                            operation_code: OperationCode::Publish,
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
                                    "PublishResponse[stream-id={}] transferred to channel",
                                    self.request.stream_id
                                );
                            }
                            Err(e) => {
                                error!(
                                    self.logger,
                                    "Failed to transfer publish-response to channel. Cause: {:?}",
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        error!(self.logger, "Failed to append {:?}", e);
                    }
                },
                Err(e) => {
                    error!(self.logger, "Failed to receive for StoreCommand {:?}", e);
                }
            }
        */
    }
}
