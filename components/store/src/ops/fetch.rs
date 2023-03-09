use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Future;

use super::Fetch;
use crate::error::ReadError;

#[derive(Debug)]
pub struct FetchResult {
    pub stream_id: i64,
    pub offset: i64,
    pub payload: bytes::Bytes,
}

impl<Op> Future for Fetch<Op>
where
    Op: Future<Output = Result<FetchResult, ReadError>>,
{
    type Output = Result<FetchResult, ReadError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => {
                // Convert res to desired type.
                Poll::Ready(res)
            }
        }
    }
}
