use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::error::AppendError;

use super::Append;
use futures::Future;

#[derive(Debug)]
pub struct AppendResult {
    pub(crate) stream_id: i64,
    pub(crate) offset: i64,
}

impl<Op> Future for Append<Op>
where
    Op: Future<Output = Result<AppendResult, AppendError>>,
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
    }
}
