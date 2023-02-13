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
    }
}
