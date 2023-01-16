use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Future;

use crate::error::PutError;

use super::Put;

#[derive(Debug)]
pub struct PutResult {}

impl<Op> Future for Put<Op> {
    type Output = Result<PutResult, PutError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(PutResult {}))
    }
}
