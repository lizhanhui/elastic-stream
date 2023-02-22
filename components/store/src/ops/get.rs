use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Future;

use super::Get;
use crate::{error::ReadError, AppendRecordRequest};

impl Future for Get {
    type Output = Result<AppendRecordRequest, ReadError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}
