use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;

use crate::AppendRecordRequest;

use super::Scan;

impl Stream for Scan {
    type Item = AppendRecordRequest;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
