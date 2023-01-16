use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;

use crate::Record;

use super::Scan;

impl Stream for Scan {
    type Item = Record;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
