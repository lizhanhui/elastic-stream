use std::{
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Future, Stream};
use local_sync::{mpsc::bounded::Tx, oneshot::Sender};
use thiserror::Error;

use crate::error::StoreError;

#[derive(Debug)]
pub struct AppendResult {}

#[derive(Debug, Default)]
pub struct WriteOptions {}

#[derive(Debug, Default)]
pub struct ReadOptions {}

#[derive(Debug, Error)]
pub enum AppendError {}

#[derive(Debug, Error)]
pub enum ReadError {}

pub struct Record {}

pub struct PutFuture {}

impl Future for PutFuture {
    type Output = Result<AppendResult, AppendError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(AppendResult {}))
    }
}

pub struct GetFuture {}

impl Future for GetFuture {
    type Output = Result<Record, ReadError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}

pub struct TailingStream {}

impl Stream for TailingStream {
    type Item = Record;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

pub trait Store {
    fn put(&self, options: WriteOptions, record: Record) -> PutFuture;

    fn get(&self, options: ReadOptions, paritition_id: u64, offset: u64) -> GetFuture;

    fn tail(&self, options: ReadOptions, paritition_id: u64, start: u64) -> TailingStream;
}

pub struct AppendRecordRequest {
    pub buf: Bytes,
    pub sender: Sender<Result<AppendResult, StoreError>>,
}

pub enum Command {
    Append(AppendRecordRequest),
}

pub trait Segment {}

pub struct Cursor {
    written: u64,
    committed: u64,
}

impl Cursor {
    pub fn new() -> Self {
        Self {
            written: 0,
            committed: 0,
        }
    }

    pub fn alloc(&mut self, len: u64) -> u64 {
        let current = self.written;
        self.written += len;
        current
    }

    pub fn committed(&self) -> u64 {
        self.committed
    }

    pub fn commit(&mut self, pos: u64, len: u64) -> bool {
        if self.committed == pos {
            self.committed += len;
            return true;
        }
        false
    }
}
