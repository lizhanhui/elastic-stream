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
pub struct PutResult {}

#[derive(Debug, Default)]
pub struct WriteOptions {}

#[derive(Debug, Default)]
pub struct ReadOptions {}

#[derive(Debug, Error)]
pub enum PutError {
    #[error("Failed to send PutRequest")]
    SubmissionQueue,

    #[error("Recv from oneshot channel failed")]
    ChannelRecv,

    #[error("Internal error")]
    Internal,
}

#[derive(Debug, Error)]
pub enum ReadError {}

pub struct Record {
    pub buffer: bytes::Bytes,
}

pub struct Put {}

impl Future for Put {
    type Output = Result<PutResult, PutError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(PutResult {}))
    }
}

pub struct Get {}

impl Future for Get {
    type Output = Result<Record, ReadError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}

pub struct Tail {}

impl Stream for Tail {
    type Item = Record;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

pub trait Store {
    fn put(&self, options: WriteOptions, record: Record) -> Put;

    fn get(&self, options: ReadOptions, paritition_id: u64, offset: u64) -> Get;

    fn tail(&self, options: ReadOptions, paritition_id: u64, start: u64) -> Tail;
}

pub struct AppendRecordRequest {
    pub buf: Bytes,
    pub sender: Sender<Result<PutResult, StoreError>>,
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
