use bytes::Bytes;
use local_sync::oneshot::Sender;
use pin_project::pin_project;

use crate::error::StoreError;

use self::put::PutResult;

mod get;
pub mod put;
mod scan;

#[pin_project]
pub struct Put<Op> {
    #[pin]
    pub(crate) inner: Op,
}

pub struct Get {}

pub struct Scan {}

pub struct AppendRecordRequest {
    pub buf: Bytes,
    pub sender: Sender<Result<PutResult, StoreError>>,
}

pub enum Command {
    Append(AppendRecordRequest),
}
