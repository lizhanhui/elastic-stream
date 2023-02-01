use std::{
    error::Error,
    net::ToSocketAddrs,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Sink;
use model::{error::ProducerError, Record, RecordReceipt};
use tokio::sync::oneshot;

pub struct Producer {}

impl Producer {
    /// Create a new `Producer` instance to send records to `ElasticStore`.
    ///
    ///
    pub fn new<A>(_addr: A) -> Self
    where
        A: ToSocketAddrs,
    {
        Self {}
    }

    pub async fn send(&self, _record: &Record) -> Result<RecordReceipt, Box<dyn Error>> {
        Ok(RecordReceipt::new(0, 0))
    }
}
