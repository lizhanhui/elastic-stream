use std::{
    error::Error,
    net::ToSocketAddrs,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use model::Record;

pub struct Consumer {}

impl Consumer {
    pub fn new<A>(_addr: A) -> Self
    where
        A: ToSocketAddrs,
    {
        Self {}
    }

    pub async fn open(&self, _partition_id: i32) -> Result<Cursor, Box<dyn Error>> {
        Ok(Cursor::new())
    }
}

pub enum Whence {
    /// The offset is set to the cursor.
    SeekSet,

    /// The cursor offset is set to its current position plus offset.
    SeekCurrent,

    /// The cursor offset is set to the end of the corresponding partition plus offset.
    SeekEnd,
}

pub struct Cursor {
    read: usize,
}

impl Cursor {
    fn new() -> Self {
        Self { read: 0 }
    }

    /// Re-position the read cursor, similar to [lseek](https://man7.org/linux/man-pages/man2/lseek.2.html)
    ///
    ///
    pub fn seek(&mut self, _offset: i64, _whence: Whence) {}
}

impl Stream for Cursor {
    type Item = Record;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Mock implementation

        if self.read >= 3 {
            return Poll::Ready(None);
        }
        self.get_mut().read += 1;

        use bytes::BytesMut;
        let body = BytesMut::with_capacity(128).freeze();
        let record = match Record::new_builder()
            .with_topic(String::from("topic"))
            .with_partition(1)
            .with_body(body)
            .build()
        {
            Ok(r) => r,
            Err(_e) => {
                return Poll::Ready(None);
            }
        };

        return Poll::Ready(Some(record));
    }
}
