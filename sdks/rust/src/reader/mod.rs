use std::{
    error::Error,
    net::ToSocketAddrs,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use model::Record;

/// Reader to access records stored in partitions.
///
///
/// # Examples
///
/// ```
/// use std::error::Error;
/// use front_end_sdk::{Reader, Whence};
/// use futures::StreamExt;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn Error>>  {
///    let access_point = "localhost:80";
///    let partition = 1;
///    let consumer = Reader::new(access_point);
///   
///    let mut cursor = consumer.open(partition).await?;
///    cursor.seek(3, Whence::SeekSet);
///    while let Some(record) = cursor.next().await {
///        println!("Got a record {record:#?}");
///    }
///    Ok(())
/// }
/// ```
pub struct Reader {}

impl Reader {
    /// Returns a `Reader` connecting to the given address.
    ///
    /// * `addr` - Access point exposed to application developers
    ///
    pub fn new<A>(_addr: A) -> Self
    where
        A: ToSocketAddrs,
    {
        Self {}
    }

    /// Opens a `Cursor` to the specified partition.
    ///
    /// Returns `Ok(Cursor)` on success, an error otherwise.
    ///
    /// * `_partition_id` - Partition identifier
    ///
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

/// A cursor, similar to Linux file descriptor, represents an active and ongoing access to `Partition`.
///
/// `Cursor` provides `seek` method to re-position location to read.
///
/// On drop, the cursor, along with assocated resources, shall be properly closed or released.  
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
            .with_stream_id(3)
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
