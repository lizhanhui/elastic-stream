use std::{error::Error, net::ToSocketAddrs};

use model::{error::RecordError, Record, RecordMetadata};

/// Writes `Record`s to `Stream`s.
///
///# Examples
/// ```
/// use std::error::Error;
/// use bytes::BytesMut;
/// use front_end_sdk::Writer;
/// use model::Record;
///
/// #[tokio::main]
/// pub async fn main() -> Result<(), Box<dyn Error>> {
///     let access_point = "localhost:80";
///     let writer = Writer::new(access_point);
///
///     let body = BytesMut::with_capacity(128).freeze();
///     let record = Record::new_builder()
///         .with_stream_name("test_stream_name")
///         .with_body(body)
///         .build()?;
///
///     match writer.append(&record).await {
///         Ok(receipt) => {
///             println!("Send record OK {receipt:#?}")
///         }
///         Err(e) => {
///             eprintln!("Failed to send record. Cause {e:?}");
///         }
///     }
///
///     Ok(())
/// }
/// ```
pub struct Writer {}

impl Writer {
    /// Create a new `Writer` instance to append records to `Partition`s.
    ///
    ///
    pub fn new<A>(_addr: A) -> Self
    where
        A: ToSocketAddrs,
    {
        Self {}
    }

    /// Append the specified record to partition.
    ///
    /// * `record` - A `Record` that contains headers, properties and body.
    ///
    pub async fn append(&self, record: &Record) -> Result<RecordMetadata, Box<dyn Error>> {
        Ok(RecordMetadata::new(
            record.stream_name(),
            0,
        ))
    }
}
