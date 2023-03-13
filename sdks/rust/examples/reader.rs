use std::error::Error;

use front_end_sdk::{Reader, Whence};
use futures::StreamExt;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let access_point = "localhost:80";
    let stream_id = 1;
    let consumer = Reader::new(access_point);

    let mut cursor = consumer.open(stream_id).await?;
    cursor.seek(3, Whence::SeekSet);
    while let Some(record) = cursor.next().await {
        println!("Got a record {record:#?}");
    }
    Ok(())
}
