use std::error::Error;

use bytes::BytesMut;
use front_end_sdk::Producer;
use model::Record;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let producer = Producer::new();

    let body = BytesMut::with_capacity(128).freeze();
    let record = Record::new_builder()
        .with_topic(String::from("topic"))
        .with_partition(1)
        .with_body(body)
        .build()?;

    match producer.send(&record).await {
        Ok(receipt) => {
            println!("Send record OK {receipt:#?}")
        }
        Err(e) => {
            eprintln!("Failed to send record. Cause {e:?}");
        }
    }

    Ok(())
}
