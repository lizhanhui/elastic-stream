use std::error::Error;

use bytes::BytesMut;
use front_end_sdk::Writer;
use model::Record;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let access_point = "localhost:80";
    let writer = Writer::new(access_point);

    let body = BytesMut::with_capacity(128).freeze();
    let record = Record::new_builder()
        .with_stream_id(3)
        .with_body(body)
        .build()?;

    match writer.append(&record).await {
        Ok(receipt) => {
            println!("Send record OK {receipt:#?}")
        }
        Err(e) => {
            eprintln!("Failed to send record. Cause {e:?}");
        }
    }

    Ok(())
}
