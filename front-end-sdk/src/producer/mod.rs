use std::error::Error;

use model::{Record, RecordReceipt};

pub struct Producer {}

impl Producer {

    /// Create a new `Producer` instance to send records to `ElasticStore`.
    /// 
    /// 
    pub fn new() -> Self {
        Self {}
    }

    pub async fn send(&self, _record: &Record) -> Result<RecordReceipt, Box<dyn Error>> {
        Ok(RecordReceipt::new(0, 0))
    }
}
