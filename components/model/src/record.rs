use crate::{error::RecordError, header::Headers};
use bytes::Bytes;
use std::{collections::HashMap, error::Error, time::Instant};

#[derive(Debug, Clone)]
pub struct Record {
    headers: Headers,
    properties: HashMap<String, String>,
    body: Bytes,
}

impl Record {
    fn new(headers: Headers, body: Bytes) -> Self {
        Self {
            headers,
            properties: HashMap::new(),
            body,
        }
    }

    pub fn new_builder() -> RecordBuilder {
        RecordBuilder::default()
    }

    pub fn topic(&self) -> Option<&str> {
        self.headers.topic()
    }

    pub fn partition(&self) -> Option<i32> {
        self.headers.partition()
    }

    pub fn key(&self) -> Option<&str> {
        self.headers.key()
    }

    pub fn offset(&self) -> Option<i64> {
        self.headers.offset()
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn add_property(&mut self, key: String, value: String) -> Option<String> {
        self.headers.add_property(key, value)
    }

    pub fn body(&self) -> Bytes {
        self.body.clone()
    }
}

#[derive(Debug, Default)]
pub struct RecordBuilder {
    topic: Option<String>,
    body: Option<Bytes>,
    partition: Option<i32>,
}

impl RecordBuilder {
    pub fn with_topic(mut self, topic: String) -> Self {
        self.topic = Some(topic);
        self
    }

    pub fn with_body(mut self, body: Bytes) -> Self {
        self.body = Some(body);
        self
    }

    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn build(self) -> Result<Record, Box<dyn Error>> {
        let topic = self.topic.ok_or(RecordError::RequiredFieldMissing)?;
        let body = self.body.ok_or(RecordError::RequiredFieldMissing)?;
        let partition = self.partition.ok_or(RecordError::RequiredFieldMissing)?;
        let header = Headers::new(topic, partition);
        Ok(Record::new(header, body))
    }
}

#[derive(Debug)]
pub struct RecordReceipt {
    /// The partition the record was saved into
    pub partition: i32,

    /// The offset of the record in the topic/partition.
    pub offset: i64,

    /// Timestamp when the record was received and persisted by `ElasticStore`.
    pub timestamp: Instant,
}

impl Default for RecordReceipt {
    fn default() -> Self {
        Self {
            partition: -1,
            offset: -1,
            timestamp: Instant::now(),
        }
    }
}

impl RecordReceipt {
    pub fn new(partition: i32, offset: i64) -> Self {
        Self {
            partition,
            offset,
            ..Default::default()
        }
    }
}
