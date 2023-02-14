use crate::{error::RecordError, header::Headers};
use bytes::Bytes;
use std::{collections::HashMap, error::Error, time::Instant};

// The below definitions of Record and BatchRecord are inspired by Kafka's protocols
#[derive(Debug, Clone)]
pub struct Record {
    length: i32,
    offset_delta: i32,
    timestamp_delta: i32,
    headers: Headers,
    properties: HashMap<String, String>,
    body: Bytes,
}

pub struct BatchRecord {
    base_offset: i64,
    last_offset_delta: i32,
    batch_length: i32,
    magic: i8,
    crc: i32,
    first_timestamp: i64,
    last_timestamp: i64,
    attributes: i8,
    records: Vec<Record>,
}

impl Record {
    fn new(headers: Headers, body: Bytes) -> Self {
        Self {
            headers,
            properties: HashMap::new(),
            body,
            length: 0,
            offset_delta: 0,
            timestamp_delta: 0,
        }
    }

    pub fn new_builder() -> RecordBuilder {
        RecordBuilder::default()
    }

    pub fn key(&self) -> Option<&str> {
        self.headers.key()
    }

    pub fn stream(&self) -> Option<i32> {
        self.headers.stream()
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
    body: Option<Bytes>,
    stream: Option<i32>,
}

impl RecordBuilder {
    pub fn with_body(mut self, body: Bytes) -> Self {
        self.body = Some(body);
        self
    }

    pub fn with_stream(mut self, partition: i32) -> Self {
        self.stream = Some(partition);
        self
    }

    pub fn build(self) -> Result<Record, Box<dyn Error>> {
        let body = self.body.ok_or(RecordError::RequiredFieldMissing)?;
        let stream = self.stream.ok_or(RecordError::RequiredFieldMissing)?;
        let header = Headers::new(stream);
        Ok(Record::new(header, body))
    }
}

#[derive(Debug)]
pub struct RecordMetadata {
    /// The stream the record was saved into
    pub stream: i32,

    /// The offset of the record in the stream.
    pub offset: i64,

    /// Timestamp when the record was received and persisted by `ElasticStore`.
    pub timestamp: Instant,
}

impl Default for RecordMetadata {
    fn default() -> Self {
        Self {
            stream: -1,
            offset: -1,
            timestamp: Instant::now(),
        }
    }
}

impl RecordMetadata {
    pub fn new(stream: i32, offset: i64) -> Self {
        Self {
            stream,
            offset,
            ..Default::default()
        }
    }
}
