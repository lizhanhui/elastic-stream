use crate::{error::RecordError, header::Headers};
use bytes::Bytes;
use std::{collections::HashMap, error::Error, time::Instant};

#[derive(Debug, Clone)]
pub struct Record {
    stream_name: String,
    keys: Option<String>,
    tag: Option<String>,
    record_id: Option<String>,
    created_at: Option<Instant>,
    properties: HashMap<String, String>,
    body: Bytes,
}

pub struct BatchRecord {
    stream_name: String,
    records: Vec<Record>,
}

impl Record {
    fn new(stream_name: String, body: Bytes) -> Self {
        Self {
            stream_name: stream_name,
            properties: HashMap::new(),
            body,
            keys: None,
            tag: None,
            record_id: None,
            created_at: None,
        }
    }

    pub fn new_builder() -> RecordBuilder {
        RecordBuilder::default()
    }

    pub fn stream_name(&self) -> String {
        self.stream_name.clone()
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn add_property(&mut self, key: String, value: String) -> Option<String> {
        self.properties.insert(key, value)
    }

    pub fn body(&self) -> Bytes {
        self.body.clone()
    }
}

#[derive(Debug, Default)]
pub struct RecordBuilder {
    body: Option<Bytes>,
    stream_name: Option<String>,
}

impl RecordBuilder {
    pub fn with_body(mut self, body: Bytes) -> Self {
        self.body = Some(body);
        self
    }

    pub fn with_stream_name(mut self, stream_name: String) -> Self {
        self.stream_name = Some(stream_name);
        self
    }

    pub fn build(self) -> Result<Record, Box<dyn Error>> {
        let body = self.body.ok_or(RecordError::RequiredFieldMissing)?;
        let stream = self.stream_name.ok_or(RecordError::RequiredFieldMissing)?;
        Ok(Record::new(stream, body))
    }
}

#[derive(Debug)]
pub struct RecordMetadata {
    /// The stream the record was saved into
    pub stream_name: String,

    /// The offset of the record in the stream.
    pub offset: i64,

    /// Timestamp when the record was received and persisted by `ElasticStore`.
    pub timestamp: Instant,
}

impl RecordMetadata {
    pub fn new(stream_name: String, offset: i64) -> Self {
        Self {
            stream_name,
            offset,
            timestamp: Instant::now(),
        }
    }
}
