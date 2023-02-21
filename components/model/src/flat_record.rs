use std::str::FromStr;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use chrono::Utc;
use flatbuffers::FlatBufferBuilder;
use protocol::flat_model::{
    KeyValue, KeyValueArgs, RecordBatchMeta, RecordBatchMetaArgs, RecordMeta, RecordMetaArgs,
};

use crate::{error::DecodeError, header::Common, Record, RecordBatch};

enum RecordMagic {
    Magic0 = 0, // The first version of the record batch format.
}

/// FlatRecordBatch is a flattened version of RecordBatch that is used for serialization.
/// As the storage and network layout, the schema of FlatRecordBatch with magic 0 is given below:
///
/// RecordBatch =>
///  TotalLen => Int32
///  Magic => Int8
///  Checksum => Int32
///  RecordsCount => Int32
///  MetaLength => Int32
///  Meta => RecordBatchMeta
///  Records => [Record]
///
/// The record scheam is given below:
/// Record =>
///   MetaLength => Int32
///   BodyLength => Int32
///   Meta => RecordMeta
///   Body => Bytes
///
/// The RecordMeta and RecordBatchMeta are under the layout of the flatbuffers schema, other contents are mananged by ourselves.
#[derive(Debug, Default)]
struct FlatRecordBatch {
    magic: Option<i8>,
    checksum: Option<i32>,
    meta_buffer: Bytes,
    records: Vec<FlatRecord>,
}

impl FlatRecordBatch {
    /// Converts a RecordBatch to a FlatRecordBatch.
    pub fn init_from_struct(record_batch: RecordBatch) -> Self {
        // Build up a serialized buffer for the specfic record_batch.
        // Initialize it with a capacity of 1024 bytes.
        let mut builder = FlatBufferBuilder::with_capacity(1024);

        // Make a RecordBatchMeta
        let args = RecordBatchMetaArgs {
            stream_id: record_batch.stream_id(),
            base_timestamp: record_batch.base_timestamp(),
            // TODO: add other meta fields
            ..Default::default()
        };

        // Serialize the data to the FlatBuffer
        // The returned value is an offset used to track the location of this serializaed data.
        let record_batch_meta_offset = RecordBatchMeta::create(&mut builder, &args);

        // Serialize the root of the object, without providing a file identifier.
        builder.finish(record_batch_meta_offset, None);

        let result_buf = builder.finished_data();

        let mut flat_record_batch = FlatRecordBatch {
            magic: Some(RecordMagic::Magic0 as i8),
            checksum: None, // TODO: calculate the checksum later
            meta_buffer: Bytes::copy_from_slice(result_buf),
            records: Vec::new(),
        };

        let base_timestamp = record_batch.base_timestamp();

        for (index, record) in record_batch.take_records().into_iter().enumerate() {
            // Reuse the builder
            builder.reset();

            let mut headers = Vec::new();
            let mut properties = Vec::new();
            for (key, value) in record.headers_iter() {
                let args = KeyValueArgs {
                    key: Some(builder.create_string(&key.to_string())),
                    value: Some(builder.create_string(value)),
                };
                headers.push(KeyValue::create(&mut builder, &args));
            }

            for (key, value) in record.properties_iter() {
                let args = KeyValueArgs {
                    key: Some(builder.create_string(key)),
                    value: Some(builder.create_string(value)),
                };
                properties.push(KeyValue::create(&mut builder, &args));
            }

            let now = Utc::now().timestamp();
            let record_timestamp = record
                .created_at()
                .map_or(now, |ts| ts.parse().unwrap_or(now));

            let args = RecordMetaArgs {
                offset_delta: index as i32,
                timestamp_delta: (record_timestamp - base_timestamp) as i32,
                headers: Some(builder.create_vector(headers.as_slice())),
                properties: Some(builder.create_vector(properties.as_slice())),
            };

            let record_meta_offset = RecordMeta::create(&mut builder, &args);
            builder.finish(record_meta_offset, None);
            let result_buf = builder.finished_data();

            let flat_record = FlatRecord {
                meta_buffer: Bytes::copy_from_slice(result_buf),
                body: record.take_body(),
            };
            flat_record_batch.records.push(flat_record);
        }

        flat_record_batch
    }

    /// Inits a FlatRecordBatch from a buffer of bytes received from storage or network layer.
    /// TODO: Handle the error case.
    pub fn init_from_buf(mut buf: Bytes) -> Result<Self, DecodeError> {
        if buf.len() < 4 {
            return Err(DecodeError::DataLengthMismatch)
        }
        // Backup the buffer for the slice operation.
        let buf_for_slice = buf.slice(0..);

        let mut cur_ptr = 0;
        // Read the total length
        let total_len = buf.get_i32();
        if total_len as usize - 4 != buf.len() {
            return Err(DecodeError::DataLengthMismatch)
        }
        // Read the magic
        let magic = buf.get_i8();
        // Read the checksum
        let checksum = buf.get_i32();
        // Read the records count
        let records_count = buf.get_i32();
        // Read the meta length
        let meta_len = buf.get_i32();

        cur_ptr += 1 + 4 + 4 + 4 + 4;
        // Read the meta buffer
        let meta_buffer = buf_for_slice.slice(cur_ptr..(cur_ptr + meta_len as usize));

        buf.advance(meta_len as usize);
        cur_ptr = cur_ptr + meta_len as usize;

        let mut flat_record_batch = FlatRecordBatch {
            magic: Some(magic),
            checksum: Some(checksum),
            meta_buffer,
            records: Vec::new(),
        };

        for _ in 0..records_count {
            // Read the meta length
            let meta_len = buf.get_i32();

            // Read the body length
            let body_len = buf.get_i32();

            cur_ptr += 4 + 4;
            // Read the meta buffer
            let meta_buffer = buf_for_slice.slice(cur_ptr..(cur_ptr + meta_len as usize));
            buf.advance(meta_len as usize);
            cur_ptr += meta_len as usize;

            // Read the body buffer
            let body = buf_for_slice.slice(cur_ptr..(cur_ptr + body_len as usize));
            buf.advance(body_len as usize);
            cur_ptr += body_len as usize;

            let flat_record = FlatRecord { meta_buffer, body };
            flat_record_batch.records.push(flat_record);
        }

        Ok(flat_record_batch)
    }

    pub fn encode(self) -> Vec<Bytes> {
        let mut bytes_vec = Vec::new();
        let mut total_len = 0;
        let meta_len = self.meta_buffer.len();
        let records_count = self.records.len();
        // Store the Magic to MetaLength
        bytes_vec.push(self.meta_buffer);
        total_len += meta_len;

        for record in self.records {
            let mut meta_part = BytesMut::with_capacity(8);
            meta_part.put_i32(record.meta_buffer.len() as i32);
            meta_part.put_i32(record.body.len() as i32);

            total_len += 8 + record.meta_buffer.len() + record.body.len();
            bytes_vec.push(meta_part.freeze());
            bytes_vec.push(record.meta_buffer);
            bytes_vec.push(record.body);
        }

        total_len += 17;
        let mut basic_part = BytesMut::with_capacity(17);
        basic_part.put_i32(total_len as i32);
        basic_part.put_i8(self.magic.unwrap_or(0));
        basic_part.put_i32(self.checksum.unwrap_or(0));
        basic_part.put_i32(records_count as i32);
        basic_part.put_i32(meta_len as i32);

        bytes_vec.insert(0, basic_part.freeze());

        bytes_vec
    }

    pub fn decode(self) -> Result<RecordBatch, DecodeError> {
        // TODO: Validate the checksum and do decode according to the magic
        let mut record_batch_builder = RecordBatch::new_builder();

        let batch_meta = root_as_record_batch_meta_unchecked(self.meta_buffer.as_ref());
        let stream_id = batch_meta.stream_id();
        record_batch_builder = record_batch_builder.with_stream_id(stream_id);

        for flat_record in self.records {
            let record_meta = root_as_record_meta_unchecked(flat_record.meta_buffer.as_ref());
            let mut record = Record::new_builder()
                .with_stream_id(stream_id)
                .with_body(flat_record.body)
                .build()?;

            if let Some(properties) = record_meta.properties() {
                for property in properties {
                    if let Some(key) = property.key() {
                        if let Some(value) = property.value() {
                            record.add_property(key.to_string(), value.to_string());
                        }
                    }
                }
            }

            if let Some(headers) = record_meta.headers() {
                for header in headers {
                    if let (Some(key), Some(value)) = (header.key(), header.value()) {
                        if let Ok(header_key) = Common::from_str(key) {
                            record.add_header(header_key, value.to_string());
                        }
                    }
                }
            }
            record_batch_builder = record_batch_builder.add_record(record);
        }
        let record_batch = record_batch_builder.build()?;
        Ok(record_batch)
    }
}

/// FlatRecord is a flattened version of Record that is used for serialization and zero deserialization.
#[derive(Debug, Default)]
struct FlatRecord {
    meta_buffer: Bytes,
    body: Bytes,
}

/// Verifies that a buffer of bytes contains a `RecordBatchMeta` and returns it.
/// For this unchecked behavior to be maximally peformant, use unchecked function
fn root_as_record_batch_meta(
    buf: &[u8],
) -> Result<RecordBatchMeta, flatbuffers::InvalidFlatbuffer> {
    flatbuffers::root::<RecordBatchMeta>(buf)
}

/// Assumes, without verification, that a buffer of bytes contains a RecordBatchMeta and returns it.
/// # Safety
/// Callers must trust the given bytes do indeed contain a valid `RecordBatchMeta`.
fn root_as_record_batch_meta_unchecked(buf: &[u8]) -> RecordBatchMeta {
    unsafe { flatbuffers::root_unchecked::<RecordBatchMeta>(buf) }
}

/// Verifies that a buffer of bytes contains a `RecordMeta` and returns it.
/// For this unchecked behavior to be maximally peformant, use unchecked function
fn root_as_record_meta(buf: &[u8]) -> Result<RecordMeta, flatbuffers::InvalidFlatbuffer> {
    flatbuffers::root::<RecordMeta>(buf)
}

/// Assumes, without verification, that a buffer of bytes contains a RecordMeta and returns it.
/// # Safety
/// Callers must trust the given bytes do indeed contain a valid `RecordMeta`.
fn root_as_record_meta_unchecked(buf: &[u8]) -> RecordMeta {
    unsafe { flatbuffers::root_unchecked::<RecordMeta>(buf) }
}

#[cfg(test)]
mod tests {
    use crate::record::{RecordBatchBuilder, RecordBuilder};

    use super::*;

    #[test]
    fn test_encode_and_decode_of_flat_batch() {
        let stream_id = 1 as i64;
        let mut record1 = RecordBuilder::default()
            .with_stream_id(stream_id)
            .with_body(Bytes::from("hello"))
            .with_keys("foo bar".to_string())
            .with_tag("baz".to_string())
            .with_record_id("123".to_string())
            .build()
            .unwrap();
        record1.add_property("my_property_key".to_string(), "my_property_value".to_string());
        record1.add_property("my_another_property_key".to_string(), "my_another_property_value".to_string());  
        let base_timestamp = record1.created_at().unwrap().parse::<i64>().unwrap();
        let mut record2 = RecordBuilder::default()
            .with_stream_id(stream_id)
            .with_body(Bytes::from("world"))
            .with_keys("foo bar".to_string())
            .with_tag("baz".to_string())
            .with_record_id("234".to_string())
            .build()
            .unwrap();
        record2.add_property("my_property_key".to_string(), "my_property_value".to_string());  
        let batch = RecordBatchBuilder::default()
            .with_stream_id(stream_id)
            .add_record(record1)
            .add_record(record2)
            .build()
            .unwrap();

        // Init a FlatRecordBatch from original RecordBatch
        let flat_batch = FlatRecordBatch::init_from_struct(batch);

        assert_eq!(flat_batch.magic, Some(0));
        assert_eq!(flat_batch.records.len(), 2);
        assert_eq!(flat_batch.records[0].body, Bytes::from("hello"));
        assert_eq!(flat_batch.records[1].body, Bytes::from("world"));

        // Encode the above flat_batch to Vec[Bytes]
        let bytes_vec = flat_batch.encode();
        let mut bytes_mute = BytesMut::with_capacity(1024);
        for ele in bytes_vec {
            bytes_mute.put_slice(&ele);
        }

        // Decode the above bytes to FlatRecordBatch
        let flat_batch = FlatRecordBatch::init_from_buf(bytes_mute.freeze()).unwrap();
        let record_batch = flat_batch.decode().unwrap();

        assert_eq!(record_batch.stream_id(), stream_id);
        assert_eq!(record_batch.base_timestamp(), base_timestamp);
        assert_eq!(record_batch.records().len(), 2);
        assert_eq!(record_batch.records()[0].body(), &Bytes::from("hello"));
        assert_eq!(record_batch.records()[1].body(), &Bytes::from("world"));

        // Test the properties
        let mut properties: Vec<(&String, &String)> = record_batch.records()[0].properties_iter().collect();
        assert_eq!(properties.len(), 2);
        properties.sort_by(|a, b| a.0.cmp(b.0));
        assert_eq!(properties[1].0, "my_property_key");
        assert_eq!(properties[1].1, "my_property_value");
        assert_eq!(properties[0].0, "my_another_property_key");
        assert_eq!(properties[0].1, "my_another_property_value");

        // Test the headers
        assert_eq!(record_batch.records()[0].tag(), Some(&"baz".to_string()));
        assert_eq!(record_batch.records()[1].tag(), Some(&"baz".to_string()));
        assert_eq!(record_batch.records()[0].record_id(), Some(&"123".to_string()));
        assert_eq!(record_batch.records()[1].record_id(), Some(&"234".to_string()));
        assert_eq!(record_batch.records()[0].keys(), Some(&"foo bar".to_string()));
        assert_eq!(record_batch.records()[1].keys(), Some(&"foo bar".to_string()));
    }
}
