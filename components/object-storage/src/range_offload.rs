use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use config::ObjectStorageConfig;
use log::debug;
use log::warn;
use mockall::lazy_static;
use model::error::DecodeError;
use model::object::gen_footer;
use model::object::gen_object_key;
use model::object::BLOCK_DELIMITER;
use model::record::flat_record::RecordMagic;
use monoio::task::JoinHandle;
use observation::metrics::object_metrics;
use opendal::Operator;
use opendal::Writer;
use protocol::flat_model::records::RecordBatchMeta;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use std::time::Instant;
use std::vec::Vec;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Semaphore;
use tokio::sync::SemaphorePermit;
use tokio::time::sleep;
use util::bytes::vec_bytes_to_bytes;

use crate::ObjectManager;
use model::object::ObjectMetadata;

const SPARSE_SIZE: u32 = 16 * 1024 * 1024;
lazy_static! {
    static ref OBJECT_WRITE_LIMITER: Semaphore = Semaphore::new(100);
}

/// Object format:
///   data block            => bytes
///   delimiter magic       => u8
///   sparse index *
///     relative end offset => u32
///     position            => u32
///   footer                => fix size, 48 bytes
///     sparse index pos    => u32
///     sparse index size   => u32
///     padding
///     magic               => u64
///
///
pub struct RangeOffload<M: ObjectManager + 'static> {
    stream_id: u64,
    range_index: u32,
    epoch: u16,
    multi_part_object: RefCell<Option<MultiPartObject>>,
    op: Operator,
    object_manager: Rc<M>,
    cluster: String,
    object_size: u32,
}

impl<M> RangeOffload<M>
where
    M: ObjectManager + 'static,
{
    pub fn new(
        stream_id: u64,
        range_index: u32,
        epoch: u16,
        op: Operator,
        object_manager: Rc<M>,
        config: &ObjectStorageConfig,
    ) -> RangeOffload<M> {
        let cluster = config.cluster.clone();
        let object_size = config.object_size;
        Self {
            stream_id,
            range_index,
            epoch,
            multi_part_object: RefCell::new(None),
            op,
            object_manager,
            cluster,
            object_size,
        }
    }

    pub async fn write(&self, start_offset: u64, payload: Vec<Bytes>) -> u64 {
        let payload_length: usize = payload.iter().map(Bytes::len).sum();
        let payload_length = payload_length as u32;
        let key = gen_object_key(
            &self.cluster,
            self.stream_id,
            self.range_index,
            self.epoch,
            start_offset,
        );

        {
            let mut multi_part_object = self.multi_part_object.borrow_mut();
            if let Some(multi_part_obj) = multi_part_object.as_ref() {
                // the last multi-part object exist, then write to it.
                let (object_full, end_offset) = multi_part_obj.write(payload);
                if object_full {
                    multi_part_obj.close(None);
                    *multi_part_object = None;
                }
                return end_offset;
            }
        }

        if payload_length >= self.object_size {
            let object_metadata =
                ObjectMetadata::new(self.stream_id, self.range_index, self.epoch, start_offset);
            // direct write when payload length is larger than object_size.
            let object = Object {
                key,
                op: self.op.clone(),
                object_manager: self.object_manager.clone(),
            };
            let (end_offset, _) = object.write(payload, object_metadata).await;
            return end_offset;
        }

        // start a new multi-part object.
        let permit = OBJECT_WRITE_LIMITER.acquire().await.unwrap();
        let object_metadata =
            ObjectMetadata::new(self.stream_id, self.range_index, self.epoch, start_offset);
        let new_multi_part_object = MultiPartObject::new(
            object_metadata,
            key,
            self.op.clone(),
            self.object_manager.clone(),
            self.object_size,
            permit,
        );
        let (_, end_offset) = new_multi_part_object.write(payload);
        *self.multi_part_object.borrow_mut() = Some(new_multi_part_object);
        end_offset
    }

    /// force inflight multi-part object to complete.
    pub fn trigger_flush(&self) {
        if let Some(multi_part_object) = self.multi_part_object.take() {
            multi_part_object.close(None);
        }
    }

    pub async fn flush(&self) {
        let (tx, rx) = oneshot::channel::<()>();
        if let Some(multi_part_object) = self.multi_part_object.take() {
            multi_part_object.close(Some(tx));
            let _ = rx.await;
        }
    }
}

struct MultiPartObject {
    start_offset: u64,
    object_size: u32,
    size: RefCell<u32>,
    last_pass_through_size: RefCell<u32>,
    tx: mpsc::UnboundedSender<MultiPartWriteEvent>,
}

impl MultiPartObject {
    pub fn new<M: ObjectManager + 'static>(
        object_metadata: ObjectMetadata,
        key: String,
        op: Operator,
        object_manager: Rc<M>,
        object_size: u32,
        permit: SemaphorePermit<'static>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<MultiPartWriteEvent>();
        let this = Self {
            start_offset: object_metadata.start_offset,
            object_size,
            size: RefCell::new(0),
            last_pass_through_size: RefCell::new(0),
            tx,
        };
        {
            let key = key;
            let op = op;
            Self::write_loop(object_metadata, key, op, object_manager, rx, permit);
        }
        this
    }

    pub fn write(&self, part: Vec<Bytes>) -> (bool, u64) {
        let mut size = self.size.borrow_mut();
        let part_length: usize = part.iter().map(Bytes::len).sum();
        *size += part_length as u32;

        let (index, new_end_offset, remain_pass_through_size) = gen_sparse_index(
            self.start_offset,
            &part,
            *self.last_pass_through_size.borrow(),
            SPARSE_SIZE,
        )
        .unwrap_or_else(|_| panic!("parse record fail {part:?}"));
        *self.last_pass_through_size.borrow_mut() = remain_pass_through_size;
        let _ = self.tx.send(MultiPartWriteEvent {
            data: part,
            index,
            end_offset: new_end_offset,
            tx: None,
        });
        (*size >= self.object_size, new_end_offset)
    }

    pub fn close(&self, tx: Option<oneshot::Sender<()>>) {
        let _ = self.tx.send(MultiPartWriteEvent {
            data: Vec::new(),
            index: Bytes::new(),
            end_offset: 0,
            tx,
        });
    }

    fn write_loop<M: ObjectManager + 'static>(
        mut object_metadata: ObjectMetadata,
        key: String,
        op: Operator,
        object_manager: Rc<M>,
        mut rx: mpsc::UnboundedReceiver<MultiPartWriteEvent>,
        permit: SemaphorePermit<'static>,
    ) {
        monoio::spawn(async move {
            // TODO: delay init multi-part object, if there is only one part or several parts is too small. the multi-part object is not necessary.
            // use single Object put instead to save API call.
            let mut data_len = 0;
            let mut end_offset = object_metadata.start_offset;
            let mut sparse_index = BytesMut::with_capacity(64);
            let mut writer = None;
            loop {
                match rx.recv().await {
                    Some(event) => {
                        let data = event.data;
                        let index = event.index;
                        let new_end_offset = event.end_offset;
                        let tx = event.tx;
                        sparse_index.extend(index);
                        if new_end_offset > end_offset {
                            end_offset = new_end_offset;
                        }
                        if writer.is_none() {
                            loop {
                                writer = match op.writer(&key).await {
                                    Ok(w) => Some(w),
                                    Err(e) => {
                                        warn!("{key} get writer fail, retry later, {e}");
                                        sleep(Duration::from_secs(1)).await;
                                        continue;
                                    }
                                };
                                break;
                            }
                        }
                        let writer = writer.as_mut().unwrap();

                        // write part or complete object.
                        let payload_length: usize = data.iter().map(Bytes::len).sum();
                        data_len += payload_length;
                        let mut bytes = BytesMut::with_capacity(payload_length);
                        for b in &data {
                            bytes.extend_from_slice(b);
                        }
                        let bytes = bytes.freeze();
                        let close_part = bytes.is_empty();
                        if close_part {
                            // write index and footer.
                            let mut left_part = BytesMut::with_capacity(256);
                            left_part.put_u8(BLOCK_DELIMITER);
                            let index_len = sparse_index.len();
                            left_part.extend_from_slice(&sparse_index);
                            left_part
                                .extend_from_slice(&gen_footer(data_len as u32, index_len as u32));
                            let left_part = left_part.freeze();
                            Self::write_part(writer, &key, &left_part).await;

                            // complete multi-part object.
                            Self::complete_object(writer, &key).await;

                            object_metadata.end_offset_delta =
                                (end_offset - object_metadata.start_offset) as u32;
                            object_metadata.data_len = data_len as u32;
                            commit_object(&object_manager, object_metadata).await;

                            if let Some(tx) = tx {
                                let _ = tx.send(());
                            }
                            drop(permit);
                            return;
                        }
                        Self::write_part(writer, &key, &bytes).await;
                        if let Some(tx) = tx {
                            let _ = tx.send(());
                        }
                    }
                    None => {
                        todo!("handle rx close")
                    }
                }
            }
        });
    }

    async fn write_part(writer: &mut Writer, key: &str, bytes: &Bytes) {
        loop {
            let start = Instant::now();
            match writer.write(bytes.clone()).await {
                Ok(_) => {
                    object_metrics::multi_part_object_write(bytes.len() as u32, start.elapsed());
                    debug!(
                        "{key} write multi-part object part[len={}] success",
                        bytes.len()
                    );
                    break;
                }
                Err(e) => {
                    warn!("{key} write part fail, retry later, {e}");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
    }

    async fn complete_object(writer: &mut Writer, key: &str) {
        loop {
            match writer.close().await {
                Ok(_) => {
                    object_metrics::multi_part_object_complete();
                    debug!("{key} complete multi-part object success");
                    break;
                }
                Err(e) => {
                    warn!("{key} complete multi-part object fail, retry later, {e}");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
    }
}

struct MultiPartWriteEvent {
    data: Vec<Bytes>,
    index: Bytes,
    end_offset: u64,
    tx: Option<oneshot::Sender<()>>,
}

struct Object<M: ObjectManager + 'static> {
    key: String,
    op: Operator,
    object_manager: Rc<M>,
}

impl<M> Object<M>
where
    M: ObjectManager + 'static,
{
    pub async fn write(
        &self,
        payload: Vec<Bytes>,
        object_metadata: ObjectMetadata,
    ) -> (u64, JoinHandle<()>) {
        let permit = OBJECT_WRITE_LIMITER.acquire().await.unwrap();
        self.write0(payload, object_metadata, SPARSE_SIZE, permit)
    }

    pub fn write0(
        &self,
        payload: Vec<Bytes>,
        mut object_metadata: ObjectMetadata,
        sparse_size: u32,
        permit: SemaphorePermit<'static>,
    ) -> (u64, JoinHandle<()>) {
        let key = self.key.clone();
        let op = self.op.clone();
        let object_manager = self.object_manager.clone();
        let (sparse_index, end_offset, _) =
            gen_sparse_index(object_metadata.start_offset, &payload, 0, sparse_size)
                .unwrap_or_else(|_| panic!("parse record fail {payload:?}"));
        let join_handle = monoio::spawn(async move {
            object_metadata.end_offset_delta = (end_offset - object_metadata.start_offset) as u32;
            // data block
            let payload_length: usize = payload.iter().map(Bytes::len).sum();
            object_metadata.data_len = payload_length as u32;
            let mut bytes =
                BytesMut::with_capacity(payload_length + 256 /* sparse index + footer */);
            for b in &payload {
                bytes.extend_from_slice(b);
            }
            // delimiter
            bytes.put_u8(BLOCK_DELIMITER);
            // sparse index
            let sparse_index_len = sparse_index.len();
            bytes.extend_from_slice(&sparse_index);
            // footer
            bytes.extend_from_slice(&gen_footer(payload_length as u32, sparse_index_len as u32));
            let bytes = bytes.freeze();
            Self::write_object(&op, &key, &bytes).await;
            commit_object(&object_manager, object_metadata).await;
            // explicit ref permit in async function to force move permit to async block.
            drop(permit);
        });
        (end_offset, join_handle)
    }

    async fn write_object(op: &Operator, key: &str, bytes: &Bytes) {
        loop {
            let data_len = bytes.len();
            debug!("{key} start write object[len={}]", data_len);
            let start = Instant::now();
            match op.write(key, bytes.clone()).await {
                Ok(_) => {
                    debug!("{key} write object success[len={}]", data_len);
                    object_metrics::object_complete(data_len as u32, start.elapsed());
                    break;
                }
                Err(e) => {
                    // retry later until success.
                    warn!("{key} write object, retry later, {e}");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
    }
}

/// sparse index format:
///   (
///   record relative end offset: u32,
///   position: u32,
///   )*
///
/// return (sparse index bytes, record end offset, remain pass through size)
fn gen_sparse_index(
    start_offset: u64,
    payload: &Vec<Bytes>,
    init_pass_through_size: u32,
    sparse_size: u32,
) -> Result<(Bytes, u64, u32), DecodeError> {
    // TODO: refactor this function to avoid copy payload.
    let payload = vec_bytes_to_bytes(payload);
    let mut cursor = &payload[..];
    let mut sparse_index_bytes = BytesMut::with_capacity(payload.len() / sparse_size as usize);
    let mut pass_through_size = init_pass_through_size;
    let mut record_position = 0;
    let mut last_record_size = 0;
    // construct a sparse index after pass through sparse size records.
    loop {
        if !cursor.has_remaining() {
            break;
        }
        if cursor.remaining() < 9 {
            return Err(DecodeError::DataLengthMismatch);
        }
        let magic_code = cursor.get_i8();
        if magic_code != RecordMagic::Magic0 as i8 {
            return Err(DecodeError::InvalidMagic);
        }
        let metadata_len = cursor.get_i32() as usize;
        if cursor.remaining() < metadata_len {
            return Err(DecodeError::DataLengthMismatch);
        }
        record_position += last_record_size;
        let record_sparse_index = pass_through_size >= sparse_size;
        if record_sparse_index {
            let mut metadata_slice = BytesMut::zeroed(metadata_len);
            cursor.copy_to_slice(&mut metadata_slice);
            let metadata = flatbuffers::root::<RecordBatchMeta>(&metadata_slice)?;
            let record_end_offset =
                metadata.base_offset() as u64 + metadata.last_offset_delta() as u64;
            sparse_index_bytes.put_u32((record_end_offset - start_offset) as u32);
            sparse_index_bytes.put_u32(record_position);
        } else {
            cursor.advance(metadata_len);
        }
        let payload_len = cursor.get_i32() as usize;
        if cursor.remaining() < payload_len {
            return Err(DecodeError::DataLengthMismatch);
        }
        cursor.advance(payload_len);
        last_record_size = (9 + metadata_len + payload_len) as u32;
        if record_sparse_index {
            pass_through_size = 0;
        } else {
            pass_through_size += last_record_size;
        }
    }
    // get last record end_offset
    let mut cursor = &payload[..];
    cursor.advance((record_position + 1) as usize);
    let metadata_len = cursor.get_i32() as usize;
    let mut metadata_slice = BytesMut::zeroed(metadata_len);
    cursor.copy_to_slice(&mut metadata_slice);
    let metadata = flatbuffers::root::<RecordBatchMeta>(&metadata_slice)?;
    let end_offset = metadata.base_offset() as u64 + metadata.last_offset_delta() as u64;
    Ok((sparse_index_bytes.freeze(), end_offset, pass_through_size))
}

async fn commit_object<M: ObjectManager>(object_manager: &Rc<M>, object_metadata: ObjectMetadata) {
    loop {
        match object_manager.commit_object(object_metadata.clone()).await {
            Ok(_) => {
                return;
            }
            Err(e) => {
                warn!("commit object {object_metadata:?} fail, {e}");
                sleep(Duration::from_secs(3)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use model::{object::FOOTER_MAGIC, record::flat_record::FlatRecordBatch, RecordBatch};
    use opendal::services::Fs;

    use crate::MockObjectManager;

    use super::*;

    #[test]
    fn test_sparse_index() {
        let mut payload: Vec<Bytes> = Vec::new();
        for i in 0..10 {
            let builder = RecordBatch::new_builder()
                .with_stream_id(1)
                .with_range_index(0)
                .with_base_offset(233 + i * 10)
                .with_last_offset_delta(10)
                .with_payload(Bytes::from("test"));
            let record_batch = builder.build().unwrap();
            let flat: FlatRecordBatch = record_batch.into();
            // encoded size = 69
            let (mut encoded, _) = flat.encode();
            payload.append(&mut encoded);
        }
        let (mut sparse_index, end_offset, remain) =
            gen_sparse_index(233, &payload, 0, 100).unwrap();

        assert_eq!(333, end_offset);
        assert_eq!(69, remain);

        assert_eq!(3 * 8, sparse_index.len());
        // the 3th record
        assert_eq!(30, sparse_index.get_u32());
        assert_eq!(138, sparse_index.get_u32());
        // the 6th record
        assert_eq!(60, sparse_index.get_u32());
        assert_eq!(345, sparse_index.get_u32());
        // the 9th record
        assert_eq!(90, sparse_index.get_u32());
        assert_eq!(552, sparse_index.get_u32());
    }

    #[monoio::test]
    async fn test_object_write() {
        let mut fs_builder = Fs::default();
        fs_builder.root("/tmp/estest/");
        let op = Operator::new(fs_builder).unwrap().finish();

        let builder = RecordBatch::new_builder()
            .with_stream_id(1)
            .with_range_index(0)
            .with_base_offset(233)
            .with_last_offset_delta(10)
            .with_payload(Bytes::from("test"));
        let record_batch = builder.build().unwrap();
        let flat: FlatRecordBatch = record_batch.into();
        // encoded size = 69
        let (encoded, _) = flat.encode();

        let mut object_manager = MockObjectManager::new();
        object_manager
            .expect_commit_object()
            .times(1)
            .returning(|_| Ok(()));

        let obj = Object {
            key: "test_object_write".to_string(),
            op: op.clone(),
            object_manager: Rc::new(object_manager),
        };

        let object_metadata = ObjectMetadata::new(1, 0, 0, 233);
        let (end_offset, join_handle) = obj.write(encoded.clone(), object_metadata).await;
        assert_eq!(243, end_offset);
        join_handle.await;

        // data
        let read_data = op.read("test_object_write").await.unwrap();
        let record_slice = &read_data[0..69];
        let record_batch =
            FlatRecordBatch::decode_to_record_batch(&mut Bytes::copy_from_slice(record_slice))
                .unwrap();
        assert_eq!(1, record_batch.stream_id());
        assert_eq!(0, record_batch.range_index());
        assert_eq!(233, record_batch.base_offset());
        assert_eq!(10, record_batch.last_offset_delta());
        let mut buf = Bytes::copy_from_slice(&read_data[69..]);
        assert_eq!(BLOCK_DELIMITER, buf.get_u8());
        // empty index
        // footer
        assert_eq!(48, buf.remaining());
        assert_eq!(70, buf.get_u32());
        assert_eq!(0, buf.get_u32());
        buf.advance(32);
        assert_eq!(FOOTER_MAGIC, buf.get_u64());
    }
}
