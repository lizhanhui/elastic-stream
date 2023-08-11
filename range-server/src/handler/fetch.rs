use super::util::{root_as_rpc_request, MIN_BUFFER_SIZE};
use crate::range_manager::RangeManager;
use codec::frame::Frame;
use flatbuffers::FlatBufferBuilder;
use log::{trace, warn};
use minstant::Instant;
use protocol::rpc::header::{
    ErrorCode, FetchRequest, FetchResponse, FetchResponseArgs, FetchResponseT, ObjectMetadataT,
    Status, StatusArgs, StatusT,
};
use std::{cell::UnsafeCell, fmt, rc::Rc};
use store::{error::FetchError, option::ReadOptions, Store};

#[derive(Debug)]
pub(crate) struct Fetch<'a> {
    /// The append request already parsed by flatbuffers
    fetch_request: FetchRequest<'a>,
}

impl<'a> Fetch<'a> {
    pub(crate) fn parse_frame(request: &Frame) -> Result<Fetch, ErrorCode> {
        let request_buf = match request.header {
            Some(ref buf) => buf,
            None => {
                warn!(
                    "FetchRequest[stream-id={}] received without payload",
                    request.stream_id
                );
                return Err(ErrorCode::BAD_REQUEST);
            }
        };

        let fetch_request = match root_as_rpc_request::<FetchRequest>(request_buf) {
            Ok(request) => request,
            Err(e) => {
                warn!(
                    "FetchRequest[stream-id={}] received with invalid payload. Cause: {:?}",
                    request.stream_id, e
                );
                return Err(ErrorCode::BAD_REQUEST);
            }
        };
        trace!("Received {fetch_request:?}");
        Ok(Fetch { fetch_request })
    }

    /// Apply the fetch requests to the store
    pub(crate) async fn apply<S, M>(
        &self,
        store: Rc<S>,
        range_manager: Rc<UnsafeCell<M>>,
        response: &mut Frame,
    ) where
        S: Store,
        M: RangeManager,
    {
        let mut builder = FlatBufferBuilder::with_capacity(MIN_BUFFER_SIZE);

        let mut option = match self.build_read_opt(unsafe { &mut *range_manager.get() }) {
            Ok(opt) => opt,
            Err(_e) => {
                Self::handle_fetch_error(_e, &mut builder, response);
                return;
            }
        };

        let (objects, cover_all) = unsafe { &*range_manager.get() }
            .get_objects(
                option.stream_id as u64,
                option.range,
                option.offset as u64,
                option.max_offset,
                option.max_bytes as u32,
            )
            .await;
        read_option(&mut option, cover_all);

        let payload = if option.offset as u64 >= option.max_offset || option.max_bytes == 0 {
            None
        } else {
            let start = Instant::now();
            match store.fetch(option).await {
                Ok(fetch_result) => {
                    trace!(
                        "Fetch records from store took {:?}us",
                        start.elapsed().as_micros()
                    );
                    let buffers = fetch_result
                        .results
                        .into_iter()
                        .flat_map(|i| i.into_iter())
                        .collect::<Vec<_>>();
                    Some(buffers)
                }
                Err(e) => {
                    Self::handle_fetch_error(e, &mut builder, response);
                    return;
                }
            }
        };
        let status_args = StatusArgs {
            code: ErrorCode::OK,
            ..Default::default()
        };
        let status = Status::create(&mut builder, &status_args);

        let objects = objects
            .into_iter()
            .map(|o| Into::<ObjectMetadataT>::into(o).pack(&mut builder))
            .collect::<Vec<_>>();
        let objects = builder.create_vector(&objects);

        let fetch_response_args = FetchResponseArgs {
            status: Some(status),
            throttle_time_ms: -1,
            object_metadata_list: Some(objects),
        };
        let fetch_response = FetchResponse::create(&mut builder, &fetch_response_args);
        builder.finish(fetch_response, None);
        let data = builder.finished_data();
        response.header = Some(bytes::Bytes::copy_from_slice(data));
        response.payload = payload;
    }

    fn handle_fetch_error(
        e: FetchError,
        builder: &mut FlatBufferBuilder<'_>,
        response: &mut Frame,
    ) {
        let mut fetch_response = FetchResponseT::default();
        let mut status = StatusT::default();
        Self::convert_store_error(&e, &mut status);
        fetch_response.status = Box::new(status);
        let fetch_response = fetch_response.pack(builder);
        builder.finish(fetch_response, None);
        let data = builder.finished_data();
        response.header = Some(bytes::Bytes::copy_from_slice(data));
    }

    fn build_read_opt<M>(&self, range_manager: &M) -> Result<ReadOptions, FetchError>
    where
        M: RangeManager,
    {
        // Retrieve stream id from req.range
        let stream_id = self.fetch_request.range().stream_id();
        let range_index = self.fetch_request.range().index();
        let offset = self.fetch_request.offset();
        let limit = self.fetch_request.limit();

        // If the stream-range exists and contains the requested offset, build the read options
        if range_manager.has_range(stream_id as u64, range_index as u32) {
            Ok(ReadOptions {
                stream_id,
                range: range_index as u32,
                offset,
                max_offset: limit as u64,
                max_wait_ms: self.fetch_request.max_wait_ms(),
                max_bytes: self.fetch_request.max_bytes(),
            })
        } else {
            Err(FetchError::RangeNotFound)
        }
    }

    fn convert_store_error(err: &FetchError, status: &mut StatusT) {
        status.code = match err {
            FetchError::SubmissionQueue => ErrorCode::RS_INTERNAL_SERVER_ERROR,
            FetchError::ChannelRecv => ErrorCode::RS_INTERNAL_SERVER_ERROR,
            FetchError::TranslateIndex => ErrorCode::RS_INTERNAL_SERVER_ERROR,
            FetchError::NoRecord => ErrorCode::NO_NEW_RECORD,
            FetchError::RangeNotFound => ErrorCode::RANGE_NOT_FOUND,
            FetchError::BadRequest => ErrorCode::BAD_REQUEST,
            FetchError::DataCorrupted => ErrorCode::RS_DATA_CORRUPTED,
        };
        status.message = Some(err.to_string());
    }
}

impl<'a> fmt::Display for Fetch<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Fetch[{:?}]", self.fetch_request)
    }
}

#[cfg(not(feature = "object-first"))]
fn read_option(_option: &mut ReadOptions, _objects_cover_all: bool) {}

#[cfg(feature = "object-first")]
fn read_option(option: &mut ReadOptions, objects_cover_all: bool) {
    if objects_cover_all {
        option.max_bytes = 0;
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        metadata::MockMetadataManager,
        range_manager::{
            fetcher::MockPlacementFetcher, manager::DefaultRangeManager, RangeManager,
        },
    };
    use codec::frame::Frame;
    use model::stream::StreamMetadata;
    use object_storage::MockObjectStorage;
    use protocol::rpc::header::{ErrorCode, FetchRequestT, FetchResponse, OperationCode, RangeT};
    use std::{cell::UnsafeCell, error::Error, rc::Rc, sync::Arc};
    use store::error::FetchError;
    use tokio::sync::mpsc;

    fn build_fetch_request() -> Frame {
        let mut request = Frame::new(OperationCode::FETCH);
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let mut fetch_request = FetchRequestT::default();
        let mut range = RangeT::default();
        range.stream_id = 1;
        range.index = 0;
        range.start = 0;
        range.end = 100;
        fetch_request.range = Box::new(range);
        fetch_request.offset = 0;
        fetch_request.limit = 100;
        fetch_request.max_wait_ms = 1000;
        let fetch_request = fetch_request.pack(&mut builder);
        builder.finish(fetch_request, None);
        let data = builder.finished_data();
        request.header = Some(bytes::Bytes::copy_from_slice(data));
        request
    }

    #[monoio::test]
    async fn test_fetch_no_new_record() -> Result<(), Box<dyn Error>> {
        let mut mock_store = store::MockStore::new();
        mock_store
            .expect_get_range_end_offset()
            .returning(|_, _| Ok(Some(100)));
        mock_store.expect_id().return_const(0);
        mock_store
            .expect_fetch()
            .returning(|_| Err(FetchError::NoRecord));

        mock_store
            .expect_config()
            .returning(|| Arc::new(config::Configuration::default()));

        let mut mock_fetcher = MockPlacementFetcher::new();
        mock_fetcher.expect_bootstrap().returning(|_| {
            let range = model::range::RangeMetadata::new(1, 0, 0, 0, Some(100));
            Ok(vec![range])
        });

        mock_fetcher.expect_describe_stream().returning(|_| {
            Ok(StreamMetadata {
                stream_id: Some(1),
                replica: 3,
                ack_count: 2,
                retention_period: std::time::Duration::from_secs(1),
                start_offset: 0,
            })
        });
        let mut object_storage = MockObjectStorage::new();
        object_storage
            .expect_get_objects()
            .returning(|_, _, _, _, _| (vec![], false));

        let mut metadata_manager = MockMetadataManager::default();
        metadata_manager.expect_start().return_const(());
        metadata_manager.expect_watch().returning(|| {
            let (_tx, rx) = mpsc::unbounded_channel();
            Ok(rx)
        });

        let store = Rc::new(mock_store);
        let range_manager = Rc::new(UnsafeCell::new(DefaultRangeManager::new(
            mock_fetcher,
            Rc::clone(&store),
            Rc::new(object_storage),
            metadata_manager,
        )));

        let sm = unsafe { &mut *range_manager.get() };
        sm.start().await.unwrap();

        let request = build_fetch_request();
        let mut response = Frame::new(OperationCode::FETCH);
        let handler = super::Fetch::parse_frame(&request).expect("Failed to parse request frame");
        handler
            .apply(Rc::clone(&store), range_manager, &mut response)
            .await;
        let header = response.header.unwrap();
        let fetch_response = flatbuffers::root::<FetchResponse>(&header).unwrap();
        let status = fetch_response.status();
        assert_eq!(status.code(), ErrorCode::NO_NEW_RECORD);
        Ok(())
    }
}
