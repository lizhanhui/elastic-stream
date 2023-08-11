use core::fmt;
use std::{cell::UnsafeCell, rc::Rc};

use bytes::Bytes;
use codec::frame::Frame;
use log::{error, trace, warn};
use model::range::RangeMetadata;
use protocol::rpc::header::{CreateRangeRequest, ErrorCode, RangeT, SealRangeResponseT, StatusT};
use store::Store;

use crate::range_manager::RangeManager;

use super::util::root_as_rpc_request;

#[derive(Debug)]
pub(crate) struct CreateRange<'a> {
    request: CreateRangeRequest<'a>,
}

impl<'a> CreateRange<'a> {
    pub(crate) fn parse_frame(frame: &'a Frame) -> Result<Self, ErrorCode> {
        let request = frame
            .header
            .as_ref()
            .map(|buf| root_as_rpc_request::<CreateRangeRequest>(buf))
            .ok_or(ErrorCode::BAD_REQUEST)?
            .map_err(|_e| {
                warn!(
                    "Received an invalid create range request[stream-id={}]",
                    frame.stream_id
                );
                ErrorCode::BAD_REQUEST
            })?;

        Ok(Self { request })
    }

    pub(crate) async fn apply<S, M>(
        &self,
        store: Rc<S>,
        range_manager: Rc<UnsafeCell<M>>,
        response: &mut Frame,
    ) where
        S: Store,
        M: RangeManager,
    {
        let request = self.request.unpack();

        let mut create_response = SealRangeResponseT::default();

        let manager = unsafe { &mut *range_manager.get() };

        let range = request.range;
        let range: RangeMetadata = Into::<RangeMetadata>::into(&*range);
        let mut status = StatusT::default();

        // Notify range creation to `RangeManager`
        if let Err(e) = manager.create_range(range.clone()) {
            error!("Failed to create range: {:?}", e);
            // TODO: Map service error to the corresponding error code.
            status.code = ErrorCode::RS_INTERNAL_SERVER_ERROR;
            status.message = Some(format!("Failed to create range: {}", e));
            create_response.status = Box::new(status);
        } else {
            // Notify range creation to `Store`
            if let Err(e) = store.create(range.clone()).await {
                error!("Failed to create-range in store: {}", e.to_string());
                status.code = ErrorCode::RS_CREATE_RANGE;
                status.message = Some(format!("Failed to create range: {}", e));
                create_response.status = Box::new(status);
                self.build_response(response, &create_response);
                return;
            }

            status.code = ErrorCode::OK;
            status.message = Some(String::from("OK"));
            create_response.status = Box::new(status);
            // Write back the range metadata to client so that client may maintain consistent code.
            create_response.range = Some(Box::new(Into::<RangeT>::into(&range)));
            trace!("Created range={:?}", range);
        }

        self.build_response(response, &create_response);
    }

    #[inline]
    fn build_response(&self, frame: &mut Frame, create_response: &SealRangeResponseT) {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let resp = create_response.pack(&mut builder);
        builder.finish(resp, None);
        let data = builder.finished_data();
        frame.header = Some(Bytes::copy_from_slice(data));
    }
}

impl<'a> fmt::Display for CreateRange<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.request)
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::UnsafeCell, rc::Rc};

    use bytes::Bytes;
    use codec::frame::Frame;
    use flatbuffers::FlatBufferBuilder;
    use protocol::rpc::header::{
        CreateRangeRequestT, CreateRangeResponse, ErrorCode, OperationCode, RangeT,
    };
    use store::{error::StoreError, MockStore};

    use crate::{error::ServiceError, range_manager::MockRangeManager};

    fn create_range_request() -> Frame {
        let mut frame = Frame::new(OperationCode::CREATE_RANGE);
        let mut request = CreateRangeRequestT::default();
        let mut range = RangeT::default();
        range.ack_count = 2;
        range.replica_count = 3;
        range.index = 0;
        range.stream_id = 1;
        range.epoch = 0;
        range.start = 0;
        request.range = Box::new(range);
        request.timeout_ms = 200;

        let mut fbb = FlatBufferBuilder::new();
        let req = request.pack(&mut fbb);
        fbb.finish(req, None);
        let data = fbb.finished_data();
        frame.header = Some(Bytes::copy_from_slice(data));
        frame
    }

    #[test]
    fn test_create_range_parse_frame() {
        let frame = create_range_request();
        super::CreateRange::parse_frame(&frame).expect("Parse frame");
    }

    #[test]
    fn test_create_range_parse_frame_bad_request() {
        ulog::try_init_log();
        let mut frame = Frame::new(OperationCode::CREATE_RANGE);
        frame.header = Some(Bytes::from_static(b"abc"));
        match super::CreateRange::parse_frame(&frame) {
            Ok(_) => {
                panic!("Should report bad request");
            }

            Err(ec) => {
                assert_eq!(ec, ErrorCode::BAD_REQUEST);
            }
        }
    }

    #[monoio::test]
    async fn test_create_range_apply() {
        ulog::try_init_log();
        let mut store = MockStore::default();
        let rm = Rc::new(UnsafeCell::new(MockRangeManager::default()));
        let request = create_range_request();
        let handler =
            super::CreateRange::parse_frame(&request).expect("Parse frame should NOT fail");
        let mut response = Frame::new(OperationCode::UNKNOWN);

        let stream_manager = unsafe { &mut *rm.get() };
        stream_manager
            .expect_create_range()
            .once()
            .returning(|_metadata| Ok(()));

        store.expect_create().once().returning(|_metadata| Ok(()));

        let store = Rc::new(store);
        handler.apply(store, rm, &mut response).await;

        if let Some(buf) = &response.header {
            let resp = flatbuffers::root::<CreateRangeResponse>(buf)
                .expect("Should get a valid create-range response");
            assert_eq!(ErrorCode::OK, resp.status().code());
        } else {
            panic!("Create range should not fail");
        }
    }

    #[monoio::test]
    async fn test_create_range_apply_when_range_manager_fails() {
        ulog::try_init_log();
        let store = MockStore::default();
        let rm = Rc::new(UnsafeCell::new(MockRangeManager::default()));
        let request = create_range_request();
        let handler =
            super::CreateRange::parse_frame(&request).expect("Parse frame should NOT fail");
        let mut response = Frame::new(OperationCode::UNKNOWN);

        let stream_manager = unsafe { &mut *rm.get() };
        stream_manager
            .expect_create_range()
            .once()
            .returning(|_metadata| Err(ServiceError::Internal("Test".to_owned())));

        let store = Rc::new(store);
        handler.apply(store, rm, &mut response).await;

        if let Some(buf) = &response.header {
            let resp = flatbuffers::root::<CreateRangeResponse>(buf)
                .expect("Should get a valid create-range response");
            assert_eq!(ErrorCode::RS_INTERNAL_SERVER_ERROR, resp.status().code());
        } else {
            panic!("Create range should not fail");
        }
    }

    #[monoio::test]
    async fn test_create_range_apply_when_store_fails() {
        ulog::try_init_log();
        let mut store = MockStore::default();
        let rm = Rc::new(UnsafeCell::new(MockRangeManager::default()));
        let request = create_range_request();
        let handler =
            super::CreateRange::parse_frame(&request).expect("Parse frame should NOT fail");
        let mut response = Frame::new(OperationCode::UNKNOWN);

        let stream_manager = unsafe { &mut *rm.get() };
        stream_manager
            .expect_create_range()
            .once()
            .returning(|_metadata| Ok(()));

        store.expect_create().once().returning(|metadata| {
            assert_eq!(1, metadata.stream_id());
            Err(StoreError::Internal("Test".to_string()))
        });

        let store = Rc::new(store);
        handler.apply(store, rm, &mut response).await;

        if let Some(buf) = &response.header {
            let resp = flatbuffers::root::<CreateRangeResponse>(buf)
                .expect("Should get a valid create-range response");
            assert_eq!(ErrorCode::RS_CREATE_RANGE, resp.status().code());
        } else {
            panic!("Create range should not fail");
        }
    }
}
