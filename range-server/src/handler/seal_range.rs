use std::{cell::UnsafeCell, fmt, rc::Rc};

use bytes::Bytes;
use codec::frame::Frame;
use log::{error, warn};
use model::range::RangeMetadata;
use protocol::rpc::header::{ErrorCode, RangeT, SealRangeRequest, SealRangeResponseT, StatusT};
use store::Store;

use crate::{error::ServiceError, range_manager::RangeManager};

use super::util::root_as_rpc_request;

#[derive(Debug)]
pub(crate) struct SealRange<'a> {
    request: SealRangeRequest<'a>,
}

impl<'a> SealRange<'a> {
    pub(crate) fn parse_frame(frame: &'a Frame) -> Result<Self, ErrorCode> {
        let request = frame
            .header
            .as_ref()
            .map(|buf| root_as_rpc_request::<SealRangeRequest>(buf))
            .ok_or(ErrorCode::BAD_REQUEST)?
            .map_err(|_e| {
                warn!(
                    "Received an invalid seal range request[stream-id={}]",
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

        let mut seal_response = SealRangeResponseT::default();
        let mut status = StatusT::default();
        status.code = ErrorCode::OK;
        status.message = Some(String::from("OK"));
        let manager = unsafe { &mut *range_manager.get() };

        let range = request.range;
        let mut range = Into::<RangeMetadata>::into(&*range);

        match manager.seal(&mut range) {
            Ok(_) => {
                status.code = ErrorCode::OK;
                status.message = Some(String::from("OK"));
                seal_response.range = Some(Box::new(Into::<RangeT>::into(&range)));
            }

            Err(e) => {
                error!(
                    "Failed to seal stream-id={}, range_index={}",
                    range.stream_id(),
                    range.index()
                );
                match e {
                    ServiceError::NotFound(_) => status.code = ErrorCode::RANGE_NOT_FOUND,
                    ServiceError::AlreadySealed => status.code = ErrorCode::RANGE_ALREADY_SEALED,
                    _ => status.code = ErrorCode::RS_INTERNAL_SERVER_ERROR,
                }
                status.message = Some(e.to_string());
                seal_response.status = Box::new(status);
                self.build_response(response, &seal_response);
                return;
            }
        }

        if let Err(e) = store.seal(range.clone()).await {
            error!(
                "Failed to seal stream-id={}, range_index={} in store",
                range.stream_id(),
                range.index()
            );
            status.code = ErrorCode::RS_SEAL_RANGE;
            status.message = Some(e.to_string());
            seal_response.status = Box::new(status);
            self.build_response(response, &seal_response);
            return;
        }

        seal_response.status = Box::new(status);
        self.build_response(response, &seal_response);
    }

    #[inline]
    fn build_response(&self, frame: &mut Frame, seal_response: &SealRangeResponseT) {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let resp = seal_response.pack(&mut builder);
        builder.finish(resp, None);
        let data = builder.finished_data();
        frame.header = Some(Bytes::copy_from_slice(data));
    }
}

impl<'a> fmt::Display for SealRange<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let range = self.request.range();
        write!(
            f,
            "SealRangeHandler[stream-id={}, range-index={}, start={}, end={}]",
            range.stream_id(),
            range.index(),
            range.start(),
            range.end()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::UnsafeCell, error::Error, rc::Rc};

    use bytes::Bytes;
    use codec::frame::Frame;
    use protocol::rpc::header::{
        ErrorCode, OperationCode, RangeT, SealKind, SealRangeRequestT, SealRangeResponse,
    };
    use store::{error::StoreError, MockStore};

    use crate::{error::ServiceError, range_manager::MockRangeManager};

    fn seal_range_request() -> Frame {
        let mut frame = Frame::new(OperationCode::SEAL_RANGE);

        let mut request = SealRangeRequestT::default();
        request.timeout_ms = 10;
        request.kind = SealKind::RANGE_SERVER;

        let mut range = RangeT::default();
        range.stream_id = 1;
        range.index = 1;
        range.start = 10;
        range.end = 20;
        range.ack_count = 2;
        range.replica_count = 3;

        request.range = Box::new(range);

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let req = request.pack(&mut fbb);
        fbb.finish(req, None);
        let data = fbb.finished_data();
        frame.header = Some(Bytes::copy_from_slice(data));
        frame
    }

    #[test]
    fn test_parse_frame() -> Result<(), Box<dyn Error>> {
        let frame = seal_range_request();
        let handler = super::SealRange::parse_frame(&frame).expect("Should not fail");
        assert_eq!(
            "SealRangeHandler[stream-id=1, range-index=1, start=10, end=20]",
            format!("{}", handler)
        );
        Ok(())
    }

    #[test]
    fn test_parse_frame_error() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        let mut frame = Frame::new(OperationCode::SEAL_RANGE);
        match super::SealRange::parse_frame(&frame) {
            Ok(_) => {
                panic!("Should have failed");
            }
            Err(ec) => {
                assert_eq!(ErrorCode::BAD_REQUEST, ec);
            }
        }

        frame.header = Some(Bytes::from_static(b"abc"));
        match super::SealRange::parse_frame(&frame) {
            Ok(_) => {
                panic!("Should have failed");
            }
            Err(ec) => {
                assert_eq!(ErrorCode::BAD_REQUEST, ec);
            }
        }

        Ok(())
    }

    #[test]
    fn test_apply() {
        let mut store = MockStore::default();
        store.expect_seal().once().returning(|_metadata| Ok(()));

        let mut range_manager = MockRangeManager::default();
        range_manager
            .expect_seal()
            .once()
            .returning(|_metadata| Ok(()));

        let request = seal_range_request();
        let mut response = Frame::new(OperationCode::SEAL_RANGE);
        let handler = super::SealRange::parse_frame(&request).expect("Parse frame should be OK");

        tokio_uring::start(async move {
            handler
                .apply(
                    Rc::new(store),
                    Rc::new(UnsafeCell::new(range_manager)),
                    &mut response,
                )
                .await;

            if let Some(ref buf) = response.header {
                let resp =
                    flatbuffers::root::<SealRangeResponse>(buf).expect("Decode should not fail");
                assert_eq!(resp.status().code(), ErrorCode::OK);
            } else {
                panic!("Seal range should succeed");
            }
        })
    }

    #[test]
    fn test_apply_when_already_sealed() {
        let store = MockStore::default();

        let mut range_manager = MockRangeManager::default();
        range_manager
            .expect_seal()
            .once()
            .returning(|_metadata| Err(ServiceError::AlreadySealed));

        let request = seal_range_request();
        let mut response = Frame::new(OperationCode::SEAL_RANGE);
        let handler = super::SealRange::parse_frame(&request).expect("Parse frame should be OK");

        tokio_uring::start(async move {
            handler
                .apply(
                    Rc::new(store),
                    Rc::new(UnsafeCell::new(range_manager)),
                    &mut response,
                )
                .await;

            if let Some(ref buf) = response.header {
                let resp =
                    flatbuffers::root::<SealRangeResponse>(buf).expect("Decode should not fail");
                assert_eq!(resp.status().code(), ErrorCode::RANGE_ALREADY_SEALED);
            } else {
                panic!("Seal range should have a header");
            }
        });
    }

    #[test]
    fn test_apply_when_range_not_found() {
        let store = MockStore::default();

        let mut range_manager = MockRangeManager::default();
        range_manager
            .expect_seal()
            .once()
            .returning(|_metadata| Err(ServiceError::NotFound("Test".to_owned())));

        let request = seal_range_request();
        let mut response = Frame::new(OperationCode::SEAL_RANGE);
        let handler = super::SealRange::parse_frame(&request).expect("Parse frame should be OK");

        tokio_uring::start(async move {
            handler
                .apply(
                    Rc::new(store),
                    Rc::new(UnsafeCell::new(range_manager)),
                    &mut response,
                )
                .await;

            if let Some(ref buf) = response.header {
                let resp =
                    flatbuffers::root::<SealRangeResponse>(buf).expect("Decode should not fail");
                assert_eq!(resp.status().code(), ErrorCode::RANGE_NOT_FOUND);
            } else {
                panic!("Seal range should have a header");
            }
        });
    }

    #[test]
    fn test_apply_when_internal_error() {
        let store = MockStore::default();

        let mut range_manager = MockRangeManager::default();
        range_manager
            .expect_seal()
            .once()
            .returning(|_metadata| Err(ServiceError::Internal("Test".to_owned())));

        let request = seal_range_request();
        let mut response = Frame::new(OperationCode::SEAL_RANGE);
        let handler = super::SealRange::parse_frame(&request).expect("Parse frame should be OK");

        tokio_uring::start(async move {
            handler
                .apply(
                    Rc::new(store),
                    Rc::new(UnsafeCell::new(range_manager)),
                    &mut response,
                )
                .await;

            if let Some(ref buf) = response.header {
                let resp =
                    flatbuffers::root::<SealRangeResponse>(buf).expect("Decode should not fail");
                assert_eq!(resp.status().code(), ErrorCode::RS_INTERNAL_SERVER_ERROR);
            } else {
                panic!("Seal range should have a header");
            }
        });
    }

    #[test]
    fn test_apply_when_store_error() {
        let mut store = MockStore::default();
        store
            .expect_seal()
            .once()
            .returning(|_metadata| Err(StoreError::System(22)));

        let mut range_manager = MockRangeManager::default();
        range_manager
            .expect_seal()
            .once()
            .returning(|_metadata| Ok(()));

        let request = seal_range_request();
        let mut response = Frame::new(OperationCode::SEAL_RANGE);
        let handler = super::SealRange::parse_frame(&request).expect("Parse frame should be OK");

        tokio_uring::start(async move {
            handler
                .apply(
                    Rc::new(store),
                    Rc::new(UnsafeCell::new(range_manager)),
                    &mut response,
                )
                .await;

            if let Some(ref buf) = response.header {
                let resp =
                    flatbuffers::root::<SealRangeResponse>(buf).expect("Decode should not fail");
                assert_eq!(resp.status().code(), ErrorCode::RS_SEAL_RANGE);
            } else {
                panic!("Seal range should have a header");
            }
        });
    }
}
