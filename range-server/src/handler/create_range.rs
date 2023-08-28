use super::util::root_as_rpc_request;
use crate::range_manager::RangeManager;
use bytes::Bytes;
use codec::frame::Frame;
use core::fmt;
use log::{error, warn};
use model::range::RangeMetadata;
use protocol::rpc::header::{CreateRangeRequest, CreateRangeResponseT, ErrorCode, StatusT};
use std::rc::Rc;

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

    pub(crate) async fn apply<M>(&self, range_manager: Rc<M>, response: &mut Frame)
    where
        M: RangeManager,
    {
        let request = self.request.unpack();

        let mut create_response = CreateRangeResponseT::default();
        let range = request.range;
        let range_metadata = Into::<RangeMetadata>::into(&*range);
        let mut status = StatusT::default();
        status.code = ErrorCode::OK;

        // Notify range creation to `RangeManager` in case it has not received it from PD
        match range_manager.create_range(range_metadata.clone()) {
            Err(e) => {
                error!("Failed to create range: {:?}", e);
                // TODO: Map service error to the corresponding error code.
                status.code = ErrorCode::RS_CREATE_RANGE;
                status.message = Some(format!("Failed to create range: {}", e));
            }
            Ok(()) => {
                // `RangeManager` should NOT modify range metadata, so it's safe to write it back
                // directly.
                create_response.range = Some(range);
            }
        }
        create_response.status = Box::new(status);
        self.build_response(response, &create_response);
    }

    #[inline]
    fn build_response(&self, frame: &mut Frame, create_response: &CreateRangeResponseT) {
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
    use crate::{error::ServiceError, range_manager::MockRangeManager};
    use bytes::Bytes;
    use codec::frame::Frame;
    use flatbuffers::FlatBufferBuilder;
    use protocol::rpc::header::{
        CreateRangeRequestT, CreateRangeResponse, ErrorCode, OperationCode, RangeT,
    };
    use std::rc::Rc;

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

    #[test]
    fn test_create_range_apply() {
        ulog::try_init_log();
        let mut range_manager = MockRangeManager::default();
        range_manager
            .expect_create_range()
            .once()
            .returning_st(|_range| Ok(()));
        let range_manager = Rc::new(range_manager);
        let request = create_range_request();
        let handler =
            super::CreateRange::parse_frame(&request).expect("Parse frame should NOT fail");
        let mut response = Frame::new(OperationCode::UNKNOWN);

        tokio_uring::start(async move {
            handler.apply(range_manager, &mut response).await;
            if let Some(buf) = &response.header {
                let resp = flatbuffers::root::<CreateRangeResponse>(buf)
                    .expect("Should get a valid create-range response");
                assert_eq!(ErrorCode::OK, resp.status().code());
            } else {
                panic!("Create range should not fail");
            }
        })
    }

    #[test]
    fn test_create_range_apply_when_range_manager_fails() {
        ulog::try_init_log();
        let mut range_manager = MockRangeManager::default();
        range_manager
            .expect_create_range()
            .once()
            .returning(|_metadata| Err(ServiceError::Internal("Test".to_owned())));
        let rm = Rc::new(range_manager);
        let request = create_range_request();
        let handler =
            super::CreateRange::parse_frame(&request).expect("Parse frame should NOT fail");
        let mut response = Frame::new(OperationCode::UNKNOWN);

        tokio_uring::start(async move {
            handler.apply(rm, &mut response).await;

            if let Some(buf) = &response.header {
                let resp = flatbuffers::root::<CreateRangeResponse>(buf)
                    .expect("Should get a valid create-range response");
                assert_eq!(ErrorCode::RS_CREATE_RANGE, resp.status().code());
            } else {
                panic!("Create range should not fail");
            }
        })
    }

    #[test]
    fn test_create_range_apply_when_store_fails() {
        ulog::try_init_log();
        let mut range_manager = MockRangeManager::default();
        range_manager
            .expect_create_range()
            .once()
            .returning(|metadata| {
                assert_eq!(1, metadata.stream_id());
                Err(ServiceError::Internal("Test".to_string()))
            });
        let range_manager = Rc::new(range_manager);
        let request = create_range_request();
        let handler =
            super::CreateRange::parse_frame(&request).expect("Parse frame should NOT fail");
        let mut response = Frame::new(OperationCode::UNKNOWN);

        tokio_uring::start(async move {
            handler.apply(range_manager, &mut response).await;
            if let Some(buf) = &response.header {
                let resp = flatbuffers::root::<CreateRangeResponse>(buf)
                    .expect("Should get a valid create-range response");
                assert_eq!(ErrorCode::RS_CREATE_RANGE, resp.status().code());
            } else {
                panic!("Create range should not fail");
            }
        })
    }
}
