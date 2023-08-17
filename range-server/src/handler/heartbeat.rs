use super::util::root_as_rpc_request;
use crate::range_manager::RangeManager;
use bytes::Bytes;
use codec::frame::Frame;
use log::trace;
use protocol::rpc::header::{ErrorCode, HeartbeatRequest, HeartbeatResponseT, StatusT};
use std::{fmt, rc::Rc};

#[derive(Debug)]
pub(crate) struct Heartbeat<'a> {
    request: HeartbeatRequest<'a>,
}

impl<'a> Heartbeat<'a> {
    pub(crate) fn parse_frame(frame: &'a Frame) -> Result<Self, ErrorCode> {
        let request = frame
            .header
            .as_ref()
            .map(|buf| root_as_rpc_request::<HeartbeatRequest>(buf))
            .ok_or(ErrorCode::BAD_REQUEST)?
            .map_err(|_e| ErrorCode::BAD_REQUEST)?;

        Ok(Self { request })
    }

    pub(crate) async fn apply<M>(&self, _range_manager: Rc<M>, response: &mut Frame)
    where
        M: RangeManager,
    {
        trace!("Prepare heartbeat response header for {:?}", self.request);

        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let mut response_header = HeartbeatResponseT::default();

        let mut status = StatusT::default();
        status.code = ErrorCode::OK;
        status.message = Some(String::from("OK"));
        response_header.status = Box::new(status);

        response_header.client_id = self.request.client_id().map(|id| id.to_owned());
        response_header.client_role = self.request.client_role();

        let header = response_header.pack(&mut builder);
        builder.finish(header, None);
        let data = builder.finished_data();
        response.header = Some(Bytes::copy_from_slice(data));

        trace!("Heartbeat response header built");
    }
}

impl<'a> fmt::Display for Heartbeat<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.request)
    }
}

#[cfg(test)]
mod tests {
    use crate::range_manager::MockRangeManager;
    use bytes::Bytes;
    use codec::frame::Frame;
    use protocol::rpc::header::{
        ClientRole, ErrorCode, HeartbeatRequestT, HeartbeatResponse, OperationCode,
    };
    use std::{error::Error, rc::Rc};

    fn heartbeat_request() -> Frame {
        let mut frame = Frame::new(OperationCode::HEARTBEAT);

        let mut request = HeartbeatRequestT::default();
        request.client_id = Some("sample-client-id".to_owned());
        request.client_role = ClientRole::CLIENT_ROLE_FRONTEND;

        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let req = request.pack(&mut builder);
        builder.finish(req, None);
        let data = builder.finished_data();
        frame.header = Some(Bytes::copy_from_slice(data));
        frame
    }

    #[test]
    fn test_parse_frame() -> Result<(), Box<dyn Error>> {
        let frame = heartbeat_request();
        super::Heartbeat::parse_frame(&frame).unwrap();
        Ok(())
    }

    #[test]
    fn test_parse_frame_bad_request() {
        let mut frame = Frame::new(OperationCode::HEARTBEAT);
        frame.header = Some(Bytes::from_static(b"xxx"));
        match super::Heartbeat::parse_frame(&frame) {
            Ok(_) => {
                panic!("Should be bad request");
            }
            Err(ec) => {
                assert_eq!(ec, ErrorCode::BAD_REQUEST);
            }
        }
    }

    #[test]
    fn test_heartbeat_apply() {
        let frame = heartbeat_request();
        let handler = super::Heartbeat::parse_frame(&frame).unwrap();

        let mut response = Frame::new(OperationCode::UNKNOWN);
        let rm = Rc::new(MockRangeManager::default());

        tokio_uring::start(async move {
            handler.apply(rm, &mut response).await;
            if let Some(header) = response.header {
                let resp = flatbuffers::root::<HeartbeatResponse>(&header[..]).unwrap();
                let ec = resp.status().code();
                assert_eq!(ec, ErrorCode::OK);
                assert_eq!(ClientRole::CLIENT_ROLE_FRONTEND, resp.client_role());
                assert_eq!(resp.client_id(), Some("sample-client-id"));
            } else {
                panic!("Should have a valid response header");
            }
        })
    }
}
