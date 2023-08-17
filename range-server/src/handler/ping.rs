use crate::range_manager::RangeManager;
use codec::frame::Frame;
use log::trace;
use std::{fmt, rc::Rc};

/// Process Ping request
///
/// Ping-pong mechanism is designed to be a light weight API to probe liveness of range-server.
/// The Pong response return the same header and payload as the Ping request.
#[derive(Debug)]
pub(crate) struct Ping<'a> {
    request: &'a Frame,
}

impl<'a> Ping<'a> {
    pub(crate) fn new(frame: &'a Frame) -> Self {
        Self { request: frame }
    }

    pub(crate) async fn apply<M>(&self, _range_manager: Rc<M>, response: &mut Frame)
    where
        M: RangeManager,
    {
        trace!("Ping[stream-id={}] received", self.request.stream_id);
        response.header = self.request.header.clone();
        response.payload = self.request.payload.clone();
    }
}

impl<'a> fmt::Display for Ping<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Ping[stream-id={}]", self.request.stream_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::range_manager::MockRangeManager;
    use codec::frame::Frame;
    use protocol::rpc::header::OperationCode;
    use std::{error::Error, rc::Rc};

    #[test]
    fn test_ping() -> Result<(), Box<dyn Error>> {
        let request = Frame::new(OperationCode::PING);
        let mut response = Frame::new(OperationCode::UNKNOWN);
        tokio_uring::start(async move {
            let ping = super::Ping::new(&request);
            let msg = format!("{}", ping);
            assert_eq!("Ping[stream-id=1]", msg);
            let range_manager = Rc::new(MockRangeManager::new());
            ping.apply(range_manager, &mut response).await;
            Ok(())
        })
    }
}
