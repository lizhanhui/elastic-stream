use super::session_state::SessionState;
use super::{config, response};
use codec::frame::{Frame, OperationCode};
use model::range::StreamRange;
use model::Status;
use model::{client_role::ClientRole, request::Request};
use protocol::rpc::header::{
    HeartbeatResponse, IdAllocationResponse, ListRangesResponse, SystemErrorResponse,
};
use slog::{error, trace, warn, Logger};
use std::{
    cell::UnsafeCell,
    collections::HashMap,
    rc::Rc,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;
use tokio_uring::net::TcpStream;
use transport::channel::Channel;

pub(crate) struct Session {
    config: Rc<config::ClientConfig>,

    log: Logger,

    state: SessionState,

    channel: Rc<Channel>,

    /// In-flight requests.
    inflight_requests: Rc<UnsafeCell<HashMap<u32, oneshot::Sender<response::Response>>>>,

    idle_since: Instant,
}

impl Session {
    /// Spawn a loop to continuously read responses and server-side requests.
    fn spawn_read_loop(
        channel: Rc<Channel>,
        inflight_requests: Rc<UnsafeCell<HashMap<u32, oneshot::Sender<response::Response>>>>,
        log: Logger,
    ) {
        tokio_uring::spawn(async move {
            loop {
                match channel.read_frame().await {
                    Err(e) => {
                        // Handle connection reset
                        todo!()
                    }
                    Ok(Some(frame)) => {
                        trace!(log, "Read a frame from channel");
                        let inflight = unsafe { &mut *inflight_requests.get() };
                        if frame.is_response() {
                            Session::on_response(inflight, frame, &log);
                        } else {
                            warn!(log, "Received an unexpected request frame");
                        }
                    }
                    Ok(None) => {
                        // TODO: Handle normal connection close
                        break;
                    }
                }
            }
        });
    }

    pub(crate) fn new(
        stream: TcpStream,
        endpoint: &str,
        config: &Rc<config::ClientConfig>,
        log: &Logger,
    ) -> Self {
        let channel = Rc::new(Channel::new(stream, endpoint, log.clone()));
        let inflight = Rc::new(UnsafeCell::new(HashMap::new()));

        Self::spawn_read_loop(Rc::clone(&channel), Rc::clone(&inflight), log.clone());

        Self {
            config: Rc::clone(config),
            log: log.clone(),
            state: SessionState::Active,
            channel,
            inflight_requests: inflight,
            idle_since: Instant::now(),
        }
    }

    pub(crate) async fn write(
        &mut self,
        request: &Request,
        response_observer: oneshot::Sender<response::Response>,
    ) -> Result<(), oneshot::Sender<response::Response>> {
        trace!(self.log, "Sending request {:?}", request);

        // Update last read/write instant.
        self.idle_since = Instant::now();
        let mut frame = Frame::new(OperationCode::Unknown);
        let inflight_requests = unsafe { &mut *self.inflight_requests.get() };
        inflight_requests.insert(frame.stream_id, response_observer);

        match request {
            Request::Heartbeat { .. } => {
                frame.operation_code = OperationCode::Heartbeat;
                let header = request.into();
                frame.header = Some(header);
                match self.channel.write_frame(&frame).await {
                    Ok(_) => {
                        trace!(
                            self.log,
                            "Write `Heartbeat` request to {}, stream-id={}",
                            self.channel.peer_address(),
                            frame.stream_id,
                        );
                    }
                    Err(e) => {
                        error!(
                            self.log,
                            "Failed to write request to network. Cause: {:?}", e
                        );
                        if let Some(observer) = inflight_requests.remove(&frame.stream_id) {
                            return Err(observer);
                        }
                    }
                }
            }

            Request::ListRanges { .. } => {
                frame.operation_code = OperationCode::ListRanges;
                let header = request.into();
                frame.header = Some(header);

                match self.channel.write_frame(&frame).await {
                    Ok(_) => {
                        trace!(
                            self.log,
                            "Write `ListRange` request to {}, stream-id={}",
                            self.channel.peer_address(),
                            frame.stream_id,
                        );
                    }
                    Err(e) => {
                        warn!(
                            self.log,
                            "Failed to write `ListRange` request to {}. Cause: {:?}",
                            self.channel.peer_address(),
                            e
                        );
                        if let Some(observer) = inflight_requests.remove(&frame.stream_id) {
                            return Err(observer);
                        }
                    }
                }
            }

            Request::AllocateId { .. } => {
                frame.operation_code = OperationCode::AllocateId;
                let header = request.into();
                frame.header = Some(header);
                match self.channel.write_frame(&frame).await {
                    Ok(_) => {
                        trace!(
                            self.log,
                            "Write `AllocateId` request to {}, stream-id={}",
                            self.channel.peer_address(),
                            frame.stream_id
                        );
                    }
                    Err(e) => {
                        error!(
                            self.log,
                            "Failed to write `AllocateId` request to network. Cause: {:?}", e
                        );
                        if let Some(observer) = inflight_requests.remove(&frame.stream_id) {
                            return Err(observer);
                        }
                    }
                }
            }
        };

        Ok(())
    }

    pub(crate) fn state(&self) -> SessionState {
        self.state
    }

    fn active(&self) -> bool {
        SessionState::Active == self.state
    }

    pub(crate) fn need_heartbeat(&self, duration: &Duration) -> bool {
        self.active() && (Instant::now() - self.idle_since >= *duration)
    }

    pub(crate) async fn heartbeat(&mut self) -> Option<oneshot::Receiver<response::Response>> {
        // If the current session is being closed, heartbeat will be no-op.
        if self.state == SessionState::Closing {
            return None;
        }

        let request = Request::Heartbeat {
            client_id: self.config.client_id.clone(),
            role: ClientRole::DataNode,
            data_node: self.config.data_node.clone(),
        };
        let (response_observer, rx) = oneshot::channel();
        if self.write(&request, response_observer).await.is_ok() {
            trace!(
                self.log,
                "Heartbeat sent to {}",
                self.channel.peer_address()
            );
        }
        Some(rx)
    }

    fn on_response(
        inflight: &mut HashMap<u32, oneshot::Sender<response::Response>>,
        frame: Frame,
        log: &Logger,
    ) {
        let stream_id = frame.stream_id;
        trace!(
            log,
            "Received {} response for stream-id={}",
            frame.operation_code,
            stream_id
        );

        match inflight.remove(&stream_id) {
            Some(sender) => {
                let res = match frame.operation_code {
                    OperationCode::Heartbeat => {
                        let mut resp = response::Response::Heartbeat {
                            status: Status::ok(),
                        };
                        if let Some(buf) = frame.header {
                            if let Ok(heartbeat) = flatbuffers::root::<HeartbeatResponse>(&buf) {
                                trace!(log, "Heartbeat response: {:?}", heartbeat);
                                let hb = heartbeat.unpack();
                                let _client_id = hb.client_id;
                                let _client_role = hb.client_role;
                                let _status = hb.status;
                                if let response::Response::Heartbeat { ref mut status } = resp {
                                    if let Some(_status) = _status {
                                        status.code = _status.code;
                                        if let Some(msg) = _status.message {
                                            status.message = msg;
                                        }
                                    }
                                }
                            }
                        }
                        resp
                    }
                    OperationCode::ListRanges => {
                        let mut response = response::Response::ListRange {
                            status: Status::ok(),
                            ranges: None,
                        };
                        if let Some(hdr) = frame.header {
                            if let Ok(list_ranges) = flatbuffers::root::<ListRangesResponse>(&hdr) {
                                let _ranges = list_ranges
                                    .unpack()
                                    .list_responses
                                    .iter()
                                    .flat_map(|result| result.iter())
                                    .flat_map(|res| res.ranges.as_ref())
                                    .flat_map(|e| e.iter())
                                    .map(|range| {
                                        if range.end_offset >= 0 {
                                            StreamRange::new(
                                                range.stream_id,
                                                range.range_index,
                                                range.start_offset as u64,
                                                range.next_offset as u64,
                                                Some(range.end_offset as u64),
                                            )
                                        } else {
                                            StreamRange::new(
                                                range.stream_id,
                                                range.range_index,
                                                range.start_offset as u64,
                                                range.next_offset as u64,
                                                None,
                                            )
                                        }
                                    })
                                    .collect::<Vec<_>>();
                                if let response::Response::ListRange { ranges, .. } = &mut response
                                {
                                    *ranges = Some(_ranges);
                                }
                            }
                        }
                        response
                    }
                    OperationCode::Unknown => {
                        warn!(log, "Received an unknown operation code");
                        return;
                    }
                    OperationCode::Ping => todo!(),
                    OperationCode::GoAway => todo!(),
                    OperationCode::AllocateId => {
                        let mut resp = response::Response::AllocateId {
                            status: Status::decode(),
                            id: -1,
                        };

                        if frame.system_error() {
                            if let Some(ref buf) = frame.header {
                                match flatbuffers::root::<SystemErrorResponse>(buf) {
                                    Ok(error_response) => {
                                        let err = error_response.unpack();
                                        if let Some(status) = err.status {
                                            let st = Status {
                                                code: status.code,
                                                message: status.message.unwrap_or_default(),
                                                details: status.detail.map(|b| b.into()),
                                            };

                                            // Update status
                                            if let response::Response::AllocateId {
                                                ref mut status,
                                                ..
                                            } = resp
                                            {
                                                *status = st;
                                            }
                                        }
                                    }

                                    Err(e) => {
                                        // Deserialize error
                                        warn!(log, "Failed to decode `SystemErrorResponse` using FlatBuffers. Cause: {}", e);
                                    }
                                }
                            }
                        } else if let Some(ref buf) = frame.header {
                            match flatbuffers::root::<IdAllocationResponse>(buf) {
                                Ok(response) => {
                                    let response = response.unpack();
                                    if let response::Response::AllocateId {
                                        ref mut id,
                                        ref mut status,
                                    } = resp
                                    {
                                        *id = response.id;
                                        *status = Status::ok();
                                    }
                                }
                                Err(e) => {
                                    // Deserialize error
                                    warn!(log, "Failed to decode `IdAllocation` response header using FlatBuffers. Cause: {}", e);
                                }
                            }
                        }

                        resp
                    }
                    OperationCode::Append => {
                        warn!(log, "Received an unexpected `Append` response");
                        return;
                    }
                    OperationCode::Fetch => {
                        warn!(log, "Received an unexpected `Fetch` response");
                        return;
                    }
                    OperationCode::SealRanges => {
                        warn!(log, "Received an unexpected `SealRanges` response");
                        return;
                    }
                    OperationCode::SyncRanges => {
                        warn!(log, "Received an unexpected `SyncRanges` response");
                        return;
                    }
                    OperationCode::DescribeRanges => {
                        warn!(log, "Received an unexpected `DescribeRanges` response");
                        return;
                    }
                    OperationCode::CreateStreams => {
                        warn!(log, "Received an unexpected `CreateStreams` response");
                        return;
                    }
                    OperationCode::DeleteStreams => {
                        warn!(log, "Received an unexpected `DeleteStreams` response");
                        return;
                    }
                    OperationCode::UpdateStreams => {
                        warn!(log, "Received an unexpected `UpdateStreams` response");
                        return;
                    }
                    OperationCode::DescribeStreams => {
                        warn!(log, "Received an unexpected `DescribeStreams` response");
                        return;
                    }
                    OperationCode::TrimStreams => todo!(),
                    OperationCode::ReportMetrics => todo!(),
                };
                sender.send(res).unwrap_or_else(|response| {
                    warn!(log, "Failed to forward response to client: {:?}", response);
                });
            }
            None => {
                warn!(
                    log,
                    "Expected inflight request[stream-id={}] is missing", frame.stream_id
                );
            }
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let requests = unsafe { &mut *self.inflight_requests.get() };
        requests.drain().for_each(|(_stream_id, sender)| {
            let aborted_response = response::Response::ListRange {
                status: Status::pm_internal("Aborted".to_owned()),
                ranges: None,
            };
            sender.send(aborted_response).unwrap_or_else(|_response| {
                warn!(self.log, "Failed to notify connection reset");
            });
        });
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use model::data_node::DataNode;
    use protocol::rpc::header::ErrorCode;
    use std::{error::Error, time::Duration};
    use test_util::{run_listener, terminal_logger};
    use tokio::time::timeout;

    /// Verify it's OK to create a new session.
    #[test]
    fn test_new() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async {
            let logger = terminal_logger();
            let port = run_listener(logger.clone()).await;
            let target = format!("127.0.0.1:{}", port);
            let stream = TcpStream::connect(target.parse()?).await?;
            let config = Rc::new(config::ClientConfig::default());
            let session = Session::new(stream, &target, &config, &logger);

            assert_eq!(SessionState::Active, session.state());

            assert_eq!(false, session.need_heartbeat(&Duration::from_secs(1)));

            Ok(())
        })
    }

    #[test]
    fn test_heartbeat() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async {
            let logger = terminal_logger();
            let port = 2378;
            let port = run_listener(logger.clone()).await;
            let target = format!("127.0.0.1:{}", port);
            let stream = TcpStream::connect(target.parse()?).await?;
            let mut config = config::ClientConfig::default();
            let data_node = DataNode {
                node_id: 1,
                advertise_address: "localhost:1234".to_owned(),
            };
            config.with_data_node(data_node);
            let config = Rc::new(config);
            let mut session = Session::new(stream, &target, &config, &logger);

            let result = session.heartbeat().await;
            let response = result.unwrap().await?;
            trace!(logger, "Heartbeat response: {:?}", response);
            if let response::Response::Heartbeat { ref status } = response {
                assert_eq!(ErrorCode::OK, status.code);
            } else {
                panic!("Unexpected response type");
            }
            Ok(())
        })
    }

    #[test]
    #[ignore]
    fn test_heartbeat_timeout() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async {
            let logger = terminal_logger();
            let port = run_listener(logger.clone()).await;
            let target = format!("127.0.0.1:{}", port);
            let stream = TcpStream::connect(target.parse()?).await?;
            let mut config = config::ClientConfig::default();
            let data_node = DataNode {
                node_id: 1,
                advertise_address: "localhost:1234".to_owned(),
            };
            config.with_data_node(data_node);
            let config = Rc::new(config);
            let mut session = Session::new(stream, &target, &config, &logger);

            if let Some(rx) = session.heartbeat().await {
                let start = Instant::now();
                let result = timeout(Duration::from_millis(200), rx).await;
                let duration = start.elapsed();
                assert!(duration.as_millis() >= 200, "Timeout should work");
                if let Err(ref _elapsed) = result {
                } else {
                    panic!("Should get timeout");
                }
            }
            Ok(())
        })
    }
}
