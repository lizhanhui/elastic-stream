use crate::{
    error::ClientError,
    heartbeat::HeartbeatData,
    request::{self, Request},
    response::{self, Response},
    state::SessionState,
    NodeRole,
};
use codec::{error::FrameError, frame::Frame};

use crate::invocation_context::InvocationContext;
use futures::Future;
use local_sync::oneshot;
use log::{error, info, trace, warn};
use monoio::{
    io::{AsyncReadRent, Splitable},
    net::TcpStream,
};
use protocol::rpc::header::{ClientRole, GoAwayFlags, OperationCode, RangeServerState};
use std::{
    cell::{RefCell, UnsafeCell},
    collections::HashMap,
    net::SocketAddr,
    rc::{Rc, Weak},
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};
use tower::Service;
use transport::connection::{ChannelReader, ChannelWriter};

pub(crate) struct Session {
    pub(crate) target: SocketAddr,

    config: Arc<config::Configuration>,

    connection: ChannelWriter,

    /// In-flight requests.
    inflight_requests: Rc<UnsafeCell<HashMap<u32, InvocationContext>>>,

    idle_since: Rc<RefCell<Instant>>,

    /// Role of the peer node in its cluster.
    role: Rc<RefCell<NodeRole>>,

    /// State of the current session.
    ///
    /// By default, it is `Active`. Once it received `GoAway` frame from peer server, will mark itself as
    /// `GoAway`.
    ///
    /// Session at `GoAway` state should close itself as soon as possible.
    state: Rc<RefCell<SessionState>>,
}

impl Session {
    /// Spawn a loop to continuously read responses and server-side requests.
    fn spawn_read_loop<S>(
        mut connection: ChannelReader<S>,
        inflight_requests: Rc<UnsafeCell<HashMap<u32, InvocationContext>>>,
        sessions: Weak<RefCell<HashMap<SocketAddr, Session>>>,
        state: Rc<RefCell<SessionState>>,
        target: SocketAddr,
    ) where
        S: AsyncReadRent + 'static,
    {
        monoio::spawn(async move {
            trace!("Start read loop for session[target={}]", target);
            loop {
                match connection.read_frame().await {
                    Err(e) => {
                        match e {
                            FrameError::Incomplete => {
                                // More data needed
                                continue;
                            }
                            FrameError::ConnectionReset => {
                                error!("Connection to {} reset by peer", target);
                            }
                            FrameError::BadFrame(message) => {
                                error!(
                                    "Read a bad frame from target={}. Cause: {}",
                                    target, message
                                );
                            }
                            FrameError::TooLongFrame { found, max } => {
                                error!(
                                    "Read a frame with excessive length={}, max={}, target={}",
                                    found, max, target
                                );
                            }
                            FrameError::MagicCodeMismatch { found, expected } => {
                                error!( "Read a frame with incorrect magic code. Expected={}, actual={}, target={}", expected, found, target);
                            }
                            FrameError::TooLongFrameHeader { found, expected } => {
                                error!( "Read a frame with excessive header length={}, max={}, target={}", found, expected, target);
                            }
                            FrameError::PayloadChecksumMismatch { expected, actual } => {
                                error!( "Read a frame with incorrect payload checksum. Expected={}, actual={}, target={}", expected, actual, target);
                            }
                        }
                        // Close the session
                        if let Some(sessions) = sessions.upgrade() {
                            let mut sessions = sessions.borrow_mut();
                            if let Some(_session) = sessions.remove(&target) {
                                warn!("Closing session to {}", target);
                            }
                        }
                        break;
                    }
                    Ok(Some(frame)) => {
                        trace!("Read a frame from channel={}", target);
                        let inflight = unsafe { &mut *inflight_requests.get() };
                        if frame.is_response() {
                            Session::handle_response(inflight, frame, target);
                        } else if frame.operation_code == OperationCode::GOAWAY {
                            *state.borrow_mut() = SessionState::GoAway;
                            let reason = if frame.has_go_away_flag(GoAwayFlags::SERVER_MAINTENANCE)
                            {
                                "server maintenance"
                            } else {
                                "connection being idle"
                            };
                            info!(
                                "Peer server[{}] has notified to go-away due to {}.",
                                target, reason
                            );
                        } else {
                            warn!(
                                "Received an unexpected request frame from target={}",
                                target
                            );
                        }
                    }
                    Ok(None) => {
                        info!("Connection to {} is closed by peer", target);
                        if let Some(sessions) = sessions.upgrade() {
                            let mut sessions = sessions.borrow_mut();
                            if let Some(_session) = sessions.remove(&target) {
                                info!("Remove session to {} from composite-session", target);
                            }
                        }
                        break;
                    }
                }

                let inflight = unsafe { &mut *inflight_requests.get() };
                inflight.retain(|stream_id, ctx| {
                    if ctx.is_closed() {
                        info!(
                            "Caller has cancelled request[stream-id={}], potentially due to timeout",
                            stream_id
                        );
                    }
                    !ctx.is_closed()
                });
            }
            trace!("Read loop for session[target={}] completed", target);
        });
    }

    pub(crate) fn new(
        target: SocketAddr,
        stream: TcpStream,
        endpoint: &str,
        config: &Arc<config::Configuration>,
        sessions: Weak<RefCell<HashMap<SocketAddr, Session>>>,
    ) -> Self {
        let (read_half, write_half) = stream.into_split();
        let channel_reader = ChannelReader::new(read_half, endpoint.to_string());
        let channel_writer = ChannelWriter::new(write_half, endpoint.to_string());
        let inflight = Rc::new(UnsafeCell::new(HashMap::new()));

        let state = Rc::new(RefCell::new(SessionState::Active));

        Self::spawn_read_loop(
            channel_reader,
            Rc::clone(&inflight),
            sessions,
            Rc::clone(&state),
            target,
        );

        Self {
            config: Arc::clone(config),
            target,
            connection: channel_writer,
            inflight_requests: inflight,
            idle_since: Rc::new(RefCell::new(Instant::now())),
            role: Rc::new(RefCell::new(NodeRole::Unknown)),
            state,
        }
    }

    pub(crate) async fn write(
        &self,
        request: request::Request,
        response_observer: oneshot::Sender<response::Response>,
    ) -> Result<(), InvocationContext> {
        trace!("Sending {} to {}", request, self.target);

        // Update last read/write instant.
        *self.idle_since.borrow_mut() = Instant::now();
        let mut frame = Frame::new(OperationCode::UNKNOWN);

        // Set frame header
        frame.header = Some((&request).into());

        // Set operation code
        match &request.headers {
            request::Headers::Heartbeat { .. } => {
                frame.operation_code = OperationCode::HEARTBEAT;
            }

            request::Headers::CreateStream { .. } => {
                frame.operation_code = OperationCode::CREATE_STREAM;
            }

            request::Headers::DescribeStream { .. } => {
                frame.operation_code = OperationCode::DESCRIBE_STREAM;
            }

            request::Headers::UpdateStreamEpoch { .. } => {
                frame.operation_code = OperationCode::UPDATE_STREAM_EPOCH;
            }

            request::Headers::ListRange { .. } => {
                frame.operation_code = OperationCode::LIST_RANGE;
            }

            request::Headers::AllocateId { .. } => {
                frame.operation_code = OperationCode::ALLOCATE_ID;
            }

            request::Headers::DescribePlacementDriver { .. } => {
                frame.operation_code = OperationCode::DESCRIBE_PLACEMENT_DRIVER;
            }

            request::Headers::CreateRange { .. } => {
                frame.operation_code = OperationCode::CREATE_RANGE;
            }

            request::Headers::SealRange { .. } => {
                frame.operation_code = OperationCode::SEAL_RANGE;
            }

            request::Headers::Append => {
                frame.operation_code = OperationCode::APPEND;
            }

            request::Headers::Fetch { .. } => {
                frame.operation_code = OperationCode::FETCH;
            }

            request::Headers::ReportMetrics { .. } => {
                frame.operation_code = OperationCode::REPORT_METRICS;
            }

            request::Headers::ReportRangeProgress { .. } => {
                frame.operation_code = OperationCode::REPORT_REPLICA_PROGRESS;
            }

            request::Headers::CommitObject { .. } => {
                frame.operation_code = OperationCode::COMMIT_OBJECT;
            }

            request::Headers::ListResource { .. } => {
                frame.operation_code = OperationCode::LIST_RESOURCE;
            }

            request::Headers::WatchResource { .. } => {
                frame.operation_code = OperationCode::WATCH_RESOURCE;
            }
        };

        frame.payload = request.body.clone();

        let inflight_requests = unsafe { &mut *self.inflight_requests.get() };
        let context = InvocationContext::new(self.target, request.clone(), response_observer);
        inflight_requests.insert(frame.stream_id, context);

        // Write frame to network
        let stream_id = frame.stream_id;
        let opcode = frame.operation_code;
        match self.connection.write_frame(frame).await {
            Ok(_) => {
                trace!(
                    "Write request[opcode={}] bounded for {} using stream-id={} to socket buffer",
                    opcode.variant_name().unwrap_or("INVALID_OPCODE"),
                    self.connection.peer_address(),
                    stream_id,
                );
            }
            Err(e) => {
                error!(
                    "Failed to write request[opcode={}] bounded for {} to socket buffer. Cause: {:?}",
                    opcode.variant_name().unwrap_or("INVALID_OPCODE"), self.connection.peer_address(), e
                );
                if let Some(ctx) = inflight_requests.remove(&stream_id) {
                    return Err(ctx);
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn heartbeat(&self, data: &HeartbeatData) {
        let last = *self.idle_since.borrow();
        if !data.mandatory() && Instant::now() - last < self.config.client_heartbeat_interval() {
            return;
        }

        let request = request::Request {
            timeout: self.config.client_io_timeout(),
            headers: request::Headers::Heartbeat {
                client_id: self.config.client.client_id.clone(),
                range_server: if data.role == ClientRole::CLIENT_ROLE_RANGE_SERVER {
                    let mut range_server = self.config.server.range_server();
                    range_server.state = data
                        .state
                        .unwrap_or(RangeServerState::RANGE_SERVER_STATE_READ_WRITE);
                    Some(range_server)
                } else {
                    None
                },
                role: data.role,
            },
            body: None,
        };

        let mut frame = Frame::new(OperationCode::HEARTBEAT);
        frame.header = Some((&request).into());
        let stream_id = frame.stream_id;
        let opcode = frame.operation_code;
        match self.connection.write_frame(frame).await {
            Ok(_) => {
                trace!(
                    "Write request[opcode={}] bounded for {} using stream-id={} to socket buffer",
                    opcode.variant_name().unwrap_or("INVALID_OPCODE"),
                    self.connection.peer_address(),
                    stream_id,
                );
                *self.idle_since.borrow_mut() = Instant::now();
            }
            Err(e) => {
                error!(
                    "Failed to write request[opcode={}] bounded for {} to socket buffer. Cause: {:?}",
                    opcode.variant_name().unwrap_or("INVALID_OPCODE"), self.connection.peer_address(), e
                );
            }
        }
    }

    pub(crate) fn role(&self) -> NodeRole {
        *self.role.borrow()
    }

    pub(crate) fn set_role(&self, state: NodeRole) {
        let current = *self.role.borrow();
        if current != state {
            info!(
                "Node-state of {} is changed: {:?} --> {:?}",
                self.target, current, state
            );
            *self.role.borrow_mut() = state;
        }
    }

    pub fn state(&self) -> SessionState {
        *self.state.borrow()
    }

    fn handle_response(
        inflight: &mut HashMap<u32, InvocationContext>,
        frame: Frame,
        target: SocketAddr,
    ) {
        let stream_id = frame.stream_id;
        trace!(
            "Received {} response for stream-id={} from {}",
            frame
                .operation_code
                .variant_name()
                .unwrap_or("INVALID_OPCODE"),
            stream_id,
            target
        );

        if frame.operation_code == OperationCode::HEARTBEAT {
            return;
        }

        match inflight.remove(&stream_id) {
            Some(mut ctx) => {
                let mut response = response::Response::new(frame.operation_code);
                if frame.system_error() {
                    response.on_system_error(&frame);
                } else {
                    match frame.operation_code {
                        OperationCode::LIST_RANGE => {
                            response.on_list_ranges(&frame);
                        }

                        OperationCode::UNKNOWN => {
                            warn!("Received an unknown operation code");
                            return;
                        }

                        OperationCode::PING => todo!(),

                        OperationCode::GOAWAY => todo!(),

                        OperationCode::ALLOCATE_ID => {
                            response.on_allocate_id(&frame);
                        }

                        OperationCode::APPEND => {
                            response.on_append(&frame, &ctx);
                        }

                        OperationCode::FETCH => {
                            response.on_fetch(&frame, &ctx);
                        }

                        OperationCode::CREATE_RANGE => {
                            response.on_create_range(&frame, &ctx);
                        }

                        OperationCode::SEAL_RANGE => {
                            response.on_seal_range(&frame, &ctx);
                        }

                        OperationCode::SYNC_RANGE => {
                            warn!("Received an unexpected `SyncRanges` response");
                            return;
                        }

                        OperationCode::CREATE_STREAM => {
                            response.on_create_stream(&frame, &ctx);
                        }

                        OperationCode::DESCRIBE_STREAM => {
                            response.on_describe_stream(&frame, &ctx);
                        }

                        OperationCode::DELETE_STREAM => {
                            warn!("Received an unexpected `DeleteStreams` response");
                            return;
                        }

                        OperationCode::UPDATE_STREAM => {
                            warn!("Received an unexpected `UpdateStreams` response");
                            return;
                        }

                        OperationCode::TRIM_STREAM => todo!(),

                        OperationCode::UPDATE_STREAM_EPOCH => {
                            response.on_update_stream_epoch(&frame, &ctx);
                        }

                        OperationCode::REPORT_METRICS => {
                            response.on_report_metrics(&frame);
                        }

                        OperationCode::DESCRIBE_PLACEMENT_DRIVER => {
                            response.on_describe_placement_driver(&frame);
                        }

                        OperationCode::HEARTBEAT => {
                            unreachable!();
                        }

                        OperationCode::REPORT_REPLICA_PROGRESS => {
                            response.on_report_replica_progress(&frame);
                        }

                        OperationCode::COMMIT_OBJECT => {
                            response.on_commit_object(&frame);
                        }

                        OperationCode::LIST_RESOURCE => {
                            response.on_list_resource(&frame);
                        }

                        OperationCode::WATCH_RESOURCE => {
                            response.on_watch_resource(&frame);
                        }

                        _ => {
                            unreachable!("Unsupported operation code");
                        }
                    }
                }

                ctx.write_response(response);
            }
            None => {
                warn!(
                    "Expected inflight request[stream-id={}] is missing",
                    frame.stream_id
                );
            }
        }
    }
}

impl Clone for Session {
    fn clone(&self) -> Self {
        Self {
            target: self.target,
            config: Arc::clone(&self.config),
            connection: self.connection.clone(),
            inflight_requests: Rc::clone(&self.inflight_requests),
            idle_since: Rc::clone(&self.idle_since),
            role: Rc::clone(&self.role),
            state: Rc::clone(&self.state),
        }
    }
}

/// A `tower::Service` implementation for `Session`.
///
/// # Note feature `GAT` and `type-alias-impl-trait` are required.
impl Service<Request> for Session {
    type Response = Response;

    type Error = ClientError;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let this = self.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            if let Err(_e) = this.write(req, tx).await {
                return Err(ClientError::BadRequest);
            }
            rx.await.map_err(|_| {
                ClientError::ChannelClosing("Underlying connection is closed".to_owned())
            })
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use log::debug;
    use mock_server::run_listener;
    use std::{error::Error, time::Duration};
    use tower::timeout::Timeout;

    /// Verify it's OK to create a new session.
    #[test]
    fn test_new() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async {
            let port = run_listener().await;
            let target = format!("127.0.0.1:{}", port);
            let stream = TcpStream::connect(target.parse()?).await?;
            let config = Arc::new(config::Configuration::default());
            let sessions = Rc::new(RefCell::new(HashMap::new()));
            let (tx, _rx) = broadcast::channel(1);
            let _session = Session::new(
                target.parse()?,
                stream,
                &target,
                &config,
                Rc::downgrade(&sessions),
                tx,
            );

            Ok(())
        })
    }

    /// Verify it's OK to wrap `Session` into `Timeout` tower middleware.
    #[test]
    fn test_session_service() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        tokio_uring::start(async {
            let port = run_listener().await;
            let target = format!("127.0.0.1:{}", port);
            let stream = TcpStream::connect(target.parse()?).await?;
            let config = Arc::new(config::Configuration::default());
            let sessions = Rc::new(RefCell::new(HashMap::new()));
            let (tx, _rx) = broadcast::channel(1);
            let session = Session::new(
                target.parse()?,
                stream,
                &target,
                &config,
                Rc::downgrade(&sessions),
                tx,
            );

            let mut timeout_session = Timeout::new(session, Duration::from_secs(1));
            let req = crate::request::Request {
                timeout: Duration::from_secs(1),
                headers: request::Headers::Heartbeat {
                    client_id: "test".to_owned(),
                    role: ClientRole::CLIENT_ROLE_FRONTEND,
                    range_server: None,
                },
                body: None,
            };
            match timeout_session.call(req).await {
                Ok(_resp) => {
                    panic!("Should not receive a heartbeat response");
                }
                Err(e) => {
                    assert_eq!(&e.to_string(), "request timed out");
                }
            }
            Ok(())
        })
    }

    struct LogService<S> {
        inner: S,
    }

    impl<S, Request> tower::Service<Request> for LogService<S>
    where
        S: tower::Service<Request> + Clone,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, req: Request) -> Self::Future {
            let mut svc = self.inner.clone();
            async move {
                debug!("Before calling inner service");
                let res = svc.call(req).await;
                debug!("After calling inner service");
                res
            }
        }
    }

    /// Verify it's OK to wrap `Session` into `LogService` tower middleware.
    #[test]
    fn test_session_service_with_timeout_and_logging() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        tokio_uring::start(async {
            let port = run_listener().await;
            let target = format!("127.0.0.1:{}", port);
            let stream = TcpStream::connect(target.parse()?).await?;
            let config = Arc::new(config::Configuration::default());
            let sessions = Rc::new(RefCell::new(HashMap::new()));
            let (tx, _rx) = broadcast::channel(1);
            let session = Session::new(
                target.parse()?,
                stream,
                &target,
                &config,
                Rc::downgrade(&sessions),
                tx,
            );

            let timeout_session = Timeout::new(session, Duration::from_secs(1));
            let req = crate::request::Request {
                timeout: Duration::from_secs(1),
                headers: request::Headers::Heartbeat {
                    client_id: "test".to_owned(),
                    role: ClientRole::CLIENT_ROLE_FRONTEND,
                    range_server: None,
                },
                body: None,
            };
            let mut svc = LogService {
                inner: timeout_session,
            };
            match svc.call(req).await {
                Ok(_resp) => {
                    panic!("Should not receive a heartbeat response");
                }
                Err(e) => {
                    assert_eq!(&e.to_string(), "request timed out");
                }
            }
            Ok(())
        })
    }
}
