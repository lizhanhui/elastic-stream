use crate::{
    error::ClientError,
    heartbeat::HeartbeatData,
    request::{self, Request},
    response::{self, Response},
    session_state::SessionState,
    NodeRole,
};
use codec::{error::FrameError, frame::Frame};
use futures::Future;
use protocol::rpc::header::{ClientRole, GoAwayFlags, OperationCode, RangeServerState};
use tower::Service;
use transport::{connection::Connection, connection_state::ConnectionState};

use local_sync::oneshot;
use log::{error, info, trace, warn};
use std::{
    cell::{RefCell, UnsafeCell},
    collections::HashMap,
    fmt::Display,
    net::SocketAddr,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};
use tokio::sync::broadcast::{self, error::RecvError};
use tokio_uring::net::TcpStream;

use crate::invocation_context::InvocationContext;

pub(crate) struct Session {
    config: Arc<config::Configuration>,

    // Unlike {tokio, monoio}::TcpStream where we need to split underlying TcpStream into two owned mutable,
    // tokio_uring::TcpStream requires immutable references only to perform read/write.
    pub(crate) connection: Rc<Connection>,

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
    fn spawn_read_loop(
        connection: Rc<Connection>,
        inflight_requests: Rc<UnsafeCell<HashMap<u32, InvocationContext>>>,
        state: Rc<RefCell<SessionState>>,
        mut shutdown: broadcast::Receiver<()>,
    ) {
        tokio_uring::spawn(async move {
            trace!("Start read loop for session[{}]", connection);
            loop {
                tokio::select! {
                    stop = shutdown.recv() => {
                        match stop {
                            Ok(_) => {
                                info!("Received a shutdown signal. Stop session read loop");
                            },
                            Err(RecvError::Closed) => {
                                // should NOT reach here.
                                info!("Shutdown broadcast channel is closed. Stop session read loop");
                            },
                            Err(RecvError::Lagged(_)) => {
                                // should not reach here.
                                info!("Shutdown broadcast channel is lagged behind. Stop session read loop");
                            }
                        }
                        break;
                    },
                    read = connection.read_frame() => {
                        match read {
                            Err(e) => {
                                match e {
                                    FrameError::Incomplete => {
                                        // More data needed
                                        continue;
                                    }
                                    FrameError::ConnectionReset => {
                                        error!( "Connection {} is reset by peer", connection);
                                    }
                                    FrameError::BadFrame(message) => {
                                        error!( "Read a bad frame from connection {}. Cause: {}", connection, message);
                                    }
                                    FrameError::TooLongFrame{found, max} => {
                                        error!( "Read a frame with excessive length={}, max={}, from connection {}", found, max, connection);
                                    }
                                    FrameError::MagicCodeMismatch{found, expected} => {
                                        error!( "Read a frame with incorrect magic code. Expected={}, actual={}, from connection {}", expected, found, connection);
                                    }
                                    FrameError::TooLongFrameHeader{found, expected} => {
                                        error!( "Read a frame with excessive header length={}, max={}, from connection {}", found, expected, connection);
                                    }
                                    FrameError::PayloadChecksumMismatch{expected, actual} => {
                                        error!( "Read a frame with incorrect payload checksum. Expected={}, actual={}, from connection {}", expected, actual, connection);
                                    }
                                }
                                break;
                            }
                            Ok(Some(frame)) => {
                                trace!( "Read a frame from channel {}", connection);
                                let inflight = unsafe { &mut *inflight_requests.get() };
                                if frame.is_response() {
                                    Session::handle_response(inflight, frame, connection.remote_addr());
                                } else if frame.operation_code == OperationCode::GOAWAY {
                                    *state.borrow_mut() = SessionState::GoAway;
                                    let reason = if frame.has_go_away_flag(GoAwayFlags::SERVER_MAINTENANCE) {
                                        "server maintenance"
                                    } else {"connection being idle"};
                                    info!("Received go-away from [{}] because of '{}'.", connection, reason);
                                } else {
                                    warn!( "Received an unexpected request frame from {}", connection);
                                }
                            }
                            Ok(None) => {
                                info!( "Connection {} is closed by peer", connection);
                                break;
                            }

                        }
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
            if let Err(e) = connection.close() {
                warn!("Failed to close connection {}: {:?}", connection, e);
            }
            info!("Read loop for session[{}] completed", connection);
        });
    }

    pub(crate) fn new(
        remote_addr: SocketAddr,
        stream: TcpStream,
        config: &Arc<config::Configuration>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        let connection = Rc::new(Connection::new(stream, remote_addr));
        let inflight = Rc::new(UnsafeCell::new(HashMap::new()));

        let state = Rc::new(RefCell::new(SessionState::Active));

        Self::spawn_read_loop(
            Rc::clone(&connection),
            Rc::clone(&inflight),
            Rc::clone(&state),
            shutdown.subscribe(),
        );

        Self {
            config: Arc::clone(config),
            connection,
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
        trace!("Sending {} to {}", request, self.connection);

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
            request::Headers::UpdateStream { .. } => {
                frame.operation_code = OperationCode::UPDATE_STREAM;
            }
            request::Headers::TrimStream { .. } => {
                frame.operation_code = OperationCode::TRIM_STREAM;
            }
            request::Headers::DeleteStream { .. } => {
                frame.operation_code = OperationCode::DELETE_STREAM;
            }
        };

        frame.payload = request.body.clone();

        let inflight_requests = unsafe { &mut *self.inflight_requests.get() };
        let context = InvocationContext::new(
            self.connection.remote_addr(),
            request.clone(),
            response_observer,
        );
        inflight_requests.insert(frame.stream_id, context);

        // Write frame to network
        let stream_id = frame.stream_id;
        let opcode = frame.operation_code;
        match self.connection.write_frame(frame).await {
            Ok(_) => {
                trace!(
                    "Write request[opcode={}] bounded for {} using stream-id={} to socket buffer",
                    opcode.variant_name().unwrap_or("INVALID_OPCODE"),
                    self.connection.remote_addr(),
                    stream_id,
                );
            }
            Err(e) => {
                error!(
                    "Failed to write request[opcode={}] bounded for {} to socket buffer. Cause: {:?}",
                    opcode.variant_name().unwrap_or("INVALID_OPCODE"), self.connection.remote_addr(), e
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
                    self.connection.remote_addr(),
                    stream_id,
                );
                *self.idle_since.borrow_mut() = Instant::now();
            }
            Err(e) => {
                error!(
                    "Failed to write request[opcode={}] bounded for {} to socket buffer. Cause: {:?}",
                    opcode.variant_name().unwrap_or("INVALID_OPCODE"), self.connection.remote_addr(), e
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
                self.connection.remote_addr(),
                current,
                state
            );
            *self.role.borrow_mut() = state;
        }
    }

    pub fn going_away(&self) -> bool {
        *self.state.borrow() == SessionState::GoAway
    }

    pub fn active(&self) -> bool {
        match self.connection.state() {
            ConnectionState::Active => {}
            ConnectionState::Closed => return false,
        }

        return *self.state.borrow() == SessionState::Active;
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
                            response.on_delete_stream(&frame);
                        }

                        OperationCode::UPDATE_STREAM => {
                            response.on_update_stream(&frame);
                        }

                        OperationCode::TRIM_STREAM => {
                            response.on_trim_stream(&frame);
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
            config: Arc::clone(&self.config),
            connection: Rc::clone(&self.connection),
            inflight_requests: Rc::clone(&self.inflight_requests),
            idle_since: Rc::clone(&self.idle_since),
            role: Rc::clone(&self.role),
            state: Rc::clone(&self.state),
        }
    }
}

impl Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.connection.fmt(f)
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
    fn test_session_new() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        tokio_uring::start(async {
            let port = run_listener().await;
            let target = format!("127.0.0.1:{}", port);
            let stream = TcpStream::connect(target.parse()?).await?;
            let config = Arc::new(config::Configuration::default());
            let (tx, _rx) = broadcast::channel(1);
            let _session = Session::new(target.parse()?, stream, &config, tx);

            Ok(())
        })
    }

    /// Verify it's OK to wrap `Session` into `Timeout` tower middleware.
    #[test]
    fn test_session_service_heartbeat() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        tokio_uring::start(async {
            let port = run_listener().await;
            let target = format!("127.0.0.1:{}", port);
            let stream = TcpStream::connect(target.parse()?).await?;
            let config = Arc::new(config::Configuration::default());
            let (tx, _rx) = broadcast::channel(1);
            let session = Session::new(target.parse()?, stream, &config, tx.clone());
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
            let (tx, _rx) = broadcast::channel(1);
            let session = Session::new(target.parse()?, stream, &config, tx.clone());
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
