use std::{
    cell::UnsafeCell, collections::HashMap, io::ErrorKind, net::SocketAddr, rc::Rc, str::FromStr,
    time::Duration,
};

use codec::frame::Frame;
use local_sync::{mpsc::unbounded, oneshot};
use monoio::{
    io::{OwnedWriteHalf, Splitable},
    net::TcpStream,
    time::Instant,
};
use slog::{debug, error, info, o, trace, warn, Discard, Logger};
use transport::channel::{ChannelReader, ChannelWriter};

use crate::{
    error::{ClientError, ListRangeError, SessionError},
    SessionState,
};

use self::naming::Endpoints;

mod config;
mod naming;
mod request;
mod response;

pub(crate) struct ClientBuilder {
    target: String,
    config: config::ClientConfig,
    log: Logger,
}

struct Session {
    idle_interval: Duration,

    log: Logger,

    state: SessionState,

    writer: ChannelWriter<OwnedWriteHalf<TcpStream>>,

    /// In-flight requests.
    inflight_requests: Rc<UnsafeCell<HashMap<u32, oneshot::Sender<response::Response>>>>,

    last_write_instant: Instant,
}

impl Session {
    fn new(stream: TcpStream, endpoint: &str, logger: &Logger) -> Self {
        let (read_half, write_half) = stream.into_split();
        let writer = ChannelWriter::new(write_half, &endpoint, logger.clone());
        let mut reader = ChannelReader::new(read_half, &endpoint, logger.clone());
        let inflights = Rc::new(UnsafeCell::new(HashMap::new()));

        {
            let inflights = Rc::clone(&inflights);
            let log = logger.clone();
            monoio::spawn(async move {
                let inflight_requests = inflights;
                loop {
                    match reader.read_frame().await {
                        Err(e) => {
                            // Handle connection reset
                            todo!()
                        }
                        Ok(Some(response)) => {
                            let inflights = unsafe { &mut *inflight_requests.get() };
                            Session::on_response(inflights, response, &log);
                        }
                        Ok(None) => {
                            // TODO: Handle normal connection close
                            break;
                        }
                    }
                }
            });
        }

        Self {
            log: logger.clone(),
            // TODO: configured from config
            idle_interval: Duration::from_secs(30),
            state: SessionState::Active,
            writer,
            inflight_requests: inflights,
            last_write_instant: Instant::now(),
        }
    }

    async fn write(
        &mut self,
        request: &request::Request,
        response_observer: oneshot::Sender<response::Response>,
    ) -> Result<(), oneshot::Sender<response::Response>> {
        Ok(())
    }

    async fn try_heartbeat(&mut self) -> Option<oneshot::Receiver<response::Response>> {
        let elapsed = Instant::now() - self.last_write_instant;
        if elapsed < self.idle_interval {
            return None;
        }

        let request = request::Request::Heartbeat;
        let (response_observer, rx) = oneshot::channel();
        if let Ok(_) = self.write(&request, response_observer).await {
            self.last_write_instant = Instant::now();
        }
        Some(rx)
    }

    fn on_response(
        inflights: &mut HashMap<u32, oneshot::Sender<response::Response>>,
        response: Frame,
        log: &Logger,
    ) {
        match inflights.remove(&response.stream_id) {
            Some(sender) => {
                let res = response::Response::ListRange;
                sender.send(res).unwrap_or_else(|_resp| {
                    warn!(log, "Failed to forward response to Client");
                });
            }
            None => {
                warn!(
                    log,
                    "Expected in-flight request[stream-id={}] is missing", response.stream_id
                );
            }
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let requests = unsafe { &mut *self.inflight_requests.get() };
        requests.drain().for_each(|(_stream_id, sender)| {
            sender
                .send(response::Response::ListRange)
                .unwrap_or_else(|_response| {
                    warn!(self.log, "Failed to notify connection reset");
                });
        });
    }
}

enum LBPolicy {
    PickFirst,
}

struct SessionManager {
    /// Configuration for the transport layer.
    config: config::ClientConfig,

    /// Receiver of SubmitRequestChannel.
    /// It is used by `Client` to submit requst to `SessionManager`. Requests are expected to be converted into `Command`s and then
    /// forwarded to transport layer.
    rx: unbounded::Rx<(request::Request, oneshot::Sender<response::Response>)>,

    log: Logger,

    /// Parsed endpoints from target url.
    endpoints: naming::Endpoints,

    // Session management
    lb_policy: LBPolicy,
    sessions: Rc<UnsafeCell<HashMap<SocketAddr, Session>>>,
    session_mgr_tx: unbounded::Tx<(SocketAddr, oneshot::Sender<bool>)>,

    // MPMC channel
    stop_tx: async_channel::Sender<()>,
}

impl SessionManager {
    fn new(
        target: &str,
        config: &config::ClientConfig,
        rx: unbounded::Rx<(request::Request, oneshot::Sender<response::Response>)>,
        log: &Logger,
    ) -> Result<Self, ClientError> {
        let (session_mgr_tx, mut session_mgr_rx) =
            unbounded::channel::<(SocketAddr, oneshot::Sender<bool>)>();
        let sessions = Rc::new(UnsafeCell::new(HashMap::new()));

        // Handle session re-connect event.
        {
            let sessions = Rc::clone(&sessions);
            let timeout = config.connect_timeout;
            let logger = log.clone();

            monoio::spawn(async move {
                loop {
                    match session_mgr_rx.recv().await {
                        Some((addr, tx)) => {
                            let sessions = unsafe { &mut *sessions.get() };
                            match SessionManager::connect(&addr, timeout, &logger).await {
                                Ok(session) => {
                                    sessions.insert(addr.clone(), session);
                                    match tx.send(true) {
                                        Ok(_) => {}
                                        Err(res) => {
                                            debug!(
                                                logger,
                                                "Failed to notify session creation result: `{}`",
                                                res
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        logger,
                                        "Failed to connect to `{:?}`. Cause: `{:?}`", addr, e
                                    );
                                    match tx.send(false) {
                                        Ok(_) => {}
                                        Err(res) => {
                                            debug!(
                                                logger,
                                                "Failed to notify session creation result: `{}`",
                                                res
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
            });
        }

        // Heartbeat
        let (stop_tx, stop_rx) = async_channel::bounded::<()>(1);
        {
            let sessions = Rc::clone(&sessions);
            let logger = log.clone();
            let idle_interval = config.heartbeat_interval;

            monoio::spawn(async move {
                monoio::pin! {
                    let stop_fut = stop_rx.recv();
                    let sleep = monoio::time::sleep(idle_interval);
                }

                loop {
                    monoio::select! {
                        _ = &mut stop_fut => {
                            info!(logger, "Got notified to stop");
                            break;
                        }

                        hb = &mut sleep => {
                            sleep.as_mut().reset(Instant::now() + idle_interval);

                            let sessions = unsafe {&mut *sessions.get()};
                            let mut futs = Vec::with_capacity(sessions.len());
                            for (_addr, session) in sessions.iter_mut() {
                                 futs.push(session.try_heartbeat());
                            }
                            futures::future::join_all(futs).await;
                        }
                    }
                }
            });
        }

        let endpoints = Endpoints::from_str(target)?;

        Ok(Self {
            config: config.clone(),
            rx,
            log: log.clone(),
            endpoints,
            lb_policy: LBPolicy::PickFirst,
            session_mgr_tx,
            sessions,
            stop_tx,
        })
    }

    async fn poll_enqueue(&mut self) -> Result<(), ClientError> {
        trace!(self.log, "poll_enqueue");
        match self.rx.recv().await {
            Some((req, response_observer)) => {
                trace!(self.log, "Received a request {:?}", req);
                self.dispatch(req, response_observer).await;
            }
            None => {
                return Err(ClientError::ChannelClosing(
                    "SubmitRequestChannel".to_owned(),
                ));
            }
        };
        Ok(())
    }

    async fn dispatch(
        &mut self,
        request: request::Request,
        mut response_observer: oneshot::Sender<response::Response>,
    ) {
        debug!(self.log, "Received a request `{:?}`", request);

        let sessions = unsafe { &mut *self.sessions.get() };
        if sessions.is_empty() {
            // Create a new session
        }

        let mut retry = 0;
        for (addr, session) in sessions.iter_mut() {
            retry += 1;
            if retry > 3 {
                break;
            }
            response_observer = match session.write(&request, response_observer).await {
                Ok(_) => {
                    trace!(self.log, "Request[`{request:?}`] forwarded to {addr:?}");
                    break;
                }
                Err(observer) => {
                    error!(self.log, "Failed to forward request to {addr:?}");
                    observer
                }
            }
        }
    }

    async fn run(&mut self) {
        trace!(self.log, "run");
        loop {
            if let Err(ClientError::ChannelClosing(_)) = self.poll_enqueue().await {
                info!(self.log, "SubmitRequsetChannel is half closed");
                break;
            }
        }
    }

    async fn connect(
        addr: &SocketAddr,
        timeout: Duration,
        log: &Logger,
    ) -> Result<Session, ClientError> {
        let endpoint = addr.to_string();
        let stream = monoio::net::TcpStream::connect_addr(addr.clone());
        let timeout = monoio::time::sleep(timeout);
        monoio::pin!(stream, timeout);
        let stream = monoio::select! {
            _ = timeout => {
                error!(log, "Timeout when connecting {}", endpoint);
                return Err(ClientError::ConnectTimeout(format!("Timeout when connecting {:?}", endpoint)));
            }
            conn = stream => {
                match conn {
                    Ok( connection) => {
                        connection.set_nodelay(true).map_err(|e| {
                            error!(log, "Failed to disable Nagle's algorithm. Cause: {:?}", e);
                            ClientError::DisableNagleAlgorithm
                        })?;
                        connection
                    },
                    Err(e) => {
                        match e.kind() {
                            ErrorKind::ConnectionRefused => {
                                error!(log, "Connection to {} is refused", endpoint);
                                return Err(ClientError::ConnectionRefused(format!("{:?}", endpoint)));
                            }
                            _ => {
                                return Err(ClientError::ConnectFailure(format!("{:?}", e)));
                            }
                        }

                    }
                }
            }
        };

        Ok(Session::new(stream, &endpoint, log))
    }
}

impl ClientBuilder {
    pub(crate) fn new(target: &str) -> Self {
        let drain = Discard;
        let root = Logger::root(drain, o!());
        Self {
            target: target.to_owned(),
            config: config::ClientConfig::default(),
            log: root,
        }
    }

    pub(crate) fn set_log(mut self, log: Logger) -> Self {
        self.log = log;
        self
    }

    pub(crate) fn set_config(mut self, config: config::ClientConfig) -> Self {
        self.config = config;
        self
    }

    pub(crate) async fn build(self) -> Result<Client, ClientError> {
        let (tx, rx) = unbounded::channel();

        let mut session_manager = SessionManager::new(&self.target, &self.config, rx, &self.log)?;
        monoio::spawn(async move {
            session_manager.run().await;
        });
        Ok(Client { tx })
    }
}

pub(crate) struct Client {
    tx: unbounded::Tx<(request::Request, oneshot::Sender<response::Response>)>,
}

impl Client {
    pub async fn list_range(
        &self,
        partition_id: i64,
    ) -> Result<response::Response, ListRangeError> {
        let (tx, rx) = oneshot::channel();
        let request = request::Request::ListRange {
            partition_id: partition_id,
        };
        self.tx
            .send((request, tx))
            .map_err(|e| ListRangeError::Internal)?;
        let result = rx.await.map_err(|e| ListRangeError::Internal)?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use monoio::net::TcpListener;
    use slog::Drain;

    use super::*;

    async fn run_listener(logger: Logger) -> u16 {
        let (tx, rx) = oneshot::channel();
        monoio::spawn(async move {
            let listener = TcpListener::bind("0.0.0.0:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            tx.send(port).unwrap();
            trace!(logger, "Listening 0.0.0.0:{}", port);
            listener.accept().await.unwrap();
            trace!(logger, "Accepted a connection");
        });
        rx.await.unwrap()
    }

    #[monoio::test(timer = true)]
    async fn test_builder() -> Result<(), ClientError> {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        let log = slog::Logger::root(drain, o!());

        let config = config::ClientConfig {
            connect_timeout: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(30),
        };

        let logger = log.clone();
        let port = run_listener(logger).await;
        let addr = format!("dns:localhost:{}", port);
        trace!(log, "Target endpoint: `{}`", addr);

        ClientBuilder::new(&addr)
            .set_log(log)
            .set_config(config)
            .build()
            .await?;
        Ok(())
    }

    #[ignore]
    #[monoio::test(timer = true)]
    async fn test_list_range() -> Result<(), ListRangeError> {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let log = slog::Logger::root(drain, o!());

        let port = run_listener(log.clone()).await;
        let addr = format!("dns:localhost:{}", port);
        let client = ClientBuilder::new(&addr)
            .set_log(log)
            .build()
            .await
            .map_err(|_e| ListRangeError::Internal)?;

        for i in 0..3 {
            client.list_range(i as i64).await?;
        }

        Ok(())
    }
}
