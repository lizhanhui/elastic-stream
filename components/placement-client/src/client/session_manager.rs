use std::{
    borrow::Borrow,
    cell::UnsafeCell,
    collections::{HashMap, HashSet},
    io::ErrorKind,
    net::SocketAddr,
    rc::Rc,
    str::FromStr,
    time::Duration,
};

use model::request::Request;
use slog::{debug, error, info, trace, warn, Logger};
use tokio::{
    sync::{mpsc, oneshot},
    time::{timeout, Instant},
};
use tokio_uring::net::TcpStream;

use crate::{client::response::Status, error::ClientError, notifier::Notifier};

use super::{
    config,
    lb_policy::LBPolicy,
    naming::{self, Endpoints},
    response,
    session::Session,
};

pub struct SessionManager {
    /// Configuration for the transport layer.
    config: Rc<config::ClientConfig>,

    /// Receiver of SubmitRequestChannel.
    /// It is used by `Client` to submit request to `SessionManager`. Requests are expected to be converted into `Command`s and then
    /// forwarded to transport layer.
    rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<response::Response>)>,

    log: Logger,

    /// Parsed endpoints from target url.
    endpoints: naming::Endpoints,

    /// Session management
    lb_policy: LBPolicy,
    sessions: Rc<UnsafeCell<HashMap<SocketAddr, Session>>>,
    session_mgr_tx: mpsc::UnboundedSender<(SocketAddr, oneshot::Sender<bool>)>,

    /// MPMC channel
    stop_tx: mpsc::Sender<()>,
}

impl SessionManager {
    pub(crate) fn new(
        target: &str,
        config: &Rc<config::ClientConfig>,
        rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<response::Response>)>,
        notifier: Rc<dyn Notifier>,
        log: &Logger,
    ) -> Result<Self, ClientError> {
        let (session_mgr_tx, mut session_mgr_rx) =
            mpsc::unbounded_channel::<(SocketAddr, oneshot::Sender<bool>)>();
        let sessions = Rc::new(UnsafeCell::new(HashMap::new()));

        // Handle session re-connect event.
        {
            let sessions = Rc::clone(&sessions);
            let timeout = config.connect_timeout;
            let logger = log.clone();
            let _config = Rc::clone(config);
            let _notifier = Rc::clone(&notifier);
            tokio_uring::spawn(async move {
                let config = _config;
                let notifier = _notifier;
                while let Some((addr, tx)) = session_mgr_rx.recv().await {
                    let sessions = unsafe { &mut *sessions.get() };
                    match SessionManager::connect(
                        &addr,
                        timeout,
                        &config,
                        Rc::clone(&notifier),
                        &logger,
                    )
                    .await
                    {
                        Ok(session) => {
                            sessions.insert(addr, session);
                            match tx.send(true) {
                                Ok(_) => {
                                    trace!(logger, "Session creation is notified");
                                }
                                Err(res) => {
                                    debug!(
                                        logger,
                                        "Failed to notify session creation result: `{}`", res
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
                                        "Failed to notify session creation result: `{}`", res
                                    );
                                }
                            }
                        }
                    }
                }
            });
        }

        // Heartbeat
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        {
            let sessions = Rc::clone(&sessions);
            let logger = log.clone();
            let idle_interval = config.heartbeat_interval;

            tokio_uring::spawn(async move {
                tokio::pin! {
                    let stop_fut = stop_rx.recv();

                    // Interval to check if a session needs to send a heartbeat request.
                    let sleep = tokio::time::sleep(idle_interval);
                }

                loop {
                    tokio::select! {
                        _ = &mut stop_fut => {
                            info!(logger, "Got notified to stop");
                            break;
                        }

                        hb = &mut sleep => {
                            sleep.as_mut().reset(Instant::now() + idle_interval);

                            let sessions = unsafe {&mut *sessions.get()};
                            let mut futs = Vec::with_capacity(sessions.len());
                            for (_addr, session) in sessions.iter_mut() {
                                if session.need_heartbeat(&idle_interval) {
                                    futs.push(session.heartbeat());
                                }
                            }
                            futures::future::join_all(futs).await;
                        }
                    }
                }
            });
        }

        let endpoints = Endpoints::from_str(target)?;

        Ok(Self {
            config: Rc::clone(config),
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
        trace!(self.log, "poll_enqueue"; "struct" => "SessionManager");
        self.init_sessions().await?;
        match self.rx.recv().await {
            Some((req, response_observer)) => {
                trace!(self.log, "Received a request `{:?}`", req; "method" => "poll_enqueue");
                let sessions = Rc::clone(&self.sessions);
                let log = self.log.clone();
                let max_attempt_times = self.config.max_attempt as usize;
                let session_mgr = self.session_mgr_tx.clone();

                tokio_uring::spawn(async move {
                    SessionManager::dispatch(
                        log,
                        sessions,
                        session_mgr,
                        req,
                        response_observer,
                        max_attempt_times,
                    )
                    .await;
                });
            }
            None => {
                return Err(ClientError::ChannelClosing(
                    "SubmitRequestChannel".to_owned(),
                ));
            }
        };
        Ok(())
    }

    async fn init_sessions(&mut self) -> Result<(), ClientError> {
        let sessions = unsafe { &mut *self.sessions.get() };
        if sessions.is_empty() {
            // Create a new session
            match self.lb_policy {
                LBPolicy::PickFirst => {
                    if let Some(&socket_addr) = self.endpoints.get() {
                        let (tx, rx) = oneshot::channel();
                        self.session_mgr_tx
                            .send((socket_addr, tx))
                            .unwrap_or_else(|e| {
                                error!(self.log, "Failed to create a new session. Cause: {:?}", e);
                            });
                        match rx.await {
                            Ok(result) => {
                                if result {
                                    trace!(self.log, "Session to {:?} was created", socket_addr);
                                } else {
                                    warn!(
                                        self.log,
                                        "Failed to create session to {:?}", socket_addr
                                    );
                                }
                            }
                            Err(e) => {
                                warn!(
                                    self.log,
                                    "Got an error while creating session to {:?}. Error: {:?}",
                                    socket_addr,
                                    e
                                );
                                return Err(ClientError::ConnectFailure(e.to_string()));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn dispatch(
        log: Logger,
        sessions: Rc<UnsafeCell<HashMap<SocketAddr, Session>>>,
        session_mgr: mpsc::UnboundedSender<(SocketAddr, oneshot::Sender<bool>)>,
        request: Request,
        mut response_observer: oneshot::Sender<response::Response>,
        max_attempt_times: usize,
    ) {
        trace!(log, "Received a request `{:?}`", request; "method" => "dispatch");

        loop {
            let sessions = unsafe { &mut *sessions.get() };
            if !sessions.is_empty() {
                break;
            }
        }

        let sessions = unsafe { &mut *sessions.get() };
        let mut attempt = 0;
        let mut attempted = HashSet::new();
        loop {
            attempt += 1;
            if attempt > max_attempt_times {
                match request {
                    Request::Heartbeat { .. } => {}
                    Request::ListRanges { .. } => {
                        let response = response::Response::ListRange {
                            status: Status::Unavailable,
                            ranges: None,
                        };
                        match response_observer.send(response) {
                            Ok(_) => {}
                            Err(e) => {
                                warn!(log, "Failed to propagate error response. Cause: {:?}", e);
                            }
                        }
                    }
                }
                break;
            }
            trace!(
                log,
                "Attempt to write {:?} for the {} time",
                request,
                ordinal::Ordinal(attempt)
            );

            let res = sessions
                .iter_mut()
                .try_find(|(k, _)| Some(!attempted.contains(k.borrow())))
                .map(|e| {
                    if let Some((k, v)) = e {
                        attempted.insert(k.clone());
                        Some((k, v))
                    } else {
                        None
                    }
                })
                .flatten();

            if let Some((addr, session)) = res {
                response_observer = match session.write(&request, response_observer).await {
                    Ok(_) => {
                        trace!(log, "Request[`{request:?}`] forwarded to {addr:?}");
                        break;
                    }
                    Err(observer) => {
                        error!(log, "Failed to forward request to {addr:?}");
                        observer
                    }
                }
            }
        }
    }

    pub(super) async fn run(&mut self) {
        trace!(self.log, "run"; "struct" => "SessionManager");
        loop {
            if let Err(ClientError::ChannelClosing(_)) = self.poll_enqueue().await {
                info!(self.log, "SubmitRequestChannel is half closed");
                break;
            }
        }
    }

    async fn connect(
        addr: &SocketAddr,
        duration: Duration,
        config: &Rc<config::ClientConfig>,
        notifier: Rc<dyn Notifier>,
        log: &Logger,
    ) -> Result<Session, ClientError> {
        trace!(log, "Establishing connection to {:?}", addr);
        let endpoint = addr.to_string();
        let connect = TcpStream::connect(*addr);
        let stream = match timeout(duration, connect).await {
            Ok(res) => match res {
                Ok(connection) => {
                    trace!(log, "Connection to {:?} established", addr);
                    connection.set_nodelay(true).map_err(|e| {
                        error!(log, "Failed to disable Nagle's algorithm. Cause: {:?}", e);
                        ClientError::DisableNagleAlgorithm
                    })?;
                    connection
                }
                Err(e) => match e.kind() {
                    ErrorKind::ConnectionRefused => {
                        error!(log, "Connection to {} is refused", endpoint);
                        return Err(ClientError::ConnectionRefused(format!("{:?}", endpoint)));
                    }
                    _ => {
                        return Err(ClientError::ConnectFailure(format!("{:?}", e)));
                    }
                },
            },
            Err(e) => {
                let description = format!("Timeout when connecting {}, elapsed: {}", endpoint, e);
                error!(log, "{}", description);
                return Err(ClientError::ConnectTimeout(description));
            }
        };

        Ok(Session::new(stream, &endpoint, config, notifier, log))
    }
}
