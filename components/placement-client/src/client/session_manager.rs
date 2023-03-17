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
use crate::{error::ClientError, notifier::Notifier};
use model::Status;
use super::{
    config::{self, ClientConfig},
    lb_policy::LBPolicy,
    naming::Endpoints,
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

    /// Session management
    lb_policy: LBPolicy,
    sessions: Rc<UnsafeCell<HashMap<SocketAddr, Session>>>,
    session_mgr_tx: mpsc::UnboundedSender<(SocketAddr, oneshot::Sender<bool>)>,

    /// MPMC channel
    stop_tx: mpsc::Sender<()>,
}

impl SessionManager {
    fn reconnect(
        mut reconnect_rx: mpsc::UnboundedReceiver<(SocketAddr, oneshot::Sender<bool>)>,
        sessions: Rc<UnsafeCell<HashMap<SocketAddr, Session>>>,
        config: Rc<ClientConfig>,
        log: Logger,
        notifier: Rc<dyn Notifier>,
    ) {
        let timeout = config.connect_timeout;
        tokio_uring::spawn(async move {
            while let Some((addr, tx)) = reconnect_rx.recv().await {
                trace!(log, "Creating a session to {}", addr);
                let sessions = unsafe { &mut *sessions.get() };
                match SessionManager::connect(&addr, timeout, &config, Rc::clone(&notifier), &log)
                    .await
                {
                    Ok(session) => {
                        sessions.insert(addr, session);
                        match tx.send(true) {
                            Ok(_) => {
                                trace!(log, "Session creation is notified");
                            }
                            Err(res) => {
                                debug!(log, "Failed to notify session creation result: `{}`", res);
                            }
                        }
                    }
                    Err(e) => {
                        error!(log, "Failed to connect to `{:?}`. Cause: `{:?}`", addr, e);
                        match tx.send(false) {
                            Ok(_) => {}
                            Err(res) => {
                                debug!(log, "Failed to notify session creation result: `{}`", res);
                            }
                        }
                    }
                }
            }
        });
    }

    fn heartbeat(
        logger: Logger,
        config: Rc<ClientConfig>,
        mut stop_rx: mpsc::Receiver<()>,
        sessions: Rc<UnsafeCell<HashMap<SocketAddr, Session>>>,
    ) {
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

    pub(crate) fn new(
        target: &str,
        config: &Rc<config::ClientConfig>,
        rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<response::Response>)>,
        notifier: Rc<dyn Notifier>,
        log: &Logger,
    ) -> Result<Self, ClientError> {
        let (reconnect_tx, reconnect_rx) =
            mpsc::unbounded_channel::<(SocketAddr, oneshot::Sender<bool>)>();
        let sessions = Rc::new(UnsafeCell::new(HashMap::new()));

        // Handle session re-connect event.
        Self::reconnect(
            reconnect_rx,
            Rc::clone(&sessions),
            Rc::clone(&config),
            log.clone(),
            Rc::clone(&notifier),
        );

        // Heartbeat
        let (stop_tx, stop_rx) = mpsc::channel::<()>(1);
        Self::heartbeat(
            log.clone(),
            Rc::clone(&config),
            stop_rx,
            Rc::clone(&sessions),
        );

        let endpoints = Endpoints::from_str(target)?;

        endpoints.addrs.into_iter().for_each(|socket_address| {
            let (tx, _rx) = oneshot::channel();
            match reconnect_tx.send((socket_address, tx)) {
                Ok(_) => {
                    trace!(log, "Notify to create a session to {}", socket_address);
                }
                Err(_e) => {
                    error!(log, "Failed to initiate connection to {}", socket_address);
                }
            }
        });

        Ok(Self {
            config: Rc::clone(config),
            rx,
            log: log.clone(),
            lb_policy: LBPolicy::PickFirst,
            session_mgr_tx: reconnect_tx,
            sessions,
            stop_tx,
        })
    }

    async fn poll_enqueue(&mut self) -> Result<(), ClientError> {
        trace!(self.log, "poll_enqueue"; "struct" => "SessionManager");
        match self.rx.recv().await {
            Some((request, response_observer)) => {
                trace!(self.log, "Received a request `{:?}`", request; "method" => "poll_enqueue");
                let sessions = Rc::clone(&self.sessions);
                let log = self.log.clone();
                let max_attempt_times = self.config.max_attempt as usize;
                let session_mgr = self.session_mgr_tx.clone();

                tokio_uring::spawn(async move {
                    SessionManager::dispatch(
                        log,
                        sessions,
                        session_mgr,
                        request,
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

    async fn dispatch(
        log: Logger,
        sessions: Rc<UnsafeCell<HashMap<SocketAddr, Session>>>,
        session_mgr: mpsc::UnboundedSender<(SocketAddr, oneshot::Sender<bool>)>,
        request: Request,
        mut response_observer: oneshot::Sender<response::Response>,
        max_attempt_times: usize,
    ) {
        trace!(log, "Received a request `{:?}`", request; "method" => "dispatch");
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
                            status: Status::internal("Connection timeout".to_owned()),
                            ranges: None,
                        };
                        match response_observer.send(response) {
                            Ok(_) => {
                                trace!(log, "Unavailable error response propagated");
                            }
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
            } else {
                warn!(log, "No active session is available, wait for 10ms");
                let start = std::time::Instant::now();
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                let elapsed = std::time::Instant::now() - start;
                trace!(log, "Waited for {}ms", elapsed.as_millis());
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
