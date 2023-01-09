use std::{
    cell::UnsafeCell, collections::HashMap, io::ErrorKind, net::SocketAddr, rc::Rc, str::FromStr,
    time::Duration,
};

use local_sync::{mpsc::unbounded, oneshot};
use monoio::time::Instant;
use slog::{debug, error, info, trace, warn, Logger};

use crate::error::ClientError;

use super::{
    config,
    naming::{self, Endpoints},
    request, response,
    session::Session,
    LBPolicy,
};

pub struct SessionManager {
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
    pub(crate) fn new(
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
                                        Ok(_) => {
                                            trace!(logger, "Session creation is notified");
                                        }
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

                    // Interval to check if a session needs to send a heartbeat request.
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
        trace!(self.log, "poll_enqueue"; "struct" => "SessionManager");
        match self.rx.recv().await {
            Some((req, response_observer)) => {
                trace!(self.log, "Received a request `{:?}`", req; "method" => "poll_enqueue");
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
        trace!(self.log, "Received a request `{:?}`", request; "method" => "dispatch");

        let sessions = unsafe { &mut *self.sessions.get() };
        if sessions.is_empty() {
            // Create a new session
            match self.lb_policy {
                LBPolicy::PickFirst => {
                    if let Some(&socket_addr) = self.endpoints.get() {
                        let (tx, rx) = oneshot::channel();
                        self.session_mgr_tx
                            .send((socket_addr.clone(), tx))
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
                            }
                        }
                    } else {
                        error!(self.log, "No endpoints available to connect.");
                        response_observer
                            .send(response::Response::ListRange)
                            .unwrap_or_else(|_response| {
                                warn!(self.log, "Failed to write response to `Client`");
                            });
                        return;
                    }
                }
            }
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

    pub(super) async fn run(&mut self) {
        trace!(self.log, "run"; "struct" => "SessionManager");
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
        trace!(log, "Establishing connection to {:?}", addr);
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
                        trace!(log, "Connection to {:?} established", addr);
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
