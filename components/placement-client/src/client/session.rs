use std::{
    cell::UnsafeCell,
    collections::HashMap,
    rc::Rc,
    time::{Duration, Instant},
};

use codec::frame::{Frame, OperationCode};
use model::{request::Request, client_role::ClientRole};
use slog::{error, trace, warn, Logger};
use tokio::sync::oneshot;
use tokio_uring::net::TcpStream;
use transport::channel::{ChannelReader, ChannelWriter};

use super::session_state::SessionState;
use crate::client::response::Status;

use super::{config, response};

pub(crate) struct Session {
    config: Rc<config::ClientConfig>,

    log: Logger,

    state: SessionState,

    writer: ChannelWriter,

    /// In-flight requests.
    inflight_requests: Rc<UnsafeCell<HashMap<u32, oneshot::Sender<response::Response>>>>,

    last_rw_instant: Instant,
}

impl Session {
    pub(crate) fn new(
        stream: TcpStream,
        endpoint: &str,
        config: &Rc<config::ClientConfig>,
        logger: &Logger,
    ) -> Self {
        let stream = Rc::new(stream);
        let writer = ChannelWriter::new(Rc::clone(&stream), endpoint, logger.clone());
        let mut reader = ChannelReader::new(Rc::clone(&stream), endpoint, logger.clone());
        let in_flights = Rc::new(UnsafeCell::new(HashMap::new()));

        {
            let in_flights = Rc::clone(&in_flights);
            let log = logger.clone();
            tokio_uring::spawn(async move {
                let inflight_requests = in_flights;
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
            config: Rc::clone(config),
            log: logger.clone(),
            state: SessionState::Active,
            writer,
            inflight_requests: in_flights,
            last_rw_instant: Instant::now(),
        }
    }

    pub(crate) async fn write(
        &mut self,
        request: &Request,
        response_observer: oneshot::Sender<response::Response>,
    ) -> Result<(), oneshot::Sender<response::Response>> {
        trace!(self.log, "Sending request {:?}", request);

        // Update last read/write instant.
        self.last_rw_instant = Instant::now();

        match request {
            Request::Heartbeat { .. } => {
                let mut frame = Frame::new(OperationCode::Heartbeat);
                let header = request.into();
                frame.header = Some(header);
                match self.writer.write_frame(&frame).await {
                    Ok(_) => {
                        let inflight_requests = unsafe { &mut *self.inflight_requests.get() };
                        inflight_requests.insert(frame.stream_id, response_observer);
                        trace!(
                            self.log,
                            "Write `Heartbeat` request to {}",
                            self.writer.peer_address()
                        );
                    }
                    Err(e) => {
                        error!(
                            self.log,
                            "Failed to write request to network. Cause: {:?}", e
                        );
                    }
                }
            }

            Request::ListRanges { .. } => {
                let mut list_range_frame = Frame::new(OperationCode::ListRanges);
                let header = request.into();
                list_range_frame.header = Some(header);

                match self.writer.write_frame(&list_range_frame).await {
                    Ok(_) => {
                        let inflight_requests = unsafe { &mut *self.inflight_requests.get() };
                        inflight_requests.insert(list_range_frame.stream_id, response_observer);
                        trace!(
                            self.log,
                            "Write `ListRange` request to {}",
                            self.writer.peer_address()
                        );
                    }
                    Err(e) => {
                        warn!(
                            self.log,
                            "Failed to write `ListRange` request to {}. Cause: {:?}",
                            self.writer.peer_address(),
                            e
                        );
                        return Err(response_observer);
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
        self.active() && (Instant::now() - self.last_rw_instant >= *duration)
    }

    pub(crate) async fn heartbeat(&mut self) -> Option<oneshot::Receiver<response::Response>> {
        // If the current session is being closed, heartbeat will be no-op.
        if self.state == SessionState::Closing {
            return None;
        }

        let request = Request::Heartbeat {
            client_id: self.config.client_id.clone(),
            role: ClientRole::DataNode,
            data_node: None
        };
        let (response_observer, rx) = oneshot::channel();
        if self.write(&request, response_observer).await.is_ok() {
            trace!(self.log, "Heartbeat sent to {}", self.writer.peer_address());
        }
        Some(rx)
    }

    fn on_response(
        inflights: &mut HashMap<u32, oneshot::Sender<response::Response>>,
        response: Frame,
        log: &Logger,
    ) {
        let stream_id = response.stream_id;
        trace!(
            log,
            "Received {} response for stream-id={}",
            response.operation_code,
            stream_id
        );

        match inflights.remove(&stream_id) {
            Some(sender) => {
                let res = match response.operation_code {
                    OperationCode::Heartbeat => {
                        trace!(log, "Mock parsing {} response", response.operation_code);
                        response::Response::Heartbeat { status: Status::OK }
                    }
                    OperationCode::ListRanges => {
                        trace!(log, "Mock parsing {} response", response.operation_code);
                        response::Response::ListRange {
                            status: Status::OK,
                            ranges: None,
                        }
                    }
                    _ => {
                        warn!(log, "Unsupported operation {}", response.operation_code);
                        return;
                    }
                };

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
            let aborted_response = response::Response::ListRange {
                status: Status::Aborted,
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

    use std::error::Error;

    use test_util::{run_listener, terminal_logger};

    use super::*;

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
            let port = run_listener(logger.clone()).await;
            let target = format!("127.0.0.1:{}", port);
            let stream = TcpStream::connect(target.parse()?).await?;
            let config = Rc::new(config::ClientConfig::default());
            let mut session = Session::new(stream, &target, &config, &logger);

            let result = session.heartbeat().await;
            let response = result.unwrap().await;
            assert_eq!(true, response.is_ok());
            Ok(())
        })
    }
}
