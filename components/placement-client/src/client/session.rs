use std::{cell::UnsafeCell, collections::HashMap, rc::Rc, time::Duration};

use codec::frame::Frame;
use local_sync::oneshot;
use monoio::{
    io::{OwnedWriteHalf, Splitable},
    net::TcpStream,
    time::Instant,
};
use slog::{trace, warn, Logger};
use transport::channel::{ChannelReader, ChannelWriter};

use crate::SessionState;

use super::{request, response};

pub(crate) struct Session {
    log: Logger,

    state: SessionState,

    writer: ChannelWriter<OwnedWriteHalf<TcpStream>>,

    /// In-flight requests.
    inflight_requests: Rc<UnsafeCell<HashMap<u32, oneshot::Sender<response::Response>>>>,

    last_rw_instant: Instant,
}

impl Session {
    pub(crate) fn new(stream: TcpStream, endpoint: &str, logger: &Logger) -> Self {
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
            state: SessionState::Active,
            writer,
            inflight_requests: inflights,
            last_rw_instant: Instant::now(),
        }
    }

    pub(crate) async fn write(
        &mut self,
        request: &request::Request,
        response_observer: oneshot::Sender<response::Response>,
    ) -> Result<(), oneshot::Sender<response::Response>> {
        trace!(self.log, "Sending request {:?}", request);

        // Update last read/write instant.
        self.last_rw_instant = Instant::now();

        response_observer
            .send(response::Response::ListRange)
            .unwrap_or_else(|res| {
                warn!(
                    self.log,
                    "Failed to send response to `Client`. Dropped {:?}", res
                );
            });
        trace!(self.log, "Response sent to `Client`");
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

        let request = request::Request::Heartbeat;
        let (response_observer, rx) = oneshot::channel();
        if let Ok(_) = self.write(&request, response_observer).await {
            trace!(self.log, "Heartbeat sent to {}", self.writer.peer_address());
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

#[cfg(test)]
mod tests {

    use std::error::Error;

    use util::test::{run_listener, terminal_logger};

    use super::*;

    /// Verify it's OK to create a new session.
    #[monoio::test]
    async fn test_new() -> Result<(), Box<dyn Error>> {
        let logger = terminal_logger();
        let port = run_listener(logger.clone()).await;
        let target = format!("127.0.0.1:{}", port);
        let stream = TcpStream::connect(&target).await?;
        let session = Session::new(stream, &target, &logger);

        assert_eq!(SessionState::Active, session.state());

        assert_eq!(false, session.need_heartbeat(&Duration::from_secs(1)));

        Ok(())
    }

    #[monoio::test]
    async fn test_heartbeat() -> Result<(), Box<dyn Error>> {
        let logger = terminal_logger();
        let port = run_listener(logger.clone()).await;
        let target = format!("127.0.0.1:{}", port);
        let stream = TcpStream::connect(&target).await?;
        let mut session = Session::new(stream, &target, &logger);

        let result = session.heartbeat().await;
        let response = result.unwrap().await;
        assert_eq!(true, response.is_ok());

        Ok(())
    }
}
