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
    idle_interval: Duration,

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
            // TODO: configured from config
            idle_interval: Duration::from_secs(30),
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

    pub(crate) async fn try_heartbeat(&mut self) -> Option<oneshot::Receiver<response::Response>> {
        let elapsed = Instant::now() - self.last_rw_instant;
        if elapsed < self.idle_interval {
            return None;
        }

        let request = request::Request::Heartbeat;
        let (response_observer, rx) = oneshot::channel();
        if let Ok(_) = self.write(&request, response_observer).await {
            trace!(
                self.log,
                "Heartbeat sent to {}",
                self.writer.peer_address()
            );
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
