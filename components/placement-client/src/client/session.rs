use std::{cell::UnsafeCell, collections::HashMap, rc::Rc, time::Duration};

use bytes::BytesMut;
use codec::frame::{Frame, HeaderFormat, OperationCode};
use local_sync::oneshot;
use monoio::{
    io::{OwnedWriteHalf, Splitable},
    net::TcpStream,
    time::Instant,
};
use slog::{error, trace, warn, Logger};
use transport::channel::{ChannelReader, ChannelWriter};

use crate::{
    client::response::Status,
    generated::rpc_generated::elastic::store::{
        Heartbeat, HeartbeatArgs, ListRange, ListRangeArgs,
    },
    SessionState,
};

use super::{
    config,
    request::{self, Request},
    response,
};

pub(crate) struct Session {
    config: Rc<config::ClientConfig>,

    log: Logger,

    state: SessionState,

    writer: ChannelWriter<OwnedWriteHalf<TcpStream>>,

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
            config: Rc::clone(config),
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

        match request {
            request::Request::Heartbeat { .. } => {
                let mut frame = Frame::new(OperationCode::Heartbeat);
                Session::build_frame_header(&mut frame, request);
                match self.writer.write_frame(&frame).await {
                    Ok(_) => {
                        let inflight_requests = unsafe { &mut *self.inflight_requests.get() };
                        inflight_requests.insert(frame.stream_id, response_observer);
                        trace!(
                            self.log,
                            "Write `ListRange` request to {}",
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

            request::Request::ListRange { .. } => {
                let mut list_range_frame = Frame::new(OperationCode::ListRange);
                Session::build_frame_header(&mut list_range_frame, request);

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

    fn build_frame_header(frame: &mut Frame, request: &Request) {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        match request {
            Request::Heartbeat { client_id } => {
                let client_id = builder.create_string(client_id);
                let heartbeat = Heartbeat::create(
                    &mut builder,
                    &HeartbeatArgs {
                        client_id: Some(client_id),
                    },
                );
                builder.finish(heartbeat, None);
            }

            Request::ListRange { partition_id } => {
                let list_range = ListRange::create(
                    &mut builder,
                    &ListRangeArgs {
                        partition_id: *partition_id,
                    },
                );
                builder.finish(list_range, None);
            }
        }

        let buf = builder.finished_data();
        let mut buffer = BytesMut::with_capacity(buf.len());
        buffer.extend_from_slice(buf);
        frame.header = Some(buffer.freeze());
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

        let request = request::Request::Heartbeat {
            client_id: self.config.client_id.clone(),
        };
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
                let res = response::Response::ListRange {
                    status: Status::Internal,
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
        let aborted_response = response::Response::ListRange {
            status: Status::Aborted,
        };
        requests.drain().for_each(|(_stream_id, sender)| {
            sender.send(aborted_response).unwrap_or_else(|_response| {
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
        let config = Rc::new(config::ClientConfig::default());
        let session = Session::new(stream, &target, &config, &logger);

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
        let config = Rc::new(config::ClientConfig::default());
        let mut session = Session::new(stream, &target, &config, &logger);

        let result = session.heartbeat().await;
        // let response = result.unwrap().await;
        // assert_eq!(true, response.is_ok());

        Ok(())
    }
}
