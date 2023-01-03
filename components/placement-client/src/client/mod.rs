use futures::Future;
use local_sync::{mpsc::unbounded, oneshot};
use pin_project::pin_project;
use slog::{o, trace, warn, Discard, Logger};
use std::{
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
};

use crate::error::{ClientError, ListRangeError};

mod config;
mod naming;
mod request;
mod response;

pub(crate) struct ClientBuilder {
    config: config::ClientConfig,
    log: Logger,
}

#[pin_project]
struct SessionManager {
    #[pin]
    rx: unbounded::Rx<(request::Request, oneshot::Sender<response::Response>)>,

    log: Logger,
}

impl Future for SessionManager {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        trace!(self.log, "poll");
        let mut this = self.project();
        let log = this.log;
        loop {
            match this.rx.poll_recv(cx) {
                Poll::Ready(Some((req, tx))) => {
                    trace!(log, "Received a request {:?}", req);
                    // Generate mock response
                    let response = response::Response::ListRange;
                    match tx.send(response) {
                        Ok(_) => {}
                        Err(_r) => {
                            warn!(log, "Failed to send response to one-shot channel");
                        }
                    }
                }
                Poll::Ready(None) => {
                    return Poll::Ready(());
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            };
        }
    }
}

impl ClientBuilder {
    pub(crate) fn new() -> Self {
        let drain = Discard;
        let root = Logger::root(drain, o!());
        Self {
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

    pub(crate) fn connect(self, addr: &str) -> Result<Client, ClientError> {
        let endpoints = naming::Endpoints::from_str(addr)?;
        let (tx, rx) = unbounded::channel();
        monoio::spawn(SessionManager {
            rx,
            log: self.log.clone(),
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

    use slog::Drain;

    use super::*;

    #[monoio::test]
    async fn test_builder() -> Result<(), ClientError> {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        let log = slog::Logger::root(drain, o!());

        let config = config::ClientConfig {
            connect_timeout: Duration::from_secs(5),
        };

        let addr = "dns:localhost:1234";

        ClientBuilder::new()
            .set_log(log)
            .set_config(config)
            .connect(addr)?;
        Ok(())
    }

    #[monoio::test]
    async fn test_list_range() -> Result<(), ListRangeError> {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let log = slog::Logger::root(drain, o!());

        let addr = "dns:localhost:1234";
        let client = ClientBuilder::new()
            .set_log(log)
            .connect(addr)
            .map_err(|_e| ListRangeError::Internal)?;

        for i in 0..3 {
            client.list_range(i as i64).await?;
        }

        Ok(())
    }
}
