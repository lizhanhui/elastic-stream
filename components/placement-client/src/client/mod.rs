use local_sync::{mpsc::unbounded, oneshot};
use slog::{o, Discard, Logger};
use std::{
    future::Future,
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

struct SessionManager {
    rx: unbounded::Rx<(request::Request, oneshot::Sender<response::Response>)>,
}

impl Future for SessionManager {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(())
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
        monoio::spawn(SessionManager { rx });
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
}
