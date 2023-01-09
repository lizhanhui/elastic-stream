use crate::error::{ClientError, ListRangeError};
use local_sync::{mpsc::unbounded, oneshot};
use slog::{error, o, trace, Discard, Logger};

mod config;
mod naming;
mod request;
mod response;
mod session;
mod session_manager;

use session_manager::SessionManager;

pub(crate) struct ClientBuilder {
    target: String,
    config: config::ClientConfig,
    log: Logger,
}

enum LBPolicy {
    PickFirst,
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
        Ok(Client { tx, log: self.log })
    }
}

pub(crate) struct Client {
    tx: unbounded::Tx<(request::Request, oneshot::Sender<response::Response>)>,
    log: Logger,
}

impl Client {
    pub async fn list_range(
        &self,
        partition_id: i64,
    ) -> Result<response::Response, ListRangeError> {
        trace!(self.log, "list_range"; "partition-id" => partition_id);
        let (tx, rx) = oneshot::channel();
        let request = request::Request::ListRange {
            partition_id: partition_id,
        };
        self.tx.send((request, tx)).map_err(|e| {
            error!(self.log, "Failed to forward request. Cause: {:?}", e; "struct" => "Client");
            ListRangeError::Internal
        })?;
        trace!(self.log, "Request forwarded"; "struct" => "Client");
        let result = rx.await.map_err(|e| {
            error!(
                self.log,
                "Failed to receive response from broken channel. Cause: {:?}", e; "struct" => "Client"
            );
            ListRangeError::Internal
        })?;
        trace!(self.log, "Response received from channel"; "struct" => "Client");
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use slog::trace;
    use util::test::{run_listener, terminal_logger};

    use super::*;

    #[monoio::test(timer = true)]
    async fn test_builder() -> Result<(), ClientError> {
        let log = terminal_logger();

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

    #[monoio::test(timer = true)]
    async fn test_list_range() -> Result<(), ListRangeError> {
        let log = terminal_logger();

        let port = run_listener(log.clone()).await;
        let addr = format!("ipv4:127.0.0.1:{}", port);
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
