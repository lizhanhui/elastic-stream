use std::{rc::Rc, time::Duration};

use crate::error::{ClientError, ListRangeError};
use local_sync::{mpsc::unbounded, oneshot};
use monoio::time::{self, error::Elapsed};
use slog::{error, o, trace, warn, Discard, Logger};

mod config;
mod naming;
mod request;
mod response;
mod session;
mod session_manager;

use session_manager::SessionManager;

use self::config::ClientConfig;

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

    pub(crate) async fn build(self) -> Result<PlacementClient, ClientError> {
        let (tx, rx) = unbounded::channel();

        let config = Rc::new(self.config);

        let mut session_manager = SessionManager::new(&self.target, &config, rx, &self.log)?;
        monoio::spawn(async move {
            session_manager.run().await;
        });

        Ok(PlacementClient {
            tx,
            log: self.log,
            config,
        })
    }
}

pub(crate) struct PlacementClient {
    tx: unbounded::Tx<(request::Request, oneshot::Sender<response::Response>)>,
    log: Logger,
    config: Rc<ClientConfig>,
}

impl PlacementClient {
    pub async fn list_range(
        &self,
        partition_id: i64,
        timeout: Duration,
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

        time::timeout(timeout, rx).await.map_err(|elapsed| {
            warn!(self.log, "Timeout when list range. {}", elapsed);
            ListRangeError::Timeout
        })?.map_err(|e| {
            error!(
                self.log,
                "Failed to receive response from broken channel. Cause: {:?}", e; "struct" => "Client"
            );
            ListRangeError::Internal
        })
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

        let config = config::ClientConfig::default();

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
        let addr = format!("dns:localhost:{}", port);
        let client = ClientBuilder::new(&addr)
            .set_log(log)
            .build()
            .await
            .map_err(|_e| ListRangeError::Internal)?;

        let timeout = Duration::from_millis(100);

        for i in 0..3 {
            client.list_range(i as i64, timeout).await.unwrap();
        }

        Ok(())
    }
}
