use super::{client::Client, config, session_manager::SessionManager};
use crate::error::ClientError;
use slog::{o, Discard, Logger};
use std::{cell::UnsafeCell, rc::Rc};

pub struct ClientBuilder {
    config: config::ClientConfig,
    pub(crate) log: Logger,
}

impl ClientBuilder {
    pub fn new() -> Self {
        let drain = Discard;
        let root = Logger::root(drain, o!());
        Self {
            config: config::ClientConfig::default(),
            log: root,
        }
    }

    pub fn set_log(mut self, log: Logger) -> Self {
        self.log = log;
        self
    }

    pub fn set_config(mut self, config: config::ClientConfig) -> Self {
        self.config = config;
        self
    }

    pub fn build(self) -> Result<Client, ClientError> {
        let (stop_tx, stop_rx) = async_channel::unbounded();
        let config = Rc::new(self.config);
        let session_manager = Rc::new(UnsafeCell::new(SessionManager::new(
            &config, stop_rx, &self.log,
        )));

        Ok(Client {
            session_manager,
            log: self.log,
            config,
        })
    }
}


impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use slog::trace;
    use test_util::{run_listener, terminal_logger};

    use crate::{client::config, error::ClientError};

    #[test]
    fn test_builder() -> Result<(), ClientError> {
        let log = terminal_logger();
        let config = config::ClientConfig::default();
        let logger = log.clone();
        tokio_uring::start(async {
            let port = run_listener(logger).await;
            let addr = format!("localhost:{}", port);
            trace!(log, "Target endpoint: `{}`", addr);
            let client = ClientBuilder::new()
                .set_log(log)
                .set_config(config)
                .build()?;
            client.start();
            Ok(())
        })
    }
}
