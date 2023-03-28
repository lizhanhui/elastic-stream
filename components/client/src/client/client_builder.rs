use super::{config, client::Client, session_manager::SessionManager};
use crate::error::ClientError;
use slog::{o, Discard, Logger};
use std::rc::Rc;
use tokio::sync::mpsc;

pub struct ClientBuilder {
    target: String,
    config: config::ClientConfig,
    pub(crate) log: Logger,
}

impl ClientBuilder {
    pub fn new(target: &str) -> Self {
        let drain = Discard;
        let root = Logger::root(drain, o!());
        Self {
            target: target.to_owned(),
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
        let (tx, rx) = mpsc::unbounded_channel();

        let config = Rc::new(self.config);

        let session_manager = SessionManager::new(&self.target, &config, rx, &self.log)?;

        Ok(Client {
            session_manager: Some(session_manager),
            tx,
            log: self.log,
            config,
        })
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
        tokio_uring::start(async {
            let log = terminal_logger();

            let config = config::ClientConfig::default();

            let logger = log.clone();
            let port = run_listener(logger).await;
            let addr = format!("dns:localhost:{}", port);
            trace!(log, "Target endpoint: `{}`", addr);

            ClientBuilder::new(&addr)
                .set_log(log)
                .set_config(config)
                .build()?;
            Ok(())
        })
    }
}
