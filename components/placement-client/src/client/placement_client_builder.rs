use std::rc::Rc;

use slog::{o, Discard, Logger};
use tokio::sync::mpsc;

use crate::{
    error::ClientError,
    notifier::{self, Notifier, UnsupportedNotifier},
};

use super::{config, placement_client::PlacementClient, session_manager::SessionManager};

pub struct PlacementClientBuilder {
    target: String,
    config: config::ClientConfig,
    notifier: Rc<dyn Notifier>,
    pub(crate) log: Logger,
}

impl PlacementClientBuilder {
    pub(crate) fn new(target: &str) -> Self {
        let drain = Discard;
        let root = Logger::root(drain, o!());
        Self {
            target: target.to_owned(),
            config: config::ClientConfig::default(),
            notifier: Rc::new(UnsupportedNotifier {}),
            log: root,
        }
    }

    pub fn set_notifier(mut self, notifier: Rc<dyn Notifier>) -> Self {
        self.notifier = notifier;
        self
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
        let (tx, rx) = mpsc::unbounded_channel();

        let config = Rc::new(self.config);

        let mut session_manager =
            SessionManager::new(&self.target, &config, rx, self.notifier, &self.log)?;

        tokio_uring::spawn(async move {
            session_manager.run().await;
        });

        Ok(PlacementClient {
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

            PlacementClientBuilder::new(&addr)
                .set_log(log)
                .set_config(config)
                .build()
                .await?;
            Ok(())
        })
    }
}
