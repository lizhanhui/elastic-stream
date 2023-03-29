//! This module contains a trait and a simple implementation to generate unique ID for data node.

use slog::{error, trace, Logger};
use std::time::Duration;
use tokio::sync::oneshot;

use crate::{
    client::{client_builder::ClientBuilder, config::ClientConfig},
    error::ClientError,
};

/// A trait that generates unique ID.
pub trait IdGenerator {
    fn generate(&self) -> Result<i32, ClientError>;
}

/// Generate unique ID across the whole cluster by placement manager.
///
pub struct PlacementManagerIdGenerator {
    log: Logger,
    placement_manager_address: String,
    host: String,
}

impl PlacementManagerIdGenerator {
    pub fn new(log: Logger, addr: &str, host: &str) -> Self {
        Self {
            log,
            placement_manager_address: addr.to_owned(),
            host: host.to_owned(),
        }
    }
}

impl IdGenerator for PlacementManagerIdGenerator {
    fn generate(&self) -> Result<i32, ClientError> {
        let (tx, rx) = oneshot::channel();
        tokio_uring::start(async {
            let client_config = ClientConfig::default();
            let client = match ClientBuilder::new().set_config(client_config).build() {
                Ok(client) => client,
                Err(_e) => {
                    let _ = tx.send(Err(()));
                    return;
                }
            };

            client.start();

            match client
                .allocate_id(
                    &self.placement_manager_address,
                    &self.host,
                    Duration::from_secs(3),
                )
                .await
            {
                Ok(id) => {
                    trace!(
                        self.log,
                        "Acquired ID={} for data-node[host={}]",
                        id,
                        self.host
                    );
                    let _ = tx.send(Ok(id));
                    return;
                }
                Err(e) => {
                    error!(
                        self.log,
                        "Failed to acquire ID for data-node. Cause: {:?}", e
                    );
                    let _ = tx.send(Err(()));
                }
            }
        });

        match rx.blocking_recv() {
            Ok(Ok(id)) => Ok(id),
            Ok(Err(_)) => {
                return Err(ClientError::ClientInternal);
            }
            Err(_e) => {
                return Err(ClientError::ClientInternal);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use tokio::sync::oneshot;

    use super::{IdGenerator, PlacementManagerIdGenerator};

    #[test]
    fn test_generate() -> Result<(), Box<dyn Error>> {
        let log = test_util::terminal_logger();
        let path = test_util::create_random_path()?;
        let _guard = test_util::DirectoryRemovalGuard::new(log.clone(), path.as_path());

        let (stop_tx, stop_rx) = oneshot::channel();
        let (port_tx, port_rx) = oneshot::channel();

        let logger = log.clone();
        let handle = std::thread::spawn(move || {
            tokio_uring::start(async {
                let port = test_util::run_listener(logger).await;
                let _ = port_tx.send(port);
                let _ = stop_rx.await;
            });
        });

        let port = port_rx.blocking_recv().unwrap();
        let pm_address = format!("localhost:{}", port);
        let generator = PlacementManagerIdGenerator::new(log, &pm_address, "dn-host");
        let id = generator.generate()?;
        assert_eq!(1, id);
        let _ = stop_tx.send(());
        let _ = handle.join();
        Ok(())
    }
}
