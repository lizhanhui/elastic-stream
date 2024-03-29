//! This module contains a trait and a simple implementation to generate unique ID for range server.

use log::{error, trace};
use std::sync::Arc;
use tokio::sync::{broadcast, oneshot};

use crate::{client::Client, error::ClientError, DefaultClient};

/// A trait that generates unique ID.
pub trait IdGenerator {
    fn generate(&self) -> Result<i32, ClientError>;
}

/// Generate unique ID across the whole cluster by placement driver.
///
pub struct PlacementDriverIdGenerator {
    config: Arc<config::Configuration>,
}

impl PlacementDriverIdGenerator {
    pub fn new(config: &config::Configuration) -> Self {
        Self {
            config: Arc::new(config.clone()),
        }
    }
}

impl IdGenerator for PlacementDriverIdGenerator {
    fn generate(&self) -> Result<i32, ClientError> {
        let (tx, rx) = oneshot::channel();
        let config = Arc::clone(&self.config);
        tokio_uring::start(async {
            let (shutdown_tx, _shutdown_rx) = broadcast::channel(1);
            let client = DefaultClient::new(config, shutdown_tx);

            match client.allocate_id(&self.config.server.advertise_addr).await {
                Ok(id) => {
                    trace!(
                        "Acquired ID={} for range-server[{}]",
                        id,
                        self.config.server.advertise_addr
                    );
                    let _ = tx.send(Ok(id));
                }
                Err(e) => {
                    error!("Failed to acquire ID for range-server. Cause: {:?}", e);
                    let _ = tx.send(Err(()));
                }
            }
        });

        match rx.blocking_recv() {
            Ok(Ok(id)) => Ok(id),
            Ok(Err(_)) => Err(ClientError::ClientInternal),
            Err(_e) => Err(ClientError::ClientInternal),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IdGenerator, PlacementDriverIdGenerator};
    use mock_server::run_listener;
    use std::{error::Error, sync::Arc};
    use tokio::sync::oneshot;

    #[test]
    fn test_generate() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        let (stop_tx, stop_rx) = oneshot::channel();
        let (port_tx, port_rx) = oneshot::channel();

        let handle = std::thread::spawn(move || {
            tokio_uring::start(async {
                let port = run_listener().await;
                let _ = port_tx.send(port);
                let _ = stop_rx.await;
            });
        });

        let port = port_rx.blocking_recv().unwrap();

        let cfg = config::Configuration {
            placement_driver: format!("127.0.0.1:{}", port),
            ..Default::default()
        };
        let config = Arc::new(cfg);
        let generator = PlacementDriverIdGenerator::new(&config);
        let id = generator.generate()?;
        assert_eq!(1, id);
        let _ = stop_tx.send(());
        let _ = handle.join();
        Ok(())
    }
}
