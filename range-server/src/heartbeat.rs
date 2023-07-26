use std::{cell::RefCell, rc::Rc, sync::Arc};

use client::{client::Client, heartbeat::HeartbeatData};
use config::Configuration;
use log::info;
use protocol::rpc::header::{ClientRole, RangeServerState};
use tokio::sync::broadcast;

#[derive(Debug)]
pub(crate) struct Heartbeat<C> {
    client: Rc<C>,
    config: Arc<Configuration>,
    state: Rc<RefCell<RangeServerState>>,
    shutdown: broadcast::Sender<()>,
}

impl<C> Clone for Heartbeat<C> {
    fn clone(&self) -> Self {
        Self {
            client: Rc::clone(&self.client),
            config: Arc::clone(&self.config),
            state: Rc::clone(&self.state),
            shutdown: self.shutdown.clone(),
        }
    }
}

impl<C> Heartbeat<C>
where
    C: Client + 'static,
{
    pub(crate) fn new(
        client: Rc<C>,
        config: Arc<Configuration>,
        state: Rc<RefCell<RangeServerState>>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            client,
            config,
            state,
            shutdown,
        }
    }

    pub(crate) fn run(&self) {
        let this = self.clone();
        tokio_uring::spawn(async move {
            let mut interval = tokio::time::interval(this.config.client_heartbeat_interval());
            let mut heartbeat_data = HeartbeatData {
                role: ClientRole::CLIENT_ROLE_RANGE_SERVER,
                state: Some(*this.state.borrow()),
            };

            let mut shutdown = this.shutdown.subscribe();
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        info!("Received shutdown signal.");
                        // Update range server state.
                        *this.state.borrow_mut() = RangeServerState::RANGE_SERVER_STATE_OFFLINE;
                        // Refresh range sever state of heartbeat data
                        heartbeat_data.set_state(RangeServerState::RANGE_SERVER_STATE_OFFLINE);
                        // Notify placement driver that this range server is now
                        let _ = this.client.broadcast_heartbeat(&heartbeat_data).await;
                        break;
                    }

                    _ = interval.tick() => {
                        // Refresh range server state
                        heartbeat_data.set_state(*this.state.borrow());

                        let _ = this.client
                        .broadcast_heartbeat(&heartbeat_data)
                        .await;
                    }

                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, error::Error, rc::Rc, sync::Arc, time::Duration};

    use client::client::MockClient;
    use config::Configuration;
    use log::debug;
    use protocol::rpc::header::RangeServerState;
    use tokio::{sync::broadcast, time::sleep};

    use super::Heartbeat;

    #[test]
    fn test_heartbeat_run() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        tokio_uring::start(async move {
            let mut client = MockClient::default();

            client
                .expect_broadcast_heartbeat()
                .times(1..)
                .returning_st(|_data| {
                    debug!("Client broadcasts heartbeat: {:?}", _data);
                });
            let mut config = Configuration::default();
            config.client.heartbeat_interval = 1;

            let config = Arc::new(config);

            let client = Rc::new(client);
            let state = Rc::new(RefCell::new(
                RangeServerState::RANGE_SERVER_STATE_READ_WRITE,
            ));
            let (tx, _rx) = broadcast::channel(1);
            let heartbeat = Heartbeat::new(client, config, Rc::clone(&state), tx.clone());
            heartbeat.run();
            sleep(Duration::from_millis(200)).await;
            tx.send(())?;
            sleep(Duration::from_millis(500)).await;
            assert_eq!(
                *state.borrow(),
                RangeServerState::RANGE_SERVER_STATE_OFFLINE
            );
            Ok(())
        })
    }
}
