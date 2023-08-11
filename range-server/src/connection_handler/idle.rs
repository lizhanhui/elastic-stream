use std::{
    cell::RefCell,
    net::SocketAddr,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use config::Configuration;
use log::info;
use monoio::io::AsyncWriteRent;
use transport::connection::ChannelWriter;

use crate::connection_tracker::ConnectionTracker;

pub(crate) struct IdleHandler {
    config: Arc<Configuration>,
    last_read: Rc<RefCell<Instant>>,
    last_write: Rc<RefCell<Instant>>,
}

impl IdleHandler {
    pub(crate) fn new<S>(
        connection: Rc<ChannelWriter<S>>,
        addr: SocketAddr,
        config: Arc<Configuration>,
        conn_tracker: Rc<RefCell<ConnectionTracker>>,
    ) -> Rc<Self>
    where
        S: AsyncWriteRent + 'static,
    {
        let handler = Rc::new(Self {
            config: Arc::clone(&config),
            last_read: Rc::new(RefCell::new(Instant::now())),
            last_write: Rc::new(RefCell::new(Instant::now())),
        });
        Self::run(Rc::clone(&handler), connection, addr, config, conn_tracker);
        handler
    }

    pub(crate) fn on_read(&self) {
        *self.last_read.borrow_mut() = Instant::now();
    }

    pub(crate) fn on_write(&self) {
        *self.last_write.borrow_mut() = Instant::now();
    }

    fn read_idle(&self) -> bool {
        *self.last_read.borrow() + self.config.connection_idle_duration() < Instant::now()
    }

    fn write_idle(&self) -> bool {
        *self.last_write.borrow() + self.config.connection_idle_duration() < Instant::now()
    }

    fn read_elapsed(&self) -> Duration {
        self.last_read.borrow().elapsed()
    }

    fn write_elapsed(&self) -> Duration {
        self.last_write.borrow().elapsed()
    }

    fn run<S>(
        handler: Rc<Self>,
        connection: Rc<ChannelWriter<S>>,
        addr: SocketAddr,
        config: Arc<Configuration>,
        conn_tracker: Rc<RefCell<ConnectionTracker>>,
    ) where
        S: AsyncWriteRent + 'static,
    {
        monoio::spawn(async move {
            let mut interval = monoio::time::interval(config.connection_idle_duration());
            loop {
                interval.tick().await;
                if handler.read_idle() && handler.write_idle() {
                    conn_tracker.borrow_mut().remove(&addr);
                    if let Ok(()) = connection.close().await {
                        info!(
                            "Close connection to {} since read has been idle for {}ms and write has been idle for {}ms",
                            connection.peer_address(),
                            handler.read_elapsed().as_millis(),
                            handler.write_elapsed().as_millis(),
                        );
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::connection_tracker::ConnectionTracker;
    use config::Configuration;
    use mock_server::run_listener;
    use monoio::{io::Splitable, net::TcpStream};
    use std::{cell::RefCell, error::Error, net::SocketAddr, rc::Rc, sync::Arc};
    use transport::connection::ChannelWriter;

    #[monoio::test]
    async fn test_read_idle() -> Result<(), Box<dyn Error>> {
        let mut config = Configuration::default();
        config.server.connection_idle_duration = 1;
        let config = Arc::new(config);
        let port = run_listener().await;
        let target = format!("127.0.0.1:{}", port);
        let addr = target.parse::<SocketAddr>().unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let (_read_half, write_half) = stream.into_split();
        let connection = Rc::new(ChannelWriter::new(write_half, target.clone()));
        let conn_tracker = Rc::new(RefCell::new(ConnectionTracker::new()));
        let handler = super::IdleHandler::new(
            Rc::clone(&connection),
            addr,
            Arc::clone(&config),
            conn_tracker,
        );
        tokio::time::sleep(config.connection_idle_duration()).await;
        assert!(handler.read_idle(), "Read should be idle");
        assert!(handler.write_idle(), "Write should be idle");

        handler.on_read();
        assert!(!handler.read_idle(), "Read should NOT be idle");
        assert!(handler.write_idle(), "Write should be idle");

        handler.on_write();
        assert!(!handler.write_idle(), "Read should NOT be idle");

        Ok(())
    }
}
