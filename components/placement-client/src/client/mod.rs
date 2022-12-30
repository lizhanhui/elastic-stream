mod config;
mod naming;

use crate::error::{ClientError, CommandError};
use std::{cell::UnsafeCell, net::SocketAddr, rc::Rc, str::FromStr};

use config::ClientConfig;
use local_sync::{
    mpsc::unbounded::{channel, Rx, Tx},
    oneshot::Sender,
};
use model::range::PartitionRange;
use monoio::{
    io::{OwnedReadHalf, OwnedWriteHalf},
    net::TcpStream,
};
use slog::{debug, error, warn, Logger};
use transport::channel::{ChannelReader, ChannelWriter};

use self::naming::Endpoints;

#[derive(Debug, Clone)]
pub enum Command {
    UnitTest,
    ListRange { partition_id: u64 },
}

#[derive(Debug, PartialEq)]
pub enum Reply {
    UnitTest,
    PartitionRanges { items: Vec<PartitionRange> },
}

#[derive(Debug)]
struct Request {
    target: String,
    command: Command,
    sender: Sender<Result<Reply, CommandError>>,
}

#[derive(Debug)]
enum ChannelState {
    /// Channel is establishing a TCP connection, potentially handshaking with its peer.
    Connecting,

    /// Connection has already been established and ready to send/recieve data to its peer.
    Active,

    /// The underlying connection is being shut down.
    Closing,

    /// The associated connection is already closed.
    Closed,
}

struct SubChannel {
    addr: SocketAddr,
    state: ChannelState,
    reader: Option<ChannelReader<OwnedReadHalf<TcpStream>>>,
    writer: Option<ChannelWriter<OwnedWriteHalf<TcpStream>>>,
}

enum LBPolicy {
    PickFirst { current_index: usize },
    RoundRobin,
}

struct ManagedChannel {
    /// Naming of the target endpoints.
    /// Typically, it could be of `host-name:port`
    endpoints: Endpoints,
    channels: Vec<SubChannel>,
    lb_policy: LBPolicy,
}

struct Client {
    config: ClientConfig,
    managed_channel: UnsafeCell<ManagedChannel>,
    log: Logger,
    tx: Tx<Request>,
    rx: UnsafeCell<Rx<Request>>,
}

impl Client {
    /// Build a new `Client`
    ///
    /// - `config` Configure various aspects of `Client`, including network connect timeout, retry policy etc.
    /// - `address` Preferred way to configure address of placement managers is `dns:domain-name:port`
    pub fn new(config: ClientConfig, address: &str, log: Logger) -> Result<Rc<Self>, ClientError> {
        let endpoints = Endpoints::from_str(address)?;

        let channels: Vec<_> = endpoints
            .addrs
            .iter()
            .map(|addr| SubChannel {
                addr: addr.clone(),
                state: ChannelState::Connecting,
                reader: None,
                writer: None,
            })
            .collect();

        let managed_channel = ManagedChannel {
            endpoints,
            channels,
            lb_policy: LBPolicy::PickFirst { current_index: 0 },
        };

        let (tx, rx) = channel();
        Ok(Rc::new(Self {
            config,
            managed_channel: UnsafeCell::new(managed_channel),
            log,
            tx,
            rx: UnsafeCell::new(rx),
        }))
    }

    pub async fn start(client: &Rc<Client>) -> Result<(), ClientError> {
        let this = Rc::clone(&client);
        monoio::spawn(async move {
            this.do_loop().await;
        });

        Ok(())
    }

    pub fn post(&self, request: Request) {
        self.tx.send(request).unwrap_or_else(|e| {
            error!(self.log, "Failed to dispatch request. Cause: {:?}", e);
        });
    }

    async fn do_loop(&self) {
        let rx = unsafe { &mut *self.rx.get() };
        debug!(self.log, "Await requests to dispatch");
        loop {
            match rx.recv().await {
                Some(request) => {
                    self.process(request).await.unwrap_or_else(|e| {
                        warn!(self.log, "Failed to process request. Cause: {:?}", e);
                    });
                }
                None => {
                    warn!(
                        self.log,
                        "RX#recv returns None. Its TX should have been destructed"
                    );
                    break;
                }
            }
        }
    }

    async fn process(&self, request: Request) -> Result<(), CommandError> {
        match request.command {
            Command::UnitTest => request
                .sender
                .send(Ok(Reply::UnitTest))
                .map_err(|e| e.err().unwrap()),
            Command::ListRange { partition_id } => {
                let result = self.list_range(partition_id).await;
                request.sender.send(result).unwrap_or_else(|e| {
                    warn!(self.log, "Failed to forward reply {:?}", e);
                });
                Ok(())
            }
        }
    }

    async fn list_range(&self, partition_id: u64) -> Result<Reply, CommandError> {
        let reply = Reply::PartitionRanges { items: vec![] };
        Ok(reply)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use slog::{o, Drain};

    fn get_logger() -> Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let log = slog::Logger::root(drain, o!());
        log
    }

    #[test]
    fn test_new() -> Result<(), ClientError> {
        let log = get_logger();
        let config = ClientConfig::default();
        let address = "dns:localhost:9876";
        let client = Client::new(config, address, log)?;
        let managed_channel = unsafe { &mut *client.managed_channel.get() };
        assert_eq!(1, managed_channel.channels.len());
        Ok(())
    }

    /*
    #[monoio::test(enable_timer = true)]
    async fn test_start() {
        let log = get_logger();

        let client = Client::new(log.clone());
        Client::start(&client);
        let (sender, receiver) = local_sync::oneshot::channel();
        let request = Request {
            target: "localhost:1234".to_owned(),
            command: Command::UnitTest,
            sender,
        };

        client.post(request);

        match receiver.await {
            Ok(reply) => {
                assert_eq!(reply, Ok(Reply::UnitTest));
            }
            Err(e) => {
                panic!("{}", e)
            }
        };
    }
     */
}
