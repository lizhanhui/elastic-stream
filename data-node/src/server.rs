use std::{
    error::Error,
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
};

use crate::{cfg::ServerConfig, handler::ServerCall};

use core_affinity::CoreId;
use monoio::{
    io::Splitable,
    net::{TcpListener, TcpStream},
    FusionDriver, RuntimeBuilder,
};
use slog::{debug, error, info, o, trace, warn, Drain, Logger};
use slog_async::Async;
use slog_term::{CompactFormat, TermDecorator};
use store::{
    ElasticStore,
    option::{StoreOptions, StorePath},
};
use transport::channel::{ChannelReader, ChannelWriter};

struct NodeConfig {
    core_id: CoreId,
    server_config: ServerConfig,
    sharing_uring: RawFd,
}

struct Node {
    config: NodeConfig,
    store: Rc<ElasticStore>,
    logger: Logger,
}

impl Node {
    pub fn new(config: NodeConfig, store: ElasticStore, logger: &Logger) -> Self {
        Self {
            config,
            store: Rc::new(store),
            logger: logger.clone(),
        }
    }

    pub fn serve(&mut self) {
        monoio::utils::bind_to_cpu_set([self.config.core_id.id]).unwrap();
        let mut driver = match RuntimeBuilder::<FusionDriver>::new()
            .enable_timer()
            .with_entries(self.config.server_config.queue_depth)
            .uring_builder(io_uring::IoUring::builder().setup_attach_wq(self.config.sharing_uring))
            .build()
        {
            Ok(driver) => driver,
            Err(e) => {
                error!(
                    self.logger,
                    "Failed to create runtime. Cause: {}",
                    e.to_string()
                );
                panic!("Failed to create runtime driver. {}", e.to_string());
            }
        };

        driver.block_on(async {
            let bind_address = format!("0.0.0.0:{}", self.config.server_config.port);
            let listener = match TcpListener::bind(&bind_address) {
                Ok(listener) => {
                    info!(self.logger, "Server starts OK, listening {}", bind_address);
                    listener
                }
                Err(e) => {
                    eprintln!("{}", e.to_string());
                    return;
                }
            };

            match self.run(listener, self.logger.new(o!())).await {
                Ok(_) => {}
                Err(e) => {
                    error!(self.logger, "Runtime failed. Cause: {}", e.to_string());
                }
            }
        });
    }

    async fn run(&self, listener: TcpListener, logger: Logger) -> Result<(), Box<dyn Error>> {
        loop {
            let incoming = listener.accept().await;
            let logger = logger.new(o!());
            let (stream, _socket_address) = match incoming {
                Ok((stream, socket_addr)) => {
                    debug!(logger, "Accepted a new connection from {socket_addr:?}");
                    stream.set_nodelay(true).unwrap_or_else(|e| {
                        warn!(logger, "Failed to disable Nagle's algorithm. Cause: {e:?}, PeerAddress: {socket_addr:?}");
                    });
                    debug!(logger, "Nagle's algorithm turned off");

                    (stream, socket_addr)
                }
                Err(e) => {
                    error!(
                        logger,
                        "Failed to accept a connection. Cause: {}",
                        e.to_string()
                    );
                    break;
                }
            };

            let store = Rc::clone(&self.store);
            monoio::spawn(async move {
                Node::process(store, stream, logger).await;
            });
        }

        Ok(())
    }

    async fn process(store: Rc<ElasticStore>, stream: TcpStream, logger: Logger) {
        let peer_address = match stream.peer_addr() {
            Ok(addr) => addr.to_string(),
            Err(_e) => "Unknown".to_owned(),
        };

        let (read_half, write_half) = stream.into_split();
        let mut channel_reader = ChannelReader::new(read_half, &peer_address, logger.new(o!()));
        let mut channel_writer = ChannelWriter::new(write_half, &peer_address, logger.new(o!()));
        let (tx, rx) = async_channel::unbounded();

        let request_logger = logger.clone();
        monoio::spawn(async move {
            let logger = request_logger;
            loop {
                match channel_reader.read_frame().await {
                    Ok(Some(frame)) => {
                        let log = logger.clone();
                        let sender = tx.clone();
                        let store = Rc::clone(&store);
                        let mut server_call = ServerCall {
                            request: frame,
                            sender,
                            logger: log,
                            store,
                        };
                        monoio::spawn(async move {
                            server_call.call().await;
                        });
                    }
                    Ok(None) => {
                        info!(logger, "Connection to {} is closed", peer_address);
                        break;
                    }
                    Err(e) => {
                        warn!(
                            logger,
                            "Connection reset. Peer address: {}. Cause: {e:?}", peer_address
                        );
                        break;
                    }
                }
            }
        });

        monoio::spawn(async move {
            let peer_address = channel_writer.peer_address().to_owned();
            loop {
                match rx.recv().await {
                    Ok(frame) => match channel_writer.write_frame(&frame).await {
                        Ok(_) => {
                            trace!(
                                logger,
                                "Response[stream-id={:?}] written to {}",
                                frame.stream_id,
                                peer_address
                            );
                        }
                        Err(e) => {
                            warn!(
                                logger,
                                "Failed to write response[stream-id={:?}] to {}. Cause: {}",
                                frame.stream_id,
                                peer_address,
                                e
                            );
                            break;
                        }
                    },
                    Err(e) => {
                        if rx.is_closed() {
                            debug!(
                                logger,
                                "Multiplex channel is closed. Session to {} will be terminated",
                                peer_address
                            );
                            break;
                        }

                        info!(
                            logger,
                            "Failed to receive response frame from channel. Session to {} will be terminated due to RecvError: `{}`",
                            peer_address,
                            e);
                        break;
                    }
                }
            }
        });
    }
}

pub fn launch(cfg: &ServerConfig) -> Result<(), Box<dyn Error>> {
    let decorator = TermDecorator::new().build();
    let drain = CompactFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!());

    let core_ids = core_affinity::get_core_ids().ok_or_else(|| {
        warn!(log, "No cores are available to set affinity");
        crate::error::LaunchError::NoCoresAvailable
    })?;
    let available_core_len = core_ids.len();

    let storage_uring = io_uring::IoUring::builder()
        .setup_iopoll()
        .setup_sqpoll(2000)
        .setup_sqpoll_cpu(1)
        .setup_r_disabled()
        .dontfork()
        .build(cfg.concurrency as u32)?;

    let submitter = storage_uring.submitter();
    submitter.register_iowq_max_workers(&mut [2, 2])?;
    // Register buffers
    submitter.register_enable_rings()?;

    let uring_fd = storage_uring.as_raw_fd();

    let store = ElasticStore::new()?;

    let handles = core_ids
        .into_iter()
        .skip(available_core_len - cfg.concurrency)
        .map(|core_id| {
            let server_config = cfg.clone();
            let logger = log.new(o!());
            let store = store.clone();
            std::thread::Builder::new()
                .name("Worker".to_owned())
                .spawn(move || {
                    let node_config = NodeConfig {
                        core_id: core_id.clone(),
                        server_config: server_config.clone(),
                        sharing_uring: uring_fd,
                    };
                    let mut node = Node::new(node_config, store, &logger);
                    node.serve()
                })
        })
        .collect::<Vec<_>>();

    for handle in handles.into_iter() {
        let _result = handle.unwrap().join();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cmp::min;

    #[test]
    fn test_core_affinity() {
        let core_ids = core_affinity::get_core_ids().unwrap();
        let core_count = min(core_ids.len(), 2);

        let len = core_ids.len();
        let handles = core_ids
            .into_iter()
            .skip(len - core_count)
            .map(|processor_id| {
                std::thread::Builder::new()
                    .name("Worker".into())
                    .spawn(move || {
                        if core_affinity::set_for_current(processor_id) {
                            println!(
                                "Set affinity for worker thread {:?} OK",
                                std::thread::current()
                            );
                        }
                    })
            })
            .collect::<Vec<_>>();

        for handle in handles.into_iter() {
            handle.unwrap().join().unwrap();
        }
    }
}
