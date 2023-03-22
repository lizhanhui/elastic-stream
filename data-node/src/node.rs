use std::{cell::RefCell, error::Error, rc::Rc};

use slog::{debug, error, info, o, trace, warn, Logger};
use store::ElasticStore;
use tokio::sync::mpsc;
use tokio_uring::net::{TcpListener, TcpStream};
use transport::channel::Channel;

use crate::{
    handler::ServerCall,
    node_config::NodeConfig,
    workspace::stream_manager::{fetcher::FetchRangeTask, StreamManager},
};

pub(crate) struct Node {
    logger: Logger,
    config: NodeConfig,
    store: Rc<ElasticStore>,
    stream_manager: Rc<RefCell<StreamManager>>,
    channels: Option<Vec<mpsc::UnboundedReceiver<FetchRangeTask>>>,
}

impl Node {
    pub fn new(
        config: NodeConfig,
        store: ElasticStore,
        stream_manager: Rc<RefCell<StreamManager>>,
        channels: Option<Vec<mpsc::UnboundedReceiver<FetchRangeTask>>>,
        logger: &Logger,
    ) -> Self {
        Self {
            config,
            store: Rc::new(store),
            stream_manager,
            channels,
            logger: logger.clone(),
        }
    }

    pub fn serve(&mut self) {
        core_affinity::set_for_current(self.config.core_id);
        tokio_uring::builder()
            .entries(self.config.server_config.queue_depth)
            .uring_builder(
                tokio_uring::uring_builder()
                    .dontfork()
                    .setup_attach_wq(self.config.sharing_uring),
            )
            .start(async {
                let bind_address = format!("0.0.0.0:{}", self.config.server_config.port);
                let listener =
                    match TcpListener::bind(bind_address.parse().expect("Failed to bind")) {
                        Ok(listener) => {
                            info!(self.logger, "Server starts OK, listening {}", bind_address);
                            listener
                        }
                        Err(e) => {
                            eprintln!("{}", e);
                            return;
                        }
                    };

                self.stream_manager.borrow_mut().start();

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
            let (stream, peer_socket_address) = match incoming {
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
            let stream_manager = Rc::clone(&self.stream_manager);
            let peer_address = peer_socket_address.to_string();
            tokio_uring::spawn(async move {
                Node::process(store, stream_manager, stream, peer_address, logger).await;
            });
        }

        Ok(())
    }

    async fn process(
        store: Rc<ElasticStore>,
        stream_manager: Rc<RefCell<StreamManager>>,
        stream: TcpStream,
        peer_address: String,
        logger: Logger,
    ) {
        let channel = Rc::new(Channel::new(stream, &peer_address, logger.new(o!())));
        let (tx, mut rx) = mpsc::unbounded_channel();

        let request_logger = logger.clone();
        let _channel = Rc::clone(&channel);
        tokio_uring::spawn(async move {
            let channel = _channel;
            let logger = request_logger;
            loop {
                match channel.read_frame().await {
                    Ok(Some(frame)) => {
                        let log = logger.clone();
                        let sender = tx.clone();
                        let store = Rc::clone(&store);
                        let stream_manager = Rc::clone(&stream_manager);
                        let mut server_call = ServerCall {
                            request: frame,
                            sender,
                            logger: log,
                            store,
                            stream_manager,
                        };
                        tokio_uring::spawn(async move {
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

        tokio_uring::spawn(async move {
            let peer_address = channel.peer_address().to_owned();
            loop {
                match rx.recv().await {
                    Some(frame) => match channel.write_frame(&frame).await {
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
                    None => {
                        info!(
                            logger,
                            "Failed to receive response frame from channel. Session to {} will be terminated",
                            peer_address);
                        break;
                    }
                }
            }
        });
    }
}
