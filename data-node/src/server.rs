use std::error::Error;

use crate::{cfg::ServerConfig, handler::ServerCall};

use monoio::{
    io::Splitable,
    net::{TcpListener, TcpStream},
    FusionDriver, RuntimeBuilder,
};
use slog::{debug, error, info, o, warn, Drain, Logger};
use slog_async::Async;
use slog_term::{CompactFormat, TermDecorator};
use transport::channel::{ChannelReader, ChannelWriter};

pub fn launch(cfg: &ServerConfig) {
    let decorator = TermDecorator::new().build();
    let drain = CompactFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!());

    let core_ids = match core_affinity::get_core_ids() {
        Some(ids) => ids,
        None => {
            warn!(log, "No cores are available to set affinity");
            return;
        }
    };
    let available_core_len = core_ids.len();

    let handles = core_ids
        .into_iter()
        .skip(available_core_len - cfg.concurrency)
        .map(|core_id| {
            let server_config = cfg.clone();
            let logger = log.new(o!());
            std::thread::Builder::new()
                .name("Worker".to_owned())
                .spawn(move || {
                    monoio::utils::bind_to_cpu_set([core_id.id]).unwrap();
                    let mut driver = match RuntimeBuilder::<FusionDriver>::new()
                        .enable_timer()
                        .with_entries(server_config.queue_depth)
                        .build()
                    {
                        Ok(driver) => driver,
                        Err(e) => {
                            error!(logger, "Failed to create runtime. Cause: {}", e.to_string());
                            panic!("Failed to create runtime driver. {}", e.to_string());
                        }
                    };

                    driver.block_on(async {
                        let bind_address = format!("0.0.0.0:{}", server_config.port);
                        let listener = match TcpListener::bind(&bind_address) {
                            Ok(listener) => {
                                info!(logger, "Server starts OK, listening {}", bind_address);
                                listener
                            }
                            Err(e) => {
                                eprintln!("{}", e.to_string());
                                return;
                            }
                        };

                        match run(listener, logger.new(o!())).await {
                            Ok(_) => {}
                            Err(e) => {
                                error!(logger, "Runtime failed. Cause: {}", e.to_string());
                            }
                        }
                    });
                })
        })
        .collect::<Vec<_>>();

    for handle in handles.into_iter() {
        let _result = handle.unwrap().join();
    }
}

async fn run(listener: TcpListener, logger: Logger) -> Result<(), Box<dyn Error>> {
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

        monoio::spawn(async move {
            process(stream, logger).await;
        });
    }

    Ok(())
}

async fn process(stream: TcpStream, logger: Logger) {
    let peer_address = match stream.peer_addr() {
        Ok(addr) => addr.to_string(),
        Err(_e) => "Unknown".to_owned(),
    };

    let (read_half, write_half) = stream.into_split();
    let mut channel_reader = ChannelReader::new(read_half, &peer_address, logger.new(o!()));
    let mut channel_writer = ChannelWriter::new(write_half, &peer_address, logger.new(o!()));
    let (tx, rx) = async_channel::unbounded();

    let receive_request_logger = logger.clone();
    monoio::spawn(async move {
        let logger = receive_request_logger;
        loop {
            match channel_reader.read_frame().await {
                Ok(Some(frame)) => {
                    let log = logger.new(o!());
                    let sender = tx.clone();

                    let mut server_call = ServerCall {
                        request: frame,
                        sender,
                        logger: log,
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
        loop {
            match rx.recv().await {
                Ok(frame) => match channel_writer.write_frame(&frame).await {
                    Ok(_) => {
                        debug!(
                            logger,
                            "Response[stream-id={:?}] written to network", frame.stream_id
                        );
                    }
                    Err(e) => {
                        warn!(
                            logger,
                            "Failed to write response[stream-id={:?}] to network. Cause: {:?}",
                            frame.stream_id,
                            e
                        );
                        break;
                    }
                },
                Err(e) => {
                    warn!(
                        logger,
                        "Failed to receive response frame from MSPC channel. Cause: {:?}", e
                    );
                    break;
                }
            }
        }
    });
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
