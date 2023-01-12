//! Util functions for tests.
//!

use codec::frame::OperationCode;
use local_sync::oneshot;
use monoio::{io::Splitable, net::TcpListener};
use slog::{debug, error, info, o, warn, Drain, Logger};
use slog_async::OverflowStrategy;
use transport::channel::{ChannelReader, ChannelWriter};

/// Run a dummy listening server.
/// Once it accepts a connection, it quits immediately.
pub async fn run_listener(logger: Logger) -> u16 {
    let (tx, rx) = oneshot::channel();
    monoio::spawn(async move {
        // We are using dual-stack mode.
        // Binding to "[::]:0", the any address for IPv6, will also listen for IPv4.
        let listener = TcpListener::bind("[::]:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        debug!(logger, "TestServer is up, listening {}", port);
        tx.send(port).unwrap();
        loop {
            if let Ok((conn, sock_addr)) = listener.accept().await {
                debug!(
                    logger,
                    "TestServer accepted a connection from {:?}", sock_addr
                );
                let log = logger.clone();

                monoio::spawn(async move {
                    let logger = log.clone();
                    let addr = sock_addr.to_string();
                    let (read_half, write_half) = conn.into_split();
                    let mut reader = ChannelReader::new(read_half, &addr, logger.clone());
                    let mut writer = ChannelWriter::new(write_half, &addr, logger.clone());

                    loop {
                        if let Ok(frame) = reader.read_frame().await {
                            if let Some(frame) = frame {
                                info!(
                                    logger,
                                    "TestServer is processing a `{}` request", frame.operation_code
                                );
                                match frame.operation_code {
                                    OperationCode::Heartbeat => {
                                        match writer.write_frame(&frame).await {
                                            Ok(_) => {
                                                info!(
                                                    logger,
                                                    "TestServer writes the `{}` request back directly",
                                                    frame.operation_code
                                                );
                                            }
                                            Err(e) => {
                                                error!(
                                                    logger,
                                                    "TestServer failed to process `{}`. Cause: {:?}",
                                                    frame.operation_code,
                                                    e
                                                );
                                            }
                                        };
                                    }
                                    OperationCode::ListRange => {
                                        match writer.write_frame(&frame).await {
                                            Ok(_) => {
                                                info!(
                                                    logger,
                                                    "TestServer writes the `{}` request back directly",
                                                    frame.operation_code
                                                );
                                            }
                                            Err(e) => {
                                                error!(
                                                    logger,
                                                    "TestServer failed to process `{}`. Cause: {:?}",
                                                    frame.operation_code,
                                                    e
                                                );
                                            }
                                        };
                                    }
                                    _ => {}
                                }
                            } else {
                                debug!(logger, "Connection from {} is closed", addr);
                                break;
                            }
                        } else {
                            warn!(
                                logger,
                                "Connection from {} is reset, dropping some buffered data", addr
                            );
                            break;
                        }
                    }
                });
            } else {
                break;
            }
        }
        info!(logger, "TestServer shut down OK");
    });
    rx.await.unwrap()
}

/// Create logger with terminal sinks.
///
/// # Note
/// The created logger has only a buffer size of 1, thus, is test-purpose only.
pub fn terminal_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain)
        .overflow_strategy(OverflowStrategy::Block)
        .chan_size(1)
        .build()
        .fuse();
    slog::Logger::root(drain, o!())
}
