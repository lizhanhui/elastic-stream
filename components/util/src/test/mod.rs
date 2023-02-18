//! Util functions for tests.
//!

use std::rc::Rc;

use codec::frame::OperationCode;
use slog::{debug, error, info, warn, Drain, Logger};

use tokio::sync::oneshot;
use tokio_uring::net::TcpListener;
use transport::channel::{ChannelReader, ChannelWriter};

/// Run a dummy listening server.
/// Once it accepts a connection, it quits immediately.
pub async fn run_listener(logger: Logger) -> u16 {
    let (tx, rx) = oneshot::channel();
    tokio_uring::spawn(async move {
        // We are using dual-stack mode.
        // Binding to "[::]:0", the any address for IPv6, will also listen for IPv4.
        let listener = TcpListener::bind("[::]:0".parse().unwrap()).unwrap();
        let port = listener.local_addr().unwrap().port();
        debug!(logger, "TestServer is up, listening {}", port);
        tx.send(port).unwrap();
        while let Ok((conn, sock_addr)) = listener.accept().await {
            debug!(
                logger,
                "TestServer accepted a connection from {:?}", sock_addr
            );
            let log = logger.clone();

            tokio_uring::spawn(async move {
                let logger = log.clone();
                let addr = sock_addr.to_string();
                let stream = Rc::new(conn);
                let mut reader = ChannelReader::new(Rc::clone(&stream), &addr, logger.clone());
                let mut writer = ChannelWriter::new(Rc::clone(&stream), &addr, logger.clone());

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
        }
        info!(logger, "TestServer shut down OK");
    });
    rx.await.unwrap()
}

pub mod fs;
pub mod log;
