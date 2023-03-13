//! Util functions for tests.
//!

use std::rc::Rc;

use bytes::Bytes;
use codec::frame::{Frame, OperationCode};
use protocol::rpc::header::{HeartbeatRequest, HeartbeatResponseT, ListRangesRequest};
use slog::{debug, error, info, warn, Drain, Logger};

use tokio::sync::oneshot;
use tokio_uring::net::TcpListener;
use transport::channel::Channel;

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
                let channel = Channel::new(conn, &addr, logger.clone());

                loop {
                    if let Ok(frame) = channel.read_frame().await {
                        if let Some(frame) = frame {
                            info!(
                                logger,
                                "TestServer is processing a `{}` request", frame.operation_code
                            );

                            match frame.operation_code {
                                OperationCode::Heartbeat => {
                                    if let Some(buf) = &frame.header {
                                        let req = flatbuffers::root::<HeartbeatRequest>(buf);
                                        match req {
                                            Ok(heartbeat) => {
                                                debug!(logger, "{:?}", heartbeat);
                                                let mut response = HeartbeatResponseT::default();
                                                response.client_id = heartbeat
                                                    .client_id()
                                                    .map(|client_id| client_id.to_owned());
                                                response.client_role = heartbeat.client_role();
                                                response.data_node = heartbeat
                                                    .data_node()
                                                    .map(|dn| Box::new(dn.unpack()));
                                                let mut builder =
                                                    flatbuffers::FlatBufferBuilder::with_capacity(
                                                        1024,
                                                    );
                                                let resp = response.pack(&mut builder);
                                                builder.finish(resp, None);

                                                let hdr = builder.finished_data();

                                                let mut response_frame =
                                                    Frame::new(OperationCode::Heartbeat);
                                                response_frame.flag_response();
                                                response_frame.stream_id = frame.stream_id;
                                                let buf = Bytes::copy_from_slice(hdr);
                                                response_frame.header = Some(buf);

                                                match channel.write_frame(&response_frame).await {
                                                    Ok(_) => {
                                                        info!(
                                                            logger,
                                                            "TestServer writes the `{}` response back directly",
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
                                            Err(_e) => {}
                                        };
                                    }
                                }
                                OperationCode::ListRanges => {
                                    match channel.write_frame(&frame).await {
                                        Ok(_) => {
                                            info!(
                                                logger,
                                                "TestServer writes the `{}` request back directly",
                                                frame.operation_code
                                            );
                                            if let Some(header) = &frame.header {
                                                let req =
                                                    flatbuffers::root::<ListRangesRequest>(header);
                                                match req {
                                                    Ok(list_ranges) => {
                                                        debug!(
                                                            logger,
                                                            "ListRanges: {:?}", list_ranges
                                                        );
                                                    }
                                                    Err(_e) => {}
                                                }
                                            }
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
