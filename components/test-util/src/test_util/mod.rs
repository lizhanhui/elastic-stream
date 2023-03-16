//! Util functions for tests.
//!

use bytes::Bytes;
use codec::frame::{Frame, OperationCode};
use protocol::rpc::header::{
    HeartbeatRequest, HeartbeatResponseT, ListRangesRequest, ListRangesResponseT,
    ListRangesResultT, RangeT,
};
use slog::{debug, error, info, trace, warn, Logger};

use tokio::sync::oneshot;
use tokio_uring::net::TcpListener;
use transport::channel::Channel;

fn serve_heartbeat(log: &Logger, request: &HeartbeatRequest, frame: &mut Frame) {
    debug!(log, "{:?}", request);
    frame.operation_code = OperationCode::Heartbeat;
    let mut response = HeartbeatResponseT::default();
    response.client_id = request.client_id().map(|client_id| client_id.to_owned());
    response.client_role = request.client_role();
    response.data_node = request.data_node().map(|dn| Box::new(dn.unpack()));
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
    let resp = response.pack(&mut builder);
    builder.finish(resp, None);

    let hdr = builder.finished_data();

    let buf = Bytes::copy_from_slice(hdr);
    frame.header = Some(buf);
}

fn serve_list_ranges(log: &Logger, request: &ListRangesRequest, frame: &mut Frame) {
    trace!(log, "Received a list-ranges request: {:?}", request);
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
    let mut resp = ListRangesResponseT::default();

    {
        let mut result = ListRangesResultT::default();
        let ranges = (0..10)
            .into_iter()
            .map(|i| {
                let mut range = RangeT::default();
                range.stream_id = 0;
                range.range_index = i as i32;
                range.start_offset = i * 100;
                range.next_offset = (i + 1) * 100;
                range.end_offset = (i + 1) * 100;
                range
            })
            .collect::<Vec<_>>();

        result.ranges = Some(ranges);
        resp.list_responses = Some(vec![result]);
    }

    let resp = resp.pack(&mut builder);
    builder.finish(resp, None);
    let buf = builder.finished_data();
    frame.header = Some(Bytes::copy_from_slice(buf));
}

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

                            let mut response_frame = Frame::new(OperationCode::Unknown);
                            response_frame.flag_response();
                            response_frame.stream_id = frame.stream_id;

                            match frame.operation_code {
                                OperationCode::Heartbeat => {
                                    if let Some(buf) = &frame.header {
                                        if let Ok(heartbeat) =
                                            flatbuffers::root::<HeartbeatRequest>(buf)
                                        {
                                            serve_heartbeat(&log, &heartbeat, &mut response_frame);
                                        } else {
                                            error!(
                                                log,
                                                "Failed to decode heartbeat request header"
                                            );
                                        }
                                    }
                                }
                                OperationCode::ListRanges => {
                                    response_frame.operation_code = OperationCode::ListRanges;
                                    if let Some(buf) = frame.header.as_ref() {
                                        if let Ok(req) = flatbuffers::root::<ListRangesRequest>(buf)
                                        {
                                            serve_list_ranges(&log, &req, &mut response_frame);
                                        } else {
                                            error!(
                                                log,
                                                "Failed to decode list-range-request header"
                                            );
                                        }
                                    }
                                }
                                _ => {
                                    warn!(
                                        log,
                                        "Unsupported operation code: {}", frame.operation_code
                                    );
                                    unimplemented!("Unimplemented operation code");
                                }
                            }

                            match channel.write_frame(&response_frame).await {
                                Ok(_) => {
                                    trace!(
                                        logger,
                                        "TestServer writes the `{}` response back directly",
                                        response_frame.operation_code
                                    );
                                }
                                Err(e) => {
                                    error!(
                                        logger,
                                        "TestServer failed to process `{}`. Cause: {:?}",
                                        response_frame.operation_code,
                                        e
                                    );
                                }
                            };
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
