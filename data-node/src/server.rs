use crate::cfg::ServerConfig;
use bytes::{BufMut, BytesMut};
use codec::frame::{Frame, OperationCode};
use monoio::net::{TcpListener, TcpStream};
use slog::{debug, error, info, o, warn, Drain, Logger};
use transport::connection::Connection;

pub fn launch(cfg: &ServerConfig) {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(drain, o!());

    let (profiler_tx, profiler_rx) = crossbeam::channel::bounded(1);

    let profiler_logger = log.new(o!());
    let profiler_handle = std::thread::Builder::new()
        .name("Profiler".to_owned())
        .spawn(move || profile(profiler_rx, profiler_logger));

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
            let profiler_sender = profiler_tx.clone();
            std::thread::Builder::new()
                .name("Worker".to_owned())
                .spawn(move || {
                    let uring_available = monoio::utils::detect_uring();
                    info!(logger, "Detect uring availablility: {}", uring_available);
                    if !core_affinity::set_for_current(core_id) {
                        error!(
                            logger,
                            "Failed to bind the worker thread to processor: {:?}", core_id
                        );
                        return;
                    }

                    info!(logger, "Bind the worker thread to processor: {:?}", core_id);

                    let mut driver = match monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
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

                        // Generate a flamegraph every 5 minutes.
                        let profiler_logger = logger.new(o!("module" => "profiler"));
                        monoio::spawn(async move {
                            let mut i = 0;
                            loop {
                                i += 1;
                                monoio::time::sleep(std::time::Duration::from_secs(30000)).await;
                                if let Ok(_) =
                                    profiler_sender.try_send(format!("{}-{}", core_id.id, i))
                                {
                                    info!(profiler_logger, "Notify pprof to create a flamegraph");
                                }
                            }
                        });

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

    let _result = profiler_handle.unwrap().join();
}

fn profile(rx: crossbeam::channel::Receiver<String>, logger: Logger) {
    let profile_guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .unwrap();
    loop {
        match rx.recv() {
            Ok(mut file_name) => {
                if let Ok(report) = profile_guard.report().build() {
                    file_name.push_str(".svg");
                    let file = std::fs::File::create(&file_name).unwrap();
                    match report.flamegraph(file) {
                        Ok(_) => {
                            info!(logger, "Create flamegraph {}", file_name);
                        }
                        Err(e) => {
                            error!(logger, "Failed to report flamegraph. Cause: {:?}", e);
                        }
                    };
                };
            }
            Err(e) => {
                error!(logger, "Profiler receiver#recv got an error: {:?}", e);
                break;
            }
        }
    }
}

async fn run(listener: TcpListener, logger: Logger) -> Result<(), Box<dyn std::error::Error>> {
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
    let log = logger.new(o!());
    let mut connection = Connection::new(stream, log);
    let (tx, rx) = async_channel::unbounded();
    loop {
        let sender = tx.clone();
        let log = logger.new(o!());
        monoio::select! {
            request = connection.read_frame() => {
                match request {
                    Ok(Some(frame)) => {
                        monoio::spawn(async move {
                            handle_request(frame, sender, log).await;
                        });
                    }
                    Ok(None) => {
                        info!(
                            logger,
                            "Connection to {} is closed",
                            connection.peer_address()
                        );
                        break;
                    }
                    Err(e) => {
                        warn!(
                            logger,
                            "Connection reset. Peer address: {}. Cause: {e:?}",
                            connection.peer_address()
                        );
                        break;
                    }
                }
            }
            response = rx.recv() => {
                match response {
                    Ok(frame) => {
                        match connection.write_frame(&frame).await {
                            Ok(_) => {
                                debug!(logger, "Response[stream-id={:?}] written to network", frame.stream_id);
                            },
                            Err(e) => {
                                warn!(
                                    logger,
                                    "Failed to write response[stream-id={:?}] to network. Cause: {:?}",
                                    frame.stream_id,
                                    e
                                );
                            }
                        }
                    },
                    Err(e) => {
                        warn!(logger, "Failed to receive response frame from MSPC channel. Cause: {:?}", e);
                    }
                };
            }
        }
    }
}

async fn handle_request(request: Frame, sender: async_channel::Sender<Frame>, logger: Logger) {
    match request.operation_code {
        OperationCode::Unknown => {}
        OperationCode::Ping => {
            debug!(logger, "Request[stream-id={}] received", request.stream_id);
            let mut header = BytesMut::new();
            let text = format!("stream-id={}, response=true", request.stream_id);
            header.put(text.as_bytes());
            let response = Frame {
                operation_code: OperationCode::Ping,
                flag: 1u8,
                stream_id: request.stream_id,
                header_format: codec::frame::HeaderFormat::FlatBuffer,
                header: Some(header.freeze()),
                payload: None,
            };
            match sender.send(response).await {
                Ok(_) => {
                    debug!(
                        logger,
                        "Response[stream-id={}] transferred to channel", request.stream_id
                    );
                }
                Err(e) => {
                    warn!(
                        logger,
                        "Failed to send response[stream-id={}] to channel. Cause: {:?}",
                        request.stream_id,
                        e
                    );
                }
            };
        }
        OperationCode::GoAway => {}
    }
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
