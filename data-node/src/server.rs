use crate::connection::Connection;
use crate::{
    cfg::ServerConfig,
    frame::{Frame, OperationCode},
};
use monoio::net::{TcpListener, TcpStream};
use slog::{debug, error, info, o, warn, Drain, Logger};

pub fn launch(cfg: &ServerConfig) {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(drain, o!());
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
                    let uring_available = monoio::utils::detect_uring();
                    info!(logger, "Detect uring availablility: {}", uring_available);
                    if !core_affinity::set_for_current(core_id) {
                        error!(
                            logger,
                            "Failed to bind worker thread to processor: {:?}", core_id
                        );
                        return;
                    }

                    info!(logger, "Bind worker thread to processor: {:?}", core_id);

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

async fn run(listener: TcpListener, logger: Logger) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let incoming = listener.accept().await;
        let logger = logger.new(o!());
        let (stream, socket_address) = match incoming {
            Ok((stream, socket_addr)) => (stream, socket_addr),
            Err(e) => {
                error!(
                    logger,
                    "Failed to accept a connection. Cause: {}",
                    e.to_string()
                );
                break;
            }
        };

        debug!(logger, "Accept a new connection from {:?}", socket_address);

        monoio::spawn(async move {
            process(stream, logger).await;
        });
    }

    Ok(())
}

async fn process(stream: TcpStream, logger: Logger) {
    let mut connection = Connection::new(stream);
    // let mut tasks = DashMap::new();
    // TODO: Make task processing concurrent using MPSC

    loop {
        match connection.read_frame().await {
            Ok(Some(frame)) => {
                handle_request(&frame, &mut connection).await;
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
        };
    }
}

async fn handle_request(request: &Frame, channel: &mut Connection) {
    match request.operation_code {
        OperationCode::Unknown => {}
        OperationCode::Ping => {
            let response = Frame {
                operation_code: OperationCode::Ping,
                flag: 1u8,
                stream_id: request.stream_id,
                header_format: crate::frame::HeaderFormat::FlatBuffer,
                header: None,
                payload: None,
            };
            match channel.write_frame(&response).await {
                Ok(_) => {}
                Err(_) => {}
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
