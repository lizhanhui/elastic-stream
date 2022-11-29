use crate::cfg::ServerConfig;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};
use slog::{debug, error, info, o, warn, Drain, Logger};

pub fn launch(cfg: &ServerConfig) {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(drain, o!());

    let cores = cfg.actor_count();
    let mut thread_handles = vec![];
    for _ in 1..=cores {
        let server_config = cfg.clone();
        let logger = log.new(o!());
        let handle = std::thread::spawn(move || {
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
        });
        thread_handles.push(handle);
    }

    for handle in thread_handles {
        let _result = handle.join();
    }
}

async fn run(listener: TcpListener, logger: Logger) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let incoming = listener.accept().await;
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
            let _ = process(stream).await;
        });
    }

    Ok(())
}

async fn process(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(8 * 1024);
    loop {
        let (res, _buf) = stream.read(buf).await;
        buf = _buf;
        let res: usize = res?;
        if 0 == res {
            return Ok(());
        }

        let (res, _buf) = stream.write_all(buf).await;
        buf = _buf;
        res?;

        buf.clear();
    }
}
