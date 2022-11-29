use crate::cfg::ServerConfig;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};

pub fn launch(cfg: &ServerConfig) {
    let cores = cfg.actor_count();
    let mut thread_handles = vec![];
    for _ in 1..=cores {
        let server_config = cfg.clone();
        let handle = std::thread::spawn(move || {
            let mut driver = match monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                .enable_timer()
                .with_entries(server_config.queue_depth)
                .build()
            {
                Ok(driver) => driver,
                Err(e) => {
                    panic!("Failed to create runtime driver. {}", e.to_string());
                }
            };

            driver.block_on(async {
                match run().await {
                    Ok(_) => {}
                    Err(_e) => {
                        eprintln!("Runtime Failed. : {}", _e.to_string());
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

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    const ADDRESS: &str = "0.0.0.0:1234";
    let listener = TcpListener::bind(ADDRESS)?;

    loop {
        let incoming = listener.accept().await;
        let (stream, socket_address) = match incoming {
            Ok((stream, socket_addr)) => (stream, socket_addr),
            Err(e) => {
                eprintln!("Failed to accept a connection. Cause: {}", e.to_string());
                break;
            }
        };

        println!("Accept a new connection from {:?}", socket_address);

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
