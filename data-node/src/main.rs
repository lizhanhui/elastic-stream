use clap::Parser;
use data_node::ServerConfig;
use tokio::sync::broadcast;

fn main() {
    let server_config = ServerConfig::parse();
    let (shutdown_tx, _rx) = broadcast::channel(1);
    let tx = shutdown_tx.clone();
    ctrlc::set_handler(move || {
        if let Err(_) = tx.send(()) {
            eprintln!("Could not send shutdown signal to shutdown channel");
        }
    })
    .expect("Failed to set Ctrl-C");

    if let Err(e) = data_node::server::launch(&server_config, shutdown_tx) {
        eprintln!("Failed to start data-node: {:?}", e);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        time::{Duration, Instant},
    };

    use tokio::sync::broadcast;

    #[test]
    fn test_broadcast() -> Result<(), Box<dyn Error>> {
        let (tx, rx) = broadcast::channel(1);

        let handles = (0..3)
            .map(|i| {
                let mut shutdown_rx = tx.subscribe();
                std::thread::Builder::new()
                    .name(format!("thread-{}", i))
                    .spawn(move || loop {
                        let start = Instant::now();
                        if start.elapsed() > Duration::from_secs(1) {
                            panic!("Should have received signal");
                        }
                        match shutdown_rx.try_recv() {
                            Ok(_) => break,
                            Err(_e) => {
                                std::thread::sleep(Duration::from_millis(10));
                            }
                        }
                    })
            })
            .collect::<Vec<_>>();

        tx.send(())?;

        for handle in handles {
            handle?.join().unwrap();
        }

        Ok(())
    }
}
