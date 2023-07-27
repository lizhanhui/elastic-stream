use clap::Parser;
use log::info;
use range_server::{cli::Commands, Cli};
use tokio::sync::broadcast;

fn main() {
    let cli = Cli::parse();

    let config = match cli.command {
        Commands::Start(args) => {
            args.init_log().unwrap();
            match args.create_config() {
                Ok(config) => config,
                Err(e) => {
                    eprintln!(
                            "Failed to create configuration from the specified configuration file. Cause: {:?}",
                            e
                        );
                    return;
                }
            }
        }

        Commands::BuildInfo => {
            display_built_info();
            return;
        }
    };

    let (shutdown_tx, _rx) = broadcast::channel(1);
    let tx = shutdown_tx.clone();
    ctrlc::set_handler(move || {
        println!("Received shutdown signal");
        if tx.send(()).is_err() {
            eprintln!("Could not send shutdown signal to shutdown channel");
        }
    })
    .expect("Failed to set Ctrl-C");

    if let Err(e) = range_server::server::launch(config, shutdown_tx) {
        eprintln!("Failed to start range-server: {:?}", e);
    }
}

// Additively prints the built info to both stdout and log.
macro_rules! build_info {
    ($($st:tt)*) => {
        println!($($st)*);
        info!($($st)*);
    };
}

/// Display the built information.
///
/// The build information is additively printed to both stdout and log.
fn display_built_info() {
    build_info!(
        "Range Server v{}, built for {} by {}.",
        range_server::built_info::PKG_VERSION,
        range_server::built_info::TARGET,
        range_server::built_info::RUSTC_VERSION
    );

    let built_time = built::util::strptime(range_server::built_info::BUILT_TIME_UTC);
    build_info!(
        "Built with profile \"{}\", on {} ({} days ago).",
        range_server::built_info::PROFILE,
        built_time.with_timezone(&built::chrono::offset::Local),
        (built::chrono::offset::Utc::now() - built_time).num_days(),
    );

    if let (Some(v), Some(dirty), Some(hash), Some(short_hash)) = (
        range_server::built_info::GIT_VERSION,
        range_server::built_info::GIT_DIRTY,
        range_server::built_info::GIT_COMMIT_HASH,
        range_server::built_info::GIT_COMMIT_HASH_SHORT,
    ) {
        build_info!(
            "Built from git `{}`, commit {}, short_commit {}; the working directory was \"{}\".",
            v,
            hash,
            short_hash,
            if dirty { "dirty" } else { "clean" }
        );
    }

    if let Some(r) = range_server::built_info::GIT_HEAD_REF {
        build_info!("The branch was `{r}`.");
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
        let (tx, _rx) = broadcast::channel(1);

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
