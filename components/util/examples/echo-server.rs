use std::{error::Error, time::Duration};
use tokio::time;

fn main() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(async {
        let logger = util::test::terminal_logger();
        let port = util::test::run_listener(logger).await;
        println!("Listening {}", port);
        time::sleep(Duration::from_secs(300)).await;
        Ok(())
    })
}
