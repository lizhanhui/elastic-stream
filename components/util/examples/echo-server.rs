use std::{error::Error, time::Duration};

use monoio::time;

#[monoio::main(timer = true)]
async fn main() -> Result<(), Box<dyn Error>> {
    let logger = util::test::terminal_logger();
    let port = util::test::run_listener(logger).await;
    println!("Listening {}", port);
    time::sleep(Duration::from_secs(300)).await;
    Ok(())
}
