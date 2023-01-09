//! Util functions for tests.
//!

use local_sync::oneshot;
use monoio::net::TcpListener;
use slog::{debug, info, o, Drain, Logger};
use slog_async::OverflowStrategy;

/// Run a dummy listening server.
/// Once it accepts a connection, it quits immediately.
pub async fn run_listener(logger: Logger) -> u16 {
    let (tx, rx) = oneshot::channel();
    monoio::spawn(async move {
        let listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        debug!(logger, "Listening 0.0.0.0:{}", port);
        tx.send(port).unwrap();
        loop {
            if let Ok((_conn, sock_addr)) = listener.accept().await {
                debug!(logger, "Accepted a connection from {:?}", sock_addr);
            } else {
                break;
            }
        }
        info!(logger, "Listener quit");
    });
    rx.await.unwrap()
}

/// Create logger with terminal sinks.
///
/// # Note
/// The created logger has only a buffer size of 1, thus, is test-purpose only.
pub fn terminal_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain)
        .overflow_strategy(OverflowStrategy::Block)
        .chan_size(1)
        .build()
        .fuse();
    slog::Logger::root(drain, o!())
}
