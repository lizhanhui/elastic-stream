//! Util functions for tests.
//!

use bytes::BytesMut;
use local_sync::oneshot;
use monoio::{
    io::{AsyncReadRent, AsyncWriteRent},
    net::{TcpListener, TcpStream},
};
use slog::{debug, info, o, Drain, Logger};
use slog_async::OverflowStrategy;

/// Run a dummy listening server.
/// Once it accepts a connection, it quits immediately.
pub async fn run_listener(logger: Logger) -> u16 {
    let (tx, rx) = oneshot::channel();
    monoio::spawn(async move {
        // We are using dual-stack mode.
        // Binding to "[::]:0", the any address for IPv6, will also listen for IPv4.
        let listener = TcpListener::bind("[::]:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        debug!(logger, "Listening {}", port);
        tx.send(port).unwrap();
        loop {
            if let Ok((mut conn, sock_addr)) = listener.accept().await {
                debug!(logger, "Accepted a connection from {:?}", sock_addr);
                monoio::spawn(async move {
                    let mut buf = Some(BytesMut::new());
                    loop {
                        let (res, buf_r) = conn.read(buf.take().unwrap()).await;
                        if let Ok(len) = res {
                            let (res, buf_w) = conn.write(buf_r).await;
                            buf.replace(buf_w);
                        } else {
                            break;
                        }
                    }
                });
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
