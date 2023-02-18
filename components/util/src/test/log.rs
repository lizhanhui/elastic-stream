use slog::{o, Drain, Logger};
use slog_async::OverflowStrategy;

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
