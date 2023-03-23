use slog::{o, Drain, Logger};
use slog_async::OverflowStrategy;
use slog_term::PlainSyncDecorator;

/// Create logger with terminal sinks.
///
/// # Note
/// The created logger has only a buffer size of 1, thus, is test-purpose only.
pub fn terminal_logger() -> Logger {
    let decorator = PlainSyncDecorator::new(slog_term::TestStdoutWriter);
    let drain = slog_term::FullFormat::new(decorator)
        .use_file_location()
        .build()
        .fuse();
    let drain = slog_async::Async::new(drain)
        .overflow_strategy(OverflowStrategy::Block)
        .chan_size(1)
        .build()
        .fuse();
    slog::Logger::root(drain, o!())
}
