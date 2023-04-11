use std::{fs::OpenOptions, sync::Mutex};

use slog::{o, Drain, Duplicate, Logger};
use slog_async::{Async, OverflowStrategy};
use slog_term::{FullFormat, PlainDecorator, PlainSyncDecorator};

/// Create logger with terminal sinks.
///
/// # Note
/// The created logger has only a buffer size of 1, thus, is test-purpose only.
pub fn terminal_logger() -> Logger {
    let decorator = PlainSyncDecorator::new(slog_term::TestStdoutWriter);
    let drain = FullFormat::new(decorator)
        .use_file_location()
        .build()
        .fuse();
    let tmp_dir = std::env::temp_dir();
    let log_path = tmp_dir.join("test.log");
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(log_path)
        .unwrap();
    let decorator = PlainDecorator::new(file);
    let file_drain = FullFormat::new(decorator).build().fuse();
    let both = Mutex::new(Duplicate::new(drain, file_drain)).fuse();
    let drain = Async::new(both)
        .thread_name(String::from("log"))
        .chan_size(128)
        .overflow_strategy(OverflowStrategy::Block)
        .build()
        .fuse();
    slog::Logger::root(drain, o!())
}
