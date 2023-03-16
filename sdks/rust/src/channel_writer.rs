use slog::Logger;
use tokio::net::tcp::OwnedWriteHalf;

#[derive(Debug)]
pub(crate) struct ChannelWriter {
    log: Logger,
    stream: OwnedWriteHalf,
}

impl ChannelWriter {
    pub(crate) fn new(stream: OwnedWriteHalf, log: Logger) -> Self {
        Self { log, stream }
    }
}
