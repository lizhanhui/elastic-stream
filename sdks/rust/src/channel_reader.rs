use codec::frame::Frame;
use slog::Logger;
use tokio::net::tcp::OwnedReadHalf;

use crate::client_error::ClientError;

#[derive(Debug)]
pub(crate) struct ChannelReader {
    log: Logger,
    stream: OwnedReadHalf,
}

impl ChannelReader {
    pub(crate) fn new(stream: OwnedReadHalf, log: Logger) -> Self {
        Self { log, stream }
    }

    pub(crate) async fn read(&mut self) -> Result<Option<Frame>, ClientError> {
        Ok(None)
    }
}
