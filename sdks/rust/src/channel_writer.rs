use codec::frame::Frame;
use slog::{trace, Logger};
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf};

use crate::client_error::ClientError;

#[derive(Debug)]
pub(crate) struct ChannelWriter {
    log: Logger,
    stream: OwnedWriteHalf,
}

impl ChannelWriter {
    pub(crate) fn new(stream: OwnedWriteHalf, log: Logger) -> Self {
        Self { log, stream }
    }

    pub(crate) async fn write(&mut self, frame: &Frame) -> Result<(), ClientError> {
        let slices = frame.encode()?;
        for slice in slices.into_iter() {
            self.stream.write_all(&slice).await?;
            trace!(self.log, "Written {}bytes", slice.len());
        }
        Ok(())
    }
}
