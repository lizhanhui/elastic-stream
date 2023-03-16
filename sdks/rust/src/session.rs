use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use slog::Logger;
use tokio::net::TcpStream;

use crate::{
    channel_reader::ChannelReader, channel_writer::ChannelWriter, client_error::ClientError,
};

#[derive(Debug)]
pub(crate) struct Session {
    log: Logger,
    inflight: Arc<Mutex<HashMap<u64, ()>>>,
    channel_writer: ChannelWriter,
}

impl Session {
    fn spawn_read_loop(
        mut reader: ChannelReader,
        inflight: Arc<Mutex<HashMap<u64, ()>>>,
        log: Logger,
    ) {
        tokio::spawn(async move {
            loop {
                match reader.read().await {
                    Ok(Some(_frame)) => {}
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        break;
                    }
                }
            }
        });
    }

    pub(crate) async fn new(target: &str, log: Logger) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(target).await?;
        let (read_half, write_half) = stream.into_split();

        let reader = ChannelReader::new(read_half, log.clone());
        let inflight = Arc::new(Mutex::new(HashMap::new()));

        Self::spawn_read_loop(reader, Arc::clone(&inflight), log.clone());

        let writer = ChannelWriter::new(write_half, log.clone());

        Ok(Self {
            log,
            inflight: Arc::clone(&inflight),
            channel_writer: writer,
        })
    }
}
