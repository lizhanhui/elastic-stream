use std::{collections::HashMap, net::SocketAddr};

use codec::frame::{Frame, OperationCode};
use slog::{info, warn, Logger};
use tokio::sync::mpsc::UnboundedSender;

/// Track all existing connections and send the `GoAway` farewell frame to peers if graceful shutdown is desirable.
pub(crate) struct ConnectionTracker {
    log: Logger,

    /// Map between peer socket address to channel sender.
    ///
    /// Note the reader half of the channel is the connection writer, responsible of writing
    /// frames to remote peer.
    connections: HashMap<SocketAddr, UnboundedSender<Frame>>,
}

impl ConnectionTracker {
    pub(crate) fn new(log: Logger) -> Self {
        Self {
            log,
            connections: HashMap::new(),
        }
    }

    pub(crate) fn insert(&mut self, peer_address: SocketAddr, sender: UnboundedSender<Frame>) {
        self.connections.insert(peer_address, sender);
        info!(self.log, "Start to track connection to {}", peer_address);
    }

    pub(crate) fn remove(&mut self, peer_address: &SocketAddr) {
        self.connections.remove(peer_address);
        info!(self.log, "Connection to {} disconnected", peer_address);
    }

    /// Send `GoAway` frame to all existing connections.
    ///
    /// When data node executes scheduled restarts. Graceful shutdown is desirable and
    /// it is a must to disconnect existing long connections gracefully.
    ///
    /// Roughly, three steps are done for this particular purpose:
    /// 1. Stop accepting new connections;
    /// 2. Send `GoAway` frame to each existing connection, requesting them to disconnect as soon as possible;
    /// 3. Await until all connections are terminated after client migrated to other data nodes.
    pub(crate) fn go_away(&mut self) {
        self.connections.iter().for_each(|(peer_address, sender)| {
            let mut frame = Frame::new(OperationCode::GoAway);
            // Go away frame has stream_id == 0.
            frame.stream_id = 0;
            match sender.send(frame) {
                Ok(_) => {
                    info!(
                        self.log,
                        "GoAway frame sent to channel bounded to connection={}", peer_address
                    );
                }
                Err(_e) => {
                    warn!(
                        self.log,
                        "Failed to send GoAway frame to channel bounded to connection={}",
                        peer_address
                    );
                }
            }
        });
    }

    pub(crate) fn len(&self) -> usize {
        self.connections.len()
    }
}
