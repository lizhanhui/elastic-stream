use std::{cell::RefCell, net::SocketAddr, rc::Rc, sync::Arc};

use config::Configuration;
use local_sync::mpsc;
use log::{info, trace, warn};
use tokio_uring::net::TcpStream;
use transport::connection::Connection;

use crate::{
    connection_handler, connection_tracker::ConnectionTracker, handler::ServerCall,
    range_manager::RangeManager,
};

pub(crate) struct Session<M> {
    config: Arc<Configuration>,
    connection: Rc<Connection>,
    range_manager: Rc<M>,
    connection_tracker: Rc<RefCell<ConnectionTracker>>,
}

impl<M> Session<M>
where
    M: RangeManager + 'static,
{
    pub(crate) fn new(
        config: Arc<Configuration>,
        stream: TcpStream,
        addr: SocketAddr,
        range_manager: Rc<M>,
        connection_tracker: Rc<RefCell<ConnectionTracker>>,
    ) -> Self {
        let connection = Rc::new(Connection::new(stream, addr));
        Self {
            config,
            connection,
            range_manager,
            connection_tracker,
        }
    }

    pub(crate) fn process(self) {
        let connection = Rc::clone(&self.connection);
        tokio_uring::spawn(async move {
            Self::process0(
                self.range_manager,
                self.connection_tracker,
                connection,
                self.config,
            )
            .await;
        });
    }

    async fn process0(
        range_manager: Rc<M>,
        connection_tracker: Rc<RefCell<ConnectionTracker>>,
        connection: Rc<Connection>,
        server_config: Arc<Configuration>,
    ) {
        // Channel to transfer responses from handlers to the coroutine that is in charge of response write.
        let (tx, mut rx) = mpsc::unbounded::channel();

        // Put current connection into connection-tracker, such that when TERM/STOP signal is received,
        // servers send go-away frame to each connection, requesting clients to complete and migrate as soon
        // as possible.
        connection_tracker
            .borrow_mut()
            .insert(connection.remote_addr(), tx.clone());

        let idle_handler = connection_handler::idle::IdleHandler::new(
            Rc::downgrade(&connection),
            Arc::clone(&server_config),
            Rc::clone(&connection_tracker),
        );

        // Coroutine to read requests from network connection
        let connection_ = Rc::clone(&connection);
        let read_idle_handler = Rc::clone(&idle_handler);
        tokio_uring::spawn(async move {
            loop {
                match connection_.read_frame().await {
                    Ok(Some(frame)) => {
                        // Update last read instant.
                        read_idle_handler.on_read();
                        let sender = tx.clone();
                        let range_manager = Rc::clone(&range_manager);
                        let mut server_call = ServerCall {
                            request: frame,
                            sender,
                            range_manager,
                        };
                        tokio_uring::spawn(async move {
                            server_call.call().await;
                        });
                    }
                    Ok(None) => {
                        info!(
                            "Connection {:?} --> {} is closed",
                            connection_.local_addr(),
                            connection_.remote_addr()
                        );
                        break;
                    }
                    Err(e) => {
                        warn!(
                            "Connection {:?} --> {} reset. Cause: {e:?}",
                            connection_.local_addr(),
                            connection_.remote_addr()
                        );
                        break;
                    }
                }
            }

            connection_tracker
                .borrow_mut()
                .remove(&connection_.remote_addr());
        });

        // Coroutine to write responses to network connection
        tokio_uring::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(frame) => {
                        let stream_id = frame.stream_id;
                        let opcode = frame.operation_code;
                        match connection.write_frame(frame).await {
                            Ok(_) => {
                                // Update last write instant
                                idle_handler.on_write();
                                trace!(
                                    "Response frame[stream-id={}, opcode={}] written to {}",
                                    stream_id,
                                    opcode.variant_name().unwrap_or("INVALID_OPCODE"),
                                    connection.remote_addr()
                                );
                            }
                            Err(e) => {
                                warn!(
                                "Failed to write response frame[stream-id={}, opcode={}] to {}. Cause: {:?}",
                                stream_id,
                                opcode.variant_name().unwrap_or("INVALID_OPCODE"),
                                connection.remote_addr(),
                                e
                            );
                                break;
                            }
                        }
                    }
                    None => {
                        info!(
                            "Channel to receive responses from handlers has been closed. Peer[address={}] should have already closed the read-half of the connection",
                            connection.remote_addr());
                        break;
                    }
                }
            }
        });
    }
}
