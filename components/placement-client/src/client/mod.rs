use crate::error::CommandError;
use std::{cell::UnsafeCell, collections::HashMap, rc::Rc};

use local_sync::{
    mpsc::unbounded::{channel, Rx, Tx},
    oneshot::Sender,
};
use monoio::{
    io::{ReadHalf, WriteHalf},
    net::TcpStream,
};
use slog::{debug, error, warn, Logger};
use transport::channel::{ChannelReader, ChannelWriter};

#[derive(Debug, Clone)]
pub enum Command {
    UnitTest,
}

#[derive(Debug, PartialEq)]
pub enum Reply {
    UnitTest,
}

#[derive(Debug)]
struct Request {
    target: String,
    command: Command,
    sender: Sender<Result<Reply, CommandError>>,
}

struct Session<'a> {
    reader: ChannelReader<ReadHalf<'a, TcpStream>>,
    writer: ChannelWriter<WriteHalf<'a, TcpStream>>,
}

struct Client {
    sessions: UnsafeCell<HashMap<String, Session<'static>>>,
    tx: Tx<Request>,
    rx: UnsafeCell<Rx<Request>>,
    log: Logger,
}

impl Client {
    pub fn new(log: Logger) -> Rc<Client> {
        let (tx, rx) = channel();

        Rc::new(Client {
            log,
            sessions: UnsafeCell::new(HashMap::new()),
            tx,
            rx: UnsafeCell::new(rx),
        })
    }

    pub fn start(client: &Rc<Client>) {
        let this = Rc::clone(&client);
        monoio::spawn(async move {
            this.do_loop().await;
        });
    }

    pub fn post(&self, request: Request) {
        self.tx.send(request).unwrap_or_else(|e| {
            error!(self.log, "Failed to dispatch request. Cause: {:?}", e);
        });
    }

    async fn do_loop(&self) {
        let rx = unsafe { &mut *self.rx.get() };
        debug!(self.log, "Await requests to dispatch");
        loop {
            match rx.recv().await {
                Some(request) => {
                    self.process(request).await;
                }
                None => {
                    warn!(
                        self.log,
                        "RX#recv returns None. Its TX should have been destructed"
                    );
                    break;
                }
            }
        }
    }

    async fn process(&self, request: Request) {
        match request.command {
            Command::UnitTest => {
                request
                    .sender
                    .send(Ok(Reply::UnitTest))
                    .unwrap_or_else(|e| error!(self.log, "Failed to write reply. Cause: {:?}", e));
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use slog::{o, Drain};

    fn get_logger() -> Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let log = slog::Logger::root(drain, o!());
        log
    }

    #[test]
    fn test_new() {
        let log = get_logger();
        let client = Client::new(log);
        let sessions = unsafe { &mut *client.sessions.get() };
        assert_eq!(true, sessions.is_empty());
    }

    #[monoio::test(enable_timer = true)]
    async fn test_start() {
        let log = get_logger();

        let client = Client::new(log.clone());
        Client::start(&client);
        let (sender, receiver) = local_sync::oneshot::channel();
        let request = Request {
            target: "localhost:1234".to_owned(),
            command: Command::UnitTest,
            sender,
        };

        client.post(request);

        match receiver.await {
            Ok(reply) => {
                assert_eq!(reply, Ok(Reply::UnitTest));
            }
            Err(e) => {
                panic!("{}", e)
            }
        };
    }
}
