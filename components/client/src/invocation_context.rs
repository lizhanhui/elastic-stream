use std::{cell::OnceCell, net::SocketAddr};

use crate::{request, response};
use local_sync::oneshot;
use log::error;

#[derive(Debug)]
pub struct InvocationContext {
    target: SocketAddr,
    request: request::Request,
    pub(crate) response_observer: OnceCell<oneshot::Sender<response::Response>>,
}

impl InvocationContext {
    pub(crate) fn new(
        target: SocketAddr,
        request: request::Request,
        response_observer: oneshot::Sender<response::Response>,
    ) -> Self {
        let cell = OnceCell::new();
        let _ = cell.set(response_observer);
        Self {
            target,
            request,
            response_observer: cell,
        }
    }

    pub(crate) fn target(&self) -> SocketAddr {
        self.target
    }

    pub(crate) fn request(&self) -> &request::Request {
        &self.request
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.response_observer
            .get()
            .map_or(true, |tx| tx.is_closed())
    }

    pub(crate) fn write_response(&mut self, response: response::Response) {
        if let Some(tx) = self.response_observer.take() {
            if let Err(response) = tx.send(response) {
                error!("Failed to forward response: {:?}", response);
            }
        }
    }

    pub(crate) fn response_observer(&mut self) -> Option<oneshot::Sender<response::Response>> {
        self.response_observer.take()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        net::{IpAddr, Ipv4Addr},
        time::Duration,
    };

    use local_sync::oneshot;
    use protocol::rpc::header::OperationCode;

    #[test]
    fn test_invocation_new() -> Result<(), Box<dyn Error>> {
        let target = "127.0.0.1:80".parse()?;
        let (tx, mut rx) = oneshot::channel();
        let request = crate::request::Request {
            timeout: Duration::from_millis(1),
            headers: crate::request::Headers::Append,
            body: None,
        };

        let mut ctx = super::InvocationContext::new(target, request, tx);
        assert_eq!(ctx.target().ip(), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        assert_eq!(ctx.target().port(), 80);
        assert!(!ctx.is_closed());

        let observer = ctx.response_observer();
        assert!(observer.is_some());
        assert!(ctx.is_closed());

        let response = crate::response::Response::new(OperationCode::APPEND);
        observer.unwrap().send(response).unwrap();

        rx.try_recv().unwrap();

        Ok(())
    }
}
