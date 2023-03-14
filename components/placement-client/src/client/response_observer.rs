use tokio::sync::oneshot;

use super::response;

pub(crate) struct ResponseObserver {
    /// Indicate if the response has been written.
    observer: Option<oneshot::Sender<response::Response>>,
}

impl ResponseObserver {
    pub(crate) fn new(observer: oneshot::Sender<response::Response>) -> Self {
        Self {
            observer: Some(observer),
        }
    }

    pub(crate) fn on_response(&mut self, response: response::Response) {
        if let Some(observer) = self.observer.take() {
            observer.send(response).unwrap_or_else(|_e| {});
        }
    }
}

impl Drop for ResponseObserver {
    fn drop(&mut self) {
        if let Some(observer) = self.observer.take() {
            let response = response::Response::Heartbeat {
                status: response::Status::Internal,
            };
            observer.send(response).unwrap_or_else(|_e| {});
        }
    }
}
