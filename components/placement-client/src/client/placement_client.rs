use std::{rc::Rc, time::Duration};

use model::{range_criteria::RangeCriteria, request::Request};
use slog::{error, trace, warn, Logger};
use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use super::{config::ClientConfig, response, session_manager::SessionManager};
use crate::error::ListRangeError;

pub struct PlacementClient {
    pub(crate) session_manager: Option<SessionManager>,
    pub(crate) tx: mpsc::UnboundedSender<(Request, oneshot::Sender<response::Response>)>,
    pub(crate) log: Logger,
    pub(crate) config: Rc<ClientConfig>,
}

impl PlacementClient {
    pub fn start(&mut self) {
        if let Some(mut session_manager) = self.session_manager.take() {
            tokio_uring::spawn(async move {
                session_manager.run().await;
            });
        }
    }

    pub async fn list_range(
        &self,
        stream_id: Option<i64>,
        timeout: Duration,
    ) -> Result<response::Response, ListRangeError> {
        let (tx, rx) = oneshot::channel();
        let criteria = if let Some(stream_id) = stream_id {
            trace!(self.log, "list_range"; "stream-id" => stream_id);
            RangeCriteria::StreamId(stream_id)
        } else if let Some(ref data_node) = self.config.data_node {
            trace!(
                self.log,
                "List stream ranges from placement manager for {:?}",
                data_node
            );
            RangeCriteria::DataNode(data_node.clone())
        } else {
            return Err(ListRangeError::BadArguments(
                "Either stream_id or data-node is required to list range".to_owned(),
            ));
        };

        let request = Request::ListRanges {
            timeout: Duration::from_secs(3),
            criteria: vec![criteria],
        };
        self.tx.send((request, tx)).map_err(|e| {
            error!(self.log, "Failed to forward request. Cause: {:?}", e; "struct" => "Client");
            ListRangeError::Internal
        })?;
        trace!(self.log, "Request forwarded"; "struct" => "Client");

        time::timeout(timeout, rx).await.map_err(|elapsed| {
            warn!(self.log, "Timeout when list range. {}", elapsed);
            ListRangeError::Timeout
        })?.map_err(|e| {
            error!(
                self.log,
                "Failed to receive response from broken channel. Cause: {:?}", e; "struct" => "Client"
            );
            ListRangeError::Internal
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use protocol::rpc::header::ErrorCode;
    use slog::trace;
    use test_util::{run_listener, terminal_logger};

    use crate::{client::response, error::ListRangeError, PlacementClientBuilder};

    #[test]
    fn test_list_range() -> Result<(), ListRangeError> {
        tokio_uring::start(async {
            let log = terminal_logger();
            let port = 2378;
            let port = run_listener(log.clone()).await;
            let addr = format!("dns:localhost:{}", port);
            let mut client = PlacementClientBuilder::new(&addr)
                .set_log(log.clone())
                .build()
                .map_err(|_e| ListRangeError::Internal)?;

            client.start();

            let timeout = Duration::from_secs(10);

            for i in 0..3 {
                let result = client.list_range(Some(i as i64), timeout).await.unwrap();
                if let response::Response::ListRange {
                    ref ranges,
                    ref status,
                    ..
                } = result
                {
                    assert_eq!(ErrorCode::OK, status.code);
                    assert!(ranges.is_some(), "Should have got some ranges");
                    if let Some(ranges) = ranges {
                        assert_eq!(
                            false,
                            ranges.is_empty(),
                            "Test server should have fed some mocking ranges"
                        );
                        for range in ranges.iter() {
                            trace!(log, "{}", range)
                        }
                    }
                } else {
                    panic!("Incorrect response enum variant");
                }
            }

            Ok(())
        })
    }
}
