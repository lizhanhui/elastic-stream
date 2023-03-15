use std::{rc::Rc, time::Duration};

use model::{range_criteria::RangeCriteria, request::Request};
use slog::{error, trace, warn, Logger};
use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use super::{config::ClientConfig, response};
use crate::error::ListRangeError;

pub struct PlacementClient {
    pub(crate) tx: mpsc::UnboundedSender<(Request, oneshot::Sender<response::Response>)>,
    pub(crate) log: Logger,
    pub(crate) config: Rc<ClientConfig>,
}

impl PlacementClient {
    pub async fn list_range(
        &self,
        stream_id: i64,
        timeout: Duration,
    ) -> Result<response::Response, ListRangeError> {
        trace!(self.log, "list_range"; "stream-id" => stream_id);
        let (tx, rx) = oneshot::channel();
        let criteria = RangeCriteria::StreamId(stream_id);
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

    use test_util::{run_listener, terminal_logger};

    use crate::{client::response, error::ListRangeError, PlacementClientBuilder};

    #[test]
    fn test_list_range() -> Result<(), ListRangeError> {
        tokio_uring::start(async {
            let log = terminal_logger();
            let port = run_listener(log.clone()).await;
            let addr = format!("dns:localhost:{}", port);
            let client = PlacementClientBuilder::new(&addr)
                .set_log(log)
                .build()
                .await
                .map_err(|_e| ListRangeError::Internal)?;

            let timeout = Duration::from_secs(10);

            for i in 0..3 {
                let result = client.list_range(i as i64, timeout).await.unwrap();
                if let response::Response::ListRange { ref ranges, .. } = result {
                    assert!(ranges.is_some(), "Should have got some ranges");
                    if let Some(ranges) = ranges {
                        assert_eq!(false, ranges.is_empty(), "Test server should have fed some mocking ranges");
                    }
                } else {
                    panic!("Incorrect response enum variant");
                }
            }

            Ok(())
        })
    }
}
