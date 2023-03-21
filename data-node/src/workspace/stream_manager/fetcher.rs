use model::range::StreamRange;
use placement_client::PlacementClient;
use tokio::sync::{mpsc, oneshot};

use crate::error::ServiceError;

/// Non-primary `Node` uses this task to delegate query range task to the primary one.
pub struct FetchRangeTask {
    /// Stream-id to query
    stream_id: i64,

    /// Once the query completes, transfer results back to the caller through this oneshot channel.
    tx: oneshot::Sender<Result<Vec<StreamRange>, ServiceError>>,
}

pub(crate) enum Fetcher {
    /// If a `Node` of `DataNode` is playing primary role, it is carrying the responsibility of communicating with
    /// `PlacementManager`.
    ///
    /// Primary `Node` fetches ranges of a stream for itself or on behalf of other `Node`s.
    PlacementClient { client: PlacementClient },

    /// Non-primary `Node`s acquires ranges of a stream through delegating to the primary node.
    Channel {
        sender: mpsc::UnboundedSender<FetchRangeTask>,
    },
}

impl Fetcher {
    pub(crate) async fn fetch(&mut self, stream_id: i64) -> Result<Vec<StreamRange>, ServiceError> {
        match self {
            Fetcher::Channel { sender } => Self::fetch_from_peer_node(sender, stream_id).await,
            Fetcher::PlacementClient { client } => Self::fetch_by_client(client, stream_id).await,
        }
    }

    async fn fetch_by_client(
        _client: &PlacementClient,
        _stream_id: i64,
    ) -> Result<Vec<StreamRange>, ServiceError> {
        todo!()
    }

    async fn fetch_from_peer_node(
        sender: &mpsc::UnboundedSender<FetchRangeTask>,
        stream_id: i64,
    ) -> Result<Vec<StreamRange>, ServiceError> {
        let (tx, rx) = oneshot::channel();
        let task = FetchRangeTask { stream_id, tx };
        if let Err(e) = sender.send(task) {
            let task = e.0;
            let _ = task.tx.send(Err(ServiceError::AcquireRange));
        }
        rx.await
            .map_err(|_e| ServiceError::Internal("Broken oneshot channel".to_owned()))?
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use model::range::StreamRange;

    use super::Fetcher;

    #[test]
    fn test_fetch_from_peer_node() -> Result<(), Box<dyn Error>> {
        tokio_uring::start(async {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

            let mut fetcher = Fetcher::Channel { sender: tx };
            const TOTAL: i32 = 16;

            tokio_uring::spawn(async move {
                loop {
                    match rx.recv().await {
                        Some(task) => {
                            let stream_id = task.stream_id;
                            let ranges = (0..TOTAL)
                                .map(|i| {
                                    if i < TOTAL - 1 {
                                        StreamRange::new(
                                            stream_id,
                                            i,
                                            (i * 100) as u64,
                                            ((i + 1) * 100) as u64,
                                            Some(((i + 1) * 100) as u64),
                                        )
                                    } else {
                                        StreamRange::new(stream_id, i, (i * 100) as u64, 0, None)
                                    }
                                })
                                .collect::<Vec<_>>();
                            if let Err(e) = task.tx.send(Ok(ranges)) {
                                panic!("Failed to transfer mocked ranges");
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
            });

            let res = fetcher.fetch(1).await?;
            assert_eq!(res.len(), TOTAL as usize);
            drop(fetcher);
            Ok(())
        })
    }
}
