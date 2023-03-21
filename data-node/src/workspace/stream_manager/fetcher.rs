use crossbeam::channel::Sender;
use model::range::StreamRange;
use placement_client::PlacementClient;

use crate::error::ServiceError;

pub(crate) enum Fetcher {
    Channel { sender: Sender<()> },
    PlacementClient { client: PlacementClient },
}

impl Fetcher {
    pub(crate) async fn fetch(&self, stream_id: i64) -> Result<Option<StreamRange>, ServiceError> {
        match self {
            Fetcher::Channel { sender } => Self::fetch_from_peer_node(sender, stream_id).await,
            Fetcher::PlacementClient { client } => Self::fetch_by_client(client, stream_id).await,
        }
    }

    async fn fetch_by_client(
        _client: &PlacementClient,
        _stream_id: i64,
    ) -> Result<Option<StreamRange>, ServiceError> {
        todo!()
    }

    async fn fetch_from_peer_node(
        _tx: &Sender<()>,
        _stream_id: i64,
    ) -> Result<Option<StreamRange>, ServiceError> {
        todo!()
    }
}
