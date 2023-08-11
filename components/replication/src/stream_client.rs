use std::{sync::Arc, time::Duration};

use model::error::EsError;
use tokio::sync::{mpsc, oneshot};

use crate::{
    request::{
        AppendRequest, AppendResponse, CloseStreamRequest, CreateStreamRequest, OpenStreamRequest,
        ReadRequest, ReadResponse, Request, TrimRequest,
    },
    stream::stream_manager::StreamManager,
};

/// `StreamClient` is designed to be `Send`
#[derive(Debug, Clone)]
pub struct StreamClient {
    tx: mpsc::UnboundedSender<Request>,
}

impl StreamClient {
    pub fn new(config: Arc<config::Configuration>, id: usize) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let _ = std::thread::Builder::new()
            .name(format!("Runtime-{}", id))
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .with_entries(32768)
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let stream_manager = StreamManager::new(config);
                    Self::spawn_loop(stream_manager, rx).await;
                })
            });

        Self { tx }
    }

    async fn spawn_loop(
        mut stream_manager: StreamManager,
        mut rx: mpsc::UnboundedReceiver<Request>,
    ) {
        while let Some(request) = rx.recv().await {
            match request {
                Request::Append { request, tx } => {
                    stream_manager.append(request, tx);
                }
                Request::Read { request, tx } => {
                    stream_manager.fetch(request, tx);
                }
                Request::CreateStream { request, tx } => {
                    stream_manager.create(request, tx);
                }
                Request::OpenStream { request, tx } => {
                    stream_manager.open(request, tx);
                }
                Request::CloseStream { request, tx } => {
                    stream_manager.close(request, tx);
                }
                Request::StartOffset { request, tx } => {
                    stream_manager.start_offset(request, tx);
                }
                Request::NextOffset { request, tx } => {
                    stream_manager.next_offset(request, tx);
                }
                Request::Trim { request, tx } => {
                    stream_manager.trim(request, tx);
                }
            }
        }
    }

    pub async fn create_stream(
        &self,
        replica: u8,
        ack_count: u8,
        retention_period: Duration,
    ) -> Result<u64, EsError> {
        let request = CreateStreamRequest {
            replica,
            ack_count,
            retention_period,
        };

        let (tx, rx) = oneshot::channel();
        let req = Request::CreateStream { request, tx };
        self.tx.send(req).expect("create stream send request to tx");
        rx.await
            .unwrap_or_else(|_| {
                Err(EsError::unexpected(
                    "create stream fail to receive response from rx",
                ))
            })
            .map(|res| res.stream_id)
    }

    pub async fn open_stream(&self, stream_id: u64, epoch: u64) -> Result<(), EsError> {
        let request = OpenStreamRequest { stream_id, epoch };
        let (tx, rx) = oneshot::channel();
        let req = Request::OpenStream { request, tx };
        self.tx.send(req).expect("open stream send request to tx");
        rx.await
            .unwrap_or_else(|_| {
                Err(EsError::unexpected(
                    "open stream fail to receive response from rx",
                ))
            })
            .map(|_| ())
    }

    pub async fn close_stream(&self, stream_id: u64) -> Result<(), EsError> {
        let request = CloseStreamRequest { stream_id };
        let (tx, rx) = oneshot::channel();
        let req = Request::CloseStream { request, tx };
        self.tx.send(req).expect("close stream send request to tx");
        rx.await.unwrap_or_else(|_| {
            Err(EsError::unexpected(
                "close stream fail to receive response from rx",
            ))
        })
    }

    pub async fn append(&self, request: AppendRequest) -> Result<AppendResponse, EsError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::Append { tx, request };
        self.tx.send(req).expect("append send request to tx");
        rx.await.unwrap_or_else(|_| {
            Err(EsError::unexpected(
                "append receive fail to receive response from rx",
            ))
        })
    }

    pub async fn read(&self, request: ReadRequest) -> Result<ReadResponse, EsError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::Read { tx, request };
        self.tx.send(req).expect("read send request to tx");
        rx.await.unwrap_or_else(|_| {
            Err(EsError::unexpected(
                "read receive fail to receive response from rx",
            ))
        })
    }

    pub async fn start_offset(&self, stream_id: u64) -> Result<u64, EsError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::StartOffset {
            request: stream_id,
            tx,
        };
        self.tx.send(req).expect("start offset send request to tx");
        rx.await.unwrap_or_else(|_| {
            Err(EsError::unexpected(
                "start offset fail to receive response from rx",
            ))
        })
    }

    pub async fn next_offset(&self, stream_id: u64) -> Result<u64, EsError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::NextOffset {
            request: stream_id,
            tx,
        };
        self.tx.send(req).expect("next offset send request to tx");
        rx.await.unwrap_or_else(|_| {
            Err(EsError::unexpected(
                "next offset fail to receive response from rx",
            ))
        })
    }

    pub async fn trim(&self, request: TrimRequest) -> Result<(), EsError> {
        let (tx, rx) = oneshot::channel();
        let req = Request::Trim { request, tx };
        self.tx.send(req).expect("trim send request to tx");
        rx.await
            .unwrap_or_else(|_| Err(EsError::unexpected("trim fail to receive response from rx")))
    }
}
