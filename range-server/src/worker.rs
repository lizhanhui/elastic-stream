use std::{
    cell::{RefCell, UnsafeCell},
    error::Error,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    connection_tracker::ConnectionTracker,
    heartbeat::Heartbeat,
    metadata::MetadataWatcher,
    range_manager::{fetcher::FetchRangeTask, RangeManager},
    worker_config::WorkerConfig,
};
use client::{client::Client, DefaultClient};
use log::{debug, error, info, warn};
use monoio::FusionDriver;
use observation::metrics::{
    store_metrics::RangeServerStatistics,
    sys_metrics::{DiskStatistics, MemoryStatistics},
    uring_metrics::UringStatistics,
};
use pd_client::PlacementDriverClient;
use protocol::rpc::header::RangeServerState;
use store::Store;
use tokio::sync::{broadcast, mpsc};
use tokio_uring::net::TcpListener;
use util::metrics::http_serve;

/// A server aggregates one or more `Worker`s and each `Worker` takes up a dedicated CPU
/// processor, following the Thread-per-Core design paradigm.
///
/// There is only one primary worker, which is in charge of the stream/range management
/// and communication with the placement driver.
///
/// Inter-worker communications are achieved via channels.
pub(crate) struct Worker<S, M, P, W> {
    config: WorkerConfig,
    store: Rc<S>,
    range_manager: Rc<UnsafeCell<M>>,
    client: Rc<DefaultClient>,
    pd_client: Rc<P>,
    metadata_watcher: Option<W>,

    state: Rc<RefCell<RangeServerState>>,

    #[allow(dead_code)]
    channels: Option<Vec<mpsc::UnboundedReceiver<FetchRangeTask>>>,
}

impl<S, M, P, W> Worker<S, M, P, W>
where
    S: Store + 'static,
    M: RangeManager + 'static,
    P: PlacementDriverClient + 'static,
    W: MetadataWatcher + 'static,
{
    pub fn new(
        config: WorkerConfig,
        store: Rc<S>,
        range_manager: Rc<UnsafeCell<M>>,
        client: Rc<DefaultClient>,
        pd_client: Rc<P>,
        metadata_watcher: Option<W>,
        channels: Option<Vec<mpsc::UnboundedReceiver<FetchRangeTask>>>,
    ) -> Self {
        Self {
            config,
            store,
            range_manager,
            client,
            pd_client,
            metadata_watcher,
            state: Rc::new(RefCell::new(
                RangeServerState::RANGE_SERVER_STATE_READ_WRITE,
            )),
            channels,
        }
    }

    pub fn serve(&mut self, shutdown: broadcast::Sender<()>) {
        core_affinity::set_for_current(self.config.core_id);
        if self.config.primary {
            info!(
                "Bind primary worker to processor-{}",
                self.config.core_id.id
            );
        } else {
            info!("Bind worker to processor-{}", self.config.core_id.id);
        }

        info!(
            "The number of Submission Queue entries in uring: {}",
            self.config.server_config.server.uring.queue_depth
        );
        let uring_builder = io_uring::Builder::default();

        uring_builder
            .dontfork()
            .setup_attach_wq(self.config.sharing_uring);

        let mut rt = monoio::RuntimeBuilder::<FusionDriver>::new()
            .enable_all()
            .with_entries(self.config.server_config.server.uring.queue_depth)
            .uring_builder(uring_builder)
            .build()
            .unwrap();
        rt.block_on(async {
            // Run dispatching service of Store.
            self.store.start();

            if let Some(watcher) = self.metadata_watcher.as_ref() {
                watcher.start(self.pd_client.clone());
            }
            let bind_address = &self.config.server_config.server.addr;
            let listener = match TcpListener::bind(bind_address.parse().expect("Failed to bind")) {
                Ok(listener) => {
                    info!("Server starts OK, listening {}", bind_address);
                    listener
                }
                Err(e) => {
                    eprintln!("{}", e);
                    return;
                }
            };

            if unsafe { &mut *self.range_manager.get() }
                .start()
                .await
                .is_err()
            {
                eprintln!(
                    "Failed to bootstrap stream ranges from PD: {}",
                    self.config.server_config.placement_driver
                );
                panic!("Failed to retrieve bootstrap stream ranges metadata from PD");
            }

            if self.config.primary {
                let port = self.config.server_config.observation.metrics.port;
                let host = self.config.server_config.observation.metrics.host.clone();
                monoio::spawn(async move {
                    http_serve(&host, port).await;
                });
                self.report_metrics(shutdown.subscribe());
            }

            // TODO: report after pd can handle the request.
            // self.report_range_progress(shutdown.subscribe());

            self.heartbeat(shutdown.clone(), Rc::clone(&self.state));

            match self.run(listener, shutdown.subscribe()).await {
                Ok(_) => {}
                Err(e) => {
                    error!("Runtime failed. Cause: {}", e.to_string());
                }
            }
        });
    }

    fn report_metrics(&self, mut shutdown_rx: broadcast::Receiver<()>) {
        let client = Rc::clone(&self.client);
        let config = Arc::clone(&self.config.server_config);
        let state = Rc::clone(&self.state);
        monoio::spawn(async move {
            // TODO: Modify it to client report metrics interval, instead of config.client_heartbeat_interval()
            let mut interval = tokio::time::interval(config.client_heartbeat_interval());
            let mut uring_statistics = UringStatistics::new();
            let mut range_server_statistics = RangeServerStatistics::new();
            let mut disk_statistics = DiskStatistics::new();
            let mut memory_statistics = MemoryStatistics::new();
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Received shutdown signal. Stop accepting new connections.");
                        break;
                    }
                    _ = interval.tick() => {
                        uring_statistics.record();
                        range_server_statistics.record();
                        disk_statistics.record();
                        memory_statistics.record();
                        let state = *state.borrow();
                        let _ = client
                        .report_metrics(
                            &config.placement_driver, state, &uring_statistics, &range_server_statistics, &disk_statistics, &memory_statistics)
                        .await;
                    }

                }
            }
        });
    }

    fn heartbeat(&self, shutdown: broadcast::Sender<()>, state: Rc<RefCell<RangeServerState>>) {
        let client = Rc::clone(&self.client);
        let config = Arc::clone(&self.config.server_config);
        let heartbeat = Heartbeat::new(client, config, state, shutdown);
        heartbeat.run();
    }

    #[allow(dead_code)]
    fn report_range_progress(&self, mut shutdown_rx: broadcast::Receiver<()>) {
        let client = self.client.clone();
        let config = Arc::clone(&self.config.server_config);
        let range_manager = self.range_manager.clone();
        monoio::spawn(async move {
            let mut interval = tokio::time::interval(config.client_heartbeat_interval());
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Received shutdown signal. Stop report range progress.");
                        break;
                    }
                    _  = interval.tick() => {
                        let range_progress = unsafe{ & *range_manager.get()}.get_range_progress().await;
                        let _ = client.report_range_progress(range_progress).await;
                    }
                }
            }
        });
    }

    async fn run(
        &self,
        listener: TcpListener,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<(), Box<dyn Error>> {
        let connection_tracker = Rc::new(RefCell::new(ConnectionTracker::new()));
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Received shutdown signal. Stop accepting new connections.");
                    break;
                }

                incoming = listener.accept() => {
                    let (stream, peer_socket_address) = match incoming {
                        Ok((stream, socket_addr)) => {
                            debug!("Accepted a new connection from {socket_addr:?}");
                            stream.set_nodelay(true).unwrap_or_else(|e| {
                                warn!("Failed to disable Nagle's algorithm. Cause: {e:?}, PeerAddress: {socket_addr:?}");
                            });
                            debug!("Nagle's algorithm turned off");

                            (stream, socket_addr)
                        }
                        Err(e) => {
                            error!(
                                "Failed to accept a connection. Cause: {}",
                                e.to_string()
                            );
                            break;
                        }
                    };

                    let session = super::session::Session::new(
                        Arc::clone(&self.config.server_config),
                        stream, peer_socket_address,
                        Rc::clone(&self.store),
                        Rc::clone(&self.range_manager),
                        Rc::clone(&connection_tracker)
                       );
                    session.process();
                }
            }
        }

        // Send GoAway frame to all existing connections
        connection_tracker.borrow_mut().go_away();

        // Await till all existing connections disconnect
        let start = Instant::now();
        loop {
            let remaining = connection_tracker.borrow().len();
            if 0 == remaining {
                break;
            }

            info!(
                "There are {} connections left. Waited for {}/{} seconds",
                remaining,
                start.elapsed().as_secs(),
                self.config.server_config.server_grace_period().as_secs()
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
            if Instant::now() >= start + self.config.server_config.server_grace_period() {
                break;
            }
        }

        info!("Server#run completed");
        Ok(())
    }
}
