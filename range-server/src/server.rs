use crate::{
    built_info,
    metadata::{
        manager::DefaultMetadataManager, watcher::DefaultMetadataWatcher, MetadataManager,
        MetadataWatcher,
    },
    range_manager::manager::DefaultRangeManager,
    worker::Worker,
    worker_config::WorkerConfig,
};
use config::Configuration;
use core_affinity::CoreId;
use futures::executor::block_on;
use log::{error, info};
use model::error::EsError;
use object_storage::{object_storage::AsyncObjectStorage, ObjectStorage};
use pd_client::pd_client::DefaultPlacementDriverClient;
use pyroscope::PyroscopeAgent;
use pyroscope_pprofrs::{pprof_backend, PprofConfig};
use std::{
    error::Error,
    os::fd::AsRawFd,
    rc::Rc,
    sync::Arc,
    thread::{self, JoinHandle},
};
use store::{BufferedStore, ElasticStore, Store};
use tokio::sync::{broadcast, oneshot};

struct Server {
    config: Arc<Configuration>,
    store: ElasticStore,
    shutdown: broadcast::Sender<()>,
    object_storage: AsyncObjectStorage,
    metadata_watcher: DefaultMetadataWatcher,
}

impl Server {
    fn new(
        config: Arc<Configuration>,
        store: ElasticStore,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        let object_storage = AsyncObjectStorage::new(&config, store.clone());
        Self {
            config,
            store,
            shutdown,
            object_storage,
            metadata_watcher: DefaultMetadataWatcher::new(),
        }
    }

    fn start_profiler(&self) {
        #[cfg(feature = "trace")]
        observation::trace::start_trace_exporter(Arc::clone(&config), shutdown.subscribe());

        if self.config.server.profiling.enable {
            let server_endpoint = self.config.server.profiling.server_endpoint.clone();
            if server_endpoint.is_empty() {
                crate::profiling::generate_flame_graph(
                    Arc::clone(&self.config),
                    self.shutdown.subscribe(),
                );
                info!("Continuous profiling starts, generate flame graph locally");
            } else {
                let hostname = gethostname::gethostname()
                    .into_string()
                    .unwrap_or(String::from("unknown"));

                let mut tag_vec = vec![
                    ("hostname", hostname.as_str()),
                    ("version", built_info::PKG_VERSION),
                ];

                if let Some(commit_hash) = built_info::GIT_COMMIT_HASH {
                    tag_vec.push(("git_commit_hash", commit_hash))
                }

                if let Some(head_ref) = built_info::GIT_HEAD_REF {
                    tag_vec.push(("git_head_ref", head_ref))
                }

                PyroscopeAgent::builder(
                    server_endpoint.clone(),
                    "elastic-stream.range-server".to_string(),
                )
                .backend(pprof_backend(
                    PprofConfig::new()
                        .sample_rate(self.config.server.profiling.sampling_frequency as u32)
                        .report_thread_name(),
                ))
                .tags(tag_vec)
                .build()
                .expect("build pyroscope agent failed")
                .start()
                .expect("start pyroscope agent failed");
                info!("Continuous profiling starts, report to {}", server_endpoint);
            }
        }
    }

    fn start_worker(&mut self, core_id: CoreId, primary: bool) -> Result<JoinHandle<()>, EsError> {
        let server_config = self.config.clone();
        let store: ElasticStore = self.store.clone();
        let object_storage = self.object_storage.clone();

        let shutdown_tx = self.shutdown.clone();
        let metadata_watcher_rx = self.metadata_watcher.watch()?;
        let thread_name = if primary {
            "RangeServer[Primary]".to_owned()
        } else {
            "RangeServer".to_owned()
        };

        let metadata_watcher = if primary {
            Some(std::mem::take(&mut self.metadata_watcher))
        } else {
            None
        };

        let object_rx = if primary {
            Some(block_on(object_storage.watch_offload_progress()))
        } else {
            None
        };

        thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let worker_config = WorkerConfig {
                    core_id,
                    server_config: Arc::clone(&server_config),
                    sharing_uring: store.as_raw_fd(),
                    primary,
                };

                let store = Rc::new(BufferedStore::new(store));
                let client = Rc::new(client::DefaultClient::new(
                    Arc::clone(&server_config),
                    shutdown_tx.clone(),
                ));

                let mut metadata_manager = DefaultMetadataManager::new(
                    metadata_watcher_rx,
                    object_rx,
                    server_config.server.server_id,
                );

                let range_manager =
                    Rc::new(DefaultRangeManager::new(Rc::clone(&store), object_storage));

                metadata_manager.add_observer(Rc::downgrade(&(Rc::clone(&range_manager) as _)));
                metadata_manager.add_observer(Rc::downgrade(&(Rc::clone(&store) as _)));

                let pd_client = Box::new(DefaultPlacementDriverClient::new(Rc::clone(&client)));

                let mut worker = Worker::new(
                    worker_config,
                    range_manager,
                    client,
                    metadata_watcher,
                    metadata_manager,
                );
                worker.serve(pd_client, shutdown_tx)
            })
            .map_err(|e| EsError::unexpected(&e.to_string()))
    }
}

pub fn launch(
    config: Configuration,
    shutdown: broadcast::Sender<()>,
) -> Result<(), Box<dyn Error>> {
    let (recovery_completion_tx, recovery_completion_rx) = oneshot::channel();
    // Note we move the configuration into store, letting it either allocate or read existing server-id for us.
    let store = match ElasticStore::new(config, recovery_completion_tx) {
        Ok(store) => store,
        Err(e) => {
            error!("Failed to launch ElasticStore: {:?}", e);
            return Err(Box::new(e));
        }
    };
    recovery_completion_rx.blocking_recv()?;

    // Acquire a shared ref to full-fledged configuration.
    let config = store.config();

    let worker_core_ids = config::parse_cpu_set(&config.server.worker_cpu_set);
    debug_assert!(
        !worker_core_ids.is_empty(),
        "At least one core should be reserved for primary worker"
    );

    let mut server = Server::new(config, store, shutdown);

    // Build non-primary workers first
    let mut handles = worker_core_ids
        .iter()
        .rev()
        .skip(1)
        .map(|id| core_affinity::CoreId { id: *id as usize })
        .map(|core_id| server.start_worker(core_id, false))
        .collect::<Vec<_>>();

    // Build primary worker
    {
        let core_id = worker_core_ids
            .iter()
            .rev()
            .map(|id| core_affinity::CoreId { id: *id as usize })
            .next()
            .unwrap();
        let handle = server.start_worker(core_id, true);
        handles.push(handle);
    }

    server.start_profiler();

    for handle in handles.into_iter() {
        let _result = handle.unwrap().join();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cmp::min;

    use log::info;

    #[test]
    fn test_core_affinity() {
        let core_ids = core_affinity::get_core_ids().unwrap();
        let core_count = min(core_ids.len(), 2);

        let len = core_ids.len();
        let handles = core_ids
            .into_iter()
            .skip(len - core_count)
            .map(|processor_id| {
                std::thread::Builder::new()
                    .name("Worker".into())
                    .spawn(move || {
                        if core_affinity::set_for_current(processor_id) {
                            info!(
                                "Set affinity for worker thread {:?} OK",
                                std::thread::current()
                            );
                        }
                    })
            })
            .collect::<Vec<_>>();

        for handle in handles.into_iter() {
            handle.unwrap().join().unwrap();
        }
    }
}
