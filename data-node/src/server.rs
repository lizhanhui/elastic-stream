use std::{cell::RefCell, error::Error, os::fd::AsRawFd, rc::Rc, thread};

use crate::{
    node::Node,
    node_config::NodeConfig,
    workspace::stream_manager::{fetcher::Fetcher, StreamManager},
    ServerConfig,
};

use placement_client::{notifier::UnsupportedNotifier, ClientConfig, PlacementClientBuilder};
use slog::{error, o, warn, Drain, Logger};
use slog_async::Async;
use slog_term::{CompactFormat, TermDecorator};
use store::{
    option::{StoreOptions, WalPath},
    ElasticStore,
};
use tokio::sync::mpsc;

pub fn launch(cfg: &ServerConfig) -> Result<(), Box<dyn Error>> {
    let decorator = TermDecorator::new().build();
    let drain = CompactFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!());

    let core_ids = core_affinity::get_core_ids().ok_or_else(|| {
        warn!(log, "No cores are available to set affinity");
        crate::error::LaunchError::NoCoresAvailable
    })?;
    let available_core_len = core_ids.len();

    let size_10g = 10u64 * (1 << 30);
    let wal_path = WalPath::new("/data/store", size_10g)?;
    let meta_path = "/data/store/meta";
    let store_options = StoreOptions::new(&wal_path, meta_path.to_string());

    let store = match ElasticStore::new(log.clone(), store_options) {
        Ok(store) => store,
        Err(e) => {
            error!(log, "Failed to launch ElasticStore: {:?}", e);
            return Err(Box::new(e));
        }
    };

    let mut channels = vec![];

    // Build non-primary nodes first
    let mut handles = core_ids
        .iter()
        .skip(available_core_len - cfg.concurrency + 1)
        .map(|core_id| {
            let server_config = cfg.clone();
            let logger = log.new(o!());
            let store = store.clone();
            let core_id = core_id.clone();
            let (tx, rx) = mpsc::unbounded_channel();
            channels.push(rx);

            thread::Builder::new()
                .name("DataNode".to_owned())
                .spawn(move || {
                    let node_config = NodeConfig {
                        core_id,
                        server_config: server_config.clone(),
                        sharing_uring: store.as_raw_fd(),
                        primary: false,
                    };

                    let fetcher = Fetcher::Channel { sender: tx };
                    let stream_manager =
                        Rc::new(RefCell::new(StreamManager::new(logger.clone(), fetcher)));
                    let mut node = Node::new(node_config, store, stream_manager, None, &logger);
                    node.serve()
                })
        })
        .collect::<Vec<_>>();

    // Build primary node
    {
        let core_id = core_ids
            .get(available_core_len - cfg.concurrency)
            .expect("At least one core should be reserved for primary node")
            .clone();
        let server_config = cfg.clone();
        let handle = thread::Builder::new()
            .name("DataNode[Primary]".to_owned())
            .spawn(move || {
                let node_config = NodeConfig {
                    core_id,
                    server_config,
                    sharing_uring: store.as_raw_fd(),
                    primary: true,
                };

                let client_config = ClientConfig::default();

                let notifier = Rc::new(UnsupportedNotifier {});

                let placement_client =
                    PlacementClientBuilder::new(&node_config.server_config.placement_manager)
                        .set_log(log.clone())
                        .set_config(client_config)
                        .set_notifier(notifier)
                        .build()
                        .expect("Build placement client");
                let fetcher = Fetcher::PlacementClient {
                    client: placement_client,
                };
                let stream_manager =
                    Rc::new(RefCell::new(StreamManager::new(log.clone(), fetcher)));

                let mut node = Node::new(node_config, store, stream_manager, Some(channels), &log);
                node.serve()
            });
        handles.push(handle);
    }

    for handle in handles.into_iter() {
        let _result = handle.unwrap().join();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cmp::min;

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
                            println!(
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
