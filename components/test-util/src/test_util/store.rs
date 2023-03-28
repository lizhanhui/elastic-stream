use store::option::StoreOptions;
use store::option::WalPath;
use store::ElasticStore;
use tokio::sync::oneshot;

pub fn build_store(pm_address: String, wal_path: &str, index_path: &str) -> ElasticStore {
    let log = crate::terminal_logger();

    let size_10g = 10u64 * (1 << 30);

    let wal_path = WalPath::new(wal_path, size_10g).unwrap();

    let options = StoreOptions::new(
        "data-host".to_owned(),
        pm_address,
        &wal_path,
        index_path.to_string(),
    );
    let (recovery_completion_tx, recovery_completion_rx) = oneshot::channel();
    let store = match ElasticStore::new(log.clone(), options, recovery_completion_tx) {
        Ok(store) => store,
        Err(e) => {
            panic!("Failed to launch ElasticStore: {:?}", e);
        }
    };

    recovery_completion_rx
        .blocking_recv()
        .expect("Await recovery completion");

    store
}
