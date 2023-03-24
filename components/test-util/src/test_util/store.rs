use store::option::StoreOptions;
use store::option::WalPath;
use store::ElasticStore;

pub fn build_store(wal_path: &str, index_path: &str) -> ElasticStore {
    let log = crate::terminal_logger();

    let size_10g = 10u64 * (1 << 30);

    let wal_path = WalPath::new(wal_path, size_10g).unwrap();

    let options = StoreOptions::new(&wal_path, index_path.to_string());
    let store = match ElasticStore::new(log.clone(), options) {
        Ok(store) => store,
        Err(e) => {
            panic!("Failed to launch ElasticStore: {:?}", e);
        }
    };
    store
}
