#[derive(Debug, Clone, PartialEq)]
pub struct StorePath {
    pub(crate) path: String,

    /// Target size of total files under the path, in byte.
    pub(crate) target_size: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoreOptions {
    pub(crate) create_if_missing: bool,
    pub(crate) store_path: StorePath,
    pub(crate) destroy_on_exit: bool,
}

impl StoreOptions {
    pub fn new(store_path: &StorePath) -> Self {
        Self {
            create_if_missing: true,
            store_path: store_path.clone(),
            destroy_on_exit: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[monoio::test]
    async fn test_store_options_new() {
        let store_path = StorePath {
            path: "/data/store".to_owned(),
            target_size: 1024 * 1024,
        };

        let options = StoreOptions::new(&store_path);

        assert_eq!(true, options.create_if_missing);
    }
}
