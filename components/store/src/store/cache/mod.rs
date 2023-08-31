use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use futures::future::join_all;

use cache::{HierarchicalCache, SizedValue};
use config::Configuration;
use model::range::RangeMetadata;

use crate::error::{AppendError, FetchError, StoreError};
use crate::io::task::SingleFetchResult;
use crate::option::{ReadOptions, WriteOptions};
use crate::{AppendRecordRequest, AppendResult, FetchResult, Store};

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
struct CacheKey {
    stream_id: u64,
    range: u32,
    offset: u64,
}

impl CacheKey {
    fn new(stream_id: u64, range: u32, offset: u64) -> Self {
        Self {
            stream_id,
            range,
            offset,
        }
    }
}

#[derive(Debug, Clone)]
struct CacheValue {
    wal_offset: u64,
    payload: Vec<bytes::Bytes>,
}

impl SizedValue for CacheValue {
    fn size(&self) -> u64 {
        self.payload.iter().map(|bytes| bytes.len() as u64).sum()
    }
}

pub struct CachedStore<S> {
    store: S,
    cache: Rc<RefCell<HierarchicalCache<CacheKey, CacheValue>>>,
}

impl<S> CachedStore<S>
where
    S: Store,
{
    pub fn new(store: S) -> Self {
        let config = store.config();
        Self {
            store,
            cache: Rc::new(RefCell::new(HierarchicalCache::new(
                config.store.max_cache_size,
                config.store.cache_high_watermark,
            ))),
        }
    }
}

impl<S> Store for CachedStore<S>
where
    S: Store,
{
    async fn append(
        &self,
        options: &WriteOptions,
        request: AppendRecordRequest,
    ) -> Result<AppendResult, AppendError> {
        let payload = request.buffer.clone();
        let result = self.store.append(options, request).await;
        if result.is_ok() {
            let append_result = result.as_ref().unwrap();
            self.cache.borrow_mut().push_active(
                CacheKey {
                    stream_id: append_result.stream_id,
                    range: append_result.range_index,
                    offset: append_result.offset,
                },
                CacheValue {
                    wal_offset: append_result.wal_offset,
                    payload: Vec::from([payload]),
                },
            );
        }
        result
    }

    async fn fetch(&self, options: ReadOptions) -> Result<FetchResult, FetchError> {
        let result = self.cache.borrow_mut().search(
            &CacheKey::new(options.stream_id, options.range, options.offset)
                ..&CacheKey::new(options.stream_id, options.range, options.max_offset),
        );

        if !result.is_empty() {
            let mut fetch_result_vec = vec![];
            let mut need_fetch_range_vec = vec![];

            let mut except_key: CacheKey = CacheKey {
                stream_id: options.stream_id,
                range: options.range,
                offset: options.offset,
            };
            let mut fetch_bytes = 0;

            for (key, value) in result {
                // If the fetch bytes is greater than the max bytes, we need to break
                if fetch_bytes >= options.max_bytes as u64 {
                    break;
                }

                // If the key is not continuous, we need to fetch the missing range
                if key.offset > except_key.offset {
                    need_fetch_range_vec.push(except_key.offset..key.offset);
                }

                fetch_bytes += value.size();
                fetch_result_vec.push(SingleFetchResult {
                    stream_id: key.stream_id,
                    range: key.range,
                    offset: key.offset,
                    wal_offset: value.wal_offset as i64,
                    payload: value.payload,
                });
                except_key = CacheKey {
                    offset: key.offset + 1,
                    ..key
                };
            }

            // if fetch bytes is not full filled, we need to check and fetch data from max cached offset to max required offset
            if fetch_bytes < options.max_bytes as u64 && except_key.offset < options.max_offset {
                need_fetch_range_vec.push(except_key.offset..options.max_offset);
            }

            if !need_fetch_range_vec.is_empty() {
                let mut fetch_future_vec = vec![];

                // Fetch the missing range one by one
                for range in need_fetch_range_vec {
                    let future = self.store.fetch(ReadOptions {
                        offset: range.start,
                        max_offset: range.end,
                        ..options
                    });
                    fetch_future_vec.push(future);
                }
                let fetch_result = join_all(fetch_future_vec).await;

                let mut fetch_error = None;

                for result in fetch_result {
                    if let Err(error) = result {
                        // the error NoRecord is not a real error, we can ignore it
                        if error != FetchError::NoRecord {
                            fetch_error = Some(error);
                        }
                        continue;
                    }

                    let result = result.unwrap();
                    for single_result in result.results {
                        // Cache the fetch result
                        self.cache.borrow_mut().push_inactive(
                            CacheKey {
                                stream_id: single_result.stream_id,
                                range: single_result.range,
                                offset: single_result.offset,
                            },
                            CacheValue {
                                wal_offset: single_result.wal_offset as u64,
                                payload: single_result.payload.clone(),
                            },
                        );
                        fetch_result_vec.push(single_result);
                    }
                }

                // If partial fetch request failed, we need to return the error
                if let Some(error) = fetch_error {
                    return Err(error);
                }
            }

            // Sort the result by offset
            fetch_result_vec.sort_by(|a, b| a.offset.cmp(&b.offset));

            let mut start_offset = u64::MAX;
            let mut end_offset = u64::MIN;
            let mut total_len = 0;

            let mut final_result = Vec::with_capacity(fetch_result_vec.len());

            for result in fetch_result_vec {
                start_offset = std::cmp::min(start_offset, result.offset);
                end_offset = std::cmp::max(end_offset, result.offset);
                total_len += result.total_len();

                final_result.push(result);

                // Discard the remaining result when the fetch bytes is greater than the max bytes
                if total_len >= options.max_bytes as usize {
                    break;
                }
            }

            return Ok(FetchResult {
                stream_id: options.stream_id,
                range: options.range,
                start_offset,
                end_offset,
                total_len,
                results: final_result,
            });
        }

        // If cache is not hit, we need to fetch from the underlying store
        let fetch_result = self.store.fetch(options).await;
        // Cache the fetch result
        if fetch_result.is_ok() {
            let fetch_result = fetch_result.as_ref().unwrap();
            for result in fetch_result.results.iter() {
                self.cache.borrow_mut().push_inactive(
                    CacheKey {
                        stream_id: result.stream_id,
                        range: result.range,
                        offset: result.offset,
                    },
                    CacheValue {
                        wal_offset: result.wal_offset as u64,
                        payload: result.payload.clone(),
                    },
                );
            }
        }
        fetch_result
    }

    async fn list<F>(&self, filter: F) -> Result<Vec<RangeMetadata>, StoreError>
    where
        F: Fn(&RangeMetadata) -> bool + 'static,
    {
        self.store.list(filter).await
    }

    async fn list_by_stream<F>(
        &self,
        stream_id: u64,
        filter: F,
    ) -> Result<Vec<RangeMetadata>, StoreError>
    where
        F: Fn(&RangeMetadata) -> bool + 'static,
    {
        self.store.list_by_stream(stream_id, filter).await
    }

    async fn seal(&self, range: &RangeMetadata) -> Result<(), StoreError> {
        self.store.seal(range).await
    }

    async fn create(&self, range: &RangeMetadata) -> Result<(), StoreError> {
        self.store.create(range).await
    }

    fn get_range_end_offset(&self, stream_id: u64, range: u32) -> Result<Option<u64>, StoreError> {
        self.store.get_range_end_offset(stream_id, range)
    }

    fn id(&self) -> i32 {
        self.store.id()
    }

    fn config(&self) -> Arc<Configuration> {
        self.store.config()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use bytes::Bytes;
    use futures::future::join_all;
    use tokio::sync::oneshot;

    use mock_server::run_listener;

    use crate::error::AppendError;
    use crate::option::{ReadOptions, WriteOptions};
    use crate::store::cache::{CacheKey, CachedStore};
    use crate::store::elastic_store::tests::build_store;
    use crate::{AppendRecordRequest, AppendResult, Store};

    #[test]
    fn test_cached_store() -> Result<(), Box<dyn Error>> {
        crate::log::try_init_log();
        let store_dir = tempfile::tempdir()?;
        let store_path = store_dir.path().to_str().unwrap().to_owned();

        let (stop_tx, stop_rx) = oneshot::channel();
        let (port_tx, port_rx) = oneshot::channel();

        let handle = std::thread::spawn(move || {
            tokio_uring::start(async {
                let port = run_listener().await;
                let _ = port_tx.send(port);
                let _ = stop_rx.await;
            });
        });

        let port = port_rx.blocking_recv()?;
        let pd_address = format!("localhost:{}", port);

        let store = build_store(&pd_address, store_path.as_str());

        tokio_uring::start(async move {
            store.start();
            let options = WriteOptions::default();

            // Append 8 records to the underlying store directly
            let mut append_fs = vec![];
            (0..8)
                .map(|i| AppendRecordRequest {
                    stream_id: 1,
                    range_index: 0,
                    offset: i,
                    len: 1,
                    buffer: Bytes::from(format!("{}-{}", "hello, world", i)),
                })
                .for_each(|req| {
                    let append_f = store.append(&options, req);
                    append_fs.push(append_f)
                });
            let append_rs: Vec<Result<AppendResult, AppendError>> = join_all(append_fs).await;
            for result in append_rs {
                assert!(result.is_ok());
            }

            // Build the cached store
            let cached_store = CachedStore::new(store);

            // Append 2 records to the cached store
            let mut append_fs = vec![];
            (8..10)
                .map(|i| AppendRecordRequest {
                    stream_id: 1,
                    range_index: 0,
                    offset: i,
                    len: 1,
                    buffer: Bytes::from(format!("{}-{}", "hello, world", i)),
                })
                .for_each(|req| {
                    let append_f = cached_store.append(&options, req);
                    append_fs.push(append_f)
                });
            let append_rs: Vec<Result<AppendResult, AppendError>> = join_all(append_fs).await;
            for result in append_rs {
                assert!(result.is_ok());
            }

            {
                let cache_ref = cached_store.cache.borrow_mut();
                assert_eq!(cache_ref.len(), cache_ref.active_len());
                assert_eq!(cache_ref.active_len(), 2);
            }

            // Check if we can fetch the records from the cache directly
            // Try to fetch [8, 100) from the cached store
            // Currently, records with offset [8, 9] is in cache
            let fetch_result = cached_store
                .fetch(ReadOptions {
                    stream_id: 1,
                    range: 0,
                    offset: 8,
                    max_offset: 100,
                    max_wait_ms: 100,
                    max_bytes: 1024 * 1024,
                })
                .await;
            assert!(fetch_result.is_ok());
            let fetch_result = fetch_result.unwrap();
            assert_eq!(fetch_result.results.len(), 2);
            assert_eq!(
                fetch_result.total_len,
                format!("{}-{}", "hello, world", fetch_result.start_offset).len() * 2
            );
            assert_eq!(fetch_result.results[0].offset, 8);
            assert_eq!(
                fetch_result.results[0].payload[0],
                Bytes::from(format!("{}-{}", "hello, world", fetch_result.start_offset))
            );
            assert_eq!(fetch_result.results[1].offset, 9);
            assert_eq!(
                fetch_result.results[1].payload[0],
                Bytes::from(format!("{}-{}", "hello, world", fetch_result.end_offset))
            );

            // Check if the cache is hit
            {
                let cache_ref = cached_store.cache.borrow();
                assert_eq!(cache_ref.len(), cache_ref.active_len());
                let meta = cache_ref.get_meta(&CacheKey {
                    stream_id: 1,
                    range: 0,
                    offset: 8,
                });
                assert!(meta.is_some());
                assert_eq!(meta.unwrap().hit_count, 1);
            }

            // Check if we can fetch the records from the cached store that are not currently in the cache
            // Try to fetch [4, 6) from the cached store
            // Currently, records with offset [8, 9] is in cache
            let fetch_result = cached_store
                .fetch(ReadOptions {
                    stream_id: 1,
                    range: 0,
                    offset: 4,
                    max_offset: 6,
                    max_wait_ms: 100,
                    max_bytes: 1024 * 1024,
                })
                .await;
            assert!(fetch_result.is_ok());
            let fetch_result = fetch_result.unwrap();
            assert_eq!(fetch_result.results.len(), 2);
            assert_eq!(
                fetch_result.total_len,
                format!("{}-{}", "hello, world", fetch_result.start_offset).len() * 2
            );
            assert_eq!(fetch_result.results[0].offset, 4);
            assert_eq!(
                fetch_result.results[0].payload[0],
                Bytes::from(format!("{}-{}", "hello, world", fetch_result.start_offset))
            );
            assert_eq!(fetch_result.results[1].offset, 5);
            assert_eq!(
                fetch_result.results[1].payload[0],
                Bytes::from(format!("{}-{}", "hello, world", fetch_result.end_offset))
            );

            // Check if the cache is miss
            {
                let cache_ref = cached_store.cache.borrow();
                assert_eq!(cache_ref.len(), 4);
                assert_eq!(cache_ref.active_len(), 2);
                assert_eq!(cache_ref.inactive_len(), 2);
                let meta = cache_ref.get_meta(&CacheKey {
                    stream_id: 1,
                    range: 0,
                    offset: 4,
                });
                assert!(meta.is_some());
                assert_eq!(meta.unwrap().hit_count, 0);
            }

            // Check if we can fetch the records from the cached store which are partial not in the cache
            // Try to fetch [0, 100) from the cached store
            // Currently, records with offset [4, 5] and [8, 9] is in cache
            let fetch_result = cached_store
                .fetch(ReadOptions {
                    stream_id: 1,
                    range: 0,
                    offset: 0,
                    max_offset: 100,
                    max_wait_ms: 100,
                    max_bytes: 1024 * 1024,
                })
                .await;
            assert!(fetch_result.is_ok());
            let fetch_result = fetch_result.unwrap();
            assert_eq!(fetch_result.results.len(), 10);
            assert_eq!(
                fetch_result.total_len,
                format!("{}-{}", "hello, world", fetch_result.start_offset).len() * 10
            );

            // Check if the cache is hit
            {
                let cache_ref = cached_store.cache.borrow();
                assert_eq!(cache_ref.len(), 10);
                assert_eq!(cache_ref.active_len(), 2);
                assert_eq!(cache_ref.inactive_len(), 8);
                let meta = cache_ref.get_meta(&CacheKey {
                    stream_id: 1,
                    range: 0,
                    offset: 8,
                });
                assert!(meta.is_some());
                assert_eq!(meta.unwrap().hit_count, 2);
                let meta = cache_ref.get_meta(&CacheKey {
                    stream_id: 1,
                    range: 0,
                    offset: 4,
                });
                assert!(meta.is_some());
                assert_eq!(meta.unwrap().hit_count, 1);
            }
        });
        let _ = stop_tx.send(());
        let _ = handle.join();
        Ok(())
    }
}
