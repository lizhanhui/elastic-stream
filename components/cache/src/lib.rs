use std::cell::RefCell;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::ops::Range;
use std::rc::Rc;
use std::time::Instant;

use lru::LruCache;
use percentage::Percentage;

pub trait SizedValue {
    /// Returns size of cache value in bytes.
    fn size(&self) -> u64;
}

#[derive(Debug, Clone)]
pub struct CacheMeta {
    /// Weight of the cache item, if the weight is greater than 0, the item will not be reclaimed but move to the head of current list.
    pub weight: u8,

    /// Hit count of the cache item.
    pub hit_count: u64,

    /// Last access time of the cache item.
    pub last_access_time: Instant,
}

struct Value<V> {
    meta: CacheMeta,
    value: V,
}

type ValueRef<V> = Rc<RefCell<Value<V>>>;

/// [`HierarchicalCache`] is inspired by page cache in linux kernel which has active list and inactive list.
pub struct HierarchicalCache<K, V> {
    map: BTreeMap<K, ValueRef<V>>,
    active_list: LruCache<K, ValueRef<V>>,
    active_size: u64,
    max_active_size: u64,
    inactive_list: LruCache<K, ValueRef<V>>,
    inactive_size: u64,
    max_cache_size: u64,
}

impl<K: Hash + Eq + Ord + Clone, V: SizedValue + Clone> HierarchicalCache<K, V> {
    /// Create a cache with the name given parameters.
    ///
    /// # Arguments
    ///
    /// * `max_cache_size` - The maximum size of the cache in bytes
    /// * `active_percent` - The percentage of the cache that in active list
    pub fn new(max_cache_size: u64, active_percent: usize) -> Self {
        let max_active_size = Percentage::from(active_percent).apply_to(max_cache_size);
        HierarchicalCache {
            map: BTreeMap::new(),
            active_list: LruCache::unbounded(),
            active_size: 0,
            max_active_size,
            inactive_list: LruCache::unbounded(),
            inactive_size: 0,
            max_cache_size,
        }
    }

    fn check_already_cached(&mut self, key: &K) -> bool {
        if self.active_list.contains(key) {
            return true;
        }
        if self.inactive_list.contains(key) {
            return true;
        }
        false
    }

    /// Add a key-value pair to the active list.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the cache item
    /// * `value` - The value of the cache item
    pub fn push_active(&mut self, key: K, value: V) {
        self.push_active_with_weight(key, value, 0);
    }

    /// Add a key-value pair to the active list.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the cache item
    /// * `value` - The value of the cache item
    /// * `weight` - The weight of the cache item, if the weight is greater than 0, the item will not be reclaimed but move to the head of current list
    pub fn push_active_with_weight(&mut self, key: K, value: V, weight: u8) {
        let value_size = value.size();
        let value_ref = Rc::new(RefCell::new(Value {
            meta: CacheMeta {
                weight,
                hit_count: 0,
                last_access_time: Instant::now(),
            },
            value,
        }));

        if self.check_already_cached(&key) {
            // if already cached, just update the value
            self.map.insert(key, value_ref);
            return;
        }

        self.reclaim_active_list(value_size);

        self.active_list.put(key.clone(), Rc::clone(&value_ref));
        self.active_size += value_size;
        self.map.insert(key, value_ref);
    }

    /// Add a key-value pair to the inactive list.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the cache item
    /// * `value` - The value of the cache item
    pub fn push_inactive(&mut self, key: K, value: V) {
        self.push_inactive_with_weight(key, value, 0);
    }

    /// Add a key-value pair to the inactive list.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the cache item
    /// * `value` - The value of the cache item
    /// * `weight` - The weight of the cache item, if the weight is greater than 0, the item will not be reclaimed but move to the head of current list
    pub fn push_inactive_with_weight(&mut self, key: K, value: V, weight: u8) {
        let value_size = value.size();
        let value_ref = Rc::new(RefCell::new(Value {
            meta: CacheMeta {
                weight,
                hit_count: 0,
                last_access_time: Instant::now(),
            },
            value,
        }));

        if self.check_already_cached(&key) {
            // if already cached, just update the value
            self.map.insert(key, value_ref);
            return;
        }

        self.reclaim_inactive_list(value_size);

        self.inactive_list.put(key.clone(), Rc::clone(&value_ref));
        self.inactive_size += value_size;
        self.map.insert(key, value_ref);
    }

    fn reclaim_active_list(&mut self, min_free_bytes: u64) {
        // reclaim active list if we don't have enough free space
        if self.active_list.is_empty() || self.active_size + min_free_bytes <= self.max_active_size
        {
            return;
        }

        let mut demotion_size = 0;

        while let Some((key, value)) = self.active_list.pop_lru() {
            // If weight greater than 0, move to head of active list
            if value.borrow().meta.weight > 0 {
                value.borrow_mut().meta.weight -= 1;
                self.active_list.promote(&key);
                continue;
            }

            // Otherwise move to inactive list
            let value_size = value.borrow().value.size();
            self.active_size -= value_size;
            self.inactive_list.put(key.clone(), value);
            self.inactive_size += value_size;
            demotion_size += value_size;

            // stop reclaiming if we have enough free space
            if self.active_size + min_free_bytes <= self.max_active_size {
                break;
            }
        }
        self.reclaim_inactive_list(demotion_size);
    }

    fn reclaim_inactive_list(&mut self, min_free_bytes: u64) {
        if self.inactive_list.is_empty()
            || self.active_size + self.inactive_size + min_free_bytes <= self.max_cache_size
        {
            return;
        }

        while let Some((key, value)) = self.inactive_list.pop_lru() {
            let value_size = value.borrow().value.size();
            self.inactive_size -= value_size;
            self.map.remove(&key);
            if self.active_size + self.inactive_size + min_free_bytes <= self.max_cache_size {
                break;
            }
        }
    }

    /// Returns the clone of the value with the given key or `None` if it is not
    /// present in the cache. Moves the key to the head of the current list if it exists.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the cache item
    pub fn get(&mut self, key: &K) -> Option<V> {
        self.map.get(key).map(|value| {
            // Update access info
            let mut ref_mut = value.borrow_mut();
            if ref_mut.meta.weight > 0 {
                ref_mut.meta.weight -= 1;
            }
            ref_mut.meta.hit_count += 1;
            ref_mut.meta.last_access_time = Instant::now();

            self.active_list.promote(key);
            self.inactive_list.promote(key);

            ref_mut.value.clone()
        })
    }

    /// Returns the metadata of the cache item with the given key or `None` if it is not
    /// present in the cache. This function will not update the access info of the cache item.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the cache item
    pub fn get_meta(&self, key: &K) -> Option<CacheMeta> {
        self.map.get(key).map(|value| {
            // Update access info
            let ref_mut = value.borrow_mut();
            ref_mut.meta.clone()
        })
    }

    /// Returns the key-value pair in the given key range. Moves the key
    /// to the head of the current list if it is accessed.
    ///
    /// # Arguments
    ///
    /// * `key_range` - The range of the cache key
    pub fn search(&mut self, key_range: Range<&K>) -> Vec<(K, V)> {
        let mut result = Vec::new();
        for (key, value) in self.map.range(key_range) {
            // Update access info
            let mut ref_mut = value.borrow_mut();
            if ref_mut.meta.weight > 0 {
                ref_mut.meta.weight -= 1;
            }
            ref_mut.meta.hit_count += 1;
            ref_mut.meta.last_access_time = Instant::now();

            self.active_list.promote(key);
            self.inactive_list.promote(key);

            result.push((key.clone(), ref_mut.value.clone()));
        }
        result
    }

    /// Returns a bool indicating whether the cache is empty or not.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns count of cache items.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns total size of cache items in bytes.
    pub fn size(&self) -> u64 {
        self.active_size + self.inactive_size
    }

    /// Returns count of cache items in active list.
    pub fn active_len(&self) -> usize {
        self.active_list.len()
    }

    /// Returns total size of cache items in active list, measured in bytes.
    pub fn active_size(&self) -> u64 {
        self.active_size
    }

    /// Returns count of cache items in inactive list.
    pub fn inactive_len(&self) -> usize {
        self.inactive_list.len()
    }

    /// Returns total size of cache items in inactive list, measured in bytes.
    pub fn inactive_size(&self) -> u64 {
        self.inactive_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl SizedValue for u64 {
        fn size(&self) -> u64 {
            64
        }
    }

    #[test]
    fn test_push_active() {
        let mut cache = HierarchicalCache::new(64, 80);
        assert!(cache.is_empty());

        cache.push_active(1, 1);
        assert!(!cache.is_empty());
        assert_eq!(cache.active_len(), 1);
        assert_eq!(cache.active_size(), 64);
        assert_eq!(cache.size(), 64);

        // Pushing the same key again will not change the list but refresh the value and metadata
        cache.push_inactive(1, 1);
        assert_eq!(cache.active_len(), 1);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.size(), 64);

        let result = cache.get(&1);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_push_inactive() {
        let mut cache = HierarchicalCache::new(64, 80);

        cache.push_inactive(1, 1);
        assert_eq!(cache.inactive_len(), 1);
        assert_eq!(cache.inactive_size(), 64);

        // Pushing the same key again will not change the list but refresh the value and metadata
        cache.push_active(1, 1);
        assert_eq!(cache.inactive_len(), 1);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.size(), 64);

        let result = cache.get(&1);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_reclaim() {
        let mut cache = HierarchicalCache::new(100, 65);

        // Cache should be fully occupied by pushing the first item to it
        // First item pushing in each list will not trigger reclaim which may cause the cache size to exceed the limit
        cache.push_active(1, 1);
        assert_eq!(cache.active_len(), 1);
        assert_eq!(cache.active_size(), 64);
        assert_eq!(cache.size(), 64);

        cache.push_inactive(2, 2);
        assert_eq!(cache.inactive_len(), 1);
        assert_eq!(cache.inactive_size(), 64);
        assert_eq!(cache.size(), 128);

        // New inactive item will invalidate the previous inactive item
        cache.push_inactive(3, 3);
        assert_eq!(cache.inactive_len(), 1);
        assert_eq!(cache.inactive_size(), 64);
        assert_eq!(cache.size(), 128);
        assert!(cache.inactive_list.get(&3).is_some());

        // New active item will move the previous active item to inactive list and reclaim the space
        cache.push_active(4, 4);
        assert_eq!(cache.active_len(), 1);
        assert_eq!(cache.inactive_len(), 0);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.active_size(), 64);
        assert_eq!(cache.inactive_size(), 0);
        assert_eq!(cache.size(), 64);
        assert!(cache.active_list.get(&4).is_some());

        let mut cache = HierarchicalCache::new(64 * 3, 80);
        cache.push_active(1, 1);
        cache.push_active(2, 2);
        cache.push_active(3, 3);

        assert_eq!(cache.active_len(), 2);
        assert_eq!(cache.active_size(), 128);
        assert!(cache.active_list.contains(&3));
        assert!(cache.active_list.contains(&2));

        assert_eq!(cache.inactive_len(), 1);
        assert_eq!(cache.inactive_size(), 64);
        assert!(cache.inactive_list.contains(&1));

        cache.push_active(4, 4);
        assert_eq!(cache.active_len(), 2);
        assert_eq!(cache.active_size(), 128);

        assert_eq!(cache.inactive_len(), 1);
        assert_eq!(cache.inactive_size(), 64);
        assert!(cache.inactive_list.contains(&2));

        cache.push_inactive(5, 5);
        assert_eq!(cache.inactive_len(), 1);
        assert_eq!(cache.inactive_size(), 64);
        assert!(cache.inactive_list.contains(&5));
    }

    #[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
    struct CacheKey {
        stream: u64,
        range: u64,
        offset: u64,
    }

    #[test]
    fn test_search() {
        let mut cache = HierarchicalCache::new(10000, 80);
        cache.push_active(
            CacheKey {
                stream: 1,
                range: 1,
                offset: 1,
            },
            1,
        );
        cache.push_active(
            CacheKey {
                stream: 1,
                range: 1,
                offset: 2,
            },
            2,
        );
        cache.push_active(
            CacheKey {
                stream: 1,
                range: 2,
                offset: 1,
            },
            3,
        );
        cache.push_active(
            CacheKey {
                stream: 2,
                range: 1,
                offset: 1,
            },
            4,
        );

        let result = cache.search(
            &CacheKey {
                stream: 0,
                range: 1,
                offset: 1,
            }..&CacheKey {
                stream: 0,
                range: 2,
                offset: 2,
            },
        );
        assert!(result.is_empty());

        let result = cache.search(
            &CacheKey {
                stream: 1,
                range: 1,
                offset: 1,
            }..&CacheKey {
                stream: 1,
                range: 1,
                offset: 2,
            },
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, 1);

        let result = cache.search(
            &CacheKey {
                stream: 1,
                range: 1,
                offset: 1,
            }..&CacheKey {
                stream: 1,
                range: 1,
                offset: 100,
            },
        );
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1, 1);
        assert_eq!(result[1].1, 2);

        let result = cache.search(
            &CacheKey {
                stream: 1,
                range: 1,
                offset: 1,
            }..&CacheKey {
                stream: 1,
                range: 100,
                offset: 0,
            },
        );
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].1, 1);
        assert_eq!(result[1].1, 2);
        assert_eq!(result[2].1, 3);

        let result = cache.search(
            &CacheKey {
                stream: 1,
                range: 1,
                offset: 1,
            }..&CacheKey {
                stream: 100,
                range: 1,
                offset: 0,
            },
        );
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].1, 1);
        assert_eq!(result[1].1, 2);
        assert_eq!(result[2].1, 3);
        assert_eq!(result[3].1, 4);
    }
}
