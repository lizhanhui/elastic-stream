use std::cell::RefCell;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::rc::Rc;
use std::time::Instant;

use lru::LruCache;
use percentage::Percentage;

pub trait SizedValue {
    fn size(&self) -> u64;
}

struct CacheMeta {
    weight: u8,
    hit_count: u64,
    last_access_time: Instant,
}

struct Value<V> {
    meta: CacheMeta,
    value: V,
}

type ValueRef<V> = Rc<RefCell<Value<V>>>;

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

    pub fn check_already_cached(&mut self, key: &K) -> bool {
        if self.active_list.contains(key) {
            self.active_list.pop(key);
            return true;
        }
        if self.inactive_list.contains(key) {
            self.inactive_list.pop(key);
            return true;
        }
        false
    }

    pub fn push_active(&mut self, key: K, value: V) {
        self.push_active_with_weight(key, value, 0);
    }

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

    pub fn push_inactive(&mut self, key: K, value: V) {
        self.push_inactive_with_weight(key, value, 0);
    }

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

    pub fn reclaim_active_list(&mut self, min_free_bytes: u64) {
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

    pub fn reclaim_inactive_list(&mut self, min_free_bytes: u64) {
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

    pub fn get(&mut self, key: &K) -> Option<V> {
        self.map.get(key).map(|value| {
            // Update access info
            let mut ref_mut = value.borrow_mut();
            if ref_mut.meta.weight > 0 {
                ref_mut.meta.weight -= 1;
            }
            ref_mut.meta.hit_count += 1;
            ref_mut.meta.last_access_time = Instant::now();
            ref_mut.value.clone()
        })
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

        cache.push_inactive(1, 1);
        assert_eq!(cache.inactive_list.len(), 1);
        assert_eq!(cache.inactive_size, 64);

        cache.push_active(1, 1);
        assert_eq!(cache.map.len(), 1);

        let result = cache.get(&1);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_push_inactive() {
        let mut cache = HierarchicalCache::new(64, 80);

        cache.push_inactive(1, 1);
        assert_eq!(cache.inactive_list.len(), 1);
        assert_eq!(cache.inactive_size, 64);

        cache.push_active(1, 1);
        assert_eq!(cache.map.len(), 1);

        let result = cache.get(&1);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_reclaim() {
        let mut cache = HierarchicalCache::new(100, 65);

        // Cache should be fully occupied by pushing the first item to it
        cache.push_active(1, 1);
        assert_eq!(cache.active_list.len(), 1);
        cache.push_inactive(2, 2);
        assert_eq!(cache.inactive_list.len(), 1);

        cache.push_inactive(3, 3);
        assert_eq!(cache.inactive_list.len(), 1);
        assert!(cache.inactive_list.get(&3).is_some());

        cache.push_active(4, 4);
        assert_eq!(cache.active_list.len(), 1);
        assert!(cache.active_list.get(&4).is_some());
        assert_eq!(cache.inactive_list.len(), 0);

        let mut cache = HierarchicalCache::new(64 * 3, 80);
        cache.push_active(1, 1);
        cache.push_active(2, 2);
        cache.push_active(3, 3);

        assert_eq!(cache.active_list.len(), 2);
        assert!(cache.active_list.contains(&3));
        assert!(cache.active_list.contains(&2));

        assert_eq!(cache.inactive_list.len(), 1);
        assert!(cache.inactive_list.contains(&1));

        cache.push_active(4, 4);
        assert_eq!(cache.inactive_list.len(), 1);
        assert!(cache.inactive_list.contains(&2));

        cache.push_inactive(5, 5);
        assert_eq!(cache.inactive_list.len(), 1);
        assert!(cache.inactive_list.contains(&5));
    }
}
