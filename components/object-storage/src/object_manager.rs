use std::{cell::RefCell, cmp::min, collections::HashMap};

use crate::{ObjectManager, Owner, RangeKey};
use model::object::{gen_object_key, ObjectMetadata};

pub struct MemoryObjectManager {
    cluster: String,
    map: RefCell<HashMap<RangeKey, Vec<ObjectMetadata>>>,
}

impl MemoryObjectManager {
    pub fn new(cluster: &str) -> Self {
        Self {
            cluster: cluster.to_string(),
            map: RefCell::new(HashMap::new()),
        }
    }
}

impl ObjectManager for MemoryObjectManager {
    fn is_owner(&self, _stream_id: u64, _range_index: u32) -> Option<Owner> {
        Some(Owner {
            epoch: 0,
            start_offset: 0,
        })
    }

    fn commit_object(&self, object_metadata: ObjectMetadata) {
        let key = RangeKey::new(object_metadata.stream_id, object_metadata.range_index);
        let mut map = self.map.borrow_mut();
        let metas = if let Some(metas) = map.get_mut(&key) {
            metas
        } else {
            let metas = vec![];
            map.insert(key, metas);
            map.get_mut(&key).unwrap()
        };
        metas.push(object_metadata);
    }

    fn get_objects(
        &self,
        stream_id: u64,
        range_index: u32,
        start_offset: u64,
        end_offset: u64,
        size_hint: u32,
    ) -> (Vec<ObjectMetadata>, bool) {
        let key = RangeKey::new(stream_id, range_index);
        if let Some(metas) = self.map.borrow().get(&key) {
            let objects = metas
                .iter()
                .filter(|meta| {
                    meta.start_offset < end_offset
                        && (meta.end_offset_delta as u64 + meta.start_offset) >= start_offset
                })
                .map(|meta| {
                    let mut meta = meta.clone();
                    let key = gen_object_key(
                        &self.cluster,
                        stream_id,
                        range_index,
                        meta.epoch,
                        meta.start_offset,
                    );
                    meta.key = Some(key);
                    meta
                })
                .collect();

            let cover_all = is_cover_all(start_offset, end_offset, size_hint, &objects);
            (objects, cover_all)
        } else {
            (vec![], false)
        }
    }

    fn get_offloading_range(&self) -> Vec<RangeKey> {
        // TODO: replay meta event, get offloading range.
        vec![]
    }
}

/// Check whether objects cover the request range.
/// Note: expect objects is sorted by start_offset and overlap the request range.
fn is_cover_all(
    mut start_offset: u64,
    end_offset: u64,
    mut size_hint: u32,
    objects: &Vec<ObjectMetadata>,
) -> bool {
    for object in objects {
        let object_end_offset = object.start_offset + object.end_offset_delta as u64;
        if object.start_offset <= start_offset && start_offset < object_end_offset {
            if start_offset == object.start_offset {
                size_hint -= min(object.data_len, size_hint);
            }
            start_offset = object_end_offset;
            if start_offset >= end_offset || size_hint == 0 {
                return true;
            }
        } else {
            return false;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::object::ObjectMetadata;

    #[test]
    fn test_is_cover_all() {
        // sequence objects
        let objects = vec![
            new_object(100, 150, 10),
            new_object(150, 200, 100),
            new_object(200, 300, 100),
        ];
        // range match
        assert!(is_cover_all(100, 300, 100, &objects));
        assert!(is_cover_all(110, 250, 100, &objects));

        // size match
        assert!(is_cover_all(100, 1000, 210, &objects));
        assert!(is_cover_all(110, 1000, 200, &objects));
        // size not match
        assert!(!is_cover_all(110, 1000, 210, &objects));

        // hollow objects
        let objects = vec![new_object(100, 150, 10), new_object(200, 300, 100)];
        assert!(!is_cover_all(100, 300, 100, &objects));
    }

    fn new_object(start_offset: u64, end_offset: u64, size: u32) -> ObjectMetadata {
        let mut obj = ObjectMetadata::new(0, 0, 0, start_offset);
        obj.end_offset_delta = (end_offset - start_offset) as u32;
        obj.data_len = size;
        obj
    }
}
