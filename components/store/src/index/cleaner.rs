use std::{
    cmp::min,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use log::debug;
use model::range::{Range, RangeLifecycleEvent};

use super::Indexer;

type LogicOffset = u64;
type PhysicalOffset = u64;
const PHYSICAL_OFFSET_UNSET: u64 = u64::MAX;

/// LogCleaner handle RangeLifecycleEvent from MetadataManager
/// and new range create event & new index event from Indexer
/// to calculate the min physical offset that can be deleted.
///
/// LogCleaner keep the whole ranges view by:
/// 1. When bootstrap, it will get all ranges which the range server should held
/// from MetadataManager by `handle_range_event` method, and set
/// deletable_physical_offset will be calculated.
/// 2. When new range create, the RangeLifecycleEvent maybe delayed, so LogCleaner
/// will get the new range from IndexDriver  by `new_range` and `new_index` methods.
/// LogCleaner will record the new range first record wal_offset as deletable_physical_offset.
pub(crate) struct LogCleaner<I: Indexer> {
    ranges: HashMap<Range, (LogicOffset, PhysicalOffset)>,
    range_deletable_physical_offsets: BTreeMap<PhysicalOffset, ()>,
    last_index_physical_offset: PhysicalOffset,
    deletable_physical_offset: PhysicalOffset,
    indexer: Arc<I>,
}

impl<I> LogCleaner<I>
where
    I: Indexer + 'static,
{
    pub(crate) fn new(indexer: Arc<I>) -> Self {
        Self {
            ranges: HashMap::new(),
            range_deletable_physical_offsets: BTreeMap::new(),
            last_index_physical_offset: 0,
            deletable_physical_offset: 0,
            indexer,
        }
    }

    pub(crate) fn handle_new_range(&mut self, range: Range, start_offset: LogicOffset) {
        let _ = self
            .ranges
            .try_insert(range, (start_offset, PHYSICAL_OFFSET_UNSET));
    }

    pub(crate) fn handle_new_index(
        &mut self,
        range: Range,
        logic_offset: LogicOffset,
        physical_offset: PhysicalOffset,
    ) {
        if let Some(e) = self.ranges.get_mut(&range) {
            if e.1 == PHYSICAL_OFFSET_UNSET && e.0 <= logic_offset {
                e.1 = physical_offset;
                self.range_deletable_physical_offsets
                    .insert(physical_offset, ());
            }
        }
        self.last_index_physical_offset = physical_offset;
    }

    pub(crate) fn handle_range_event(
        &mut self,
        events: Vec<RangeLifecycleEvent>,
    ) -> PhysicalOffset {
        for event in events {
            match event {
                RangeLifecycleEvent::OffsetMove(range, logic_offset) => {
                    let deletable_physical_offset =
                        self.get_range_deletable_physical_offset(range, logic_offset);
                    if let Some((_, old_physical_offset)) = self
                        .ranges
                        .insert(range, (logic_offset, deletable_physical_offset))
                    {
                        self.range_deletable_physical_offsets
                            .remove(&old_physical_offset);
                    }
                    if deletable_physical_offset != PHYSICAL_OFFSET_UNSET {
                        self.range_deletable_physical_offsets
                            .insert(deletable_physical_offset, ());
                    }
                }
                RangeLifecycleEvent::Del(range) => {
                    if let Some((_, physical_offset)) = self.ranges.remove(&range) {
                        self.range_deletable_physical_offsets
                            .remove(&physical_offset);
                    }
                }
            }
        }
        self.cal_min_physical_offset()
    }

    #[allow(dead_code)]
    pub(crate) fn set_last_index_physical_offset(&mut self, offset: PhysicalOffset) {
        self.last_index_physical_offset = offset;
    }

    pub(crate) fn get_range_deletable_physical_offset(
        &self,
        range: Range,
        logic_offset: u64,
    ) -> PhysicalOffset {
        self.indexer
            .scan_record_handles_left_shift(
                range.0 as i64,
                range.1,
                logic_offset,
                logic_offset + 1,
                1,
            )
            .expect("handle_range_event indexer scan not fail")
            .map(|handles| {
                let (key, handle) = &handles[0];
                let record_end_offset = key.start_offset + handle.ext.count() as u64;
                if record_end_offset > logic_offset {
                    // means the logic_offset is in the record, and current record should not be deleted.
                    handle.wal_offset
                } else {
                    // means the logic_offset is not in the record, and current record should be deleted.
                    PHYSICAL_OFFSET_UNSET
                }
            })
            .unwrap_or(PHYSICAL_OFFSET_UNSET)
    }

    #[inline]
    fn cal_min_physical_offset(&mut self) -> PhysicalOffset {
        let new_physical_offset = self
            .range_deletable_physical_offsets
            .first_key_value()
            .map(|e| min(*e.0, self.last_index_physical_offset))
            .unwrap_or(self.last_index_physical_offset);
        if new_physical_offset > self.deletable_physical_offset {
            self.deletable_physical_offset = new_physical_offset;
            debug!(
                "update deletable_physical_offset to {}",
                self.deletable_physical_offset
            );
        }
        self.deletable_physical_offset
    }
}

#[cfg(test)]
mod tests {
    use mockall::predicate::{always, eq};
    use model::range::RangeLifecycleEvent;

    use crate::index::{
        record_handle::{HandleExt, RecordHandle},
        record_key::RecordKey,
        MockIndexer,
    };

    use super::LogCleaner;
    use super::*;

    #[test]
    fn test_handle_range_event() {
        let mut indexer = MockIndexer::default();
        indexer
            .expect_scan_record_handles_left_shift()
            .times(1)
            .with(eq(233), eq(1), eq(11), always(), always())
            .returning(|_, _, _, _, _| {
                Ok(Some(vec![(
                    RecordKey::new(233, 1, 10),
                    RecordHandle::new(60, 1, HandleExt::BatchSize(2)),
                )]))
            });
        let mut log_cleaner = LogCleaner::new(Arc::new(indexer));
        log_cleaner.set_last_index_physical_offset(100);
        // range start offset move to 11, then the deletable physical offset is record(offset=10) 's wal_offset
        assert_eq!(
            60,
            log_cleaner.handle_range_event(vec![RangeLifecycleEvent::OffsetMove((233, 1), 11)])
        );

        Arc::get_mut(&mut log_cleaner.indexer)
            .unwrap()
            .expect_scan_record_handles_left_shift()
            .times(1)
            .with(eq(233), eq(1), eq(12), always(), always())
            .returning(|_, _, _, _, _| {
                Ok(Some(vec![(
                    RecordKey::new(233, 1, 10),
                    RecordHandle::new(60, 1, HandleExt::BatchSize(2)),
                )]))
            });

        // range start offset move to the end_offset
        assert_eq!(
            100,
            log_cleaner.handle_range_event(vec![RangeLifecycleEvent::OffsetMove((233, 1), 12)])
        );

        // range insert new record
        log_cleaner.handle_new_index((233, 1), 12, 110);
        log_cleaner.set_last_index_physical_offset(200);

        Arc::get_mut(&mut log_cleaner.indexer)
            .unwrap()
            .expect_scan_record_handles_left_shift()
            .times(1)
            .with(eq(233), eq(1), eq(12), always(), always())
            .returning(|_, _, _, _, _| {
                Ok(Some(vec![(
                    RecordKey::new(233, 1, 12),
                    RecordHandle::new(110, 1, HandleExt::BatchSize(2)),
                )]))
            });

        assert_eq!(
            110,
            log_cleaner.handle_range_event(vec![RangeLifecycleEvent::OffsetMove((233, 1), 12)])
        );

        // range deleted.
        assert_eq!(
            200,
            log_cleaner.handle_range_event(vec![RangeLifecycleEvent::Del((233, 1))])
        );
    }

    #[test]
    fn test_new_range_before_event() {
        let mut indexer = MockIndexer::default();
        indexer
            .expect_scan_record_handles_left_shift()
            .times(3)
            .with(eq(233), eq(1), always(), always(), always())
            .returning(|_, _, _, _, _| {
                Ok(Some(vec![(
                    RecordKey::new(233, 1, 10),
                    RecordHandle::new(60, 1, HandleExt::BatchSize(2)),
                )]))
            });
        let mut log_cleaner = LogCleaner::new(Arc::new(indexer));
        log_cleaner.set_last_index_physical_offset(100);
        log_cleaner.handle_new_range((233, 2), 10);
        assert_eq!(
            60,
            log_cleaner.handle_range_event(vec![RangeLifecycleEvent::OffsetMove((233, 1), 11)])
        );

        assert_eq!(
            100,
            log_cleaner.handle_range_event(vec![RangeLifecycleEvent::OffsetMove((233, 1), 12)])
        );
        log_cleaner.handle_new_index((233, 2), 10, 110);
        log_cleaner.handle_new_index((233, 2), 11, 120);
        log_cleaner.set_last_index_physical_offset(200);
        assert_eq!(
            110,
            log_cleaner.handle_range_event(vec![RangeLifecycleEvent::OffsetMove((233, 1), 12)])
        );
    }
}
