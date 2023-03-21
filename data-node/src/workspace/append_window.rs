use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
};

use tokio::sync::oneshot;

pub(crate) struct AppendWindow {
    pub(crate) committed: u64,
    inflight: BTreeMap<u64, oneshot::Sender<()>>,
    completed: BinaryHeap<Reverse<u64>>,
}

impl AppendWindow {
    pub(crate) fn complete(&mut self, offset: u64) {
        self.completed.push(Reverse(offset));
        loop {
            if let Some(min) = self.completed.peek() {
                if let Some((offset, _v)) = self.inflight.first_key_value() {
                    if min.0 > *offset {
                        // A prior record write has not yet been acknowledged.
                        break;
                    }

                    debug_assert_eq!(self.committed + 1, *offset);
                }
            }

            let _ = self.completed.pop();
            if let Some((offset, v)) = self.inflight.pop_first() {
                let _ = v.send(());
                self.committed = offset;
            }
        }
    }
}
