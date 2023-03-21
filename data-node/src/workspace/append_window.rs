use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
};

use tokio::sync::oneshot;

pub(crate) struct AppendWindow {
    /// Next offset to commit.
    ///
    /// Assume the mutable range of the stream is `[n, -1)`, we are safe to acknowledge records whose
    /// index fall into `[n, commit)`. Note `commit` is exclusive.  
    pub(crate) commit: u64,

    /// Next offset to allocate.
    pub(crate) next: u64,

    inflight: BTreeMap<u64, oneshot::Sender<()>>,

    completed: BinaryHeap<Reverse<u64>>,
}

impl AppendWindow {
    pub(crate) fn new(next: u64) -> Self {
        Self {
            commit: next,
            next,
            inflight: BTreeMap::new(),
            completed: BinaryHeap::new(),
        }
    }

    pub(crate) fn alloc_slot(&mut self, tx: oneshot::Sender<()>) -> u64 {
        let offset = self.next;
        self.inflight.entry(offset).or_insert(tx);
        self.next += 1;
        offset
    }

    pub(crate) fn ack(&mut self, offset: u64) {
        self.completed.push(Reverse(offset));
        loop {
            if let Some(min) = self.completed.peek() {
                if let Some((offset, _v)) = self.inflight.first_key_value() {
                    if min.0 > *offset {
                        // A prior record write has not yet been acknowledged.
                        break;
                    }

                    debug_assert_eq!(self.commit, *offset);
                }
            }

            let _ = self.completed.pop();
            if let Some((offset, v)) = self.inflight.pop_first() {
                let _ = v.send(());
                self.commit = offset + 1;
            }
        }
    }
}
