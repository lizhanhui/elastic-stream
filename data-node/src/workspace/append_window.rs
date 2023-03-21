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
            } else {
                break;
            }

            let _ = self.completed.pop();
            if let Some((offset, v)) = self.inflight.pop_first() {
                let _ = v.send(());
                self.commit = offset + 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use tokio::sync::oneshot;

    use super::AppendWindow;

    #[test]
    fn test_alloc_slot() -> Result<(), Box<dyn Error>> {
        let mut window = AppendWindow::new(0);
        const TOTAL: u64 = 16;
        for i in 0..TOTAL {
            let (tx, _rx) = oneshot::channel();
            let slot = window.alloc_slot(tx);
            assert_eq!(i, slot);
        }

        assert_eq!(TOTAL as usize, window.inflight.len());

        Ok(())
    }

    #[test]
    fn test_ack() -> Result<(), Box<dyn Error>> {
        let mut window = AppendWindow::new(0);
        const TOTAL: u64 = 16;
        let mut v = vec![];
        for i in 0..TOTAL {
            let (tx, rx) = oneshot::channel();
            v.push(rx);
            let slot = window.alloc_slot(tx);
            assert_eq!(i, slot);
        }

        for i in 0..TOTAL {
            window.ack(i);
            assert_eq!((TOTAL - i - 1) as usize, window.inflight.len());
            assert!(window.completed.is_empty());
            assert_eq!(i + 1, window.commit);
        }

        Ok(())
    }

    #[test]
    fn test_ack_out_of_order() -> Result<(), Box<dyn Error>> {
        let mut window = AppendWindow::new(0);
        const TOTAL: u64 = 16;
        let mut v = vec![];
        for i in 0..TOTAL {
            let (tx, rx) = oneshot::channel();
            v.push(rx);
            let slot = window.alloc_slot(tx);
            assert_eq!(i, slot);
        }

        for i in (0..TOTAL).rev() {
            window.ack(i);
            if i > 0 {
                assert_eq!(window.inflight.len(), TOTAL as usize);
                continue;
            }
            assert!(window.inflight.is_empty());
            assert!(window.completed.is_empty());
        }
        assert_eq!(TOTAL, window.commit);

        Ok(())
    }
}
