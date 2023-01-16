//! Manage write and `sync` progress of each `FileSegment`.
//!
//!
pub struct Cursor {
    written: u64,
    committed: u64,
}

impl Cursor {
    pub fn new() -> Self {
        Self {
            written: 0,
            committed: 0,
        }
    }

    pub fn alloc(&mut self, len: u64) -> u64 {
        let current = self.written;
        self.written += len;
        current
    }

    pub fn committed(&self) -> u64 {
        self.committed
    }

    pub fn commit(&mut self, pos: u64, len: u64) -> bool {
        if self.committed == pos {
            self.committed += len;
            return true;
        }
        false
    }
}
