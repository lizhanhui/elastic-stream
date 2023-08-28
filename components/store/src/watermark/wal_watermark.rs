use std::sync::atomic::{AtomicU64, Ordering};

use super::Watermark;

/// Maintain two watermarks in terms of WAL offsets.
pub(crate) struct WalWatermark {
    /// Data whose WAL offset are less than `min` are expired and should be reclaimed as soon as possible.
    min: AtomicU64,

    /// Data whose WAL offset is greater than `offload` are not yet offloaded to object storage.
    /// Data within [min, offload) are regarded as warm cache on local disks. When range server
    /// runs short of free disk capacity, it may delete them to accommodate incoming traffics.
    offload: AtomicU64,
}

impl WalWatermark {
    pub(crate) fn new() -> Self {
        Self {
            min: AtomicU64::new(0),
            offload: AtomicU64::new(0),
        }
    }
}

impl Watermark for WalWatermark {
    fn min(&self) -> u64 {
        self.min.load(Ordering::Relaxed)
    }

    fn offload(&self) -> u64 {
        self.offload.load(Ordering::Relaxed)
    }

    /// Advance WAL min-offset once a segment file is deleted.
    fn set_min(&self, min: u64) {
        self.min.store(min, Ordering::Relaxed);
    }

    /// Advance WAL offload offset once a slice of range data is offloaded to object storage service.
    fn set_offload(&self, offset: u64) {
        self.offload.store(offset, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use crate::watermark::Watermark;

    use super::WalWatermark;

    #[test]
    fn test_min() {
        let watermark = WalWatermark::new();
        assert_eq!(watermark.min(), 0);
        watermark.set_min(42);
        assert_eq!(watermark.min(), 42);
    }

    #[test]
    fn test_offload() {
        let watermark = WalWatermark::new();
        assert_eq!(watermark.offload(), 0);
        watermark.set_offload(65);
        assert_eq!(watermark.offload(), 65);
    }
}
