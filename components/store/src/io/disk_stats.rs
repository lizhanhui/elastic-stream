use std::time::Duration;

use hdrhistogram::Histogram;
use log::{info, warn};
use minstant::Instant;

pub(crate) struct DiskStats {
    interval: Duration,
    histogram: Histogram<u64>,
    last: Instant,
}

impl DiskStats {
    pub(crate) fn new(interval: Duration, max: u64) -> Self {
        Self {
            interval,
            histogram: Histogram::new_with_max(max, 3).unwrap(),
            last: Instant::now(),
        }
    }

    pub(crate) fn record(&mut self, value: u64) {
        if let Err(_e) = self.histogram.record(value) {
            warn!("Failed to record {}", value);
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.last.elapsed() >= self.interval
    }

    pub(crate) fn report(&mut self, title: &str) {
        self.last = minstant::Instant::now();
        if self.histogram.is_empty() {
            return;
        }
        info!(
            "{}: [{}, {}, {}, {}, {}]",
            title,
            self.histogram.mean(),
            self.histogram.value_at_percentile(90.0),
            self.histogram.value_at_percentile(99.0),
            self.histogram.value_at_percentile(99.9),
            self.histogram.max(),
        );
        self.histogram.reset();
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};

    #[test]
    fn test_disk_stats() -> Result<(), Box<dyn Error>> {
        env_logger::try_init()?;
        let mut stats = super::DiskStats::new(Duration::from_millis(100), u16::MAX as u64);
        assert!(!stats.is_ready());

        for value in 0..100 {
            stats.record(value);
        }

        std::thread::sleep(Duration::from_millis(100));
        assert!(stats.is_ready());

        stats.report("Test");

        assert!(!stats.is_ready());
        Ok(())
    }
}
