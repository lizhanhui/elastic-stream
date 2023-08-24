use std::sync::atomic::Ordering;
use std::time::Instant;

use lazy_static::*;
use opentelemetry::metrics::{Counter, Histogram, ObservableGauge, UpDownCounter};
#[cfg(feature = "metrics")]
use opentelemetry::KeyValue;
use prometheus::core::{Atomic, AtomicF64, AtomicI64, AtomicU64};

use crate::metrics::get_meter;

pub struct UringStatistics {
    last_instant: Instant,
    uring_task_rate: i16,
}

impl Default for UringStatistics {
    fn default() -> Self {
        Self {
            last_instant: Instant::now(),
            uring_task_rate: 0,
        }
    }
}

impl UringStatistics {
    pub fn new() -> Self {
        Self::default()
    }
    /// The record() is responsible for capturing the current state of metrics,
    /// based on this data, it calculates the corresponding rates.
    pub fn record(&mut self) {
        let current_instant = Instant::now();
        // time_delta represents the time interval
        // between the last record and the current one, in seconds.
        let time_delta = current_instant
            .saturating_duration_since(self.last_instant)
            .as_millis() as u64
            / 1000;
        self.last_instant = current_instant;
        self.uring_task_rate = (COMPLETED_IO_COUNT.swap(0, Ordering::Relaxed) / time_delta) as i16;
    }
    /// The observe_latency() is responsible for recording a new latency
    /// and calculating the average latency over the past minute.
    /// It uses EWMA (Exponentially Weighted Moving Average) to determine the value.
    fn observe_latency(latency: i16) {
        COMPLETED_IO_COUNT.inc_by(1);
        let cur_observe_time = START_TIME.elapsed().as_millis() as u64;
        let last_observe_time = URING_OBSERVE_TIME.get();
        if last_observe_time == u64::MAX {
            URING_AVG_LATENCY.set(latency as f64);
            URING_OBSERVE_TIME.set(cur_observe_time);
        } else {
            let delta_observe_time = cur_observe_time - last_observe_time;
            const ONE_MINUTE: f64 = 1000.0 * 60.0;
            let w = f64::exp(-(delta_observe_time as f64) / ONE_MINUTE);
            let last_avg_latency = URING_AVG_LATENCY.get();

            // Use the latest recorded latency and the previous average latency to determine the "cur_avg_latency"
            let cur_avg_latency = last_avg_latency * w + (1.0 - w) * latency as f64;
            URING_AVG_LATENCY.set(cur_avg_latency);
            URING_OBSERVE_TIME.set(cur_observe_time);
        }
    }
    pub fn get_uring_task_rate(&self) -> i16 {
        self.uring_task_rate
    }
    pub fn get_uring_inflight_task_cnt(&self) -> i16 {
        INFLIGHT_IO_COUNT.get() as i16
    }
    pub fn get_uring_pending_task_cnt(&self) -> i32 {
        PENDING_TASK_COUNT.get() as i32
    }
    pub fn get_uring_task_avg_latency(&self) -> i16 {
        URING_AVG_LATENCY.get() as i16
    }
}

lazy_static! {
    pub static ref START_TIME: Instant = Instant::now();
    // URING_AVG_LATENCY represents the average latency of the uring task, in milliseconds.
    pub static ref URING_AVG_LATENCY: AtomicF64 = AtomicF64::new(f64::MAX);
    // URING_OBSERVE_TIME represents the timestamp of the last time we observed the latency, in milliseconds
    // It is necessary to implement EWMA, which helps determine the latest average latency
    pub static ref URING_OBSERVE_TIME: AtomicU64 = AtomicU64::new(u64::MAX);

    pub static ref COMPLETED_IO_COUNT: AtomicU64 = AtomicU64::new(0);
    pub static ref PENDING_TASK_COUNT: AtomicU64 = AtomicU64::new(0);
    pub static ref INFLIGHT_IO_COUNT: AtomicI64 = AtomicI64::new(0);

    pub static ref COUNTER_INFLIGHT_IO : UpDownCounter<i64> = get_meter()
        .i64_up_down_counter("store.io.inflight.task")
        .with_description("Number of inflight IO tasks")
        .init();

    static ref COUNTER_BYTES_TOTAL : Counter<u64> = get_meter()
        .u64_counter("store.io.bytes.total")
        .with_description("Total number of bytes read or written by uring")
        .init();

    pub static ref GAUGE_PENDING_TASK: ObservableGauge<u64> = get_meter()
        .u64_observable_gauge("store.io.pending.task")
        .with_description("Number of pending IO task")
        .init();

    // dimensions for io_uring
    static ref GAUGE_IO_DEPTH : ObservableGauge<u64> = get_meter()
        .u64_observable_gauge("store.io.depth")
        .with_description("The io depth of io_uring")
        .init();

    static ref HISTOGRAM_IO_LATENCY: Histogram<u64> = get_meter()
        .u64_histogram("store.io.latency")
        .with_description("Histogram of read IO latency in microseconds")
        .init();
}

#[cfg(feature = "metrics")]
const LABEL_IO_TYPE: &str = "io_type";
#[cfg(feature = "metrics")]
const IO_TYPE_READ: &str = "read";
#[cfg(feature = "metrics")]
const IO_TYPE_WRITE: &str = "write";

#[inline]
pub fn record_read_io(_latency: u64, _bytes: u64) {
    #[cfg(feature = "metrics")]
    {
        HISTOGRAM_IO_LATENCY.record(_latency, &[KeyValue::new(LABEL_IO_TYPE, IO_TYPE_READ)]);
        COUNTER_BYTES_TOTAL.add(_bytes, &[KeyValue::new(LABEL_IO_TYPE, IO_TYPE_READ)]);
    }
    UringStatistics::observe_latency(_latency as i16);
}

#[inline]
pub fn record_write_io(_latency: u64, _bytes: u64) {
    #[cfg(feature = "metrics")]
    {
        HISTOGRAM_IO_LATENCY.record(_latency, &[KeyValue::new(LABEL_IO_TYPE, IO_TYPE_WRITE)]);
        COUNTER_BYTES_TOTAL.add(_bytes, &[KeyValue::new(LABEL_IO_TYPE, IO_TYPE_WRITE)]);
    }
    UringStatistics::observe_latency(_latency as i16);
}

#[inline]
pub fn record_io_depth(_depth: u64) {
    #[cfg(feature = "metrics")]
    {
        GAUGE_IO_DEPTH.observe(_depth, &[]);
    }
}

#[inline]
pub fn record_pending_task(_task_count: u64) {
    #[cfg(feature = "metrics")]
    {
        GAUGE_PENDING_TASK.observe(_task_count, &[]);
    }
}

#[inline]
pub fn record_inflight_io(_io_count: i64) {
    #[cfg(feature = "metrics")]
    {
        INFLIGHT_IO_COUNT.inc_by(_io_count);
        COUNTER_INFLIGHT_IO.add(_io_count, &[]);
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use log::trace;

    use crate::metrics::uring_metrics::UringStatistics;

    #[test]
    #[ignore = "This test is just for observing the effect."]
    fn test_uring_statistics() {
        let mut statistics = UringStatistics::new();
        statistics.record();
        sleep(Duration::from_secs(2));
        statistics.record();
        assert_eq!((100 + 50) / 2, statistics.get_uring_task_rate());
        UringStatistics::observe_latency(5);
        assert_eq!(5, statistics.get_uring_task_avg_latency());
        sleep(Duration::from_secs(1));
        UringStatistics::observe_latency(5);
        sleep(Duration::from_secs(1));
        UringStatistics::observe_latency(5);
        sleep(Duration::from_secs(1));
        UringStatistics::observe_latency(5);
        sleep(Duration::from_secs(1));
        UringStatistics::observe_latency(5);
        sleep(Duration::from_secs(1));
        trace!("avg[0] = {}", statistics.get_uring_task_avg_latency());
        UringStatistics::observe_latency(1);
        trace!("avg[1] = {}", statistics.get_uring_task_avg_latency());
        sleep(Duration::from_secs(1));
        UringStatistics::observe_latency(1);
        trace!("avg[2] = {}", statistics.get_uring_task_avg_latency());
        sleep(Duration::from_secs(1));
        UringStatistics::observe_latency(1);
        trace!("avg[3] = {}", statistics.get_uring_task_avg_latency());
        sleep(Duration::from_secs(1));
        UringStatistics::observe_latency(1);
        trace!("avg[4] = {}", statistics.get_uring_task_avg_latency());
        sleep(Duration::from_secs(1));
        UringStatistics::observe_latency(1);
        trace!("avg[5] = {}", statistics.get_uring_task_avg_latency());
    }
}
