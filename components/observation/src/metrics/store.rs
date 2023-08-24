use std::sync::atomic::Ordering;
use std::time::Instant;

use lazy_static::*;
use opentelemetry::metrics::{Counter, Histogram};
#[cfg(feature = "metrics")]
use opentelemetry::KeyValue;
use prometheus::core::{Atomic, AtomicF64, AtomicU64};

use crate::metrics::get_meter;

#[derive(Debug)]
pub struct RangeServerStatistics {
    last_instant: Instant,
    network_append_rate: i16,
    network_fetch_rate: i16,
    network_failed_append_rate: i16,
    network_failed_fetch_rate: i16,
}

impl Default for RangeServerStatistics {
    fn default() -> Self {
        Self {
            last_instant: Instant::now(),
            network_append_rate: 0,
            network_fetch_rate: 0,
            network_failed_append_rate: 0,
            network_failed_fetch_rate: 0,
        }
    }
}

impl RangeServerStatistics {
    pub fn new() -> Self {
        Self::default()
    }

    /// The record() is responsible for capturing the current state of metrics,
    /// based on this data, it calculates the corresponding rates.
    pub fn record(&mut self) {
        let current_instant = Instant::now();
        let time_delta = current_instant
            .saturating_duration_since(self.last_instant)
            .as_millis() as u64
            / 1000;
        if time_delta == 0 {
            return;
        }
        self.last_instant = current_instant;
        self.network_append_rate =
            (STORE_APPEND_COUNT.swap(0, Ordering::Relaxed) / time_delta) as i16;
        self.network_fetch_rate =
            (STORE_FETCH_COUNT.swap(0, Ordering::Relaxed) / time_delta) as i16;
        self.network_failed_append_rate =
            (STORE_FAILED_APPEND_COUNT.swap(0, Ordering::Relaxed) / time_delta) as i16;
        self.network_failed_fetch_rate =
            (STORE_FAILED_FETCH_COUNT.swap(0, Ordering::Relaxed) / time_delta) as i16;
    }
    /// The observe_append_latency() is responsible for recording a new append latency
    /// and calculating the average append latency over the past minute.
    /// It uses EWMA (Exponentially Weighted Moving Average) to determine the value.
    pub fn observe_append_latency(latency: i16, success: bool) {
        let cur_observe_append_time = START_TIME.elapsed().as_millis() as u64;
        let last_observe_append_time = RANGE_SERVER_APPEND_OBSERVE_TIME.get();
        if last_observe_append_time == u64::MAX {
            RANGE_SERVER_APPEND_AVG_LATENCY.set(latency as f64);
            RANGE_SERVER_APPEND_OBSERVE_TIME.set(cur_observe_append_time);
        } else {
            let delta_observe_append_time = cur_observe_append_time - last_observe_append_time;
            const ONE_MINUTE: f64 = 1000.0 * 60.0;
            let w = f64::exp(-(delta_observe_append_time as f64) / ONE_MINUTE);
            let last_avg_append_latency = RANGE_SERVER_APPEND_AVG_LATENCY.get();
            let cur_avg_append_latency = last_avg_append_latency * w + (1.0 - w) * latency as f64;
            RANGE_SERVER_APPEND_AVG_LATENCY.set(cur_avg_append_latency);
            RANGE_SERVER_APPEND_OBSERVE_TIME.set(cur_observe_append_time);
        }
        if success {
            STORE_APPEND_COUNT.inc_by(1);
        } else {
            STORE_FAILED_APPEND_COUNT.inc_by(1);
        }
    }
    /// The observe_fetch_latency() is responsible for recording a new fetch latency
    /// and calculating the average fetch latency over the past minute.
    /// It uses EWMA (Exponentially Weighted Moving Average) to determine the value.
    pub fn observe_fetch_latency(latency: i16, success: bool) {
        let cur_observe_fetch_time = START_TIME.elapsed().as_millis() as u64;
        let last_observe_fetch_time = RANGE_SERVER_FETCH_OBSERVE_TIME.get();
        if last_observe_fetch_time == u64::MAX {
            RANGE_SERVER_FETCH_AVG_LATENCY.set(latency as f64);
            RANGE_SERVER_FETCH_OBSERVE_TIME.set(cur_observe_fetch_time);
        } else {
            let delta_observe_fetch_time = cur_observe_fetch_time - last_observe_fetch_time;
            const ONE_MINUTE: f64 = 1000.0 * 60.0;
            let w = f64::exp(-(delta_observe_fetch_time as f64) / ONE_MINUTE);
            let last_avg_fetch_latency = RANGE_SERVER_FETCH_AVG_LATENCY.get();
            let cur_avg_fetch_latency = last_avg_fetch_latency * w + (1.0 - w) * latency as f64;
            RANGE_SERVER_FETCH_AVG_LATENCY.set(cur_avg_fetch_latency);
            RANGE_SERVER_FETCH_OBSERVE_TIME.set(cur_observe_fetch_time);
        }
        if success {
            STORE_FETCH_COUNT.inc_by(1);
        } else {
            STORE_FAILED_FETCH_COUNT.inc_by(1);
        }
    }
    pub fn get_network_append_rate(&self) -> i16 {
        self.network_append_rate
    }
    pub fn get_network_fetch_rate(&self) -> i16 {
        self.network_fetch_rate
    }
    pub fn get_network_failed_append_rate(&self) -> i16 {
        self.network_failed_append_rate
    }
    pub fn get_network_failed_fetch_rate(&self) -> i16 {
        self.network_failed_fetch_rate
    }
    pub fn get_network_append_avg_latency(&self) -> i16 {
        RANGE_SERVER_APPEND_AVG_LATENCY.get() as i16
    }
    pub fn get_network_fetch_avg_latency(&self) -> i16 {
        RANGE_SERVER_FETCH_AVG_LATENCY.get() as i16
    }
}

lazy_static! {
    pub static ref START_TIME: Instant = Instant::now();
    pub static ref RANGE_SERVER_APPEND_AVG_LATENCY: AtomicF64 = AtomicF64::new(f64::MAX);
    pub static ref RANGE_SERVER_APPEND_OBSERVE_TIME: AtomicU64 = AtomicU64::new(u64::MAX);
    pub static ref RANGE_SERVER_FETCH_AVG_LATENCY: AtomicF64 = AtomicF64::new(f64::MAX);
    pub static ref RANGE_SERVER_FETCH_OBSERVE_TIME: AtomicU64 = AtomicU64::new(u64::MAX);
    pub static ref STORE_APPEND_COUNT: AtomicU64 = AtomicU64::new(0);
    pub static ref STORE_FAILED_APPEND_COUNT: AtomicU64 = AtomicU64::new(0);
    pub static ref STORE_FETCH_COUNT: AtomicU64 = AtomicU64::new(0);
    pub static ref STORE_FAILED_FETCH_COUNT: AtomicU64 = AtomicU64::new(0);
    static ref COUNTER_OPERATION_BYTES: Counter<u64> = get_meter()
        .u64_counter("store.operation.bytes.total")
        .with_description("Transferred bytes in store operation")
        .init();
    static ref HISTOGRAM_OPERATION_LATENCY: Histogram<u64> = get_meter()
        .u64_histogram("store.operation.latency")
        .with_description("Histogram of operation latency in microseconds")
        .init();
}

#[cfg(feature = "metrics")]
const LABEL_OPERATION: &str = "operation";
#[cfg(feature = "metrics")]
const OPERATION_APPEND: &str = "append";
#[cfg(feature = "metrics")]
const OPERATION_FETCH: &str = "fetch";
#[cfg(feature = "metrics")]
const LABEL_SUCCESS: &str = "success";

pub fn record_append_operation(latency: u64, _bytes: u64, success: bool) {
    #[cfg(feature = "metrics")]
    {
        HISTOGRAM_OPERATION_LATENCY.record(
            latency,
            &[
                KeyValue::new(LABEL_OPERATION, OPERATION_APPEND),
                KeyValue::new(LABEL_SUCCESS, success),
            ],
        );
        if success {
            COUNTER_OPERATION_BYTES
                .add(_bytes, &[KeyValue::new(LABEL_OPERATION, OPERATION_APPEND)]);
        }
    }
    RangeServerStatistics::observe_append_latency(latency as i16, success);
}

pub fn record_fetch_operation(latency: u64, _bytes: u64, success: bool) {
    #[cfg(feature = "metrics")]
    {
        HISTOGRAM_OPERATION_LATENCY.record(
            latency,
            &[
                KeyValue::new(LABEL_OPERATION, OPERATION_FETCH),
                KeyValue::new(LABEL_SUCCESS, success),
            ],
        );
        if success {
            COUNTER_OPERATION_BYTES.add(_bytes, &[KeyValue::new(LABEL_OPERATION, OPERATION_FETCH)]);
        }
    }
    RangeServerStatistics::observe_fetch_latency(latency as i16, success);
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use log::trace;

    use super::RangeServerStatistics;

    #[test]
    #[ignore = "Due to time jitter, it's hard to determine accuracy, so this test is just for observing the effect."]
    fn test_store_statistics() {
        let mut statistics = RangeServerStatistics::new();
        statistics.record();
        sleep(Duration::from_secs(2));
        statistics.record();
        trace!(
            "network_append_rate: {}, network_fetch_rate: {}, network_failed_append_rate: {}, get_network_failed_fetch_rate: {}",
            statistics.get_network_append_rate(),
            statistics.get_network_fetch_rate(),
            statistics.get_network_failed_append_rate(),
            statistics.get_network_failed_fetch_rate(),
        );
        RangeServerStatistics::observe_fetch_latency(5, true);
        sleep(Duration::from_secs(1));
        RangeServerStatistics::observe_fetch_latency(5, true);
        sleep(Duration::from_secs(1));
        RangeServerStatistics::observe_fetch_latency(5, true);
        sleep(Duration::from_secs(1));
        RangeServerStatistics::observe_fetch_latency(5, true);
        sleep(Duration::from_secs(1));
        RangeServerStatistics::observe_fetch_latency(5, true);
        sleep(Duration::from_secs(1));
        trace!("avg[0] = {}", statistics.get_network_fetch_avg_latency());
        RangeServerStatistics::observe_fetch_latency(1, true);
        trace!("avg[1]: {}", statistics.get_network_fetch_avg_latency());
        sleep(Duration::from_secs(1));
        RangeServerStatistics::observe_fetch_latency(1, true);
        trace!("avg[2]: {}", statistics.get_network_fetch_avg_latency());
        sleep(Duration::from_secs(1));
        RangeServerStatistics::observe_fetch_latency(1, true);
        trace!("avg[3]: {}", statistics.get_network_fetch_avg_latency());
        sleep(Duration::from_secs(1));
        RangeServerStatistics::observe_fetch_latency(1, true);
        trace!("avg[4]: {}", statistics.get_network_fetch_avg_latency());
        sleep(Duration::from_secs(1));
        RangeServerStatistics::observe_fetch_latency(1, true);
        trace!("avg[5]: {}", statistics.get_network_fetch_avg_latency());
    }
}
