use std::cell::RefCell;

use hdrhistogram::Histogram;
use log::info;

thread_local! {
    pub(crate) static METRICS: Metrics = Metrics::new();
}

pub(crate) struct Metrics {
    append_latency_histogram: RefCell<Histogram<u64>>,
    fetch_latency_histogram: RefCell<Histogram<u64>>,
    fetch_stream_latency_histogram: RefCell<Histogram<u64>>,
    fetch_object_latency_histogram: RefCell<Histogram<u64>>,
    fetch_object_size_histogram: RefCell<Histogram<u64>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            append_latency_histogram: RefCell::new(Histogram::new_with_max(u64::MAX, 3).unwrap()),
            fetch_latency_histogram: RefCell::new(Histogram::new_with_max(u64::MAX, 3).unwrap()),
            fetch_stream_latency_histogram: RefCell::new(
                Histogram::new_with_max(u64::MAX, 3).unwrap(),
            ),
            fetch_object_latency_histogram: RefCell::new(
                Histogram::new_with_max(u64::MAX, 3).unwrap(),
            ),
            fetch_object_size_histogram: RefCell::new(
                Histogram::new_with_max(u64::MAX, 3).unwrap(),
            ),
        }
    }

    pub fn record_append(&self, elapsed: u64) {
        let _ = self.append_latency_histogram.borrow_mut().record(elapsed);
    }

    pub fn record_fetch(&self, elapsed: u64) {
        let _ = self.fetch_latency_histogram.borrow_mut().record(elapsed);
    }

    pub fn record_fetch_stream(&self, elapsed: u64) {
        let _ = self
            .fetch_stream_latency_histogram
            .borrow_mut()
            .record(elapsed);
    }

    pub fn record_fetch_object(&self, size: u32, elapsed: u64) {
        let _ = self
            .fetch_object_size_histogram
            .borrow_mut()
            .record(size as u64);
        let _ = self
            .fetch_object_latency_histogram
            .borrow_mut()
            .record(elapsed);
    }

    pub fn report(&self) {
        let mut append = self.append_latency_histogram.borrow_mut();
        let mut fetch = self.fetch_latency_histogram.borrow_mut();
        let mut fetch_stream = self.fetch_stream_latency_histogram.borrow_mut();
        let mut fetch_object = self.fetch_object_latency_histogram.borrow_mut();
        let mut fetch_object_size = self.fetch_object_size_histogram.borrow_mut();
        info!(
            "
SDK Metrics:
\tappend:              [cnt={}, avg={}, 90.0={}, 99.0={}, 99.9={}, max={}]
\tfetch:               [cnt={}, avg={}, 90.0={}, 99.0={}, 99.9={}, max={}]
\tfetch_stream:        [cnt={}, avg={}, 90.0={}, 99.0={}, 99.9={}, max={}]
\tfetch_object:        [cnt={}, avg={}, 90.0={}, 99.0={}, 99.9={}, max={}]
\tfetch_object_size:   [cnt={}, avg={}, 90.0={}, 99.0={}, 99.9={}, max={}]
        ",
            append.len(),
            append.mean(),
            append.value_at_percentile(90.0),
            append.value_at_percentile(99.0),
            append.value_at_percentile(99.9),
            append.max(),
            fetch.len(),
            fetch.mean(),
            fetch.value_at_percentile(90.0),
            fetch.value_at_percentile(99.0),
            fetch.value_at_percentile(99.9),
            fetch.max(),
            fetch_stream.len(),
            fetch_stream.mean(),
            fetch_stream.value_at_percentile(90.0),
            fetch_stream.value_at_percentile(99.0),
            fetch_stream.value_at_percentile(99.9),
            fetch_stream.max(),
            fetch_object.len(),
            fetch_object.mean(),
            fetch_object.value_at_percentile(90.0),
            fetch_object.value_at_percentile(99.0),
            fetch_object.value_at_percentile(99.9),
            fetch_object.max(),
            fetch_object_size.len(),
            fetch_object_size.mean(),
            fetch_object_size.value_at_percentile(90.0),
            fetch_object_size.value_at_percentile(99.0),
            fetch_object_size.value_at_percentile(99.9),
            fetch_object_size.max(),
        );
        append.reset();
        fetch.reset();
        fetch_stream.reset();
        fetch_object.reset();
        fetch_object_size.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_report() {
        let m = Metrics::new();
        m.record_append(1000);
        m.record_fetch_object(1, 1000);
        assert_eq!(1, m.append_latency_histogram.borrow().len());
        assert_eq!(1, m.fetch_object_latency_histogram.borrow().len());
        assert_eq!(1, m.fetch_object_size_histogram.borrow().len());
        m.report();
        assert_eq!(0, m.append_latency_histogram.borrow().len());
        assert_eq!(0, m.fetch_object_latency_histogram.borrow().len());
        assert_eq!(0, m.fetch_object_size_histogram.borrow().len());
    }
}
