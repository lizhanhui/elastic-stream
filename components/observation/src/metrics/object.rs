use std::time::Duration;

use lazy_static::lazy_static;
#[cfg(feature = "metrics")]
use opentelemetry::KeyValue;

use crate::metrics::get_meter;
use opentelemetry::metrics::{Counter, Histogram};

lazy_static! {
    static ref COUNTER_NEW: Counter<u64> = get_meter()
        .u64_counter("store.object.new.total")
        .with_description("Total of new objects")
        .init();
    static ref COUNTER_API_CALL: Counter<u64> = get_meter()
        .u64_counter("store.object.api.total")
        .with_description("Total of api count")
        .init();
    static ref HISTOGRAM_WRITE_SIZE: Histogram<u64> = get_meter()
        .u64_histogram("store.object.operation.bytes")
        .with_description("Histogram of operation sizes in MiB")
        .init();
    static ref HISTOGRAM_WRITE_LATENCY: Histogram<u64> = get_meter()
        .u64_histogram("store.object.operation.latency")
        .with_description("Histogram of operation latency in milliseconds")
        .init();
}

#[cfg(feature = "metrics")]
const LABEL_OPERATION: &str = "operation";
#[cfg(feature = "metrics")]
const OPERATION_WRITE: &str = "write";

pub fn multi_part_object_write(_size: u32, _elapsed: Duration) {
    #[cfg(feature = "metrics")]
    {
        HISTOGRAM_WRITE_SIZE.record(
            (_size >> 20) as u64,
            &[KeyValue::new(LABEL_OPERATION, OPERATION_WRITE)],
        );
        HISTOGRAM_WRITE_LATENCY.record(
            _elapsed.as_millis() as u64,
            &[KeyValue::new(LABEL_OPERATION, OPERATION_WRITE)],
        );
        COUNTER_API_CALL.add(1, &[KeyValue::new(LABEL_OPERATION, OPERATION_WRITE)]);
    }
}

pub fn multi_part_object_complete() {
    #[cfg(feature = "metrics")]
    {
        COUNTER_NEW.add(1, &[]);
        // CreateMultipartUpload and CompleteMultipartUpload both count as API call
        COUNTER_API_CALL.add(2, &[KeyValue::new(LABEL_OPERATION, OPERATION_WRITE)]);
    }
}

pub fn object_complete(_size: u32, _elapsed: Duration) {
    #[cfg(feature = "metrics")]
    {
        HISTOGRAM_WRITE_SIZE.record(
            (_size >> 20) as u64,
            &[KeyValue::new(LABEL_OPERATION, OPERATION_WRITE)],
        );
        HISTOGRAM_WRITE_LATENCY.record(
            _elapsed.as_millis() as u64,
            &[KeyValue::new(LABEL_OPERATION, OPERATION_WRITE)],
        );
        COUNTER_NEW.add(1, &[]);
        COUNTER_API_CALL.add(1, &[KeyValue::new(LABEL_OPERATION, OPERATION_WRITE)]);
    }
}
