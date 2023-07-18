use std::time::Duration;

use lazy_static::lazy_static;
use prometheus::{register_histogram, register_int_counter, Histogram, IntCounter};

pub fn multi_part_object_write(size: u32, elapsed: Duration) {
    OBJECT_WRITE_API_COUNTER.inc();
    OBJECT_WRITE_SIZE_COUNTER.inc_by(size as u64);
    OBJECT_WRITE_SIZE_HISTOGRAM.observe((size >> 20) as f64);
    OBJECT_WRITE_LATENCY_HISTOGRAM.observe(elapsed.as_millis() as f64);
}

pub fn multi_part_object_complete() {
    OBJECT_NEW_COUNTER.inc();
    // CreateMultipartUpload and CompleteMultipartUpload both count as API call
    OBJECT_WRITE_API_COUNTER.inc_by(2);
}

pub fn object_complete(size: u32, elapsed: Duration) {
    OBJECT_WRITE_API_COUNTER.inc();
    OBJECT_WRITE_SIZE_COUNTER.inc_by(size as u64);
    OBJECT_WRITE_SIZE_HISTOGRAM.observe((size >> 20) as f64);
    OBJECT_NEW_COUNTER.inc();
    OBJECT_WRITE_LATENCY_HISTOGRAM.observe(elapsed.as_millis() as f64);
}

lazy_static! {
    pub static ref OBJECT_WRITE_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "object_size_histogram",
        "Histogram of object sizes in MiB",
        vec![1.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]
    )
    .unwrap();
    pub static ref OBJECT_WRITE_LATENCY_HISTOGRAM: Histogram = register_histogram!(
        "object_write_latency_histogram",
        "Histogram of object write latencies in milliseconds",
        vec![100.0, 200.0, 300.0, 400.0, 600.0, 800.0, 1000.0, 1500.0, 2000.0, 3000.0, 4000.0]
    )
    .unwrap();
    pub static ref OBJECT_NEW_COUNTER: IntCounter =
        register_int_counter!("object_new_counter", "Counter of new objects").unwrap();
    // multi-part object need to be write multiple times
    pub static ref OBJECT_WRITE_API_COUNTER: IntCounter =
        register_int_counter!("object_write_api_counter", "Counter of object writes api call").unwrap();
    pub static ref OBJECT_WRITE_SIZE_COUNTER: IntCounter = register_int_counter!(
        "object_write_size_counter",
        "Counter of object write sizes in bytes"
    ).unwrap();
}
