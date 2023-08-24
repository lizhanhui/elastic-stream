use lazy_static::*;
use opentelemetry::metrics::Histogram;
#[cfg(feature = "metrics")]
use opentelemetry::KeyValue;

use crate::metrics::get_meter;

lazy_static! {
    pub static ref HISTOGRAM_OPERATION_LATENCY: Histogram<u64> = get_meter()
        .u64_histogram("rang.server.operation.latency")
        .with_description("Histogram of records operation latency in microseconds")
        .init();
}

#[cfg(feature = "metrics")]
const LABEL_OPERATION: &str = "operation";
#[cfg(feature = "metrics")]
const OPERATION_APPEND: &str = "append";
#[cfg(feature = "metrics")]
const OPERATION_FETCH: &str = "fetch";

pub fn record_append_operation(_latency: u64) {
    #[cfg(feature = "metrics")]
    {
        HISTOGRAM_OPERATION_LATENCY.record(
            _latency,
            &[KeyValue::new(LABEL_OPERATION, OPERATION_APPEND)],
        );
    }
}

pub fn record_fetch_operation(_latency: u64) {
    #[cfg(feature = "metrics")]
    {
        HISTOGRAM_OPERATION_LATENCY
            .record(_latency, &[KeyValue::new(LABEL_OPERATION, OPERATION_FETCH)]);
    }
}
