use crate::metrics::tonic::sink;
use config::Configuration;
use opentelemetry::metrics::MetricsError;
use opentelemetry::sdk::metrics::data::{ResourceMetrics, Temporality};
use opentelemetry::sdk::metrics::reader::{
    AggregationSelector, MetricProducer, MetricReader, TemporalitySelector,
};
use opentelemetry::sdk::metrics::{Aggregation, InstrumentKind, ManualReader, Pipeline};
use opentelemetry::sdk::Resource;
use opentelemetry::Context;
use opentelemetry_otlp::{
    Error, OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, OTEL_EXPORTER_OTLP_METRICS_TIMEOUT,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;
use std::str::FromStr;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

#[derive(Debug, Clone)]
pub struct OtlpMetricsReader {
    reader: Arc<ManualReader>,
    endpoint: Endpoint,
    client: Option<MetricsServiceClient<Channel>>,
}

impl OtlpMetricsReader {
    pub(crate) fn new(config: Arc<Configuration>) -> Result<Self, Error> {
        let endpoint = match std::env::var(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT) {
            Ok(val) => val,
            Err(_) => format!("{}{}", config.observation.metrics.endpoint, "/v1/metrics"),
        };

        let timeout = match std::env::var(OTEL_EXPORTER_OTLP_METRICS_TIMEOUT) {
            Ok(val) => match u64::from_str(&val) {
                Ok(seconds) => Duration::from_secs(seconds),
                Err(_) => Duration::from_secs(config.observation.metrics.timeout),
            },
            Err(_) => Duration::from_secs(config.observation.metrics.timeout),
        };

        let endpoint = Channel::from_shared(endpoint)
            .map_err::<Error, _>(Into::into)?
            .timeout(timeout);

        Ok(Self {
            reader: Arc::new(ManualReader::default()),
            endpoint,
            client: None,
        })
    }

    pub(crate) async fn export(&mut self) -> Result<ExportMetricsServiceResponse, MetricsError> {
        if self.client.is_none() {
            let channel = self.endpoint.connect_lazy();
            self.client = Some(MetricsServiceClient::new(channel));
        }
        let mut metrics = ResourceMetrics {
            resource: Resource::empty(),
            scope_metrics: vec![],
        };
        self.reader.collect(&mut metrics)?;
        let request = tonic::Request::new(sink(&metrics));
        let result = self
            .client
            .as_mut()
            .unwrap()
            .export(request)
            .await
            .map_err::<Error, _>(Into::into)?;
        Ok(result.into_inner())
    }
}

impl TemporalitySelector for OtlpMetricsReader {
    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.reader.temporality(kind)
    }
}

impl AggregationSelector for OtlpMetricsReader {
    fn aggregation(&self, kind: InstrumentKind) -> Aggregation {
        self.reader.aggregation(kind)
    }
}

impl MetricReader for OtlpMetricsReader {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.reader.register_pipeline(pipeline)
    }

    fn register_producer(&self, producer: Box<dyn MetricProducer>) {
        self.reader.register_producer(producer)
    }

    fn collect(&self, rm: &mut ResourceMetrics) -> opentelemetry::metrics::Result<()> {
        self.reader.collect(rm)
    }

    fn force_flush(&self, cx: &Context) -> opentelemetry::metrics::Result<()> {
        self.reader.force_flush(cx)
    }

    fn shutdown(&self) -> opentelemetry::metrics::Result<()> {
        self.reader.shutdown()
    }
}
