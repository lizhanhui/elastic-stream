use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Weak};
use std::time::Duration;

use hyper::header::CONTENT_TYPE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use log::{error, info, warn};
use once_cell::sync::OnceCell;
use opentelemetry::metrics::{Meter, MeterProvider as _};
use opentelemetry::sdk::metrics::data::{ResourceMetrics, Temporality};
use opentelemetry::sdk::metrics::exporter::PushMetricsExporter;
use opentelemetry::sdk::metrics::reader::{
    AggregationSelector, DefaultAggregationSelector, DefaultTemporalitySelector, MetricProducer,
    MetricReader, TemporalitySelector,
};
use opentelemetry::sdk::metrics::{
    new_view, Aggregation, Instrument, InstrumentKind, ManualReader, MeterProvider,
    MeterProviderBuilder, Pipeline, Stream,
};
use opentelemetry::sdk::resource::{
    EnvResourceDetector, SdkProvidedResourceDetector, TelemetryResourceDetector,
};
use opentelemetry::sdk::Resource;
use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::{Protocol, TonicExporterBuilder, WithExportConfig};
use prometheus::{Encoder, TextEncoder};
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::TryRecvError;

use config::Configuration;

pub mod object;
pub mod range_server;
pub mod store;
pub mod sys;
pub mod uring;

#[derive(Debug, Clone)]
pub struct OtlpExporter {
    reader: Arc<ManualReader>,
}

impl TemporalitySelector for OtlpExporter {
    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.reader.temporality(kind)
    }
}

impl AggregationSelector for OtlpExporter {
    fn aggregation(&self, kind: InstrumentKind) -> Aggregation {
        self.reader.aggregation(kind)
    }
}

impl MetricReader for OtlpExporter {
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

static METER: OnceCell<Meter> = OnceCell::new();
static METER_PROVIDER: OnceCell<MeterProvider> = OnceCell::new();

lazy_static::lazy_static! {
    static ref REGISTRY: prometheus::Registry = prometheus::Registry::new();
    static ref OTLP_METRICS_READER: OtlpExporter = OtlpExporter { reader: Arc::new(ManualReader::default()) };
}

pub(crate) fn get_meter() -> &'static Meter {
    METER
        .get()
        .expect("Please initialize the meter before using it")
}

pub fn init_meter(config: Arc<Configuration>) {
    let resource = Resource::from_detectors(
        Duration::from_secs(0),
        vec![
            Box::new(SdkProvidedResourceDetector),
            Box::new(EnvResourceDetector::new()),
            Box::new(TelemetryResourceDetector),
        ],
    )
    .merge(&Resource::new([KeyValue::new(
        "service.name",
        "range-server",
    )]));

    let mut builder = MeterProvider::builder().with_resource(resource);

    match config.observation.metrics.mode.as_str() {
        "prometheus" => {
            let prometheus_exporter = opentelemetry_prometheus::exporter()
                .with_registry(REGISTRY.clone())
                .build()
                .unwrap();
            builder = builder.with_reader(prometheus_exporter);
        }
        "otlp" => {
            builder = builder.with_reader(OTLP_METRICS_READER.clone());
        }
        _ => {
            panic!(
                "invalid metrics exporter mode: {}",
                config.observation.metrics.mode
            );
        }
    }

    let meter_provider = init_views(builder).build();
    if !config.observation.metrics.enable {
        let _ = meter_provider.shutdown();
    }

    let _ = METER.set(meter_provider.meter("elastic-stream"));
    let _ = METER_PROVIDER.set(meter_provider);
}

fn init_views(provider: MeterProviderBuilder) -> MeterProviderBuilder {
    provider.with_view(
        new_view(
            Instrument::new().name("histogram_*"),
            Stream::new().aggregation(Aggregation::ExplicitBucketHistogram {
                boundaries: vec![
                    0.0, 5.0, 10.0, 15.0, 50.0, 100.0, 150.0, 200.0, 500.0, 1000.0,
                ],
                record_min_max: true,
            }),
        )
        .unwrap(),
    )
}

pub fn start_metrics_exporter(config: Arc<Configuration>, shutdown: broadcast::Receiver<()>) {
    if !config.observation.metrics.enable {
        return;
    }
    std::thread::Builder::new()
        .name("MetricsExporter".to_string())
        .spawn(move || {
            let core = core_affinity::CoreId { id: 0 };
            if !core_affinity::set_for_current(core) {
                warn!("Failed to set core affinity for metrics exporter thread");
            }

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            let config_clone = config.clone();
            runtime.block_on(async {
                match config.observation.metrics.mode.as_str() {
                    "prometheus" => {
                        start_prometheus_exporter(config, shutdown).await;
                    }
                    "otlp" => {
                        start_otlp_exporter(config, shutdown).await;
                    }
                    _ => {
                        panic!(
                            "invalid metrics exporter mode: {}",
                            config.observation.metrics.mode
                        );
                    }
                }
            });
            runtime.shutdown_timeout(Duration::from_millis(
                config_clone.observation.trace.timeout_ms,
            ));
        })
        .unwrap();
}

async fn start_otlp_exporter(config: Arc<Configuration>, mut shutdown: broadcast::Receiver<()>) {
    let otlp_exporter = opentelemetry_otlp::MetricsExporter::new(
        TonicExporterBuilder::default()
            .with_protocol(Protocol::Grpc)
            .with_endpoint(format!(
                "http://{};{}",
                config.observation.metrics.host, config.observation.metrics.port
            )),
        Box::new(DefaultTemporalitySelector::new()),
        Box::new(DefaultAggregationSelector::new()),
    )
    .expect("build otlp metrics exporter failed");

    loop {
        std::thread::sleep(Duration::from_secs(
            config.observation.metrics.interval as u64,
        ));

        let mut metrics = ResourceMetrics {
            resource: Resource::empty(),
            scope_metrics: vec![],
        };
        if let Ok(()) = OTLP_METRICS_READER.collect(&mut metrics) {
            if let Err(error) = otlp_exporter.export(&mut metrics).await {
                error!("Failed to export metrics: {:?}", error);
            } else {
                info!("export metrics success: {:?}", metrics);
            }
        } else {
            error!("Failed to collect metrics");
        }

        match shutdown.try_recv() {
            Err(TryRecvError::Empty) => {}
            _ => {
                info!("Shutting down otlp metrics exporter");
                break;
            }
        }
    }
}

async fn serve_metrics_req(_req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    let encode_result = encoder.encode(&metric_families, &mut buffer);
    if encode_result.is_err() {
        error!("Encode prometheus metrics failed: {:?}", encode_result)
    }

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();

    Ok(response)
}

async fn start_prometheus_exporter(
    config: Arc<Configuration>,
    mut shutdown: broadcast::Receiver<()>,
) {
    let host = config
        .observation
        .metrics
        .host
        .parse::<IpAddr>()
        .unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
    let port = config.observation.metrics.port;
    let addr = SocketAddr::new(host, port);
    info!("prometheus metrics exporter listening on http://{}", addr);

    let serve_future = Server::bind(&addr).serve(make_service_fn(|_| async {
        Ok::<_, hyper::Error>(service_fn(serve_metrics_req))
    }));

    select! {
        _ = shutdown.recv() => {
            if let Some(meter_provider) = METER_PROVIDER.get() {
                let _ = meter_provider.shutdown();
            }
            info!("Shutting down prometheus metrics exporter");
        }
        Err(err) = serve_future => {
            error!("Server error: {}", err);
        }
    }
}
