use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use hyper::header::CONTENT_TYPE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use log::{error, info, warn};
use once_cell::sync::OnceCell;
use opentelemetry::metrics::{Meter, MeterProvider as _};
use opentelemetry::sdk::metrics::{
    new_view, Aggregation, Instrument, MeterProvider, MeterProviderBuilder, Stream,
};
use opentelemetry::sdk::resource::{
    EnvResourceDetector, SdkProvidedResourceDetector, TelemetryResourceDetector,
};
use opentelemetry::sdk::Resource;
use opentelemetry::KeyValue;
use prometheus::{Encoder, TextEncoder};
use tokio::select;
use tokio::sync::broadcast;

use config::Configuration;

pub mod object;
mod otlp;
pub mod range_server;
pub mod store;
pub mod sys;
mod tonic;
pub mod uring;

static METER: OnceCell<Meter> = OnceCell::new();
static METER_PROVIDER: OnceCell<MeterProvider> = OnceCell::new();
static mut OTLP_METRICS_EXPORTER: OnceCell<otlp::OtlpMetricsReader> = OnceCell::new();

lazy_static::lazy_static! {
    static ref REGISTRY: prometheus::Registry = prometheus::Registry::new();
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
            let otlp_exporter = otlp::OtlpMetricsReader::new(config.clone())
                .expect("Build otlp metrics exporter failed");
            unsafe {
                OTLP_METRICS_EXPORTER
                    .set(otlp_exporter.clone())
                    .expect("Set otlp metrics exporter failed");
            }
            builder = builder.with_reader(otlp_exporter);
        }
        _ => {
            panic!(
                "Invalid metrics exporter mode: {}",
                config.observation.metrics.mode
            );
        }
    }

    let meter_provider = init_views(builder).build();
    if !config.observation.metrics.enable {
        let _ = meter_provider.shutdown();
    }

    METER
        .set(meter_provider.meter("elastic-stream"))
        .expect("Set otlp metrics meter failed");
    METER_PROVIDER
        .set(meter_provider)
        .expect("Set otlp metrics meter provider failed");
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
                            "Invalid metrics exporter mode: {}",
                            config.observation.metrics.mode
                        );
                    }
                }
            });
            runtime.shutdown_timeout(Duration::from_secs(
                config_clone.observation.metrics.timeout,
            ));
        })
        .unwrap();
}

async fn start_otlp_exporter(config: Arc<Configuration>, mut shutdown: broadcast::Receiver<()>) {
    unsafe {
        if OTLP_METRICS_EXPORTER.get().is_none() {
            panic!("Please initialize the otlp metrics exporter before using it");
        }
    }

    let mut interval = tokio::time::interval(Duration::from_secs(
        config.observation.metrics.interval as u64,
    ));

    loop {
        select! {
            _ = shutdown.recv() => {
                if let Some(meter_provider) = METER_PROVIDER.get() {
                    let _ = meter_provider.shutdown();
                }
                info!("Shutting down otlp metrics exporter");
                break;
            }
            _ = interval.tick() => {
                unsafe {
                    if let Err(error) = OTLP_METRICS_EXPORTER.get_mut().unwrap().export().await {
                        error!("Failed to export metrics: {:?}", error);
                    }
                }
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
    info!(
        "Prometheus metrics exporter is listening on http://{}",
        addr
    );

    let serve_future = Server::bind(&addr).serve(make_service_fn(|_| async {
        Ok::<_, hyper::Error>(service_fn(serve_metrics_req))
    }));

    if let Err(error) = serve_future
        .with_graceful_shutdown(async {
            let _ = shutdown.recv().await;
        })
        .await
    {
        error!("Start prometheus metrics exporter failed: {:?}", error);
    }

    if let Some(meter_provider) = METER_PROVIDER.get() {
        let _ = meter_provider.shutdown();
    }
    info!("Shutting down prometheus metrics exporter");
}
