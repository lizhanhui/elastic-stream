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
use prometheus::{Encoder, TextEncoder};
use tokio::select;
use tokio::sync::broadcast;

use config::Configuration;

pub mod object_metrics;
pub mod store_metrics;
pub mod sys_metrics;
pub mod uring_metrics;

static METER: OnceCell<Meter> = OnceCell::new();

lazy_static::lazy_static! {
    static ref REGISTRY: prometheus::Registry = prometheus::Registry::new();
}

pub(crate) fn get_meter() -> &'static Meter {
    METER
        .get()
        .expect("Please initialize the meter before using it")
}

pub fn init_meter(_config: Arc<Configuration>) {
    let resource = Resource::from_detectors(
        Duration::from_secs(0),
        vec![
            Box::new(SdkProvidedResourceDetector),
            Box::new(EnvResourceDetector::new()),
            Box::new(TelemetryResourceDetector),
        ],
    );

    let prometheus_exporter = opentelemetry_prometheus::exporter()
        .with_registry(REGISTRY.clone())
        .build()
        .unwrap();

    let builder = MeterProvider::builder()
        .with_resource(resource)
        .with_reader(prometheus_exporter);

    let meter_provider = init_views(builder).build();

    #[cfg(not(feature = "metrics"))]
    {
        // shutdown provider to build noop meter
        let _ = meter_provider.shutdown();
    }

    let _ = METER.set(meter_provider.meter("elastic-stream"));
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

async fn serve_req(_req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    let encode_result = encoder.encode(&metric_families, &mut buffer);
    if encode_result.is_err() {
        error!("encode prometheus metrics failed: {:?}", encode_result)
    }

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();

    Ok(response)
}

pub fn start_metrics_exporter(config: Arc<Configuration>, mut shutdown: broadcast::Receiver<()>) {
    // init meter
    init_meter(config.clone());

    // init exporter
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

            runtime.block_on(async {
                let host = config
                    .observation
                    .metrics
                    .host
                    .parse::<IpAddr>()
                    .unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
                let port = config.observation.metrics.port;
                let addr = SocketAddr::new(host, port);
                info!("prometheus exporter listening on http://{}", addr);

                let serve_future = Server::bind(&addr).serve(make_service_fn(|_| async {
                    Ok::<_, hyper::Error>(service_fn(serve_req))
                }));

                select! {
                    _ = shutdown.recv() => {
                        info!("shutting down prometheus exporter");
                    }
                    Err(err) = serve_future => {
                        error!("server error: {}", err);
                    }
                }
            });
        })
        .unwrap();
}
