use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use config::Configuration;
use lazy_static::lazy_static;
use log::{info, warn};
use minitrace::collector::EventRecord;
use minitrace::local::{LocalSpans, LocalSpansInner};
use minitrace::prelude::SpanRecord;
use minitrace::util::spsc;
use minstant::Anchor;
use opentelemetry::sdk::export::trace::{SpanData, SpanExporter};
use opentelemetry::sdk::trace::{EvictedHashMap, EvictedQueue};
use opentelemetry::sdk::Resource;
use opentelemetry::trace::{Event, SpanContext, SpanKind, Status, TraceFlags, TraceState};
use opentelemetry::{InstrumentationLibrary, Key, KeyValue, StringValue, Value};
use parking_lot::Mutex;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::TryRecvError;

const COLLECT_LOOP_INTERVAL: Duration = Duration::from_millis(10);
lazy_static! {
    static ref SPSC_RXS: Mutex<Vec<spsc::Receiver<LocalSpans>>> = Mutex::new(Vec::new());
    static ref HOSTNAME: String = gethostname::gethostname()
        .into_string()
        .unwrap_or(String::from("unknown"));
}

thread_local! {
    static LOCAL_SPANS_SENDER: spsc::Sender<LocalSpans> = {
        let (tx, rx) = spsc::bounded(1024);
        register_receiver(rx);
        tx
    };
}

fn register_receiver(rx: spsc::Receiver<LocalSpans>) {
    SPSC_RXS.lock().push(rx);
}

pub fn report_trace(spans: LocalSpans) {
    LOCAL_SPANS_SENDER.with(|sender| sender.send(spans).ok());
}

pub fn start_trace_exporter(config: Arc<Configuration>, mut shutdown: broadcast::Receiver<()>) {
    std::thread::Builder::new()
        .name("TraceExporter".to_string())
        .spawn(move || {
            let core = core_affinity::CoreId { id: 0 };
            if !core_affinity::set_for_current(core) {
                warn!("Failed to set core affinity for trace exporter thread");
            }

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            let mut grpc_exporter = runtime.block_on(async {
                opentelemetry_otlp::SpanExporter::new_tonic(
                    opentelemetry_otlp::ExportConfig {
                        endpoint: config.observation.trace.endpoint.to_string(),
                        protocol: opentelemetry_otlp::Protocol::Grpc,
                        timeout: Duration::from_millis(config.observation.trace.timeout_ms),
                    },
                    opentelemetry_otlp::TonicConfig::default(),
                )
                .expect("Failed to create trace exporter")
            });

            loop {
                match shutdown.try_recv() {
                    Err(TryRecvError::Empty) => {}
                    _ => {
                        info!("Trace exporter stopped");
                        break;
                    }
                }

                let begin_instant = std::time::Instant::now();
                let spans = collect_spans();

                if spans.is_empty() {
                    std::thread::sleep(
                        COLLECT_LOOP_INTERVAL.saturating_sub(begin_instant.elapsed()),
                    );
                    continue;
                }

                let result = runtime.block_on(grpc_exporter.export(convert(spans)));
                if let Err(error) = result {
                    warn!("Failed to export spans: {:?}", error);
                }
            }
            runtime.shutdown_timeout(Duration::from_millis(config.observation.trace.timeout_ms));
        })
        .unwrap();
}

fn collect_spans() -> Vec<Arc<LocalSpansInner>> {
    let mut span_collects = Vec::new();
    SPSC_RXS.lock().retain_mut(|rx| {
        loop {
            match rx.try_recv() {
                Ok(Some(spans)) => {
                    span_collects.push(spans.inner);
                }
                Ok(None) => {
                    // No more spans to collect.
                    return true;
                }
                Err(_) => {
                    // Channel closed. Need to discard this receiver.
                    return false;
                }
            }
        }
    });
    span_collects
}

// convert local spans to opentelemetry spans data for exporting
fn convert(local_spans: Vec<Arc<LocalSpansInner>>) -> Vec<SpanData> {
    let anchor: Anchor = Anchor::new();
    let mut span_records: Vec<SpanRecord> = Vec::new();

    // build span records for every local span
    for local_span in local_spans {
        let mut root = None;
        let trace_id = minitrace::collector::SpanContext::random().trace_id;
        let mut events = vec![];

        for span in local_span.spans.iter() {
            let begin_time_unix_ns = span.begin_instant.as_unix_nanos(&anchor);
            let parent_id = span.parent_id;

            if span.is_event {
                let event = EventRecord {
                    name: span.name,
                    timestamp_unix_ns: begin_time_unix_ns,
                    properties: span.properties.clone(),
                };
                events.push(event);
                continue;
            }

            let end_unix_time_ns = if span.end_instant == span.begin_instant {
                local_span.end_time.as_unix_nanos(&anchor)
            } else {
                span.end_instant.as_unix_nanos(&anchor)
            };
            if span.parent_id == minitrace::collector::SpanId::default() {
                root = Some(SpanRecord {
                    trace_id,
                    span_id: span.id,
                    parent_id,
                    begin_time_unix_ns,
                    duration_ns: end_unix_time_ns.saturating_sub(begin_time_unix_ns),
                    name: span.name,
                    properties: span.properties.clone(),
                    events: vec![],
                });
            } else {
                span_records.push(SpanRecord {
                    trace_id,
                    span_id: span.id,
                    parent_id,
                    begin_time_unix_ns,
                    duration_ns: end_unix_time_ns.saturating_sub(begin_time_unix_ns),
                    name: span.name,
                    properties: span.properties.clone(),
                    events: vec![],
                });
            }
        }

        let mut root = root.unwrap();
        root.events = events;
        span_records.push(root);
    }

    // convert span record to opentelemetry spans data
    span_records
        .iter()
        .map(move |span| SpanData {
            span_context: SpanContext::new(
                span.trace_id.0.into(),
                span.span_id.0.into(),
                TraceFlags::default(),
                false,
                TraceState::default(),
            ),
            parent_span_id: span.parent_id.0.into(),
            name: span.name.into(),
            start_time: UNIX_EPOCH + Duration::from_nanos(span.begin_time_unix_ns),
            end_time: UNIX_EPOCH + Duration::from_nanos(span.begin_time_unix_ns + span.duration_ns),
            attributes: convert_properties(&span.properties),
            events: convert_events(&span.events),
            links: EvictedQueue::new(0),
            status: Status::default(),
            span_kind: SpanKind::Internal,
            resource: Cow::Owned(Resource::new([
                KeyValue::new("service.name", "range-server"),
                KeyValue::new("host.name", HOSTNAME.to_string()),
            ])),
            instrumentation_lib: InstrumentationLibrary::default(),
        })
        .collect()
}

fn cow_to_otel_key(cow: Cow<'static, str>) -> Key {
    match cow {
        Cow::Borrowed(s) => Key::from_static_str(s),
        Cow::Owned(s) => Key::from(s),
    }
}

fn cow_to_otel_value(cow: Cow<'static, str>) -> Value {
    match cow {
        Cow::Borrowed(s) => Value::String(StringValue::from(s)),
        Cow::Owned(s) => Value::String(StringValue::from(s)),
    }
}

fn convert_properties(properties: &[(Cow<'static, str>, Cow<'static, str>)]) -> EvictedHashMap {
    let mut map = EvictedHashMap::new(u32::MAX, properties.len());
    for (k, v) in properties {
        map.insert(KeyValue::new(
            cow_to_otel_key(k.clone()),
            cow_to_otel_value(v.clone()),
        ));
    }
    map
}

fn convert_events(events: &[EventRecord]) -> EvictedQueue<Event> {
    let mut queue = EvictedQueue::new(u32::MAX);
    queue.extend(events.iter().map(|event| {
        Event::new(
            event.name,
            UNIX_EPOCH + Duration::from_nanos(event.timestamp_unix_ns),
            event
                .properties
                .iter()
                .map(|(k, v)| {
                    KeyValue::new(cow_to_otel_key(k.clone()), cow_to_otel_value(v.clone()))
                })
                .collect(),
            0,
        )
    }));
    queue
}
