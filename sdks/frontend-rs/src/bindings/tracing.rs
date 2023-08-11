use chrono::Local;
use log::info;
use minitrace::{local::Guard, prelude::Collector, Span};
use std::{net::SocketAddr, time::Duration};

pub struct Tracer {
    span: Span,
    collector: Option<Collector>,
    tx: flume::Sender<Collector>,
}
impl Tracer {
    pub fn new(root: Span, collector: Collector, tx: flume::Sender<Collector>) -> Self {
        Self {
            span: root,
            collector: Some(collector),
            tx,
        }
    }
    pub fn get_child_span(&self, event: &'static str) -> Span {
        Span::enter_with_parent(event, &self.span)
    }
    pub fn get_child_span_with_local_parent(&self, event: &'static str) -> Span {
        Span::enter_with_local_parent(event)
    }
    pub fn set_local_parent(&self) -> Option<Guard<impl FnOnce()>> {
        self.span.set_local_parent()
    }
}
impl Drop for Tracer {
    fn drop(&mut self) {
        // send collector to tracing report thread
        if let Some(collector) = self.collector.take() {
            let _ = self.tx.send(collector);
        }
    }
}

pub struct TracingService {
    tx: flume::Sender<Collector>,
}
impl TracingService {
    pub fn new(threshold: Duration) -> Self {
        let (tx, rx) = flume::unbounded::<Collector>();
        let _ = std::thread::Builder::new()
            .name("TracingServiceReportThread".to_string())
            .spawn(move || {
                let now = Local::now();
                let base_trace_id = now.timestamp_millis() as u64;
                let datetime_str = now.format("%Y-%m-%d %H:%M:%S").to_string();
                let service_name = "JNI#".to_owned() + &datetime_str;
                if let Ok(mut rt) = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_all()
                    .build()
                {
                    let mut trace_id = base_trace_id;
                    rt.block_on(async {
                        loop {
                            match rx.recv_async().await {
                                Ok(collector) => {
                                    trace_id += 1;
                                    let service_name = service_name.clone();
                                    monoio::spawn(async move {
                                        Self::report_tracing(
                                            threshold,
                                            collector,
                                            service_name,
                                            trace_id,
                                        )
                                        .await;
                                    });
                                }
                                Err(_) => {
                                    info!("tracing service report channel is dropped");
                                    break;
                                }
                            }
                        }
                    });
                } else {
                    info!("Failed to build tokio runtime");
                }
            });
        Self { tx }
    }
    pub fn new_tracer(&self, event: &'static str) -> Tracer {
        let (root, collector) = Span::root(event);
        Tracer::new(root, collector, self.tx.clone())
    }
    pub async fn report_tracing(
        threshold: Duration,
        collector: Collector,
        service_name: String,
        trace_id: u64,
    ) {
        let spans = collector.collect().await;
        if let Some(total_duration) = spans.iter().map(|span| span.duration_ns).max() {
            if total_duration >= threshold.as_nanos() as u64 {
                // TODO: Use configuration file to specify the IP address and port
                let addr = SocketAddr::new("127.0.0.1".parse().unwrap(), 6831);
                if let Ok(bytes) = minitrace_jaeger::encode(service_name, trace_id, 0, 0, &spans) {
                    let _ = minitrace_jaeger::report(addr, &bytes).await;
                }
            }
        }
    }
}
