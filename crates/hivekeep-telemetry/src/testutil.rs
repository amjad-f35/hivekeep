//! Test helpers: a fake OTLP collector recording every export.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tokio::sync::mpsc;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

/// What a fake collector received.
#[derive(Default)]
pub(crate) struct Received {
    pub traces: Vec<ExportTraceServiceRequest>,
    pub metrics: Vec<ExportMetricsServiceRequest>,
    pub logs: Vec<ExportLogsServiceRequest>,
    pub trace_md: Vec<MetadataMap>,
    pub metric_md: Vec<MetadataMap>,
    pub log_md: Vec<MetadataMap>,
}

/// A fake OTLP collector serving traces, metrics and logs.
#[derive(Clone)]
pub(crate) struct FakeCollector {
    pub received: Arc<Mutex<Received>>,
    got_tx: mpsc::UnboundedSender<()>,
    got_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<()>>>,
}

impl FakeCollector {
    /// Waits for the next export of any signal.
    pub async fn wait(&self, timeout: Duration) -> bool {
        let mut rx = self.got_rx.lock().await;
        tokio::time::timeout(timeout, rx.recv()).await.is_ok()
    }

    /// Spans received, over every export.
    pub fn span_count(&self) -> usize {
        let r = self.received.lock().unwrap();
        r.traces
            .iter()
            .flat_map(|t| &t.resource_spans)
            .flat_map(|rs| &rs.scope_spans)
            .map(|ss| ss.spans.len())
            .sum()
    }

    /// Metric names in the first metrics export, with their counts; `None`
    /// when nothing was pushed.
    pub fn first_metric_names(&self) -> Option<std::collections::HashMap<String, usize>> {
        let r = self.received.lock().unwrap();
        let first = r.metrics.first()?;
        let mut names = std::collections::HashMap::new();
        for rm in &first.resource_metrics {
            for sm in &rm.scope_metrics {
                for m in &sm.metrics {
                    *names.entry(m.name.clone()).or_insert(0) += 1;
                }
            }
        }
        Some(names)
    }
}

#[tonic::async_trait]
impl TraceService for FakeCollector {
    async fn export(
        &self,
        req: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        {
            let mut r = self.received.lock().unwrap();
            r.trace_md.push(req.metadata().clone());
            r.traces.push(req.into_inner());
        }
        let _ = self.got_tx.send(());
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl MetricsService for FakeCollector {
    async fn export(
        &self,
        req: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        {
            let mut r = self.received.lock().unwrap();
            r.metric_md.push(req.metadata().clone());
            r.metrics.push(req.into_inner());
        }
        let _ = self.got_tx.send(());
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl LogsService for FakeCollector {
    async fn export(
        &self,
        req: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        {
            let mut r = self.received.lock().unwrap();
            r.log_md.push(req.metadata().clone());
            r.logs.push(req.into_inner());
        }
        let _ = self.got_tx.send(());
        Ok(Response::new(ExportLogsServiceResponse::default()))
    }
}

/// Starts a fake collector on a loopback port. It runs until the test's
/// runtime shuts down.
pub(crate) async fn start_fake_collector() -> (FakeCollector, SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (got_tx, got_rx) = mpsc::unbounded_channel();
    let c = FakeCollector {
        received: Arc::default(),
        got_tx,
        got_rx: Arc::new(tokio::sync::Mutex::new(got_rx)),
    };
    let server = tonic::transport::Server::builder()
        .add_service(TraceServiceServer::new(c.clone()))
        .add_service(MetricsServiceServer::new(c.clone()))
        .add_service(LogsServiceServer::new(c.clone()));
    tokio::spawn(
        server.serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    (c, addr)
}

/// A channel that never connects until used.
pub(crate) fn lazy_channel() -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy()
}

/// A channel to `addr`.
pub(crate) fn channel_to(addr: SocketAddr) -> tonic::transport::Channel {
    tonic::transport::Endpoint::try_from(format!("http://{addr}"))
        .unwrap()
        .connect_lazy()
}
