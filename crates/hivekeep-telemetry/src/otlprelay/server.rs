//! The keeper half of the relay: an OTLP receiver on a unix socket that
//! forwards to the real collector over the node's network.
//!
//! The request message is forwarded verbatim rather than decoded into SDK
//! records and re-exported, so each worker's own resource (service name,
//! instance, pod attributes) stays intact. Verbatim applies to the payload,
//! not to the call around it: the caller's metadata is dropped and replaced
//! with the headers the keeper resolves from its own environment, since the
//! upstream leg is the keeper's connection and its credentials are the
//! keeper's.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tokio::sync::{watch, Mutex};
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use super::config::{
    upstream_compression, upstream_headers, upstream_target, validate_socket_path, Compression,
    ConfigError, ENDPOINT_ENV, LOGS_HEADERS_ENV, METRICS_HEADERS_ENV, TRACES_HEADERS_ENV,
};
use super::gate::SourceGate;
use crate::env::Env;

/// Keeps the relay socket private to root: the keeper and the worker pods
/// both run as root, and the socket lives in a root-owned host directory.
pub const SOCKET_MODE: u32 = 0o600;

/// Bounds a single export payload, so one misbehaving worker cannot make
/// the keeper allocate without limit; SDK batch processors emit far smaller
/// messages.
pub const MAX_RECV_MSG_SIZE: usize = 16 << 20;

/// The default relay socket, directly under the keeper base directory that
/// is mounted at the same path into the keeper and every worker pod.
pub const DEFAULT_SOCKET_PATH: &str = "/var/lib/hivekeep/keeper-otlp.sock";

/// Why the relay could not start or serve.
#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    /// A configuration the relay refuses.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// A socket or transport failure.
    #[error("{0}")]
    Io(String),
}

/// Attaches the keeper's headers to an upstream call, dropping the
/// caller's: a header set is a credential, not telemetry, and anything that
/// reached the socket must not choose what the keeper presents.
fn upstream_request<T>(msg: T, headers: &MetadataMap) -> Request<T> {
    let mut req = Request::new(msg);
    *req.metadata_mut() = headers.clone();
    req
}

#[derive(Clone)]
struct TraceRelay {
    upstream: TraceServiceClient<Channel>,
    headers: MetadataMap,
    gate: Arc<SourceGate>,
}

#[tonic::async_trait]
impl TraceService for TraceRelay {
    /// Forwards a batch of spans unchanged. Not wrapped in a span of the
    /// keeper's own: the relay must not inject itself into the trace it
    /// carries. A batch is refused whole rather than having the offending
    /// resource dropped: a partial success the sender reads as success
    /// loses telemetry silently.
    async fn export(
        &self,
        req: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let req = req.into_inner();
        for rs in &req.resource_spans {
            self.gate.check(rs.resource.as_ref())?;
        }
        self.upstream
            .clone()
            .export(upstream_request(req, &self.headers))
            .await
    }
}

#[derive(Clone)]
struct MetricRelay {
    upstream: MetricsServiceClient<Channel>,
    headers: MetadataMap,
    gate: Arc<SourceGate>,
}

#[tonic::async_trait]
impl MetricsService for MetricRelay {
    async fn export(
        &self,
        req: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let req = req.into_inner();
        for rm in &req.resource_metrics {
            self.gate.check(rm.resource.as_ref())?;
        }
        self.upstream
            .clone()
            .export(upstream_request(req, &self.headers))
            .await
    }
}

#[derive(Clone)]
struct LogRelay {
    upstream: LogsServiceClient<Channel>,
    headers: MetadataMap,
    gate: Arc<SourceGate>,
}

#[tonic::async_trait]
impl LogsService for LogRelay {
    async fn export(
        &self,
        req: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let req = req.into_inner();
        for rl in &req.resource_logs {
            self.gate.check(rl.resource.as_ref())?;
        }
        self.upstream
            .clone()
            .export(upstream_request(req, &self.headers))
            .await
    }
}

/// The relay server.
pub struct Server {
    sock_path: PathBuf,
    traces: TraceRelay,
    metrics: MetricRelay,
    logs: LogRelay,
    shutdown: watch::Sender<bool>,
    serving: Mutex<()>,
}

fn names(md: &MetadataMap) -> Vec<String> {
    let mut n: Vec<String> = md
        .keys()
        .map(|k| match k {
            tonic::metadata::KeyRef::Ascii(k) => k.as_str().to_string(),
            tonic::metadata::KeyRef::Binary(k) => k.as_str().to_string(),
        })
        .collect();
    n.sort();
    n.dedup();
    n
}

impl Server {
    /// Builds a relay forwarding to the collector named by the standard
    /// OTLP endpoint variables. Returns `None` when `sock_path` is empty
    /// (the relay is switched off) or no endpoint is configured: a relay
    /// with nowhere to forward to would accept spans and drop them, which
    /// is worse than the worker finding no socket and exporting directly.
    /// Must be called inside a Tokio runtime.
    pub fn new(env: &dyn Env, sock_path: &Path) -> Result<Option<Server>, RelayError> {
        if sock_path.as_os_str().is_empty() {
            return Ok(None);
        }
        validate_socket_path(sock_path)?;
        let target = upstream_target(env)?;
        if target.is_empty() {
            tracing::info!(
                env = ENDPOINT_ENV,
                "OTLP relay disabled: no collector endpoint configured"
            );
            return Ok(None);
        }
        let comp = upstream_compression(env)?;
        // Resolved before the socket exists: unparseable headers would
        // otherwise become per-export failures against a collector that
        // rejects unauthenticated calls.
        let trace_headers = upstream_headers(env, TRACES_HEADERS_ENV)?;
        let metric_headers = upstream_headers(env, METRICS_HEADERS_ENV)?;
        let log_headers = upstream_headers(env, LOGS_HEADERS_ENV)?;

        // Lazy: the keeper's startup does not wait for the collector.
        let upstream = Endpoint::try_from(format!("http://{target}"))
            .map_err(|e| RelayError::Io(format!("while dialing OTLP collector {target:?}: {e}")))?
            .connect_lazy();
        let gate = Arc::new(SourceGate::default());
        let mut tc = TraceServiceClient::new(upstream.clone());
        let mut mc = MetricsServiceClient::new(upstream.clone());
        let mut lc = LogsServiceClient::new(upstream);
        if comp == Compression::Gzip {
            tc = tc.send_compressed(CompressionEncoding::Gzip);
            mc = mc.send_compressed(CompressionEncoding::Gzip);
            lc = lc.send_compressed(CompressionEncoding::Gzip);
        }
        // Header names only: the values are credentials.
        tracing::info!(
            collector = %target,
            compression = comp.as_str(),
            trace_headers = ?names(&trace_headers),
            metric_headers = ?names(&metric_headers),
            log_headers = ?names(&log_headers),
            "OTLP relay forwarding to collector"
        );
        let (shutdown, _) = watch::channel(false);
        Ok(Some(Server {
            sock_path: sock_path.to_path_buf(),
            traces: TraceRelay {
                upstream: tc,
                headers: trace_headers,
                gate: gate.clone(),
            },
            metrics: MetricRelay {
                upstream: mc,
                headers: metric_headers,
                gate: gate.clone(),
            },
            logs: LogRelay {
                upstream: lc,
                headers: log_headers,
                gate,
            },
            shutdown,
            serving: Mutex::new(()),
        }))
    }

    /// The socket the relay listens on.
    pub fn socket_path(&self) -> &Path {
        &self.sock_path
    }

    /// Listens on the relay socket and serves until [`Server::stop`].
    #[cfg(unix)]
    pub async fn serve(&self) -> Result<(), RelayError> {
        use std::os::unix::fs::PermissionsExt;
        let _serving = self.serving.lock().await;
        if let Some(dir) = self.sock_path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| {
                RelayError::Io(format!(
                    "while creating the OTLP relay socket directory: {e}"
                ))
            })?;
        }
        // A socket left behind by a previous keeper would make the bind
        // fail even though nothing holds it.
        match std::fs::remove_file(&self.sock_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(RelayError::Io(format!(
                    "while removing a stale OTLP relay socket {}: {e}",
                    self.sock_path.display()
                )))
            }
        }
        let listener = tokio::net::UnixListener::bind(&self.sock_path).map_err(|e| {
            RelayError::Io(format!(
                "while opening the OTLP relay socket {}: {e}",
                self.sock_path.display()
            ))
        })?;
        std::fs::set_permissions(
            &self.sock_path,
            std::fs::Permissions::from_mode(SOCKET_MODE),
        )
        .map_err(|e| RelayError::Io(format!("while setting the OTLP relay socket mode: {e}")))?;
        tracing::info!(socket = %self.sock_path.display(), "OTLP relay serving");

        let mut stop = self.shutdown.subscribe();
        if *stop.borrow() {
            return Ok(());
        }
        tonic::transport::Server::builder()
            .add_service(
                TraceServiceServer::new(self.traces.clone())
                    .max_decoding_message_size(MAX_RECV_MSG_SIZE)
                    .accept_compressed(CompressionEncoding::Gzip),
            )
            .add_service(
                MetricsServiceServer::new(self.metrics.clone())
                    .max_decoding_message_size(MAX_RECV_MSG_SIZE)
                    .accept_compressed(CompressionEncoding::Gzip),
            )
            .add_service(
                LogsServiceServer::new(self.logs.clone())
                    .max_decoding_message_size(MAX_RECV_MSG_SIZE)
                    .accept_compressed(CompressionEncoding::Gzip),
            )
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                async move {
                    let _ = stop.wait_for(|s| *s).await;
                },
            )
            .await
            .map_err(|e| RelayError::Io(e.to_string()))
    }

    /// Drains the relay and removes the socket. Waits for a running
    /// [`Server::serve`] to return.
    pub async fn stop(&self) {
        let _ = self.shutdown.send(true);
        let _serving = self.serving.lock().await;
        let _ = std::fs::remove_file(&self.sock_path);
    }
}
