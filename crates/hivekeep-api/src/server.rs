//! Serving: the gRPC server, the probe server and the metrics endpoint.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hivekeep_identity::cred_bundle::{load_trust_anchors, CredentialBundle};
use hivekeep_identity::pem;
use hivekeep_proto::api::v1::control_server::ControlServer;
use hivekeep_proto::api::v1::worker_service_server::WorkerServiceServer;
use hivekeep_store::Store;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};

use crate::authn::Authenticator;
use crate::control::ControlService;
use crate::metrics::Metrics;
use crate::worker_cache::WorkerCache;
use crate::worker_service::WorkerServiceImpl;

/// The PostgreSQL settings, refusing a missing read/write connection
/// string.
pub fn pg_config(
    read_write_url: &str,
    owner_url: &str,
    read_write_role: &str,
    owner_role: &str,
    schema: &str,
    max_connections: u32,
) -> Result<hivekeep_store::PgConfig, String> {
    if read_write_url.is_empty() {
        return Err(
            "--postgres-read-write-connection-string is required (or --memory-store)".into(),
        );
    }
    let opt = |s: &str| Some(s.to_string()).filter(|s| !s.is_empty());
    Ok(hivekeep_store::PgConfig {
        read_write_url: read_write_url.into(),
        owner_url: opt(owner_url),
        read_write_role: opt(read_write_role),
        owner_role: opt(owner_role),
        schema: schema.into(),
        max_connections: (max_connections > 0).then_some(max_connections),
    })
}

/// Upper bound on any RPC.
pub const MAX_RPC_DEADLINE: Duration = Duration::from_secs(10 * 60);

/// The gRPC server's TLS: its credential bundle, and, when set, the
/// pod-identity CA client certificates are verified against. Client
/// certificates stay optional: bearer-token clients present none.
pub fn server_tls(cred_bundle: &str, client_ca: &str) -> Result<ServerTlsConfig, String> {
    let bundle = CredentialBundle::load(cred_bundle)
        .map_err(|e| format!("loading server credentials: {e}"))?;
    let cert_pem = pem::encode_certificates(bundle.chain.iter().map(Vec::as_slice));
    let key_pem = pem::encode(pem::PRIVATE_KEY, bundle.key.pkcs8_der());
    let mut tls = ServerTlsConfig::new().identity(Identity::from_pem(cert_pem, key_pem));
    if !client_ca.is_empty() {
        let roots =
            load_trust_anchors(client_ca).map_err(|e| format!("load pod-identity CA: {e}"))?;
        let pem = pem::encode_certificates(roots.iter().map(Vec::as_slice));
        tls = tls
            .client_ca_root(Certificate::from_pem(pem))
            .client_auth_optional(true);
    }
    Ok(tls)
}

/// Serves Control and WorkerService on `addr` until `shutdown`, then drains
/// for up to `drain_timeout`.
pub async fn serve_grpc(
    addr: SocketAddr,
    tls: Option<ServerTlsConfig>,
    authn: Authenticator,
    control: ControlService,
    workers: WorkerServiceImpl,
    shutdown: CancellationToken,
    drain_timeout: Duration,
) -> Result<(), String> {
    let mut builder = tonic::transport::Server::builder().timeout(MAX_RPC_DEADLINE);
    if let Some(tls) = tls {
        builder = builder.tls_config(tls).map_err(|e| e.to_string())?;
    }
    // Outermost first: every call is logged (redacted) with its status and
    // elapsed time, including requests refused for carrying fields this
    // build does not know.
    let router = builder
        .layer(hivekeep_telemetry::interceptors::GrpcLogLayer::public())
        .layer(hivekeep_telemetry::interceptors::RejectUnknownFieldsLayer)
        .add_service(ControlServer::with_interceptor(control, authn.clone()))
        .add_service(WorkerServiceServer::with_interceptor(workers, authn));
    let drained = CancellationToken::new();
    let d = drained.clone();
    let serve = router.serve_with_shutdown(addr, async move {
        shutdown.cancelled().await;
        d.cancel();
    });
    tokio::pin!(serve);
    tokio::select! {
        r = &mut serve => r.map_err(|e| e.to_string()),
        _ = async { drained.cancelled().await; tokio::time::sleep(drain_timeout).await } => {
            tracing::warn!("drain deadline exceeded; forcing stop");
            Ok(())
        }
    }
}

/// Readiness of this replica: not ready while shutting down or when the
/// store does not answer.
pub struct Readiness {
    ready: AtomicBool,
    store: Arc<dyn Store>,
}

impl Readiness {
    /// Ready, checking `store`.
    pub fn new(store: Arc<dyn Store>) -> Arc<Self> {
        Arc::new(Readiness {
            ready: AtomicBool::new(true),
            store,
        })
    }

    /// Leaves the load balancer.
    pub fn mark_not_ready(&self) {
        self.ready.store(false, Ordering::SeqCst);
    }

    /// Whether the replica should receive traffic.
    pub async fn check(&self) -> Result<(), String> {
        if !self.ready.load(Ordering::SeqCst) {
            return Err("shutting down".into());
        }
        self.store.ping().await.map_err(|e| format!("store: {e}"))
    }
}

fn text(code: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(body.into()));
    *r.status_mut() = code;
    r
}

async fn serve_http<F, Fut>(
    addr: SocketAddr,
    stop: CancellationToken,
    handler: F,
) -> Result<(), String>
where
    F: Fn(Request<Incoming>) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Response<Full<Bytes>>> + Send + 'static,
{
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("listen on {addr}: {e}"))?;
    loop {
        let (stream, _) = tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            r = listener.accept() => r.map_err(|e| e.to_string())?,
        };
        let h = handler.clone();
        tokio::spawn(async move {
            let svc = hyper::service::service_fn(move |req| {
                let h = h.clone();
                async move { Ok::<_, Infallible>(h(req).await) }
            });
            let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), svc)
                .await;
        });
    }
}

/// Serves `/healthz` (liveness) and `/readyz` (readiness).
pub async fn serve_health(
    addr: SocketAddr,
    readiness: Arc<Readiness>,
    stop: CancellationToken,
) -> Result<(), String> {
    serve_http(addr, stop, move |req: Request<Incoming>| {
        let readiness = readiness.clone();
        async move {
            match req.uri().path() {
                "/healthz" => text(StatusCode::OK, "ok"),
                "/readyz" => match readiness.check().await {
                    Ok(()) => text(StatusCode::OK, "ok"),
                    Err(e) => text(StatusCode::SERVICE_UNAVAILABLE, e),
                },
                _ => text(StatusCode::NOT_FOUND, "not found"),
            }
        }
    })
    .await
}

/// Serves `/metrics`, refreshing the worker gauges from `cache` per scrape.
pub async fn serve_metrics(
    addr: SocketAddr,
    metrics: Arc<Metrics>,
    cache: Arc<WorkerCache>,
    stop: CancellationToken,
) -> Result<(), String> {
    serve_http(addr, stop, move |req: Request<Incoming>| {
        let (metrics, cache) = (metrics.clone(), cache.clone());
        async move {
            if req.uri().path() != "/metrics" {
                return text(StatusCode::NOT_FOUND, "not found");
            }
            metrics.observe_cache(&cache);
            text(StatusCode::OK, metrics.encode())
        }
    })
    .await
}
