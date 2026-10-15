//! The router's view of its dependencies (the dataplane, the Kubernetes
//! API, the control plane), refreshed periodically, plus the readiness
//! flag and the `/healthz` and `/readyz` probe server.

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use hivekeep_proto::api::v1 as pb;
use http_body_util::Full;
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::RwLock;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::dataplane::Dataplane;
use super::loopback::http_request;

/// Budget of each dependency check.
pub const DEPENDENCY_HEALTH_CHECK_TIMEOUT: Duration = Duration::from_millis(500);
const DEADLINE: &str = "context deadline exceeded";

/// One dependency's health.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ComponentHealth {
    /// Result of the last check.
    pub healthy: bool,
    /// What the last check said.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// When a check last succeeded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success: Option<DateTime<Utc>>,
    /// When a check last failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<DateTime<Utc>>,
    /// Successful checks so far.
    pub success_count: u64,
    /// Failed checks so far.
    pub failure_count: u64,
}

impl ComponentHealth {
    fn update(&mut self, healthy: bool, message: String, at: DateTime<Utc>) {
        self.healthy = healthy;
        self.message = message;
        if healthy {
            self.last_success = Some(at);
            self.success_count += 1;
        } else {
            self.last_failure = Some(at);
            self.failure_count += 1;
        }
    }
}

/// All dependencies.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RouterHealthReport {
    /// The proxy in front of the router.
    pub dataplane: ComponentHealth,
    /// The Kubernetes API server.
    pub k8s_api: ComponentHealth,
    /// The Hivekeep control plane.
    pub hivekeep_api: ComponentHealth,
}

/// Probes the dataplane's readiness endpoint.
pub trait DataplaneProbe: Send + Sync + 'static {
    /// GETs `http://<addr><path>`, returning status and body.
    fn get(&self, addr: &str, path: &str) -> BoxFuture<'static, Result<(u16, String), String>>;
}

/// The real probe, over the loopback dialer.
pub struct HttpProbe;

impl DataplaneProbe for HttpProbe {
    fn get(&self, addr: &str, path: &str) -> BoxFuture<'static, Result<(u16, String), String>> {
        let (addr, path) = (addr.to_string(), path.to_string());
        Box::pin(async move {
            http_request(
                &addr,
                http::Method::GET,
                &path,
                DEPENDENCY_HEALTH_CHECK_TIMEOUT * 2,
            )
            .await
        })
    }
}

/// Asks the Kubernetes API for its version.
pub trait K8sProbe: Send + Sync + 'static {
    /// The API server's `gitVersion`, or `None` when the client cannot
    /// reach the discovery endpoint at all.
    fn version(&self) -> Option<BoxFuture<'static, Result<String, String>>>;
}

impl K8sProbe for kube::Client {
    fn version(&self) -> Option<BoxFuture<'static, Result<String, String>>> {
        let c = self.clone();
        Some(Box::pin(async move {
            c.apiserver_version()
                .await
                .map(|v| v.git_version)
                .map_err(|e| e.to_string())
        }))
    }
}

/// Lists one Actor to prove the control plane answers.
pub trait ApiProbe: Send + Sync + 'static {
    /// One `ListActors` call.
    fn list_actors(&self, req: pb::ListActorsRequest) -> BoxFuture<'static, Result<(), String>>;
}

impl ApiProbe for pb::control_client::ControlClient<tonic::transport::Channel> {
    fn list_actors(&self, req: pb::ListActorsRequest) -> BoxFuture<'static, Result<(), String>> {
        let mut c = self.clone();
        Box::pin(async move {
            pb::control_client::ControlClient::list_actors(&mut c, req)
                .await
                .map(|_| ())
                .map_err(|s| format!("rpc error: code = {:?} desc = {}", s.code(), s.message()))
        })
    }
}

/// Settings of the health loop.
pub struct HealthConfig {
    /// Whether this router serves ingress (and so has a dataplane to check).
    pub serves_ingress: bool,
    /// The dataplane kind.
    pub dataplane: Dataplane,
    /// How often to check.
    pub interval: Duration,
}

/// The dependency health tracker.
pub struct RouterHealth {
    report: RwLock<RouterHealthReport>,
    cfg: HealthConfig,
    k8s: Option<Arc<dyn K8sProbe>>,
    api: Option<Arc<dyn ApiProbe>>,
    dataplane_probe: Arc<dyn DataplaneProbe>,
}

async fn bounded<F: std::future::Future<Output = Result<T, String>>, T>(f: F) -> Result<T, String> {
    tokio::time::timeout(DEPENDENCY_HEALTH_CHECK_TIMEOUT, f)
        .await
        .unwrap_or_else(|_| Err(DEADLINE.into()))
}

impl RouterHealth {
    /// A tracker; a zero interval means one second.
    pub fn new(
        mut cfg: HealthConfig,
        k8s: Option<Arc<dyn K8sProbe>>,
        api: Option<Arc<dyn ApiProbe>>,
    ) -> Self {
        if cfg.interval.is_zero() {
            cfg.interval = Duration::from_secs(1);
        }
        RouterHealth {
            report: RwLock::default(),
            cfg,
            k8s,
            api,
            dataplane_probe: Arc::new(HttpProbe),
        }
    }

    /// Replaces the dataplane probe (tests).
    pub fn with_dataplane_probe(mut self, p: Arc<dyn DataplaneProbe>) -> Self {
        self.dataplane_probe = p;
        self
    }

    /// Checks now and then every interval until `cancel` fires.
    pub async fn run(&self, cancel: CancellationToken) {
        let mut ticker = tokio::time::interval(self.cfg.interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = cancel.cancelled() => return,
            }
            tokio::select! {
                _ = self.check() => {}
                _ = cancel.cancelled() => return,
            }
        }
    }

    /// Checks every dependency concurrently and records the results.
    pub async fn check(&self) {
        tracing::info!("checking health");
        let (dp, k8s, api) =
            tokio::join!(self.check_dataplane(), self.check_k8s(), self.check_api());
        let now = Utc::now();
        for (name, (ok, msg)) in [
            ("router dataplane", &dp),
            ("Kubernetes API", &k8s),
            ("Hivekeep API", &api),
        ] {
            if !ok {
                tracing::error!(msg = %msg, "{name} health check failed");
            }
        }
        let mut r = self.report.write();
        r.dataplane.update(dp.0, dp.1, now);
        r.k8s_api.update(k8s.0, k8s.1, now);
        r.hivekeep_api.update(api.0, api.1, now);
    }

    /// Probes the dataplane (skipped in egress mode).
    pub async fn check_dataplane(&self) -> (bool, String) {
        if !self.cfg.serves_ingress {
            return (true, "Skipped (egress mode)".into());
        }
        let hc = self.cfg.dataplane.health_check();
        match bounded(self.dataplane_probe.get(hc.addr, hc.path)).await {
            Err(e) => (false, e),
            Ok((status, _)) if status != 200 => (false, format!("unexpected status code {status}")),
            Ok((_, body)) if body.trim() != hc.expected_body => (
                false,
                format!("expected {} but got {:?}", hc.expected_body, body.trim()),
            ),
            Ok(_) => (true, hc.expected_body.into()),
        }
    }

    /// Asks the Kubernetes API for its version.
    pub async fn check_k8s(&self) -> (bool, String) {
        let Some(k8s) = &self.k8s else {
            return (true, "Skipped (no Kubernetes client)".into());
        };
        let Some(fut) = k8s.version() else {
            return (
                false,
                "Kubernetes discovery REST client is unavailable".into(),
            );
        };
        match bounded(fut).await {
            Ok(v) => (true, format!("Version: {v}")),
            Err(e) => (false, e),
        }
    }

    /// Lists one Actor.
    pub async fn check_api(&self) -> (bool, String) {
        let Some(api) = &self.api else {
            return (false, "No client".into());
        };
        let req = pb::ListActorsRequest {
            options: Some(pb::ListOptions {
                page_size: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        match bounded(api.list_actors(req)).await {
            Ok(()) => (true, "Connected".into()),
            Err(e) => (false, e),
        }
    }

    /// The latest report (never blocks on a check in progress).
    pub fn report(&self) -> RouterHealthReport {
        self.report.read().clone()
    }
}

/// Whether the router should receive traffic.
#[derive(Debug, Default)]
pub struct Readiness(AtomicBool);

impl Readiness {
    /// Marks the router ready.
    pub fn mark_ready(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
    /// Marks the router not ready (shutdown has begun).
    pub fn mark_not_ready(&self) {
        self.0.store(false, Ordering::SeqCst);
    }
    /// Whether the router is ready.
    pub fn ready(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Serves `/healthz` (always 200 while running) and `/readyz` (200 when
/// `readiness` is set, 503 otherwise) until `cancel` fires.
pub async fn serve_probes(
    listener: TcpListener,
    readiness: Arc<Readiness>,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    loop {
        let (tcp, _) = tokio::select! {
            r = listener.accept() => r?,
            _ = cancel.cancelled() => return Ok(()),
        };
        let readiness = readiness.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let svc =
                hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
                    let ready = readiness.ready();
                    async move {
                        let (code, body) = match req.uri().path() {
                            "/healthz" => (200, "ok"),
                            "/readyz" if ready => (200, "ok"),
                            "/readyz" => (503, "not ready"),
                            _ => (404, "not found"),
                        };
                        let mut r =
                            http::Response::new(Full::new(Bytes::from(format!("{body}\n"))));
                        *r.status_mut() = http::StatusCode::from_u16(code).expect("valid status");
                        Ok::<_, Infallible>(r)
                    }
                });
            let builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
            let conn = builder.serve_connection(TokioIo::new(tcp), svc);
            tokio::select! {
                _ = conn => {}
                _ = cancel.cancelled() => {}
            }
        });
    }
}

#[cfg(test)]
#[path = "health_tests.rs"]
mod tests;
