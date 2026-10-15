//! The `/statusz` page: build, ports, flags, dependency health, request
//! parking and the most recent routing decisions, as HTML or JSON.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use http_body_util::Full;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::extproc::{QueryRecorder, RecordedQuery};
use super::health::{ComponentHealth, RouterHealth, RouterHealthReport};
use super::ingress::ParkingStatus;

/// The build identifier shown on the page, set at build time through
/// `HK_BUILD_TAG`.
pub const BUILD_TAG: &str = match option_env!("HK_BUILD_TAG") {
    Some(t) => t,
    None => "dev",
};

/// How long the router Service lookup may take.
const ROUTER_IP_TIMEOUT: Duration = Duration::from_secs(3);

/// Looks up the router Service's cluster IP for display.
pub type RouterIpLookup = Arc<dyn Fn() -> BoxFuture<'static, String> + Send + Sync>;

/// Everything the page reports, gathered at request time.
#[derive(Clone, Default)]
pub struct StatusSource {
    /// `None` when running without a cluster.
    pub router_ip: Option<RouterIpLookup>,
    /// The router's namespace.
    pub namespace: String,
    /// `ingress`, `egress` or `both`.
    pub mode: String,
    /// The dataplane's HTTPS port.
    pub https_port: u16,
    /// The xDS port.
    pub xds_port: u16,
    /// The ext_proc port.
    pub extproc_port: u16,
    /// This page's port.
    pub status_port: u16,
    /// The command line.
    pub args: String,
    /// Every flag and its effective value.
    pub flags: BTreeMap<String, String>,
    /// Recent routing decisions.
    pub queries: Option<Arc<QueryRecorder>>,
    /// Dependency health.
    pub health: Option<Arc<RouterHealth>>,
    /// Request parking, owned by the ingress handler (absent in egress mode).
    pub parking: Option<Arc<dyn Fn() -> ParkingStatus + Send + Sync>>,
}

/// One recorded query, formatted for display.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FormattedQuery {
    /// RFC 3339.
    pub timestamp: String,
    /// The request's authority.
    pub client: String,
    /// The request's host.
    pub host: String,
    /// The path, query stripped.
    pub path: String,
    /// The HTTP method.
    pub method: String,
    /// What the router decided.
    pub action: String,
    /// Where the request went.
    pub target: String,
    /// Routing latency, e.g. `10.00 ms`.
    pub duration: String,
}

impl From<&RecordedQuery> for FormattedQuery {
    fn from(q: &RecordedQuery) -> Self {
        FormattedQuery {
            timestamp: q
                .timestamp
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            client: q.client.clone(),
            host: q.host.clone(),
            path: q.path.clone(),
            method: q.method.clone(),
            action: q.action.clone(),
            target: q.target.clone(),
            duration: format!("{:.2} ms", q.duration.as_micros() as f64 / 1000.0),
        }
    }
}

/// The page's data, also its JSON form.
#[derive(Debug, Clone, Serialize)]
pub struct DashboardContext {
    /// Build identifier.
    pub build_tag: String,
    /// The router Service's cluster IP, or why it is unknown.
    pub router_cluster_ip: String,
    /// The router's namespace.
    pub namespace: String,
    /// Serving mode.
    pub mode: String,
    /// Dataplane HTTPS port.
    #[serde(rename = "port_https")]
    pub https_port: u16,
    /// xDS port.
    #[serde(rename = "port_xds")]
    pub xds_port: u16,
    /// ext_proc port.
    #[serde(rename = "port_extproc")]
    pub extproc_port: u16,
    /// This page's port.
    pub status_port: u16,
    /// The command line.
    pub args: String,
    /// Flags and values.
    pub flags: BTreeMap<String, String>,
    /// Recent routing decisions.
    pub queries: Vec<FormattedQuery>,
    /// Dependency health.
    pub health: RouterHealthReport,
    /// Request parking.
    pub parking: ParkingStatus,
}

impl StatusSource {
    /// Snapshots everything the page shows.
    pub async fn dashboard(&self) -> DashboardContext {
        let router_cluster_ip = match &self.router_ip {
            None => "Offline Mode (No Cluster IP)".to_string(),
            Some(lookup) => tokio::time::timeout(ROUTER_IP_TIMEOUT, lookup())
                .await
                .unwrap_or_else(|_| "Lookup Failed: context deadline exceeded".into()),
        };
        let queries = self.queries.as_ref().map(|r| r.get()).unwrap_or_default();
        DashboardContext {
            build_tag: BUILD_TAG.to_string(),
            router_cluster_ip,
            namespace: self.namespace.clone(),
            mode: self.mode.clone(),
            https_port: self.https_port,
            xds_port: self.xds_port,
            extproc_port: self.extproc_port,
            status_port: self.status_port,
            args: self.args.clone(),
            flags: self.flags.clone(),
            queries: queries.iter().map(FormattedQuery::from).collect(),
            health: self.health.as_ref().map(|h| h.report()).unwrap_or_default(),
            parking: self.parking.as_ref().map_or_else(
                || ParkingStatus {
                    enabled: false,
                    active: 0,
                    max_parked: 0,
                    max_wait: String::new(),
                },
                |p| p(),
            ),
        }
    }
}

/// A [`RouterIpLookup`] reading the cluster IP of Service `name` in `namespace`.
pub fn kube_router_ip(client: kube::Client, namespace: String, name: String) -> RouterIpLookup {
    Arc::new(move || {
        let api: kube::Api<k8s_openapi::api::core::v1::Service> =
            kube::Api::namespaced(client.clone(), &namespace);
        let name = name.clone();
        Box::pin(async move {
            match api.get(&name).await {
                Err(e) => format!("Lookup Failed: {e}"),
                Ok(svc) => match svc.spec.and_then(|s| s.cluster_ip) {
                    Some(ip) if !ip.is_empty() && ip != "None" => ip,
                    _ => "ClusterIP Unassigned".into(),
                },
            }
        })
    })
}

/// Binds the status listener on `addr:port`.
pub async fn listen_status(addr: &str, port: u16) -> Result<TcpListener, String> {
    let joined = if addr.contains(':') {
        format!("[{addr}]:{port}")
    } else {
        format!("{addr}:{port}")
    };
    TcpListener::bind(&joined)
        .await
        .map_err(|e| format!("failed binding Router HTTP status server address {joined:?}: {e}"))
}

/// Answers one `/statusz` request: JSON when the client accepts it or asks
/// with `?format=json`, HTML otherwise.
pub async fn handle_statusz(
    source: &StatusSource,
    accept: &str,
    query: Option<&str>,
) -> http::Response<Full<Bytes>> {
    let data = source.dashboard().await;
    let wants_json = accept.contains("application/json")
        || url::form_urlencoded::parse(query.unwrap_or("").as_bytes())
            .any(|(k, v)| k == "format" && v == "json");
    let (content_type, mut body) = if wants_json {
        (
            "application/json",
            serde_json::to_vec(&data).expect("dashboard serializes"),
        )
    } else {
        ("text/html; charset=utf-8", render_html(&data).into_bytes())
    };
    if wants_json {
        body.push(b'\n');
    }
    http::Response::builder()
        .header(http::header::CONTENT_TYPE, content_type)
        .body(Full::new(Bytes::from(body)))
        .expect("valid response")
}

/// Serves `/statusz` on `listener` until `cancel` fires.
pub async fn serve_status(
    listener: TcpListener,
    source: Arc<StatusSource>,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    loop {
        let (tcp, _): (_, SocketAddr) = tokio::select! {
            r = listener.accept() => r?,
            _ = cancel.cancelled() => return Ok(()),
        };
        let source = source.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let svc =
                hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
                    let source = source.clone();
                    async move {
                        if req.uri().path() != "/statusz" {
                            let mut r = http::Response::new(Full::new(Bytes::from_static(
                                b"404 page not found\n",
                            )));
                            *r.status_mut() = http::StatusCode::NOT_FOUND;
                            return Ok::<_, Infallible>(r);
                        }
                        let accept = req
                            .headers()
                            .get(http::header::ACCEPT)
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("");
                        Ok(handle_statusz(&source, accept, req.uri().query()).await)
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

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

fn row(out: &mut String, label: &str, value: &str) {
    let _ = write!(
        out,
        r#"<div class="row"><span class="label">{}</span><span class="value">{}</span></div>"#,
        esc(label),
        esc(value)
    );
}

fn badge(ok: bool, yes: &str, no: &str) -> String {
    if ok {
        format!(r#"<span class="badge ok">{yes}</span>"#)
    } else {
        format!(r#"<span class="badge bad">{no}</span>"#)
    }
}

fn component(out: &mut String, name: &str, c: &ComponentHealth) {
    let lkg = c
        .last_success
        .map_or_else(|| "N/A".to_string(), |t| t.format("%H:%M:%S").to_string());
    let _ = write!(
        out,
        r#"<div class="row"><span class="label">{}</span>{}</div><div class="row small"><span>{}</span></div><div class="row small"><span>Ok: {} | Err: {}</span><span>LKG: {}</span></div>"#,
        esc(name),
        badge(c.healthy, "Healthy", "Degraded"),
        esc(&c.message),
        c.success_count,
        c.failure_count,
        lkg,
    );
}

const STYLE: &str = "body{font-family:system-ui,sans-serif;margin:0;padding:1.5rem;background:#0f172a;color:#e2e8f0}\
header{display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem}\
h1{font-size:1.4rem;margin:0}h2{font-size:1.1rem}\
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:1rem}\
.card{background:#1e293b;border-radius:8px;padding:1rem}.wide{grid-column:1/-1}\
.title{font-weight:600;margin-bottom:.5rem}.row{display:flex;justify-content:space-between;gap:1rem;padding:.2rem 0}\
.small{font-size:.8rem;color:#94a3b8}.label{color:#94a3b8}\
.badge{background:#334155;border-radius:4px;padding:.1rem .4rem;font-size:.85rem}\
.ok{color:#10b981}.bad{color:#ef4444}.mono{font-family:monospace;word-break:break-all}\
table{width:100%;border-collapse:collapse;font-size:.85rem}th,td{text-align:left;padding:.4rem;border-bottom:1px solid #334155}\
input{padding:.3rem;border-radius:4px;border:1px solid #334155;background:#0f172a;color:inherit}";

const SCRIPT: &str = "function filterTable(){var f=document.getElementById('query-filter').value.toUpperCase();\
document.querySelectorAll('#queries-table tbody tr').forEach(function(r){r.style.display=r.textContent.toUpperCase().indexOf(f)>-1?'':'none';});}";

/// Renders the dashboard page.
pub fn render_html(d: &DashboardContext) -> String {
    let mut o = String::with_capacity(8192);
    let _ = write!(
        o,
        r#"<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="10"><title>gate Router Status Dashboard</title><style>{STYLE}</style></head><body><header><h1>gate Router Status</h1><span class="badge">Build: {}</span></header><div class="grid">"#,
        esc(&d.build_tag)
    );

    o.push_str(r#"<div class="card"><div class="title">Infrastructure details</div>"#);
    row(&mut o, "Router Service IP", &d.router_cluster_ip);
    row(&mut o, "Namespace Context", &d.namespace);
    row(&mut o, "Mode", &d.mode);
    o.push_str("</div>");

    o.push_str(r#"<div class="card"><div class="title">Component Network Allocation</div>"#);
    row(
        &mut o,
        "Workload Port (HTTPS Dataplane)",
        &d.https_port.to_string(),
    );
    row(&mut o, "xDS Interface Port", &d.xds_port.to_string());
    row(
        &mut o,
        "External Processing Port",
        &d.extproc_port.to_string(),
    );
    row(&mut o, "HTTP Status Port", &d.status_port.to_string());
    o.push_str("</div>");

    let _ = write!(
        o,
        r#"<div class="card"><div class="title">Request Parking</div><div class="row"><span class="label">Status</span>{}</div>"#,
        badge(d.parking.enabled, "Enabled", "Disabled")
    );
    row(
        &mut o,
        "Currently Parked",
        &format!("{} / {}", d.parking.active, d.parking.max_parked),
    );
    row(&mut o, "Max Wait", &d.parking.max_wait);
    o.push_str("</div>");

    o.push_str(r#"<div class="card"><div class="title">System Component Health Checks</div>"#);
    component(&mut o, "Dataplane Health", &d.health.dataplane);
    component(&mut o, "Kubernetes API", &d.health.k8s_api);
    component(&mut o, "Hivekeep API gRPC", &d.health.hivekeep_api);
    o.push_str("</div>");

    let _ = write!(
        o,
        r#"<div class="card wide"><div class="title">Global Arguments &amp; Configuration</div><div class="label">CLI Arguments</div><div class="badge mono">{}</div><div class="label">Command Flags</div><div>"#,
        esc(&d.args)
    );
    for (k, v) in &d.flags {
        let _ = write!(
            o,
            r#"<span class="badge mono">{}={}</span> "#,
            esc(k),
            esc(v)
        );
    }
    o.push_str("</div></div></div>");

    o.push_str(
        r#"<h2>Recent Routing Decisions</h2><input type="text" id="query-filter" onkeyup="filterTable()" placeholder="Filter by path, host, client, target..."><table id="queries-table"><thead><tr><th>Timestamp</th><th>Client / Authority</th><th>Path / Route</th><th>Method</th><th>Matching Action Result</th><th>Selected Target IP</th><th>Latency</th></tr></thead><tbody>"#,
    );
    if d.queries.is_empty() {
        o.push_str(r#"<tr><td colspan="7">No queries have been processed yet.</td></tr>"#);
    }
    for q in &d.queries {
        let _ = write!(
            o,
            r#"<tr><td>{}</td><td>{}</td><td class="mono">{}</td><td><span class="badge">{}</span></td><td>{}</td><td><span class="badge ok">{}</span></td><td>{}</td></tr>"#,
            esc(&q.timestamp),
            esc(&q.client),
            esc(&q.path),
            esc(&q.method),
            esc(&q.action),
            esc(&q.target),
            esc(&q.duration),
        );
    }
    let _ = write!(o, "</tbody></table><script>{SCRIPT}</script></body></html>");
    o
}

#[cfg(test)]
#[path = "status_tests.rs"]
mod tests;
