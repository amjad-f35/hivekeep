//! The worker ingress server and its per-Actor activations.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use hivekeep_core::names::is_valid_resource_name;
use hivekeep_core::resource::ActorRef;
use http::header::{self, HeaderMap, HeaderValue};
use http::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::relay::relay_with_half_close;
use super::tls::IngressTls;
use super::transport::{boxed, full, ProtocolMirrorTransport, ProxyBody, Transport};
use super::{parse_port, STALE_ASSIGNMENT_HEADER, TARGET_ACTOR_HEADER, TARGET_PORT_HEADER};
use crate::headers::parse_target_actor;
use crate::tunnel::egress::BoxIo;

/// Dials `host:port` inside an Actor's network namespace.
pub type Dial = Arc<dyn Fn(String) -> BoxFuture<'static, io::Result<BoxIo>> + Send + Sync>;

/// How long dialing the Actor for a CONNECT may take.
const CONNECT_DIAL_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a client may take to send request headers.
const READ_HEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// Headers that apply to one hop only.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Worker ingress configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// The worker's serving credential bundle.
    pub credential_bundle_path: PathBuf,
    /// CAs the router's client certificate chains to.
    pub trust_bundle_path: PathBuf,
    /// The router's SPIFFE ID.
    pub allowed_client_id: String,
    /// Where Actor requests go inside its namespace (`http://host:port`).
    pub upstream: url::Url,
}

type TransportFactory = Box<dyn Fn(Dial) -> Arc<dyn Transport> + Send + Sync>;

struct Activation {
    uid: String,
    cancel: CancellationToken,
    tracker: TaskTracker,
    dial: Dial,
    transport: Mutex<Arc<dyn Transport>>,
}

struct Inner {
    tls: IngressTls,
    scheme: String,
    host: String,
    port: u16,
    new_transport: TransportFactory,
    active: Mutex<HashMap<ActorRef, Arc<Activation>>>,
}

/// The worker ingress server.
#[derive(Clone)]
pub struct Server {
    inner: Arc<Inner>,
}

fn text(status: StatusCode, body: &str) -> Response<ProxyBody> {
    let mut r = Response::new(full(format!("{body}\n")));
    *r.status_mut() = status;
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    r
}

fn reject() -> Response<ProxyBody> {
    let mut r = text(StatusCode::MISDIRECTED_REQUEST, "misdirected request");
    r.headers_mut()
        .insert(STALE_ASSIGNMENT_HEADER, HeaderValue::from_static("true"));
    r
}

fn canceled() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "ingress: actor deactivated")
}

/// Wraps `dial` so it fails once the activation ends, closing a connection
/// that completes after that.
pub fn activation_dialer(cancel: CancellationToken, dial: Dial) -> Dial {
    Arc::new(move |addr: String| {
        let (cancel, dial) = (cancel.clone(), dial.clone());
        Box::pin(async move {
            if cancel.is_cancelled() {
                return Err(canceled());
            }
            let r = tokio::select! {
                r = dial(addr) => r,
                _ = cancel.cancelled() => return Err(canceled()),
            };
            if cancel.is_cancelled() {
                drop(r);
                return Err(canceled());
            }
            r
        })
    })
}

fn split_port(authority: &str) -> Option<&str> {
    if let Some(rest) = authority.strip_prefix('[') {
        return rest.split_once("]:").map(|(_, p)| p);
    }
    authority.rsplit_once(':').map(|(_, p)| p)
}

fn is_upgrade(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| {
            v.split(',')
                .any(|t| t.trim().eq_ignore_ascii_case("upgrade"))
        })
        && headers.contains_key(header::UPGRADE)
}

fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let named: Vec<String> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(',').map(|t| t.trim().to_ascii_lowercase()))
        .collect();
    for h in HOP_BY_HOP
        .iter()
        .copied()
        .chain(named.iter().map(String::as_str))
    {
        headers.remove(h);
    }
}

impl Server {
    /// A server for `cfg`; the credential and trust bundles must load.
    pub fn new(cfg: Config) -> Result<Server, String> {
        Self::with_transport(
            cfg,
            Box::new(|dial| Arc::new(ProtocolMirrorTransport::new(Some(dial)))),
        )
    }

    fn with_transport(cfg: Config, new_transport: TransportFactory) -> Result<Server, String> {
        if cfg.credential_bundle_path.as_os_str().is_empty() {
            return Err("ingress: credential bundle path is required".into());
        }
        if cfg.trust_bundle_path.as_os_str().is_empty() {
            return Err("ingress: trust bundle path is required".into());
        }
        if cfg.allowed_client_id.is_empty() {
            return Err("ingress: allowed client identity is required".into());
        }
        let host = cfg.upstream.host_str().unwrap_or_default().to_string();
        if cfg.upstream.scheme().is_empty() || host.is_empty() {
            return Err("ingress: upstream URL is required".into());
        }
        let port = cfg.upstream.port_or_known_default().unwrap_or(80);
        let tls = IngressTls {
            credential_bundle: cfg.credential_bundle_path,
            trust_bundle: cfg.trust_bundle_path,
            allowed_client_id: cfg.allowed_client_id,
        };
        tls.server_config(&[])?;
        Ok(Server {
            inner: Arc::new(Inner {
                tls,
                scheme: cfg.upstream.scheme().to_string(),
                host,
                port,
                new_transport,
                active: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// The TLS settings (for handshake tests and serving).
    pub fn tls(&self) -> &IngressTls {
        &self.inner.tls
    }

    /// Starts routing requests for `hive/name` (incarnation `uid`) through
    /// `dial`. A different incarnation of the same Actor replaces the old
    /// one; the same incarnation twice is an error.
    pub fn activate(&self, hive: &str, name: &str, uid: &str, dial: Dial) -> Result<(), String> {
        if !is_valid_resource_name(hive) || !is_valid_resource_name(name) {
            return Err(format!(
                "ingress: invalid actor reference {hive:?}/{name:?}"
            ));
        }
        let r = ActorRef::new(hive, name);
        let mut active = self.inner.active.lock();
        if let Some(old) = active.get(&r) {
            if old.uid == uid {
                return Err(format!("ingress: actor {r} is already active"));
            }
            old.cancel.cancel();
            old.transport.lock().close_idle();
        }
        let cancel = CancellationToken::new();
        let dial = activation_dialer(cancel.clone(), dial);
        let transport = (self.inner.new_transport)(dial.clone());
        active.insert(
            r,
            Arc::new(Activation {
                uid: uid.to_string(),
                cancel,
                tracker: TaskTracker::new(),
                dial,
                transport: Mutex::new(transport),
            }),
        );
        Ok(())
    }

    /// Stops routing for `hive/name` if `uid` is its active incarnation,
    /// canceling in-flight requests and tunnels and waiting for them.
    pub async fn deactivate(&self, hive: &str, name: &str, uid: &str) {
        let r = ActorRef::new(hive, name);
        let active = {
            let mut map = self.inner.active.lock();
            match map.get(&r) {
                Some(a) if a.uid == uid => {
                    let a = map.remove(&r).expect("present");
                    a.cancel.cancel();
                    Some(a)
                }
                _ => None,
            }
        };
        let Some(active) = active else {
            return;
        };
        active.tracker.close();
        active.tracker.wait().await;
        active.transport.lock().close_idle();
    }

    /// Whether `hive/name` is active, and with which incarnation.
    pub fn active_uid(&self, hive: &str, name: &str) -> Option<String> {
        self.inner
            .active
            .lock()
            .get(&ActorRef::new(hive, name))
            .map(|a| a.uid.clone())
    }

    /// Replaces the transport of an active Actor.
    pub fn set_transport(&self, hive: &str, name: &str, t: Arc<dyn Transport>) -> bool {
        match self.inner.active.lock().get(&ActorRef::new(hive, name)) {
            Some(a) => {
                *a.transport.lock() = t;
                true
            }
            None => false,
        }
    }

    fn authorize(&self, headers: &HeaderMap) -> Option<Arc<Activation>> {
        let mut values = headers.get_all(TARGET_ACTOR_HEADER).iter();
        let value = values.next()?;
        if values.next().is_some() {
            return None;
        }
        let r = parse_target_actor(value.to_str().ok()?).ok()?;
        self.inner.active.lock().get(&r).cloned()
    }

    /// Proxies one request to its Actor.
    pub async fn handle_http<B>(
        &self,
        mut req: Request<B>,
        remote: Option<SocketAddr>,
    ) -> Response<ProxyBody>
    where
        B: hyper::body::Body<Data = bytes::Bytes> + Send + 'static,
        B::Error: Into<super::transport::BoxError>,
    {
        let Some(active) = self.authorize(req.headers()) else {
            return reject();
        };
        let in_host = req
            .headers()
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .map(str::to_string)
            .or_else(|| req.uri().authority().map(|a| a.to_string()))
            .unwrap_or_default();
        let port = req
            .headers()
            .get(TARGET_PORT_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_port)
            .unwrap_or(self.inner.port);
        let path = req
            .uri()
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/")
            .to_string();
        let host = if self.inner.host.contains(':') {
            format!("[{}]", self.inner.host)
        } else {
            self.inner.host.clone()
        };
        let Ok(uri) = format!("{}://{host}:{port}{path}", self.inner.scheme).parse::<http::Uri>()
        else {
            return text(StatusCode::BAD_GATEWAY, "bad gateway");
        };
        let upgrade = is_upgrade(req.headers());
        let upgrade_value = req.headers().get(header::UPGRADE).cloned();
        let client_upgrade = upgrade.then(|| hyper::upgrade::on(&mut req));
        let (mut parts, body) = req.into_parts();
        parts.uri = uri;
        let headers = &mut parts.headers;
        strip_hop_by_hop(headers);
        headers.remove(TARGET_PORT_HEADER);
        headers.remove(TARGET_ACTOR_HEADER);
        headers.remove(header::HOST);
        if !in_host.is_empty() {
            if let Ok(v) = HeaderValue::from_str(&in_host) {
                headers.insert(header::HOST, v.clone());
                headers.insert("x-forwarded-host", v);
            }
        }
        if let Some(remote) = remote {
            let prior = headers
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
                .map(|p| format!("{p}, {}", remote.ip()))
                .unwrap_or_else(|| remote.ip().to_string());
            if let Ok(v) = HeaderValue::from_str(&prior) {
                headers.insert("x-forwarded-for", v);
            }
        }
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        if let (true, Some(u)) = (upgrade, upgrade_value) {
            headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
            headers.insert(header::UPGRADE, u);
        }
        let out = Request::from_parts(parts, boxed(body));
        let transport = active.transport.lock().clone();
        let cancel = active.cancel.clone();
        let tracker = active.tracker.clone();
        let fut = transport.round_trip(out);
        let result = tracker
            .track_future(async move {
                tokio::select! {
                    r = fut => Some(r),
                    _ = cancel.cancelled() => None,
                }
            })
            .await;
        let mut resp = match result {
            Some(Ok(r)) => r,
            Some(Err(err)) => {
                tracing::warn!(%err, "tunnel upstream request failed");
                return text(StatusCode::BAD_GATEWAY, "bad gateway");
            }
            None => return text(StatusCode::BAD_GATEWAY, "bad gateway"),
        };
        resp.headers_mut().remove(STALE_ASSIGNMENT_HEADER);
        if resp.status() == StatusCode::SWITCHING_PROTOCOLS {
            if let Some(client_upgrade) = client_upgrade {
                let upstream_upgrade = hyper::upgrade::on(&mut resp);
                let cancel = active.cancel.clone();
                active.tracker.spawn(async move {
                    let (Ok(client), Ok(upstream)) = (client_upgrade.await, upstream_upgrade.await)
                    else {
                        return;
                    };
                    let mut client = TokioIo::new(client);
                    let mut upstream = TokioIo::new(upstream);
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut client, &mut upstream) => {}
                        _ = cancel.cancelled() => {}
                    }
                });
            }
            return resp;
        }
        strip_hop_by_hop(resp.headers_mut());
        resp
    }

    /// Opens a CONNECT tunnel to a port of the Actor.
    pub async fn handle_connect<B>(&self, mut req: Request<B>) -> Response<ProxyBody> {
        if req.method() != Method::CONNECT {
            return text(StatusCode::METHOD_NOT_ALLOWED, "CONNECT required");
        }
        let Some(active) = self.authorize(req.headers()) else {
            tracing::debug!("tunnel CONNECT rejected");
            return reject();
        };
        let authority = req
            .uri()
            .authority()
            .map(|a| a.to_string())
            .or_else(|| {
                req.headers()
                    .get(header::HOST)
                    .and_then(|h| h.to_str().ok())
                    .map(str::to_string)
            })
            .unwrap_or_default();
        let Some(port) = split_port(&authority) else {
            return text(
                StatusCode::BAD_REQUEST,
                "CONNECT authority must include a port",
            );
        };
        let Some(port) = parse_port(port) else {
            return text(StatusCode::BAD_REQUEST, "invalid CONNECT port");
        };
        let host = if self.inner.host.contains(':') {
            format!("[{}]:{port}", self.inner.host)
        } else {
            format!("{}:{port}", self.inner.host)
        };
        let upstream = match tokio::time::timeout(CONNECT_DIAL_TIMEOUT, (active.dial)(host)).await {
            Ok(Ok(u)) => u,
            Ok(Err(err)) => {
                tracing::warn!(port, %err, "tunnel CONNECT upstream failed");
                return text(StatusCode::BAD_GATEWAY, "bad gateway");
            }
            Err(_) => return text(StatusCode::BAD_GATEWAY, "bad gateway"),
        };
        let on_upgrade = hyper::upgrade::on(&mut req);
        let cancel = active.cancel.clone();
        active.tracker.spawn(async move {
            match on_upgrade.await {
                Ok(downstream) => {
                    relay_with_half_close(&cancel, upstream, TokioIo::new(downstream)).await;
                }
                Err(err) => tracing::debug!(%err, "tunnel CONNECT upgrade failed"),
            }
        });
        Response::new(super::transport::empty())
    }

    /// Serves proxied requests on `listener` until `cancel` fires.
    pub async fn serve(&self, listener: TcpListener, cancel: CancellationToken) -> io::Result<()> {
        self.serve_mode(listener, cancel, false).await
    }

    /// Serves CONNECT tunnels on `listener` until `cancel` fires.
    pub async fn serve_connect(
        &self,
        listener: TcpListener,
        cancel: CancellationToken,
    ) -> io::Result<()> {
        self.serve_mode(listener, cancel, true).await
    }

    async fn serve_mode(
        &self,
        listener: TcpListener,
        cancel: CancellationToken,
        connect: bool,
    ) -> io::Result<()> {
        let conns = TaskTracker::new();
        loop {
            let (tcp, remote) = tokio::select! {
                r = listener.accept() => r?,
                _ = cancel.cancelled() => break,
            };
            let this = self.clone();
            let cancel = cancel.clone();
            conns.spawn(async move {
                let cfg = match this.inner.tls.server_config(&[b"h2", b"http/1.1"]) {
                    Ok(c) => c,
                    Err(err) => {
                        tracing::warn!(%err, "ingress: TLS configuration");
                        return;
                    }
                };
                let tls = tokio::select! {
                    r = tokio_rustls::TlsAcceptor::from(cfg).accept(tcp) => match r {
                        Ok(t) => t,
                        Err(err) => {
                            tracing::debug!(%err, "ingress: TLS handshake failed");
                            return;
                        }
                    },
                    _ = cancel.cancelled() => return,
                };
                let svc = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                    let this = this.clone();
                    async move {
                        let resp = if connect {
                            this.handle_connect(req).await
                        } else {
                            this.handle_http(req, Some(remote)).await
                        };
                        Ok::<_, std::convert::Infallible>(resp)
                    }
                });
                let mut builder =
                    hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
                builder
                    .http1()
                    .timer(TokioTimer::new())
                    .header_read_timeout(READ_HEADER_TIMEOUT);
                builder.http2().enable_connect_protocol();
                let conn = builder.serve_connection_with_upgrades(TokioIo::new(tls), svc);
                tokio::select! {
                    _ = conn => {}
                    _ = cancel.cancelled() => {}
                }
            });
        }
        conns.close();
        conns.wait().await;
        Ok(())
    }
}

#[cfg(test)]
impl Server {
    pub(crate) fn activation_cancelled(&self, hive: &str, name: &str) -> Option<CancellationToken> {
        self.inner
            .active
            .lock()
            .get(&ActorRef::new(hive, name))
            .map(|a| a.cancel.clone())
    }
}
