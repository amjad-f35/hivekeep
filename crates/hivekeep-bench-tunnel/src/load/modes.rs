//! The traffic modes: one execution of a mode against the tunnel, and what
//! it saw.

use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use hivekeep_gate::headers::TARGET_ACTOR_HEADER;
use http::{Request, StatusCode, Uri};
use http_body_util::{BodyExt, Empty};
use hyper_util::client::legacy::connect::{Connected, Connection, HttpConnector};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::Mutex;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::pacer::{Pacer, Samples, Unit};
pub use super::scenarios::Mode;
use crate::httpio::{self, BodyKind};
use crate::report::Load;
use crate::{pki, Error, Result};

/// Where the generator sends traffic and with what credentials.
#[derive(Debug, Clone, Default)]
pub struct Targets {
    pub ingress: String,
    pub connect: String,
    pub egress: String,
    pub egress_discard: String,
    /// The name the tunnel's ingress certificate is checked for.
    pub server_name: String,
    /// The router client certificate.
    pub router_bundle: PathBuf,
    /// Verifies the tunnel.
    pub ca: PathBuf,
    /// `hive/name` values, cycled through the routing header.
    pub actors: Vec<String>,
    /// The port a CONNECT names.
    pub actor_port: u16,
}

/// Runs while a connection mode holds every connection open.
pub type Hook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// Configures one run of a mode.
#[derive(Clone, Default)]
pub struct Options {
    pub mode: Option<Mode>,
    pub rate: f64,
    pub duration: Duration,
    pub warmup: Duration,
    pub workers: usize,
    /// For EgressIdle the count to hold, for EgressStream the count to
    /// stream over.
    pub connections: usize,
    /// Turns keep-alive off for the HTTP modes.
    pub new_connection_per_request: bool,
    pub payload_bytes: usize,
    /// Called by EgressIdle with every connection still open, once the opens
    /// are done and before they are closed.
    pub while_holding: Option<Hook>,
}

impl Options {
    /// Options for `mode`.
    pub fn new(mode: Mode) -> Self {
        Options {
            mode: Some(mode),
            ..Default::default()
        }
    }
}

/// Error samples kept per run.
pub const MAX_ERROR_SAMPLES: usize = 5;

/// The counters of one execution.
#[derive(Default)]
struct Run {
    latency: Samples,
    completed: AtomicU64,
    errors: AtomicU64,
    bytes: AtomicU64,
    next_actor: AtomicUsize,
    error_samples: Mutex<Vec<String>>,
}

impl Run {
    fn fail(&self, err: impl std::fmt::Display) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        let mut s = self.error_samples.lock();
        if s.len() < MAX_ERROR_SAMPLES {
            s.push(err.to_string());
        }
    }

    /// Records a completed unit if it falls in the measured window.
    fn done(&self, u: Unit, n: u64) {
        if !u.measured {
            return;
        }
        self.latency.record(u.due.elapsed());
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(n, Ordering::Relaxed);
    }

    fn actor(&self, targets: &Targets) -> String {
        if targets.actors.is_empty() {
            return String::new();
        }
        targets.actors[self.next_actor.fetch_add(1, Ordering::Relaxed) % targets.actors.len()]
            .clone()
    }
}

/// Runs `opts` against `targets` and summarizes what it saw. Fails only
/// when the run could not start.
pub async fn execute(
    targets: &Targets,
    opts: &Options,
    cancel: &CancellationToken,
) -> Result<Load> {
    let run = Arc::new(Run::default());
    let pacer = Pacer::new(opts.rate, opts.duration, opts.warmup, opts.workers, 10_000);
    let targets = Arc::new(targets.clone());
    match opts.mode.ok_or_else(|| Error::msg("no mode"))? {
        m @ (Mode::IngressH1 | Mode::IngressH2) => {
            ingress(&run, &targets, opts, m == Mode::IngressH2, &pacer, cancel).await?
        }
        Mode::Connect => connect(&run, &targets, &pacer, cancel).await?,
        Mode::EgressHttp => egress_http(&run, &targets, opts, &pacer, cancel).await,
        Mode::EgressIdle => egress_idle(&run, &targets, opts, &pacer, cancel).await,
        Mode::EgressStream => egress_stream(&run, &targets, opts, cancel).await,
    }
    let measured = opts.duration.saturating_sub(opts.warmup).as_secs_f64();
    let mut l = Load {
        offered: opts.rate,
        measured_seconds: measured,
        scheduled: pacer.scheduled.load(Ordering::Relaxed),
        skipped: pacer.skipped.load(Ordering::Relaxed),
        completed: run.completed.load(Ordering::Relaxed),
        errors: run.errors.load(Ordering::Relaxed),
        latency: run.latency.percentiles(),
        send_lag: pacer.lag.percentiles(),
        bytes: run.bytes.load(Ordering::Relaxed),
        error_samples: run.error_samples.lock().clone(),
        ..Default::default()
    };
    if measured > 0.0 {
        l.achieved = l.completed as f64 / measured;
        l.mbytes_per_sec = l.bytes as f64 / measured / 1e6;
    }
    Ok(l)
}

const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

async fn dial(addr: &str) -> io::Result<TcpStream> {
    let conn = tokio::time::timeout(DIAL_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| {
            io::Error::new(io::ErrorKind::TimedOut, format!("dial {addr}: timed out"))
        })??;
    let _ = conn.set_nodelay(true);
    Ok(conn)
}

/// A TLS connection the HTTP client pools, telling it whether HTTP/2 was
/// negotiated.
struct TlsConn(tokio_rustls::client::TlsStream<TcpStream>);

impl Connection for TlsConn {
    fn connected(&self) -> Connected {
        if self.0.get_ref().1.alpn_protocol() == Some(b"h2") {
            Connected::new().negotiated_h2()
        } else {
            Connected::new()
        }
    }
}

impl AsyncRead for TlsConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// The router's side of the tunnel's ingress: mTLS with the router's
/// certificate.
#[derive(Clone)]
struct RouterConnector {
    tls: tokio_rustls::TlsConnector,
    server_name: ServerName<'static>,
}

impl RouterConnector {
    fn new(targets: &Targets, alpn: &[u8]) -> Result<Self> {
        let tls = pki::client_config(&targets.router_bundle, &targets.ca, &[alpn])?;
        let server_name = ServerName::try_from(targets.server_name.clone())
            .map_err(|e| Error::msg(format!("server name {:?}: {e}", targets.server_name)))?;
        Ok(RouterConnector {
            tls: tokio_rustls::TlsConnector::from(tls),
            server_name,
        })
    }

    async fn connect(&self, addr: &str) -> io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
        let tcp = dial(addr).await?;
        self.tls.connect(self.server_name.clone(), tcp).await
    }
}

impl tower::Service<Uri> for RouterConnector {
    type Response = TokioIo<TlsConn>;
    type Error = io::Error;
    type Future = BoxFuture<'static, io::Result<Self::Response>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let me = self.clone();
        Box::pin(async move {
            let addr = uri
                .authority()
                .map(|a| a.to_string())
                .ok_or_else(|| io::Error::other("no authority"))?;
            Ok(TokioIo::new(TlsConn(me.connect(&addr).await?)))
        })
    }
}

fn pool_idle(new_connection_per_request: bool) -> usize {
    if new_connection_per_request {
        0
    } else {
        usize::MAX
    }
}

/// Sends one GET and drains the response.
async fn get<C>(run: &Run, client: &Client<C, Empty<Bytes>>, u: Unit, url: &Uri, actor: &str)
where
    C: hyper_util::client::legacy::connect::Connect + Clone + Send + Sync + 'static,
{
    let mut req = Request::get(url.clone());
    if !actor.is_empty() {
        req = req.header(TARGET_ACTOR_HEADER, actor);
    }
    let req = match req.body(Empty::new()) {
        Ok(r) => r,
        Err(e) => return run.fail(e),
    };
    let exchange = async {
        let resp = client
            .request(req)
            .await
            .map_err(|e| Error::msg(error_chain(&e)))?;
        let status = resp.status();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| Error::msg(e.to_string()))?
            .to_bytes();
        Ok::<_, Error>((status, body.len() as u64))
    };
    match tokio::time::timeout(REQUEST_TIMEOUT, exchange).await {
        Err(_) => run.fail("request timed out"),
        Ok(Err(e)) => run.fail(e),
        Ok(Ok((status, _))) if status != StatusCode::OK => {
            run.fail(format!("HTTP {}", status.as_u16()))
        }
        Ok(Ok((_, n))) => run.done(u, n),
    }
}

fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    hivekeep_gate::tunnel::keeper_dial::error_chain(err)
}

async fn ingress(
    run: &Arc<Run>,
    targets: &Arc<Targets>,
    opts: &Options,
    h2: bool,
    pacer: &Arc<Pacer>,
    cancel: &CancellationToken,
) -> Result<()> {
    let connector = RouterConnector::new(targets, if h2 { b"h2" } else { b"http/1.1" })?;
    let client: Client<_, Empty<Bytes>> = Client::builder(TokioExecutor::new())
        .http2_only(h2)
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(pool_idle(opts.new_connection_per_request))
        .build(connector);
    let url: Uri = format!("https://{}/", targets.ingress)
        .parse()
        .map_err(|e| Error::msg(format!("ingress URL: {e}")))?;
    let (run2, targets) = (run.clone(), targets.clone());
    pacer
        .run(cancel, move |u| {
            let (run, client, url, targets) =
                (run2.clone(), client.clone(), url.clone(), targets.clone());
            async move {
                let actor = run.actor(&targets);
                get(&run, &client, u, &url, &actor).await;
            }
        })
        .await;
    Ok(())
}

/// Costs a TLS handshake, a CONNECT, and the tunnel's dial to the Actor per
/// unit, as a new router tunnel would.
async fn connect(
    run: &Arc<Run>,
    targets: &Arc<Targets>,
    pacer: &Arc<Pacer>,
    cancel: &CancellationToken,
) -> Result<()> {
    let connector = Arc::new(RouterConnector::new(targets, b"http/1.1")?);
    let authority = format!("actor:{}", targets.actor_port);
    let (run2, targets) = (run.clone(), targets.clone());
    pacer
        .run(cancel, move |u| {
            let (run, connector, targets, authority) = (
                run2.clone(),
                connector.clone(),
                targets.clone(),
                authority.clone(),
            );
            async move {
                let actor = run.actor(&targets);
                match tokio::time::timeout(
                    REQUEST_TIMEOUT,
                    connect_once(&connector, &targets.connect, &authority, &actor),
                )
                .await
                {
                    Err(_) => run.fail("CONNECT timed out"),
                    Ok(Err(e)) => run.fail(e),
                    Ok(Ok(n)) => run.done(u, n),
                }
            }
        })
        .await;
    Ok(())
}

async fn connect_once(
    connector: &RouterConnector,
    addr: &str,
    authority: &str,
    actor: &str,
) -> Result<u64> {
    let conn = connector.connect(addr).await?;
    let mut r = BufReader::new(conn);
    r.write_all(format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n{TARGET_ACTOR_HEADER}: {actor}\r\n\r\n").as_bytes())
        .await?;
    r.flush().await?;
    let head = httpio::read_head(&mut r)
        .await?
        .ok_or_else(|| Error::msg("CONNECT: no response"))?;
    if head.status() != Some(200) {
        return Err(Error::msg(format!(
            "CONNECT {}",
            head.status().unwrap_or(0)
        )));
    }
    r.write_all(b"GET / HTTP/1.1\r\nHost: actor\r\nConnection: close\r\n\r\n")
        .await?;
    r.flush().await?;
    let inner = httpio::read_head(&mut r)
        .await?
        .ok_or_else(|| Error::msg("no response inside tunnel"))?;
    let n = httpio::discard_body(&mut r, &inner, BodyKind::Response).await?;
    if inner.status() != Some(200) {
        return Err(Error::msg(format!(
            "HTTP {} inside tunnel",
            inner.status().unwrap_or(0)
        )));
    }
    Ok(n)
}

async fn egress_http(
    run: &Arc<Run>,
    targets: &Arc<Targets>,
    opts: &Options,
    pacer: &Arc<Pacer>,
    cancel: &CancellationToken,
) {
    let mut connector = HttpConnector::new();
    connector.set_connect_timeout(Some(DIAL_TIMEOUT));
    connector.set_nodelay(true);
    let client: Client<_, Empty<Bytes>> = Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(pool_idle(opts.new_connection_per_request))
        .build(connector);
    let Ok(url) = format!("http://{}/", targets.egress).parse::<Uri>() else {
        run.fail(format!("bad egress address {:?}", targets.egress));
        return;
    };
    let run2 = run.clone();
    pacer
        .run(cancel, move |u| {
            let (run, client, url) = (run2.clone(), client.clone(), url.clone());
            async move { get(&run, &client, u, &url, "").await }
        })
        .await;
}

/// Opens up to `connections` connections at the pacer's rate, completes one
/// request on each so its tunnel exists end to end, and holds them until
/// the duration is over. The hook runs as soon as every open has finished,
/// while the connections are still held.
async fn egress_idle(
    run: &Arc<Run>,
    targets: &Arc<Targets>,
    opts: &Options,
    pacer: &Arc<Pacer>,
    cancel: &CancellationToken,
) {
    let held = Arc::new(Mutex::new(Vec::<TcpStream>::new()));
    let end = Instant::now() + opts.duration;
    let wanted = opts.connections;
    let opened = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let all_opened = CancellationToken::new();
    if wanted == 0 {
        all_opened.cancel();
    }
    let hook = {
        let (all_opened, cancel, hook) = (
            all_opened.clone(),
            cancel.clone(),
            opts.while_holding.clone(),
        );
        tokio::spawn(async move {
            tokio::select! {
                _ = all_opened.cancelled() => {}
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep_until(end) => {}
            }
            if let Some(h) = hook {
                h().await;
            }
        })
    };
    let request = Bytes::from(format!(
        "GET / HTTP/1.1\r\nHost: {}\r\n\r\n",
        targets.egress
    ));
    let (run2, targets2, held2) = (run.clone(), targets.clone(), held.clone());
    pacer
        .run(cancel, move |u| {
            let (run, targets, held, request) = (
                run2.clone(),
                targets2.clone(),
                held2.clone(),
                request.clone(),
            );
            let (opened, finished, all_opened) =
                (opened.clone(), finished.clone(), all_opened.clone());
            async move {
                if opened.fetch_add(1, Ordering::Relaxed) >= wanted {
                    return;
                }
                let one = async {
                    let mut conn = dial(&targets.egress).await?;
                    let n = {
                        let mut r = BufReader::new(&mut conn);
                        r.write_all(&request).await?;
                        r.flush().await?;
                        let head = httpio::read_head(&mut r)
                            .await?
                            .ok_or_else(|| Error::msg("no response"))?;
                        httpio::discard_body(&mut r, &head, BodyKind::Response).await?
                    };
                    Ok::<_, Error>((conn, n))
                };
                match tokio::time::timeout(REQUEST_TIMEOUT, one).await {
                    Err(_) => run.fail("open timed out"),
                    Ok(Err(e)) => run.fail(e),
                    Ok(Ok((conn, n))) => {
                        run.done(u, n);
                        held.lock().push(conn);
                    }
                }
                if finished.fetch_add(1, Ordering::Relaxed) + 1 == wanted {
                    all_opened.cancel();
                }
            }
        })
        .await;
    // Hold for whatever is left of the duration, and until the hook is done.
    tokio::select! {
        _ = cancel.cancelled() => {}
        _ = tokio::time::sleep_until(end) => {}
    }
    let _ = hook.await;
    held.lock().clear();
}

/// Writes payloads over `connections` connections for the duration. Each
/// write is a unit, so latency is per write.
async fn egress_stream(
    run: &Arc<Run>,
    targets: &Arc<Targets>,
    opts: &Options,
    cancel: &CancellationToken,
) {
    let payload = Bytes::from(vec![0u8; opts.payload_bytes]);
    let start = Instant::now();
    let (end, measure_from) = (start + opts.duration, start + opts.warmup);
    let mut tasks = Vec::new();
    for _ in 0..opts.connections {
        let (run, targets, payload, cancel) = (
            run.clone(),
            targets.clone(),
            payload.clone(),
            cancel.clone(),
        );
        tasks.push(tokio::spawn(async move {
            let mut conn = match dial(&targets.egress_discard).await {
                Ok(c) => c,
                Err(e) => return run.fail(e),
            };
            while Instant::now() < end && !cancel.is_cancelled() {
                let due = Instant::now();
                match tokio::time::timeout(Duration::from_secs(10), conn.write_all(&payload)).await
                {
                    Err(_) => return run.fail("write timed out"),
                    Ok(Err(e)) => return run.fail(e),
                    Ok(Ok(())) => run.done(
                        Unit {
                            due,
                            measured: due > measure_from,
                        },
                        payload.len() as u64,
                    ),
                }
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
}
