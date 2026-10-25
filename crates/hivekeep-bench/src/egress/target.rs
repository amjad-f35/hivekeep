//! The endpoint every tunnel of the egress scale test leads to: answers each
//! request with a small fixed body, over plain HTTP and, given a
//! certificate, HTTPS. One Deployment of it sits behind all of the test's
//! Services; `--listen-ports` and `--tls-listen-ports` give each Service its
//! own backend port, since Services on distinct ClusterIPs that share one
//! backend port can hand the target two connections with the same 4-tuple.

use std::convert::Infallible;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// The target subcommand's options.
#[derive(Debug, Clone, clap::Args)]
pub struct TargetOptions {
    /// Address to serve plain HTTP on.
    #[arg(long, default_value = ":8080")]
    pub listen: String,
    /// Also serve plain HTTP on each port of this range, A-B.
    #[arg(long, default_value = "")]
    pub listen_ports: String,
    /// Address to serve HTTPS on; empty serves no HTTPS.
    #[arg(long, default_value = "")]
    pub tls_listen: String,
    /// Also serve HTTPS on each port of this range, A-B.
    #[arg(long, default_value = "")]
    pub tls_listen_ports: String,
    /// PEM certificate to serve HTTPS with.
    #[arg(long, default_value = "")]
    pub tls_cert_file: String,
    /// PEM private key of --tls-cert-file.
    #[arg(long, default_value = "")]
    pub tls_key_file: String,
    /// Size of the body every request gets back.
    #[arg(long, default_value_t = 16)]
    pub response_bytes: usize,
}

/// One answer: status, content type, body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    /// HTTP status.
    pub status: u16,
    /// Content-Type header.
    pub content_type: &'static str,
    /// Body.
    pub body: Bytes,
}

/// The target's routes: `GET /healthz` answers `ok`, every other GET the
/// fixed body.
#[derive(Debug, Clone)]
pub struct Handler {
    body: Bytes,
}

impl Handler {
    /// A handler answering `response_bytes` bytes.
    pub fn new(response_bytes: usize) -> Self {
        Handler {
            body: Bytes::from(vec![b'x'; response_bytes]),
        }
    }

    /// Answers `method path`.
    pub fn reply(&self, method: &str, path: &str) -> Reply {
        if method != "GET" && method != "HEAD" {
            return Reply {
                status: 405,
                content_type: "text/plain; charset=utf-8",
                body: Bytes::from_static(b"Method Not Allowed\n"),
            };
        }
        if path == "/healthz" {
            return Reply {
                status: 200,
                content_type: "text/plain; charset=utf-8",
                body: Bytes::from_static(b"ok\n"),
            };
        }
        Reply {
            status: 200,
            content_type: "text/plain",
            body: self.body.clone(),
        }
    }
}

/// A server configuration serving the certificate in `cert_file` with the
/// key in `key_file`, TLS 1.2 and up.
pub fn tls_config(cert_file: &Path, key_file: &Path) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    let load = || -> anyhow::Result<Arc<rustls::ServerConfig>> {
        let cert = std::fs::read(cert_file)?;
        let key = std::fs::read(key_file)?;
        let chain = rustls_pemfile::certs(&mut &cert[..]).collect::<Result<Vec<_>, _>>()?;
        let key = rustls_pemfile::private_key(&mut &key[..])?
            .ok_or_else(|| anyhow::anyhow!("no private key"))?;
        let mut cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS12, &rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(chain, key)?;
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Arc::new(cfg))
    };
    load().map_err(|e| anyhow::anyhow!("loading TLS certificate: {e}"))
}

/// Parses `A-B` or `A` into the ports A through B; `""` is none.
pub fn port_range(s: &str) -> Result<Vec<u16>, String> {
    if s.is_empty() {
        return Ok(Vec::new());
    }
    let (lo, hi) = s.split_once('-').unwrap_or((s, s));
    let bad = || format!("{s:?} is not a port or a port range A-B");
    let a: i64 = lo.parse().map_err(|_| bad())?;
    let b: i64 = hi.parse().map_err(|_| bad())?;
    if a < 1 || b > 65535 || a > b {
        return Err(bad());
    }
    Ok((a..=b).map(|p| p as u16).collect())
}

/// `addr`, when set, followed by every port of `ports` on the wildcard
/// address.
pub fn listen_addrs(addr: &str, ports: &str) -> Result<Vec<String>, String> {
    let ps = port_range(ports)?;
    let mut addrs = Vec::new();
    if !addr.is_empty() {
        addrs.push(addr.to_string());
    }
    addrs.extend(ps.into_iter().map(|p| format!(":{p}")));
    Ok(addrs)
}

/// Listens on every address, or on none: on an error what was opened is
/// closed.
pub async fn listen_all(addrs: &[String]) -> anyhow::Result<Vec<TcpListener>> {
    let mut lns = Vec::with_capacity(addrs.len());
    for a in addrs {
        match TcpListener::bind(crate::boot::normalize_addr(a)).await {
            Ok(l) => lns.push(l),
            Err(e) => anyhow::bail!("listening on {a}: {e}"),
        }
    }
    Ok(lns)
}

/// Serves `handler` on one accepted connection, over TLS when `tls` is set.
async fn serve_conn<S>(stream: S, handler: Handler)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
        let r = handler.reply(req.method().as_str(), req.uri().path());
        async move {
            let mut resp = hyper::Response::new(Full::new(r.body));
            *resp.status_mut() =
                hyper::StatusCode::from_u16(r.status).unwrap_or(hyper::StatusCode::OK);
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(r.content_type),
            );
            Ok::<_, Infallible>(resp)
        }
    });
    let _ = hyper::server::conn::http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(10))
        .serve_connection(TokioIo::new(stream), svc)
        .await;
}

/// Accepts on `ln` until it fails, serving each connection.
pub async fn serve_listener(
    ln: TcpListener,
    handler: Handler,
    tls: Option<TlsAcceptor>,
) -> std::io::Error {
    loop {
        let (stream, _) = match ln.accept().await {
            Ok(c) => c,
            Err(e) => return e,
        };
        let handler = handler.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            match tls {
                Some(acc) => {
                    if let Ok(s) = acc.accept(stream).await {
                        serve_conn(s, handler).await;
                    }
                }
                None => serve_conn(stream, handler).await,
            }
        });
    }
}

/// Serves until SIGINT/SIGTERM or until a listener fails.
pub async fn run(o: TargetOptions) -> anyhow::Result<()> {
    let handler = Handler::new(o.response_bytes);
    let plain = listen_all(&listen_addrs(&o.listen, &o.listen_ports).map_err(anyhow::Error::msg)?)
        .await
        .map_err(|e| anyhow::anyhow!("opening HTTP listeners: {e}"))?;
    let mut secure = Vec::new();
    let mut acceptor = None;
    if !o.tls_listen.is_empty() || !o.tls_listen_ports.is_empty() {
        acceptor = Some(TlsAcceptor::from(tls_config(
            Path::new(&o.tls_cert_file),
            Path::new(&o.tls_key_file),
        )?));
        secure = listen_all(
            &listen_addrs(&o.tls_listen, &o.tls_listen_ports).map_err(anyhow::Error::msg)?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("opening HTTPS listeners: {e}"))?;
    }
    tracing::info!(
        http_ports = plain.len(),
        https_ports = secure.len(),
        "egress-bench target serving"
    );
    // The first listener to fail stops them all, as the signal does.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<anyhow::Error>(plain.len() + secure.len() + 1);
    let mut tasks = Vec::new();
    for (ln, tls) in plain
        .into_iter()
        .map(|l| (l, None))
        .chain(secure.into_iter().map(|l| (l, acceptor.clone())))
    {
        let tx = tx.clone();
        let handler = handler.clone();
        tasks.push(tokio::spawn(async move {
            let addr = ln.local_addr().map(|a| a.to_string()).unwrap_or_default();
            let e = serve_listener(ln, handler, tls).await;
            let _ = tx.send(anyhow::anyhow!("serving on {addr}: {e}")).await;
        }));
    }
    let res = tokio::select! {
        _ = crate::boot::shutdown_signal() => Ok(()),
        Some(e) = rx.recv() => Err(e),
    };
    for t in tasks {
        t.abort();
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::egress::actor::api::SCHEME_HTTPS;
    use crate::egress::actor::classify::{classify, RequestError};
    use crate::egress::actor::client::{
        tls_config as client_tls, BoxStream, DialFn, EndpointClient, Trace,
    };
    use crate::egress::actor::endpoints::{endpoint_url, ENDPOINT_DOMAIN};
    use crate::egress::targetcert::{self, tests::roots_of};

    #[test]
    fn handler() {
        let h = Handler::new(32);
        for (path, want) in [("/", 32), ("/anything", 32), ("/healthz", 3)] {
            let r = h.reply("GET", path);
            assert_eq!((r.status, r.body.len()), (200, want), "{path}");
        }
    }

    #[tokio::test]
    async fn handler_over_tls() {
        let bundle = targetcert::generate(std::time::SystemTime::now(), ENDPOINT_DOMAIN).unwrap();
        let dir = tempfile::tempdir().unwrap();
        bundle.write(dir.path()).unwrap();
        let cfg = tls_config(&dir.path().join("tls.crt"), &dir.path().join("tls.key")).unwrap();
        let ln = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = ln.local_addr().unwrap();
        let srv = tokio::spawn(serve_listener(
            ln,
            Handler::new(32),
            Some(TlsAcceptor::from(cfg)),
        ));
        // Dial the test server whatever the URL names, so the name can be an
        // endpoint's while the TLS check runs for real.
        let dial: DialFn = Arc::new(move |_, _| {
            Box::pin(async move {
                Ok(Box::new(tokio::net::TcpStream::connect(addr).await?) as BoxStream)
            })
        });
        let tls = client_tls(Some(roots_of(&bundle.ca)));
        let mut c = EndpointClient::new(
            &endpoint_url(42, SCHEME_HTTPS),
            Some(dial.clone()),
            tls.clone(),
        )
        .unwrap();
        let status = c
            .get(&mut Trace::default())
            .await
            .expect("GET over TLS as an endpoint name");
        assert_eq!(status, 200);
        let mut other = EndpointClient::new("https://example.com/", Some(dial), tls).unwrap();
        let err = other.get(&mut Trace::default()).await.unwrap_err();
        assert!(matches!(err, RequestError::Tls(_)), "{err:?}");
        assert_eq!(classify(Some(&err), 0), "tls: hostname mismatch");
        srv.abort();
    }

    #[test]
    fn port_range_table() {
        let cases: Vec<(&str, Vec<u16>, bool)> = vec![
            ("", vec![], false),
            ("8080", vec![8080], false),
            ("10000-10002", vec![10000, 10001, 10002], false),
            ("10002-10000", vec![], true),
            ("0-3", vec![], true),
            ("65535-65536", vec![], true),
            ("a-b", vec![], true),
            ("10000-", vec![], true),
        ];
        for (input, want, want_err) in cases {
            let got = port_range(input);
            assert_eq!(got.is_err(), want_err, "{input}");
            assert_eq!(got.unwrap_or_default(), want, "{input}");
        }
    }

    #[test]
    fn listen_addrs_table() {
        assert_eq!(
            listen_addrs("127.0.0.1:0", "10000-10002").unwrap(),
            vec!["127.0.0.1:0", ":10000", ":10001", ":10002"]
        );
        assert!(listen_addrs("", "a-b").is_err());
    }

    #[tokio::test]
    async fn listen_all_closes_what_it_opened_on_failure() {
        let held = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addrs = vec![
            "127.0.0.1:0".to_string(),
            held.local_addr().unwrap().to_string(),
        ];
        assert!(
            listen_all(&addrs).await.is_err(),
            "listen_all bound a port that was taken"
        );
    }
}
