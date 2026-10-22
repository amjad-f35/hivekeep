//! Test origins for the probe's fetch tests: plain and TLS HTTP servers on
//! loopback, and a TLS listener that hangs up on the ClientHello.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

type Handler = Arc<dyn Fn(&Request<Incoming>) -> Response<Full<Bytes>> + Send + Sync>;

/// A text response with `code`.
pub fn text(code: u16, body: String) -> Response<Full<Bytes>> {
    let mut res = Response::new(Full::new(Bytes::from(body)));
    *res.status_mut() = StatusCode::from_u16(code).unwrap();
    res
}

/// A running origin, stopped when dropped.
pub struct Origin {
    /// Where it listens.
    pub addr: SocketAddr,
    /// The PEM of the CA its certificate chains to, for a TLS origin.
    pub ca_pem: Option<String>,
    hits: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn ca() -> (Issuer<'static, KeyPair>, String) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let cert = params.self_signed(&key).unwrap();
    let pem = cert.pem();
    (Issuer::new(params, key), pem)
}

/// The PEM of a CA that issued nothing the origins serve.
pub fn unrelated_ca_pem() -> String {
    ca().1
}

impl Origin {
    async fn start(handler: Handler, tls: Option<(rustls::ServerConfig, String)>) -> Origin {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let (acceptor, ca_pem) = match tls {
            Some((cfg, pem)) => (
                Some(tokio_rustls::TlsAcceptor::from(Arc::new(cfg))),
                Some(pem),
            ),
            None => (None, None),
        };
        let counter = hits.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let handler = handler.clone();
                let counter = counter.clone();
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |req: Request<Incoming>| {
                        counter.fetch_add(1, Ordering::SeqCst);
                        let res = handler(&req);
                        async move { Ok::<_, Infallible>(res) }
                    });
                    let builder = hyper::server::conn::http1::Builder::new();
                    match acceptor {
                        Some(a) => {
                            if let Ok(s) = a.accept(stream).await {
                                let _ = builder.serve_connection(TokioIo::new(s), svc).await;
                            }
                        }
                        None => {
                            let _ = builder.serve_connection(TokioIo::new(stream), svc).await;
                        }
                    }
                });
            }
        });
        Origin {
            addr,
            ca_pem,
            hits,
            task,
        }
    }

    /// A cleartext origin.
    pub async fn http<F>(f: F) -> Origin
    where
        F: Fn(&Request<Incoming>) -> Response<Full<Bytes>> + Send + Sync + 'static,
    {
        Self::start(Arc::new(f), None).await
    }

    /// A TLS origin serving a leaf for 127.0.0.1 from a fresh CA.
    pub async fn https<F>(f: F) -> Origin
    where
        F: Fn(&Request<Incoming>) -> Response<Full<Bytes>> + Send + Sync + 'static,
    {
        let (issuer, ca_pem) = ca();
        let leaf_key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.subject_alt_names = vec![SanType::IpAddress("127.0.0.1".parse().unwrap())];
        let leaf = params.signed_by(&leaf_key, &issuer).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let cfg = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(leaf.der().to_vec())],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der())),
            )
            .unwrap();
        Self::start(Arc::new(f), Some((cfg, ca_pem))).await
    }

    /// The origin's base URL.
    pub fn url(&self) -> String {
        let scheme = if self.ca_pem.is_some() {
            "https"
        } else {
            "http"
        };
        format!("{scheme}://{}/", self.addr)
    }

    /// How many requests reached the handler.
    pub fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }

    /// Writes the origin's CA as the only anchor of a trust bundle in `dir`.
    pub fn write_trust_bundle(&self, dir: &Path) -> String {
        let path = dir.join("trust-bundle.pem");
        std::fs::write(&path, self.ca_pem.as_deref().expect("a TLS origin")).unwrap();
        path.to_str().unwrap().to_string()
    }
}

/// A listener that reads the ClientHello and hangs up, the way a gateway
/// denies a TLS destination.
pub async fn close_on_client_hello() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0u8; 512];
            let _ = stream.read(&mut buf).await;
            drop(stream);
        }
    });
    addr
}
