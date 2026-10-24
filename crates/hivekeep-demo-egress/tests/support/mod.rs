//! Shared fixtures: loopback HTTP(S), WebSocket, gRPC and proxy stand-ins,
//! and helpers that drive the demo's router in process.

#![allow(dead_code)]

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Once};

use axum::body::Body;
use axum::Router;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status, Streaming};
use tower::ServiceExt;

use hivekeep_demo_egress::proto::echo_server::{Echo, EchoServer};
use hivekeep_demo_egress::proto::{EchoRequest, EchoResponse, EchoStreamRequest};
use hivekeep_demo_egress::{router, tls, AppState, Fetch, HttpFetcher};

/// One-time process setup: the TLS provider, and enough file descriptors
/// for the 250-connection mux tests (both ends live in this process).
pub fn init() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        tls::install_default_provider();
        #[cfg(unix)]
        unsafe {
            let mut limit = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
                let want: libc::rlim_t = 8192;
                let cap = if limit.rlim_max == libc::RLIM_INFINITY {
                    want
                } else {
                    limit.rlim_max.min(want)
                };
                if limit.rlim_cur < cap {
                    limit.rlim_cur = cap;
                    libc::setrlimit(libc::RLIMIT_NOFILE, &limit);
                }
            }
        }
    });
}

/// TLS material for a loopback server: a private CA and a leaf for
/// 127.0.0.1 signed by it.
pub struct TestTls {
    pub server: Arc<ServerConfig>,
    pub root_pem: String,
    pub leaf_der: Vec<u8>,
}

pub fn loopback_tls() -> TestTls {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.distinguished_name = rcgen::DistinguishedName::new();
    ca_params
        .distinguished_name
        .push(rcgen::DnType::OrganizationName, "Hivekeep Test");
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Hivekeep Test Root");
    ca_params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
    ];
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = rcgen::Issuer::new(ca_params, ca_key);

    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let mut leaf_params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
    leaf_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "loopback");
    let leaf = leaf_params.signed_by(&leaf_key, &issuer).unwrap();

    let mut server = ServerConfig::builder_with_provider(tls::provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(leaf.der().to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der())),
        )
        .unwrap();
    server.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    TestTls {
        server: Arc::new(server),
        root_pem: ca_cert.pem(),
        leaf_der: leaf.der().to_vec(),
    }
}

pub type Reply = Response<Full<Bytes>>;
pub type Handler =
    Arc<dyn Fn(Request<Incoming>) -> Pin<Box<dyn Future<Output = Reply> + Send>> + Send + Sync>;

/// Wraps an async closure as a [`Handler`].
pub fn handler<F, Fut>(f: F) -> Handler
where
    F: Fn(Request<Incoming>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Reply> + Send + 'static,
{
    Arc::new(move |req| Box::pin(f(req)))
}

/// A reply with a status and text body.
pub fn reply(status: u16, body: impl Into<Bytes>) -> Reply {
    Response::builder()
        .status(status)
        .body(Full::new(body.into()))
        .unwrap()
}

/// Serves `h` on loopback (HTTP/1.1 and HTTP/2, TLS when given) and
/// returns its base URL.
pub async fn serve(h: Handler, tls: Option<Arc<ServerConfig>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let scheme = if tls.is_some() { "https" } else { "http" };
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let h = h.clone();
            let tls = tls.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |req| {
                    let h = h.clone();
                    async move { Ok::<_, Infallible>(h(req).await) }
                });
                let builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
                match tls {
                    None => {
                        let _ = builder.serve_connection(TokioIo::new(tcp), service).await;
                    }
                    Some(config) => {
                        let Ok(stream) = TlsAcceptor::from(config).accept(tcp).await else {
                            return;
                        };
                        let _ = builder
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    }
                }
            });
        }
    });
    format!("{scheme}://{addr}")
}

/// Accepts a TCP connection, optionally under TLS.
async fn accept_any(tcp: TcpStream, tls: Option<Arc<ServerConfig>>) -> Option<Box<dyn Duplex>> {
    match tls {
        None => Some(Box::new(tcp)),
        Some(config) => {
            let mut config = (*config).clone();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            TlsAcceptor::from(Arc::new(config))
                .accept(tcp)
                .await
                .ok()
                .map(|s| Box::new(s) as Box<dyn Duplex>)
        }
    }
}

pub trait Duplex: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Duplex for T {}

/// A WebSocket server that echoes each message. With `bad_second` it drops
/// the connection, without a close frame, on receiving the second message.
pub async fn serve_websocket(tls: Option<Arc<ServerConfig>>, bad_second: bool) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let scheme = if tls.is_some() { "wss" } else { "ws" };
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let tls = tls.clone();
            tokio::spawn(async move {
                let Some(stream) = accept_any(tcp, tls).await else {
                    return;
                };
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                let mut index = 0;
                while let Some(Ok(msg)) = ws.next().await {
                    if !(msg.is_text() || msg.is_binary()) {
                        continue;
                    }
                    if bad_second && index == 1 {
                        return; // dropping the stream closes the socket abruptly
                    }
                    if ws.send(msg).await.is_err() {
                        return;
                    }
                    index += 1;
                }
            });
        }
    });
    format!("{scheme}://{addr}")
}

/// `http://` → `ws://`, `https://` → `wss://`.
pub fn ws_url(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else {
        url.replacen("http://", "ws://", 1)
    }
}

/// The Echo service as the e2e origin implements it.
#[derive(Default)]
pub struct EchoService;

#[tonic::async_trait]
impl Echo for EchoService {
    async fn echo(
        &self,
        req: GrpcRequest<EchoRequest>,
    ) -> Result<GrpcResponse<EchoResponse>, Status> {
        Ok(GrpcResponse::new(EchoResponse {
            message: req.into_inner().message,
            index: 0,
        }))
    }

    type EchoStreamStream = ReceiverStream<Result<EchoResponse, Status>>;
    async fn echo_stream(
        &self,
        req: GrpcRequest<EchoStreamRequest>,
    ) -> Result<GrpcResponse<Self::EchoStreamStream>, Status> {
        let req = req.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            for index in 0..req.count {
                if tx
                    .send(Ok(EchoResponse {
                        message: req.message.clone(),
                        index,
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(GrpcResponse::new(ReceiverStream::new(rx)))
    }

    type EchoBidiStream = ReceiverStream<Result<EchoResponse, Status>>;
    /// Answers each request as it arrives; draining the request side first
    /// would deadlock a caller that waits for each answer.
    async fn echo_bidi(
        &self,
        req: GrpcRequest<Streaming<EchoRequest>>,
    ) -> Result<GrpcResponse<Self::EchoBidiStream>, Status> {
        let mut incoming = req.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let mut index = 0;
            while let Ok(Some(msg)) = incoming.message().await {
                if tx
                    .send(Ok(EchoResponse {
                        message: msg.message,
                        index,
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
                index += 1;
            }
        });
        Ok(GrpcResponse::new(ReceiverStream::new(rx)))
    }
}

/// Serves Echo on loopback and returns `host:port`.
pub async fn start_echo_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(EchoServer::new(EchoService))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    addr.to_string()
}

/// A proxy stand-in: answers the first request on each connection with
/// `answer` verbatim, then holds the connection open as an opened tunnel
/// would. Returns its `host:port` and the request lines received.
pub async fn proxy_stub(
    answer: &'static str,
    tls: Option<Arc<ServerConfig>>,
) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let received = Arc::new(Mutex::new(Vec::new()));
    let log = received.clone();
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let tls = tls.clone();
            let log = log.clone();
            tokio::spawn(async move {
                let Some(mut conn) = accept_any(tcp, tls).await else {
                    return;
                };
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    match conn.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                let mut headers = [httparse::EMPTY_HEADER; 32];
                let mut req = httparse::Request::new(&mut headers);
                if req.parse(&buf).is_err() {
                    return;
                }
                log.lock().unwrap().push(format!(
                    "{} {}",
                    req.method.unwrap_or(""),
                    req.path.unwrap_or("")
                ));
                let _ = conn.write_all(answer.as_bytes()).await;
                let _ = conn.flush().await;
                std::future::pending::<()>().await;
            });
        }
    });
    (addr.to_string(), received)
}

/// A loopback port that refuses connections.
pub async fn closed_port() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);
    addr
}

/// The router with the server's default client.
pub fn app() -> Router {
    init();
    app_with(Arc::new(HttpFetcher::system().unwrap()))
}

/// The router with a given default client.
pub fn app_with(fetcher: Arc<dyn Fetch>) -> Router {
    init();
    router(AppState::new(fetcher))
}

/// Sends one request through `app` and returns status, headers and body.
pub async fn call(
    app: Router,
    method: &str,
    path: &str,
    body: impl Into<String>,
    headers: &[(&str, &str)],
) -> (u16, http::HeaderMap, Vec<u8>) {
    let mut req = http::Request::builder().method(method).uri(path);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let resp = app
        .oneshot(req.body(Body::from(body.into())).unwrap())
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, headers, body)
}

/// POSTs `input` as JSON to `path` and decodes the reply.
pub async fn post_json<T: Serialize, R: DeserializeOwned>(
    app: Router,
    path: &str,
    input: &T,
) -> (u16, R) {
    let (status, _, body) = call(
        app,
        "POST",
        path,
        serde_json::to_string(input).unwrap(),
        &[],
    )
    .await;
    let decoded = serde_json::from_slice(&body).unwrap_or_else(|e| {
        panic!(
            "decoding response (HTTP {status}): {e}: {}",
            String::from_utf8_lossy(&body)
        )
    });
    (status, decoded)
}

/// Status of a raw-body request, for the invalid-instruction tables.
pub async fn status_of(app: Router, method: &str, path: &str, body: &str) -> (u16, String) {
    let (status, _, body) = call(app, method, path, body, &[]).await;
    (status, String::from_utf8_lossy(&body).into_owned())
}
