//! Sending traffic to Actors through the ingress gate router, the way real
//! traffic arrives (so it is routed, and the Actor resumed if needed).
//!
//! The client port-forwards the router Service and presents the pod-identity
//! certificate of [`ROUTER_CLIENT_SERVICE_ACCOUNT`], which the router
//! allowlists. The CONNECT port is a listener separate from HTTPS: only it
//! accepts CONNECT, so each tunnel opens its own forward to it.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use hivekeep_core::resource::ActorRef;
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use rustls::ClientConfig;
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf,
};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use crate::clients::Clients;
use crate::env::{resource_name, system_namespace};
use crate::harness::T;
use crate::ingressauth::{
    router_client_certificate, router_rustls_server_name, router_tls_config,
    ROUTER_CLIENT_SERVICE_ACCOUNT,
};
use crate::install;
use crate::portforward::{self, Forward};

/// The most of a CONNECT refusal's body kept for the error.
const MAX_ERROR_BODY: usize = 4 << 10;

/// A fully read response.
#[derive(Debug, Clone)]
pub struct RouterResponse {
    /// Status code.
    pub status: u16,
    /// Response headers.
    pub headers: HeaderMap,
    /// The whole body.
    pub body: Bytes,
}

impl RouterResponse {
    /// The body as (lossy) UTF-8.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

enum Transport {
    Tls {
        config: Arc<ClientConfig>,
        server_name: ServerName<'static>,
    },
    Plain,
}

/// A client of the ingress router.
pub struct RouterClient {
    address: String,
    base_url: String,
    transport: Transport,
    kube: Option<kube::Client>,
    _forward: Option<Forward>,
    timeout: Duration,
}

impl RouterClient {
    /// Port-forwards the router's HTTPS Service port and prepares the
    /// client identity.
    pub async fn new(t: &T, clients: &Clients) -> Result<RouterClient, String> {
        let cert = router_client_certificate(t, clients, ROUTER_CLIENT_SERVICE_ACCOUNT).await;
        let mut config = router_tls_config(clients, Some(&cert)).await?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        let forward = portforward::service_port_forward(
            &clients.kube(),
            &system_namespace(),
            &resource_name(install::ROUTER_SERVICE),
            install::ROUTER_HTTPS_SERVICE_PORT,
        )
        .await?;
        let address = forward.addr();
        Ok(RouterClient {
            base_url: format!("https://{address}"),
            address,
            transport: Transport::Tls {
                config: Arc::new(config),
                server_name: router_rustls_server_name(),
            },
            kube: Some(clients.kube()),
            _forward: Some(forward),
            timeout: Duration::from_secs(30),
        })
    }

    /// A cleartext client of `address`, for tests of the request shape.
    pub fn plaintext(address: &str) -> RouterClient {
        RouterClient {
            address: address.to_string(),
            base_url: format!("http://{address}"),
            transport: Transport::Plain,
            kube: None,
            _forward: None,
            timeout: Duration::from_secs(30),
        }
    }

    /// The local forward as an `https://` URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The local forward's host:port, for a caller that speaks its own
    /// protocol to the router.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// A copy of the TLS config the client dials the router with.
    pub fn tls_config(&self) -> Option<ClientConfig> {
        match &self.transport {
            Transport::Tls { config, .. } => Some((**config).clone()),
            Transport::Plain => None,
        }
    }

    /// Opens a new forward to the router's CONNECT listener; the caller
    /// keeps the [`Forward`] alive while using it.
    pub async fn connect_port_forward(&self) -> Result<Forward, String> {
        let kube = self.kube.as_ref().ok_or("no cluster client")?;
        portforward::service_port_forward(
            kube,
            &system_namespace(),
            &resource_name(install::ROUTER_SERVICE),
            install::ROUTER_CONNECT_TLS_SERVICE_PORT,
        )
        .await
        .map_err(|e| format!("port-forwarding to the router's CONNECT listener: {e}"))
    }

    /// GET `path` on `actor` through the router.
    pub async fn get(&self, actor: &ActorRef, path: &str) -> Result<RouterResponse, String> {
        self.request(
            Method::GET,
            actor,
            path,
            Bytes::new(),
            None,
            &HeaderMap::new(),
        )
        .await
    }

    /// POST a JSON body to `path` on `actor` through the router.
    pub async fn post_json(
        &self,
        actor: &ActorRef,
        path: &str,
        body: impl Into<Bytes>,
    ) -> Result<RouterResponse, String> {
        self.request(
            Method::POST,
            actor,
            path,
            body.into(),
            Some("application/json"),
            &HeaderMap::new(),
        )
        .await
    }

    /// Sends `method path` to `actor` with extra `headers`.
    pub async fn request(
        &self,
        method: Method,
        actor: &ActorRef,
        path: &str,
        body: Bytes,
        content_type: Option<&str>,
        headers: &HeaderMap,
    ) -> Result<RouterResponse, String> {
        let mut builder = http::Request::builder()
            .method(method)
            .uri(path)
            .header(http::header::HOST, self.address.as_str());
        if let Some(ct) = content_type {
            builder = builder.header(http::header::CONTENT_TYPE, ct);
        }
        builder = builder.header(install::TARGET_ACTOR_HEADER, actor.to_string());
        let mut req = builder.body(Full::new(body)).map_err(|e| e.to_string())?;
        for (k, v) in headers {
            req.headers_mut().insert(k.clone(), v.clone());
        }
        self.send(req).await
    }

    /// Sends a prepared request (its URI is a path) to the router.
    pub async fn send(&self, req: http::Request<Full<Bytes>>) -> Result<RouterResponse, String> {
        tokio::time::timeout(self.timeout, self.send_inner(req))
            .await
            .map_err(|_| "request timed out".to_string())?
    }

    async fn send_inner(&self, req: http::Request<Full<Bytes>>) -> Result<RouterResponse, String> {
        let tcp = TcpStream::connect(&self.address)
            .await
            .map_err(|e| format!("dialing the router: {e}"))?;
        let resp = match &self.transport {
            Transport::Plain => {
                let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
                    .await
                    .map_err(|e| e.to_string())?;
                tokio::spawn(conn);
                sender.send_request(req).await.map_err(|e| e.to_string())?
            }
            Transport::Tls {
                config,
                server_name,
            } => {
                let tls = TlsConnector::from(config.clone())
                    .connect(server_name.clone(), tcp)
                    .await
                    .map_err(|e| format!("TLS handshake with the router: {e}"))?;
                let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
                    .await
                    .map_err(|e| e.to_string())?;
                tokio::spawn(conn);
                sender.send_request(req).await.map_err(|e| e.to_string())?
            }
        };
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| format!("reading the response: {e}"))?
            .to_bytes();
        Ok(RouterResponse {
            status,
            headers,
            body,
        })
    }

    /// Opens a CONNECT tunnel through the router to `port` on `actor`: the
    /// target port travels in the CONNECT authority, as a client reaching a
    /// port other than the Actor's primary one does. Dropping the tunnel
    /// also stops its forward.
    pub async fn connect(&self, actor: &ActorRef, port: u16) -> Result<Tunnel, String> {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(install::TARGET_ACTOR_HEADER),
            HeaderValue::from_str(&actor.to_string()).map_err(|e| e.to_string())?,
        );
        self.connect_with_headers(actor, port, &headers).await
    }

    /// [`RouterClient::connect`] with caller-chosen outer headers (repeated
    /// or missing target headers, say).
    pub async fn connect_with_headers(
        &self,
        actor: &ActorRef,
        port: u16,
        headers: &HeaderMap,
    ) -> Result<Tunnel, String> {
        let Transport::Tls {
            config,
            server_name,
        } = &self.transport
        else {
            return Err("CONNECT needs a TLS router client".into());
        };
        // A forward's failure on one stream must not tear down another
        // tunnel, so each gets its own.
        let forward = self.connect_port_forward().await?;
        connect_actor_with_headers(
            &forward.addr(),
            config.clone(),
            server_name.clone(),
            actor,
            port,
            headers,
            Some(forward),
        )
        .await
    }
}

/// An open CONNECT tunnel. Bytes the server sent past the response head
/// are kept and read first.
pub struct Tunnel {
    stream: BufReader<TlsStream<TcpStream>>,
    _forward: Option<Forward>,
}

impl AsyncRead for Tunnel {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Tunnel {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

/// Dials `addr` over TLS, sends `CONNECT <actor name>:<port>` with
/// `headers`, and returns the tunnel. `forward`, if given, lives as long as
/// the tunnel.
pub async fn connect_actor_with_headers(
    addr: &str,
    config: Arc<ClientConfig>,
    server_name: ServerName<'static>,
    actor: &ActorRef,
    port: u16,
    headers: &HeaderMap,
    forward: Option<Forward>,
) -> Result<Tunnel, String> {
    let tcp = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connecting to router's CONNECT listener: {e}"))?;
    let tls = TlsConnector::from(config)
        .connect(server_name, tcp)
        .await
        .map_err(|e| format!("TLS handshake with router's CONNECT listener: {e}"))?;
    let mut stream = BufReader::new(tls);
    let destination = format!("{}:{port}", actor.name);
    let mut request = format!("CONNECT {destination} HTTP/1.1\r\nHost: {destination}\r\n");
    for (k, v) in headers {
        request.push_str(&format!(
            "{}: {}\r\n",
            k.as_str(),
            v.to_str().unwrap_or_default()
        ));
    }
    request.push_str("\r\n");
    stream
        .get_mut()
        .write_all(request.as_bytes())
        .await
        .map_err(|e| format!("writing CONNECT request: {e}"))?;
    stream
        .get_mut()
        .flush()
        .await
        .map_err(|e| format!("writing CONNECT request: {e}"))?;

    let mut status_line = String::new();
    stream
        .read_line(&mut status_line)
        .await
        .map_err(|e| format!("reading CONNECT response: {e}"))?;
    let mut parts = status_line.trim_end().splitn(3, ' ');
    let (proto, code, reason) = (
        parts.next().unwrap_or(""),
        parts.next(),
        parts.next().unwrap_or(""),
    );
    let code: u16 = match code.and_then(|c| c.parse().ok()) {
        Some(c) if proto.starts_with("HTTP/") => c,
        _ => {
            return Err(format!(
                "reading CONNECT response: malformed status line {:?}",
                status_line.trim_end()
            ))
        }
    };
    let mut content_length = None;
    loop {
        let mut line = String::new();
        let n = stream
            .read_line(&mut line)
            .await
            .map_err(|e| format!("reading CONNECT response: {e}"))?;
        let h = line.trim_end();
        if n == 0 || h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse::<usize>().ok();
            }
        }
    }
    if !(200..300).contains(&code) {
        let mut body = Vec::new();
        let limit = content_length.unwrap_or(MAX_ERROR_BODY).min(MAX_ERROR_BODY) as u64;
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            (&mut stream).take(limit).read_to_end(&mut body),
        )
        .await;
        let status = format!("{code} {reason}").trim().to_string();
        let message = String::from_utf8_lossy(&body).trim().to_string();
        let message = if message.is_empty() {
            status.clone()
        } else {
            message
        };
        return Err(format!(
            "router rejected CONNECT to {destination} with {status}: {message}"
        ));
    }
    Ok(Tunnel {
        stream,
        _forward: forward,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        SanType,
    };
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::server::WebPkiClientVerifier;
    use rustls::{RootCertStore, ServerConfig};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    #[tokio::test]
    async fn post_json_carries_target_and_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (s, _) = listener.accept().await.unwrap();
            let mut s = BufReader::new(s);
            let mut head = Vec::new();
            loop {
                let mut line = String::new();
                s.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                head.push(line.trim_end().to_string());
            }
            let len: usize = head
                .iter()
                .find_map(|h| {
                    h.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(|v| v.parse().unwrap())
                })
                .unwrap();
            let mut body = vec![0; len];
            s.read_exact(&mut body).await.unwrap();
            s.get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await
                .unwrap();
            (head, String::from_utf8(body).unwrap())
        });
        let client = RouterClient::plaintext(&addr);
        let actor = ActorRef::new("demo", "fetcher");
        let resp = client
            .post_json(&actor, "/fetch", r#"{"url":"https://example.com/"}"#)
            .await
            .unwrap();
        assert_eq!((resp.status, resp.text().as_str()), (200, "ok"));
        let (head, body) = server.await.unwrap();
        assert_eq!(head[0], "POST /fetch HTTP/1.1");
        let has = |h: &str| head.iter().any(|l| l.eq_ignore_ascii_case(h));
        assert!(has("hk-target-actor: demo/fetcher"), "{head:?}");
        assert!(has(&format!("host: {addr}")), "{head:?}");
        assert!(has("content-type: application/json"), "{head:?}");
        assert_eq!(body, r#"{"url":"https://example.com/"}"#);
    }

    fn ca() -> (CertificateParams, KeyPair, rcgen::Certificate) {
        let key = KeyPair::generate().unwrap();
        let mut p = CertificateParams::default();
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        p.distinguished_name.push(DnType::CommonName, "test ca");
        let cert = p.clone().self_signed(&key).unwrap();
        (p, key, cert)
    }

    /// Connect completes a TLS handshake presenting the client certificate
    /// before it sends CONNECT; the server requires and verifies it.
    #[tokio::test]
    async fn connect_presents_the_client_certificate() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (ca_params, ca_key, ca_cert) = ca();
        let issuer = rcgen::Issuer::new(ca_params, &ca_key);
        let issue = |sans: Vec<SanType>, eku: ExtendedKeyUsagePurpose| {
            let key = KeyPair::generate().unwrap();
            let mut p = CertificateParams::default();
            p.subject_alt_names = sans;
            p.extended_key_usages = vec![eku];
            let cert = p.signed_by(&key, &issuer).unwrap();
            (
                cert.der().clone(),
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
            )
        };
        let (server_cert, server_key) = issue(
            vec![SanType::DnsName("example.com".try_into().unwrap())],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let (client_cert, client_key) = issue(
            vec![SanType::URI(
                "spiffe://cluster.local/ns/test/sa/client"
                    .try_into()
                    .unwrap(),
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        );

        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(ca_cert.der().to_vec()))
            .unwrap();
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone()))
            .build()
            .unwrap();
        let server_config = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![server_cert], server_key)
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let tls = TlsAcceptor::from(Arc::new(server_config))
                .accept(tcp)
                .await
                .unwrap();
            let peer = tls.get_ref().1.peer_certificates().unwrap()[0].to_vec();
            let mut s = BufReader::new(tls);
            let mut head = Vec::new();
            loop {
                let mut line = String::new();
                s.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                head.push(line.trim_end().to_string());
            }
            s.get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\n\r\ntunneled")
                .await
                .unwrap();
            s.get_mut().shutdown().await.unwrap();
            let (_, cert) = x509_parser::parse_x509_certificate(&peer).unwrap();
            let uri = cert
                .subject_alternative_name()
                .unwrap()
                .unwrap()
                .value
                .general_names
                .iter()
                .find_map(|n| match n {
                    x509_parser::extensions::GeneralName::URI(u) => Some(u.to_string()),
                    _ => None,
                })
                .unwrap();
            (head, uri)
        });

        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(vec![client_cert], client_key)
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(install::TARGET_ACTOR_HEADER),
            HeaderValue::from_static("demo/fetcher"),
        );
        let actor = ActorRef::new("demo", "fetcher");
        let mut tunnel = connect_actor_with_headers(
            &addr,
            Arc::new(config),
            ServerName::try_from("example.com").unwrap(),
            &actor,
            9090,
            &headers,
            None,
        )
        .await
        .unwrap();
        let mut got = String::new();
        tunnel.read_to_string(&mut got).await.unwrap();
        assert_eq!(got, "tunneled");
        let (head, peer) = server.await.unwrap();
        assert_eq!(head[0], "CONNECT fetcher:9090 HTTP/1.1");
        assert!(
            head.iter().any(|h| h == "hk-target-actor: demo/fetcher"),
            "{head:?}"
        );
        assert_eq!(peer, "spiffe://cluster.local/ns/test/sa/client");
    }
}
