//! The `http` subcommand: the HTTP/1.1 origin an Actor's egress lands on.
//!
//! - `/healthz` answers 200 with the optional `--body`, so a suite can check
//!   the destination port survives the egress tunnel (the actor fetches it on a
//!   non-standard port and the gateway's access log must carry that port) and
//!   assert on a response body inside the tunnel.
//! - `/echoheaders` returns the request headers as they arrived, which is how
//!   a suite sees what the gateway put on the wire.
//! - `POST /fetch` drains a request body before answering, for tests whose
//!   requests carry one.
//! - `/dial` reports whether this pod can open a TCP connection to an address.
//! - With `--accept-connect`, any CONNECT is answered 200 without relaying.

use std::collections::BTreeMap;
use std::time::Duration;

use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use serde::{Deserialize, Serialize};

use super::duration::parse_duration;
use super::serve::{handler, json, text, Body, Handler};

/// The `/echoheaders` response body.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EchoedHeaders {
    /// The request headers the origin received, keyed by canonical name
    /// (`X-Actor-Token`), repeated values joined by `,`.
    pub headers: BTreeMap<String, String>,
}

/// The `/dial` response body.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DialResult {
    /// The address dialed.
    pub address: String,
    /// Whether the TCP connection was established.
    pub ok: bool,
    /// Why it was not.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

/// The `/dial` timeout when the request sets none.
pub const DEFAULT_DIAL_TIMEOUT: Duration = Duration::from_secs(3);

/// Canonical header spelling: each `-`-separated word capitalized.
pub fn canonical_header_name(name: &str) -> String {
    name.split('-')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => {
                    first.to_ascii_uppercase().to_string() + &chars.as_str().to_ascii_lowercase()
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join("-")
}

fn echo_headers(req: &Request<Incoming>) -> EchoedHeaders {
    let mut headers: BTreeMap<String, String> = BTreeMap::new();
    for (name, value) in req.headers() {
        let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
        headers
            .entry(canonical_header_name(name.as_str()))
            .and_modify(|v| {
                v.push(',');
                v.push_str(&value);
            })
            .or_insert(value);
    }
    EchoedHeaders { headers }
}

fn query_param(req: &Request<Incoming>, key: &str) -> Option<String> {
    let query = req.uri().query()?;
    url_pairs(query)
        .into_iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v)
}

/// Decodes an `application/x-www-form-urlencoded` query string.
pub fn url_pairs(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                match std::str::from_utf8(&bytes[i + 1..i + 3])
                    .ok()
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                {
                    Some(b) => {
                        out.push(b);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Whether `address` is `<host>:<port>` with a non-empty port.
fn has_port(address: &str) -> bool {
    match address.rsplit_once(':') {
        Some((host, port)) => {
            !port.is_empty()
                && (!host.contains(':') || (host.starts_with('[') && host.ends_with(']')))
        }
        None => false,
    }
}

/// Opens a TCP connection to `?address=` within `?timeout=` (default 3s) and
/// closes it again. A malformed request gets a 400 rather than `ok=false`, so
/// a mistake in the caller is not mistaken for an unreachable address.
async fn dial(req: Request<Incoming>) -> Response<Body> {
    let address = query_param(&req, "address").unwrap_or_default();
    if !has_port(&address) {
        return text(
            StatusCode::BAD_REQUEST,
            format!("address parameter {address:?} is not <host>:<port>\n"),
        );
    }
    let mut timeout = DEFAULT_DIAL_TIMEOUT;
    if let Some(raw) = query_param(&req, "timeout").filter(|r| !r.is_empty()) {
        match parse_duration(&raw) {
            Ok(d) if !d.is_zero() => timeout = d,
            _ => {
                return text(
                    StatusCode::BAD_REQUEST,
                    format!("timeout parameter {raw:?} is not a positive duration\n"),
                )
            }
        }
    }
    let mut result = DialResult {
        address: address.clone(),
        ..Default::default()
    };
    let connect = tokio::net::TcpStream::connect(address.as_str());
    match tokio::time::timeout(timeout, connect).await {
        Ok(Ok(_conn)) => result.ok = true,
        Ok(Err(e)) => result.error = format!("dial tcp {address}: {e}"),
        Err(_) => result.error = format!("dial tcp {address}: i/o timeout"),
    }
    json(&result)
}

/// The origin's routes. `body` is what `/healthz` answers with.
pub fn http_handler(body: String) -> Handler {
    handler(move |req: Request<Incoming>| {
        let body = body.clone();
        async move {
            match req.uri().path() {
                "/healthz" => text(StatusCode::OK, body),
                "/echoheaders" => json(&echo_headers(&req)),
                "/fetch" => {
                    if req.method() != Method::POST {
                        return text(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed\n");
                    }
                    match req.into_body().collect().await {
                        Ok(_) => text(StatusCode::OK, ""),
                        Err(e) => text(
                            StatusCode::BAD_REQUEST,
                            format!("reading request body: {e}\n"),
                        ),
                    }
                }
                "/dial" => dial(req).await,
                _ => text(StatusCode::NOT_FOUND, "404 page not found\n"),
            }
        }
    })
}

/// Answers every CONNECT with 200, as a forward proxy does when it opens a
/// tunnel, and hands anything else to `next`. Nothing is relayed: the server
/// stands in for a proxy only so a client whose CONNECT is refused can tell
/// the refusal did not come from here.
pub fn accepting_connect(next: Handler) -> Handler {
    handler(move |req: Request<Incoming>| {
        let next = next.clone();
        async move {
            if req.method() != Method::CONNECT {
                return next(req).await;
            }
            // The server hands the connection over once a CONNECT is answered
            // 2xx; take it and hold it, discarding whatever the client sends,
            // until the client hangs up.
            tokio::spawn(async move {
                if let Ok(upgraded) = hyper::upgrade::on(req).await {
                    let mut io = hyper_util::rt::TokioIo::new(upgraded);
                    let _ = tokio::io::copy(&mut io, &mut tokio::io::sink()).await;
                }
            });
            // A 2xx to a CONNECT has no body, and the server frames it so.
            text(StatusCode::OK, "")
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use http_body_util::Full;
    use hyper_util::rt::TokioIo;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use super::super::serve::testing::{
        listen_loopback, start_origin_server, write_origin_certificate, OriginCertificate,
    };
    use super::*;

    /// A minimal HTTP/1.1 client over `stream`, returning status, headers and
    /// body.
    async fn send<S>(
        stream: S,
        req: Request<Full<bytes::Bytes>>,
    ) -> (StatusCode, http::HeaderMap, Vec<u8>, http::Version)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(conn);
        let response = sender.send_request(req).await.unwrap();
        let status = response.status();
        let version = response.version();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        (status, headers, body, version)
    }

    fn host_of(url: &str) -> String {
        url.split("://").nth(1).unwrap().to_string()
    }

    async fn get(url: &str, path: &str) -> (StatusCode, http::HeaderMap, Vec<u8>) {
        let host = host_of(url);
        let stream = TcpStream::connect(&host).await.unwrap();
        let req = Request::get(path)
            .header("host", &host)
            .body(Full::default())
            .unwrap();
        let (s, h, b, _) = send(stream, req).await;
        (s, h, b)
    }

    async fn tls_stream(
        url: &str,
        cert: &OriginCertificate,
        alpn: &[&[u8]],
    ) -> tokio_rustls::client::TlsStream<TcpStream> {
        let mut roots = rustls::RootCertStore::empty();
        for c in rustls_pemfile::certs(&mut cert.ca_pem.as_slice()) {
            roots.add(c.unwrap()).unwrap();
        }
        let mut config =
            rustls::ClientConfig::builder_with_provider(super::super::tls::crypto_provider())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
        config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
        let tcp = TcpStream::connect(host_of(url)).await.unwrap();
        let name = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();
        tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, tcp)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn http_handler_defaults_to_empty_healthz() {
        let server = start_origin_server(http_handler(String::new()), None).await;
        let (status, _, body) = get(&server.http_url, "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_empty(), "GET /healthz body = {body:?}, want empty");
    }

    #[tokio::test]
    async fn http_handler_echoes_request_headers() {
        let server = start_origin_server(http_handler(String::new()), None).await;
        // Long enough to catch a truncated value, the size of a JWT.
        let token = "eyJhbGciOiJFUzI1NiJ9.".repeat(64);
        let host = host_of(&server.http_url);
        let req = Request::get("/echoheaders")
            .header("host", &host)
            .header("authorization", "Bearer placeholder")
            .header("X-Actor-Token", &token)
            .header("X-Repeated", "one")
            .header("X-Repeated", "two")
            .body(Full::default())
            .unwrap();
        let (status, headers, body, _) = send(TcpStream::connect(&host).await.unwrap(), req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers.get("content-type").unwrap(), "application/json");
        let echoed: EchoedHeaders = serde_json::from_slice(&body).unwrap();
        for (name, want) in [
            ("Authorization", "Bearer placeholder"),
            ("X-Actor-Token", token.as_str()),
            ("X-Repeated", "one,two"),
        ] {
            assert_eq!(
                echoed.headers.get(name).map(String::as_str),
                Some(want),
                "echoed {name}"
            );
        }
        assert!(!echoed.headers.contains_key("X-Absent"));
    }

    #[tokio::test]
    async fn https_test_response_uses_http11_and_verified_identity() {
        let cert = write_origin_certificate();
        let server =
            start_origin_server(http_handler("fixture response".into()), Some(&cert)).await;
        // Offer HTTP/2 as well; the origin must pick HTTP/1.1.
        let stream = tls_stream(&server.https_url, &cert, &[b"h2", b"http/1.1"]).await;
        assert_eq!(stream.get_ref().1.alpn_protocol(), Some(&b"http/1.1"[..]));
        let req = Request::get("/healthz")
            .header("host", "127.0.0.1")
            .body(Full::default())
            .unwrap();
        let (status, _, body, version) = send(stream, req).await;
        assert_eq!(version, http::Version::HTTP_11);
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"fixture response");
    }

    #[tokio::test]
    async fn http_handler_fetch_waits_for_full_post_body() {
        let server = start_origin_server(http_handler(String::new()), None).await;
        let host = host_of(&server.http_url);
        let mut conn = TcpStream::connect(&host).await.unwrap();
        conn.write_all(
            format!("POST /fetch HTTP/1.1\r\nHost: {host}\r\nContent-Length: 10\r\n\r\npart1")
                .as_bytes(),
        )
        .await
        .unwrap();
        let mut buf = [0u8; 256];
        let early = tokio::time::timeout(Duration::from_millis(50), conn.read(&mut buf)).await;
        assert!(
            early.is_err(),
            "POST /fetch responded before body EOF: {early:?}"
        );
        conn.write_all(b"part2").await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), conn.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let head = String::from_utf8_lossy(&buf[..n]);
        assert!(
            head.starts_with("HTTP/1.1 200"),
            "POST /fetch answered {head:?}"
        );
    }

    #[tokio::test]
    async fn http_handler_fetch_rejects_get_with_405() {
        let server = start_origin_server(http_handler(String::new()), None).await;
        let (status, _, _) = get(&server.http_url, "/fetch").await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn http_serves_cleartext_beside_tls() {
        let cert = write_origin_certificate();
        let server = start_origin_server(http_handler("both schemes".into()), Some(&cert)).await;
        let (status, _, body) = get(&server.http_url, "/healthz").await;
        assert_eq!(
            (status, body.as_slice()),
            (StatusCode::OK, &b"both schemes"[..])
        );
        let stream = tls_stream(&server.https_url, &cert, &[]).await;
        let req = Request::get("/healthz")
            .header("host", "127.0.0.1")
            .body(Full::default())
            .unwrap();
        let (status, _, body, _) = send(stream, req).await;
        assert_eq!(
            (status, body.as_slice()),
            (StatusCode::OK, &b"both schemes"[..])
        );
    }

    #[tokio::test]
    async fn http_without_certificate_serves_only_cleartext() {
        let server = start_origin_server(http_handler(String::new()), None).await;
        assert!(
            server.https_url.is_empty(),
            "serves HTTPS at {}",
            server.https_url
        );
    }

    async fn get_dial(url: &str, query: &str) -> (StatusCode, DialResult) {
        let (status, _, body) = get(url, &format!("/dial?{query}")).await;
        let result = if status == StatusCode::OK {
            serde_json::from_slice(&body).unwrap()
        } else {
            DialResult::default()
        };
        (status, result)
    }

    #[tokio::test]
    async fn http_dial_reaches_listener() {
        let server = start_origin_server(http_handler(String::new()), None).await;
        let target = listen_loopback().await;
        let address = target.local_addr().unwrap().to_string();
        let (status, result) = get_dial(
            &server.http_url,
            &format!("address={}", address.replace(':', "%3A")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            result.ok && result.error.is_empty(),
            "dial {address} = {result:?}"
        );
        assert_eq!(result.address, address);
    }

    #[tokio::test]
    async fn http_dial_reports_unreachable_address() {
        let server = start_origin_server(http_handler(String::new()), None).await;
        let closed = listen_loopback().await;
        let address = closed.local_addr().unwrap().to_string();
        drop(closed);
        let (status, result) = get_dial(
            &server.http_url,
            &format!("address={}&timeout=1s", address.replace(':', "%3A")),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "an unreachable address is a result, not a request error"
        );
        assert!(
            !result.ok && !result.error.is_empty(),
            "dial {address} = {result:?}"
        );
    }

    #[tokio::test]
    async fn http_dial_rejects_malformed_request() {
        let server = start_origin_server(http_handler(String::new()), None).await;
        for (name, query) in [
            ("missing address", ""),
            ("address w/o port", "address=127.0.0.1"),
            ("empty port", "address=127.0.0.1:"),
            ("bad timeout", "address=127.0.0.1:80&timeout=soon"),
            ("zero timeout", "address=127.0.0.1:80&timeout=0s"),
        ] {
            let (status, _) = get_dial(&server.http_url, query).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{name}: GET /dial?{query}");
        }
    }

    async fn send_connect(url: &str, target: &str) -> u16 {
        let mut conn = TcpStream::connect(host_of(url)).await.unwrap();
        conn.write_all(format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut buf = [0u8; 512];
        let n = tokio::time::timeout(Duration::from_secs(2), conn.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let head = String::from_utf8_lossy(&buf[..n]);
        head.split_whitespace().nth(1).unwrap().parse().unwrap()
    }

    #[tokio::test]
    async fn http_accept_connect_answers_connect() {
        let server =
            start_origin_server(accepting_connect(http_handler(String::new())), None).await;
        assert_eq!(send_connect(&server.http_url, "example.com:443").await, 200);
        // Anything else is still the origin.
        let (status, _, _) = get(&server.http_url, "/healthz").await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn http_refuses_connect_by_default() {
        let server = start_origin_server(http_handler(String::new()), None).await;
        let status = send_connect(&server.http_url, "example.com:443").await;
        assert_ne!(status / 100, 2, "CONNECT status = {status}, want no tunnel");
    }

    #[test]
    fn canonical_names() {
        assert_eq!(canonical_header_name("x-actor-token"), "X-Actor-Token");
        assert_eq!(canonical_header_name("authorization"), "Authorization");
    }

    #[test]
    fn query_decoding() {
        assert_eq!(
            url_pairs("address=127.0.0.1%3A80&timeout=1s&x=a+b"),
            vec![
                ("address".into(), "127.0.0.1:80".into()),
                ("timeout".into(), "1s".into()),
                ("x".into(), "a b".into())
            ]
        );
    }
}
