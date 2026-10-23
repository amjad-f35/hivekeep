//! Wire-level HTTP the suite speaks itself: reading an HTTP/1.1 response
//! off a raw tunnel, and opening TLS and HTTP/2 connections to the router
//! with a protocol of the test's choosing. The library's router client
//! speaks HTTP/1.1 only, and these tests are about controlling the protocol.

use std::time::Duration;

use bytes::Bytes;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::ingressauth::router_rustls_server_name;
use hivekeep_e2e::router_client::RouterClient;
use http_body_util::Empty;
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

/// A parsed HTTP/1.1 response.
#[derive(Debug, Clone)]
pub struct WireResponse {
    /// The status code.
    pub status: u16,
    /// Header names (lowercased) and values, in order.
    pub headers: Vec<(String, String)>,
    /// The decoded body.
    pub body: Vec<u8>,
}

impl WireResponse {
    /// The first value of header `name`.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The body as lossy text.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

async fn read_line<R: AsyncBufRead + Unpin>(r: &mut R) -> Result<String, String> {
    let mut line = String::new();
    let n = r.read_line(&mut line).await.map_err(|e| e.to_string())?;
    if n == 0 {
        return Err("unexpected EOF".into());
    }
    Ok(line.trim_end_matches(['\r', '\n']).to_string())
}

/// Reads one HTTP/1.1 response: its head, then a Content-Length, chunked,
/// or read-to-EOF body.
pub async fn read_response<R: AsyncBufRead + Unpin>(r: &mut R) -> Result<WireResponse, String> {
    let status_line = read_line(r).await?;
    let mut parts = status_line.splitn(3, ' ');
    let (proto, code) = (parts.next().unwrap_or(""), parts.next());
    let status = match code.and_then(|c| c.parse::<u16>().ok()) {
        Some(c) if proto.starts_with("HTTP/") => c,
        _ => return Err(format!("malformed status line {status_line:?}")),
    };
    let mut headers = Vec::new();
    loop {
        let line = read_line(r).await?;
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
    }
    let get = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    let mut body = Vec::new();
    if get("transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked")) {
        loop {
            let size_line = read_line(r).await?;
            let size = usize::from_str_radix(size_line.split(';').next().unwrap_or("").trim(), 16)
                .map_err(|e| format!("bad chunk size {size_line:?}: {e}"))?;
            if size == 0 {
                // Trailers, if any, end with an empty line.
                while !read_line(r).await?.is_empty() {}
                break;
            }
            let mut chunk = vec![0; size];
            r.read_exact(&mut chunk).await.map_err(|e| e.to_string())?;
            body.extend_from_slice(&chunk);
            read_line(r).await?;
        }
    } else if let Some(len) = get("content-length").and_then(|v| v.parse::<usize>().ok()) {
        body.resize(len, 0);
        r.read_exact(&mut body).await.map_err(|e| e.to_string())?;
    } else if !(status == 204 || status == 304 || (100..200).contains(&status)) {
        r.read_to_end(&mut body).await.map_err(|e| e.to_string())?;
    }
    Ok(WireResponse {
        status,
        headers,
        body,
    })
}

/// Sends `GET /` with `Connection: close` through `conn` and reads the
/// answer, all within 10 s.
pub async fn request_tunneled<S: AsyncRead + AsyncWrite + Unpin>(
    conn: &mut S,
    host: &str,
) -> Result<WireResponse, String> {
    tokio::time::timeout(Duration::from_secs(10), async {
        conn.write_all(
            format!("GET / HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .map_err(|e| format!("writing tunneled request: {e}"))?;
        conn.flush()
            .await
            .map_err(|e| format!("writing tunneled request: {e}"))?;
        read_response(&mut BufReader::new(conn))
            .await
            .map_err(|e| format!("reading tunneled response: {e}"))
    })
    .await
    .map_err(|_| "tunneled request timed out".to_string())?
}

/// Sends `GET /` through `tunnel`, leaving it open, and returns the body of
/// a 200 answer.
pub async fn get_through_tunnel<S: AsyncRead + AsyncWrite + Unpin>(
    t: &T,
    tunnel: &mut S,
    host: &str,
) -> String {
    let work = async {
        tunnel
            .write_all(format!("GET / HTTP/1.1\r\nHost: {host}\r\n\r\n").as_bytes())
            .await
            .unwrap_or_else(|e| t.fatal(format!("writing a request through the tunnel: {e}")));
        tunnel
            .flush()
            .await
            .unwrap_or_else(|e| t.fatal(format!("writing a request through the tunnel: {e}")));
        // One request, one response with a length: buffering past it is
        // harmless because nothing else is read from this tunnel afterwards.
        read_response(&mut BufReader::new(&mut *tunnel))
            .await
            .unwrap_or_else(|e| t.fatal(format!("reading the response through the tunnel: {e}")))
    };
    let resp = tokio::time::timeout(Duration::from_secs(30), work)
        .await
        .unwrap_or_else(|_| t.fatal("request through the tunnel timed out"));
    if resp.status != 200 {
        t.fatal(format!(
            "request through the tunnel returned HTTP {}: {}",
            resp.status,
            resp.text()
        ));
    }
    resp.text()
}

/// Dials `addr` with the router client's TLS identity, offering `alpn`
/// (none when empty), verifying the router's serving certificate.
pub async fn router_tls(
    t: &T,
    router: &RouterClient,
    addr: &str,
    alpn: &[&[u8]],
) -> TlsStream<TcpStream> {
    let mut config = router
        .tls_config()
        .unwrap_or_else(|| t.fatal("router client has no TLS config"));
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    let tcp = TcpStream::connect(addr)
        .await
        .unwrap_or_else(|e| t.fatal(format!("dialing the router at {addr}: {e}")));
    TlsConnector::from(std::sync::Arc::new(config))
        .connect(router_rustls_server_name(), tcp)
        .await
        .unwrap_or_else(|e| t.fatal(format!("TLS handshake with the router at {addr}: {e}")))
}

/// Starts HTTP/2 on an established connection (prior knowledge when the
/// listener negotiated no ALPN).
pub async fn h2_handshake<S>(t: &T, io: S) -> SendRequest<Empty<Bytes>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io))
            .await
            .unwrap_or_else(|e| t.fatal(format!("starting HTTP/2 with the router: {e}")));
    tokio::spawn(conn);
    sender
}

#[tokio::test]
async fn reads_length_chunked_and_eof_bodies() {
    let mut r = BufReader::new(&b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-A: b\r\n\r\nhello"[..]);
    let resp = read_response(&mut r).await.unwrap();
    assert_eq!(
        (resp.status, resp.text().as_str(), resp.header("x-a")),
        (200, "hello", Some("b"))
    );

    let mut r = BufReader::new(&b"HTTP/1.1 502 Bad Gateway\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n"[..]);
    let resp = read_response(&mut r).await.unwrap();
    assert_eq!((resp.status, resp.text().as_str()), (502, "abcde"));

    let mut r = BufReader::new(&b"HTTP/1.0 200 OK\r\n\r\nuntil eof"[..]);
    assert_eq!(read_response(&mut r).await.unwrap().text(), "until eof");

    let mut r = BufReader::new(&b"garbage\r\n\r\n"[..]);
    assert!(read_response(&mut r).await.is_err());
}

#[tokio::test]
async fn tunneled_request_round_trip() {
    let (mut client, mut server) = tokio::io::duplex(4096);
    let srv = tokio::spawn(async move {
        let mut buf = vec![0; 1024];
        let n = server.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]).into_owned();
        server
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 4\r\n\r\nnope")
            .await
            .unwrap();
        req
    });
    let resp = request_tunneled(&mut client, "actor-1").await.unwrap();
    assert_eq!((resp.status, resp.text().as_str()), (404, "nope"));
    let req = srv.await.unwrap();
    assert!(
        req.starts_with("GET / HTTP/1.1\r\nHost: actor-1\r\nConnection: close\r\n"),
        "{req:?}"
    );
}
