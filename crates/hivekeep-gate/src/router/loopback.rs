//! Reaching sidecars on `localhost`: try 127.0.0.1 then ::1 regardless of
//! what the resolver says, and a tiny HTTP/1.1 client over that dial.

use std::io;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

/// Loopback addresses tried for `localhost`.
pub const LOOPBACK_IPS: &[&str] = &["127.0.0.1", "::1"];

fn join(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Dials `addr` (`host:port`); `localhost` is tried on every loopback
/// address in turn, with every failure reported.
pub async fn dial_loopback(addr: &str) -> io::Result<TcpStream> {
    let parsed = addr
        .rsplit_once(':')
        .map(|(h, p)| (h.trim_start_matches('[').trim_end_matches(']'), p));
    let Some(("localhost", port)) = parsed else {
        return TcpStream::connect(addr).await;
    };
    let mut errs = Vec::new();
    for ip in LOOPBACK_IPS {
        let target = join(ip, port);
        match TcpStream::connect(&target).await {
            Ok(c) => return Ok(c),
            Err(e) => errs.push(format!("dial tcp {target}: {e}")),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::ConnectionRefused,
        errs.join("\n"),
    ))
}

/// Sends one HTTP/1.1 request to `addr` and returns the status and body.
pub async fn http_request(
    addr: &str,
    method: http::Method,
    path: &str,
    timeout: Duration,
) -> Result<(u16, String), String> {
    let fut = async {
        let stream = dial_loopback(addr).await.map_err(|e| e.to_string())?;
        let (mut send, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(conn);
        let req = http::Request::builder()
            .method(method)
            .uri(path)
            .header(http::header::HOST, addr)
            .body(Empty::<Bytes>::new())
            .map_err(|e| e.to_string())?;
        let resp = send.send_request(req).await.map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| e.to_string())?
            .to_bytes();
        Ok((status, String::from_utf8_lossy(&body).into_owned()))
    };
    tokio::time::timeout(timeout, fut)
        .await
        .unwrap_or_else(|_| Err("context deadline exceeded".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn dial_loopback_cases() {
        for (listen, host) in [
            ("127.0.0.1:0", "localhost"),
            ("[::1]:0", "localhost"),
            ("127.0.0.1:0", "127.0.0.1"),
            ("[::1]:0", "::1"),
        ] {
            let Ok(ln) = TcpListener::bind(listen).await else {
                continue; // no such loopback on this host
            };
            let local = ln.local_addr().unwrap();
            let accept =
                tokio::spawn(async move { ln.accept().await.unwrap().0.local_addr().unwrap() });
            let c = dial_loopback(&join(host, &local.port().to_string()))
                .await
                .unwrap_or_else(|e| panic!("{listen}/{host}: {e}"));
            drop(c);
            assert_eq!(accept.await.unwrap(), local, "{listen}/{host}");
        }
    }

    #[tokio::test]
    async fn dial_loopback_nothing_listening() {
        let ln = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = ln.local_addr().unwrap().port().to_string();
        drop(ln);
        let err = dial_loopback(&format!("localhost:{port}"))
            .await
            .unwrap_err()
            .to_string();
        for ip in LOOPBACK_IPS {
            assert!(
                err.contains(&join(ip, &port)),
                "{err} does not mention {ip}"
            );
        }
    }
}
