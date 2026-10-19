//! Tunnels to Actors through the gate: an HTTP CONNECT over mutually
//! authenticated TLS, naming the target Actor in a header. The gate resumes
//! a suspended Actor before it answers.

use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::Arc;

use hivekeep_core::resource::ActorRef;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

use crate::install::TARGET_ACTOR_HEADER;

/// A bidirectional byte stream.
pub trait Conn: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Conn for T {}

/// A boxed [`Conn`].
pub type BoxedConn = Box<dyn Conn>;

/// Opens connections to a port of an Actor.
pub trait ActorDialer: Send + Sync + 'static {
    /// Connects to `port` on `actor`.
    fn dial(
        &self,
        actor: &ActorRef,
        port: u16,
    ) -> impl Future<Output = io::Result<BoxedConn>> + Send;
}

/// Most of a rejection's body included in the error.
const MAX_ERROR_BODY: usize = 4 << 10;

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::other(msg.into())
}

/// Dials the gate's CONNECT-over-TLS listener.
pub struct GateDialer {
    addr: String,
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

impl GateDialer {
    /// A dialer for the gate at `addr`, verifying its certificate against
    /// `ca_file` and the name `server_name`, and presenting the client
    /// certificate in `cert_file` with the key in `key_file`.
    pub fn new(
        addr: &str,
        ca_file: &Path,
        server_name: &str,
        cert_file: &Path,
        key_file: &Path,
    ) -> io::Result<Self> {
        let read =
            |p: &Path| std::fs::read(p).map_err(|e| invalid(format!("read {}: {e}", p.display())));
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(&read(ca_file)?) {
            let cert = cert.map_err(|e| invalid(format!("parse {}: {e}", ca_file.display())))?;
            roots
                .add(cert)
                .map_err(|e| invalid(format!("load {}: {e}", ca_file.display())))?;
        }
        if roots.is_empty() {
            return Err(invalid(format!(
                "{} holds no certificates",
                ca_file.display()
            )));
        }
        let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&read(cert_file)?)
            .collect::<Result<_, _>>()
            .map_err(|e| invalid(format!("parse {}: {e}", cert_file.display())))?;
        if chain.is_empty() {
            return Err(invalid(format!(
                "{} holds no certificates",
                cert_file.display()
            )));
        }
        let key = PrivateKeyDer::from_pem_slice(&read(key_file)?)
            .map_err(|e| invalid(format!("parse {}: {e}", key_file.display())))?;
        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
        let config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| invalid(e.to_string()))?
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .map_err(|e| invalid(format!("client certificate: {e}")))?;
        let server_name = ServerName::try_from(server_name.to_string())
            .map_err(|e| invalid(format!("invalid server name {server_name:?}: {e}")))?;
        Ok(GateDialer {
            addr: addr.to_string(),
            connector: TlsConnector::from(Arc::new(config)),
            server_name,
        })
    }
}

impl ActorDialer for GateDialer {
    async fn dial(&self, actor: &ActorRef, port: u16) -> io::Result<BoxedConn> {
        let tcp = TcpStream::connect(&self.addr)
            .await
            .map_err(|e| invalid(format!("connecting to the gate's CONNECT listener: {e}")))?;
        let tls = self
            .connector
            .connect(self.server_name.clone(), tcp)
            .await
            .map_err(|e| {
                invalid(format!(
                    "TLS handshake with the gate's CONNECT listener: {e}"
                ))
            })?;
        connect_actor(tls, actor, port).await
    }
}

/// Sends a CONNECT for `actor:port` over `stream` and returns the tunnel.
/// Bytes the gate sent past its response head are kept.
pub async fn connect_actor<S>(stream: S, actor: &ActorRef, port: u16) -> io::Result<BoxedConn>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let destination = format!("{}:{port}", actor.name);
    let mut stream = BufReader::new(stream);
    let request = format!(
        "CONNECT {destination} HTTP/1.1\r\nHost: {destination}\r\n{TARGET_ACTOR_HEADER}: {actor}\r\n\r\n"
    );
    stream
        .get_mut()
        .write_all(request.as_bytes())
        .await
        .map_err(|e| invalid(format!("writing CONNECT request: {e}")))?;
    stream.get_mut().flush().await?;

    let mut status_line = String::new();
    stream
        .read_line(&mut status_line)
        .await
        .map_err(|e| invalid(format!("reading CONNECT response: {e}")))?;
    let mut parts = status_line.trim_end().splitn(3, ' ');
    let (proto, code, reason) = (parts.next(), parts.next(), parts.next().unwrap_or(""));
    let code: u16 = match (proto, code.and_then(|c| c.parse().ok())) {
        (Some(p), Some(c)) if p.starts_with("HTTP/") => c,
        _ => {
            return Err(invalid(format!(
                "reading CONNECT response: malformed status line {:?}",
                status_line.trim_end()
            )))
        }
    };
    let mut content_length = 0usize;
    loop {
        let mut header = String::new();
        let n = stream
            .read_line(&mut header)
            .await
            .map_err(|e| invalid(format!("reading CONNECT response: {e}")))?;
        let h = header.trim_end();
        if n == 0 || h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
    }
    if !(200..300).contains(&code) {
        let mut body = vec![0u8; content_length.min(MAX_ERROR_BODY)];
        let _ = stream.read_exact(&mut body).await;
        let message = String::from_utf8_lossy(&body).trim().to_string();
        let status = format!("{code} {reason}").trim().to_string();
        let message = if message.is_empty() {
            status.clone()
        } else {
            message
        };
        return Err(invalid(format!(
            "gate rejected CONNECT to {destination} with {status}: {message}"
        )));
    }
    Ok(Box::new(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    async fn serve_once(mut server: tokio::io::DuplexStream, response: &'static str) -> String {
        let mut buf = vec![0u8; 1024];
        let mut req = Vec::new();
        while !req.ends_with(b"\r\n\r\n") {
            let n = server.read(&mut buf).await.unwrap();
            req.extend_from_slice(&buf[..n]);
        }
        server.write_all(response.as_bytes()).await.unwrap();
        // Echo what follows.
        let n = server.read(&mut buf).await.unwrap();
        server.write_all(&buf[..n]).await.unwrap();
        String::from_utf8(req).unwrap()
    }

    #[tokio::test]
    async fn tunnel_keeps_early_bytes() {
        let (client, server) = duplex(4096);
        let srv = tokio::spawn(serve_once(
            server,
            "HTTP/1.1 200 Connection established\r\n\r\nhello ",
        ));
        let mut conn = connect_actor(client, &ActorRef::new("team", "a1"), 8080)
            .await
            .unwrap();
        conn.write_all(b"ping").await.unwrap();
        let mut got = vec![0u8; 10];
        conn.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"hello ping");
        let req = srv.await.unwrap();
        assert!(req.starts_with("CONNECT a1:8080 HTTP/1.1\r\n"), "{req}");
        assert!(req.contains("Host: a1:8080\r\n"));
        assert!(req.contains("hk-target-actor: team/a1\r\n"));
    }

    #[tokio::test]
    async fn rejection_carries_the_body() {
        let (client, server) = duplex(4096);
        tokio::spawn(serve_once(
            server,
            "HTTP/1.1 403 Forbidden\r\nContent-Length: 13\r\n\r\nnot permitted",
        ));
        let err = connect_actor(client, &ActorRef::new("team", "a1"), 80)
            .await
            .err()
            .unwrap();
        assert_eq!(
            err.to_string(),
            "gate rejected CONNECT to a1:80 with 403 Forbidden: not permitted"
        );
    }

    #[tokio::test]
    async fn rejection_without_body_uses_status() {
        let (client, server) = duplex(4096);
        tokio::spawn(serve_once(server, "HTTP/1.1 502 Bad Gateway\r\n\r\n"));
        let err = connect_actor(client, &ActorRef::new("team", "a1"), 80)
            .await
            .err()
            .unwrap();
        assert!(
            err.to_string()
                .ends_with("502 Bad Gateway: 502 Bad Gateway"),
            "{err}"
        );
    }
}
