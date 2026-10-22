//! A minimal client for the egress gateway's CONNECT front door: mutual TLS
//! with a credential bundle, then one HTTP/1.1 CONNECT per tunnel.
//!
//! It is deliberately small -- one connection per tunnel, no pooling -- since
//! the egress probe opens a single tunnel per request. What it keeps exact is
//! the error shape: a caller must be able to tell a refused TLS handshake from
//! a declined CONNECT from anything else.

use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use hivekeep_identity::cred_bundle::CredentialBundle;
use hivekeep_identity::pem;

use super::tls::crypto_provider;

/// Longest CONNECT response head accepted.
const MAX_HEAD: usize = 16 << 10;
/// Most of a rejection body kept for the error message.
const MAX_REJECTION_BODY: usize = 4 << 10;

/// What a tunnel attempt failed on.
#[derive(Debug, thiserror::Error)]
pub enum EgressError {
    /// The client could not be built: a bad address, unreadable credentials.
    #[error("egress: {0}")]
    Config(String),
    /// The gateway's front door refused the connection at TLS: it rejected
    /// the client certificate, or its own did not verify.
    #[error("egress: gateway TLS handshake: {0}")]
    GatewayHandshake(String),
    /// The gateway answered the CONNECT with a non-2xx status: the caller
    /// authenticated and was declined anyway, which is what an authorization
    /// denial looks like from here.
    #[error("egress: gateway rejected CONNECT with {status}: {message}")]
    ConnectRejected {
        /// The status code, the part a caller can act on.
        status_code: u16,
        /// The status line after the version, e.g. `403 Forbidden`.
        status: String,
        /// The response body, or the status when the body is empty.
        message: String,
    },
    /// Anything else on the way to an open tunnel: DNS, TCP, a truncated
    /// response.
    #[error("egress: {0}")]
    Tunnel(String),
}

/// Where and how to reach the gateway.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// `host:port` of the CONNECT front door.
    pub gateway_address: String,
    /// The name checked on the gateway's serving certificate.
    pub server_name: String,
    /// The credential bundle (key and chain) presented to the gateway.
    pub credential_bundle: String,
    /// PEM roots the gateway's serving certificate must chain to.
    pub trust_bundle_path: String,
}

/// An egress CONNECT client.
pub struct Client {
    gateway_address: String,
    server_name: ServerName<'static>,
    connector: TlsConnector,
}

impl Client {
    /// Builds a client, reading the credential and trust bundles now.
    pub fn new(cfg: &ClientConfig) -> Result<Self, EgressError> {
        if !cfg.gateway_address.contains(':') {
            return Err(EgressError::Config(format!(
                "invalid egress gateway address {:?}: missing port",
                cfg.gateway_address
            )));
        }
        if cfg.server_name.is_empty() {
            return Err(EgressError::Config(
                "gateway server name is required".into(),
            ));
        }
        let server_name = ServerName::try_from(cfg.server_name.clone()).map_err(|e| {
            EgressError::Config(format!("gateway server name {:?}: {e}", cfg.server_name))
        })?;
        if cfg.trust_bundle_path.is_empty() {
            return Err(EgressError::Config("trust bundle path is required".into()));
        }
        let trust = std::fs::read(&cfg.trust_bundle_path)
            .map_err(|e| EgressError::Config(format!("reading trust bundle: {e}")))?;
        let mut roots = rustls::RootCertStore::empty();
        for der in
            pem::decode_certificates(&trust).map_err(|e| EgressError::Config(e.to_string()))?
        {
            roots
                .add(CertificateDer::from(der))
                .map_err(|e| EgressError::Config(format!("trust bundle: {e}")))?;
        }
        if roots.is_empty() {
            return Err(EgressError::Config(format!(
                "trust bundle {:?} contains no certificates",
                cfg.trust_bundle_path
            )));
        }
        let bundle = CredentialBundle::load(&cfg.credential_bundle)
            .map_err(|e| EgressError::Config(format!("loading credential bundle: {e}")))?;
        let chain = bundle.chain.into_iter().map(CertificateDer::from).collect();
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(bundle.key.pkcs8_der().to_vec()));
        let mut config = rustls::ClientConfig::builder_with_provider(crypto_provider())
            .with_safe_default_protocol_versions()
            .map_err(|e| EgressError::Config(e.to_string()))?
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .map_err(|e| EgressError::Config(format!("client certificate: {e}")))?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Client {
            gateway_address: cfg.gateway_address.clone(),
            server_name,
            connector: TlsConnector::from(Arc::new(config)),
        })
    }

    /// Opens a tunnel to `destination` (`ip:port`).
    pub async fn dial(&self, destination: &str) -> Result<TlsStream<TcpStream>, EgressError> {
        let tcp = TcpStream::connect(&self.gateway_address)
            .await
            .map_err(|e| {
                EgressError::Tunnel(format!("dialing gateway {}: {e}", self.gateway_address))
            })?;
        let mut tls = self
            .connector
            .connect(self.server_name.clone(), tcp)
            .await
            .map_err(|e| EgressError::GatewayHandshake(e.to_string()))?;
        // TLS 1.3 reports a refused client certificate only on the first
        // read, so the CONNECT exchange below can also surface one.
        let request = format!("CONNECT {destination} HTTP/1.1\r\nHost: {destination}\r\n\r\n");
        tls.write_all(request.as_bytes())
            .await
            .map_err(classify_io)?;
        let head = read_head(&mut tls).await?;
        let (status_code, status) = parse_status(&head)?;
        if (200..300).contains(&status_code) {
            return Ok(tls);
        }
        let mut body = read_body(&mut tls, &head).await;
        body.truncate(MAX_REJECTION_BODY);
        let message = String::from_utf8_lossy(&body).trim().to_string();
        Err(EgressError::ConnectRejected {
            status_code,
            message: if message.is_empty() {
                status.clone()
            } else {
                message
            },
            status,
        })
    }
}

fn classify_io(e: std::io::Error) -> EgressError {
    // A TLS alert from the peer after the handshake is the front door
    // refusing the certificate.
    if e.kind() == std::io::ErrorKind::InvalidData || e.to_string().contains("alert") {
        EgressError::GatewayHandshake(e.to_string())
    } else {
        EgressError::Tunnel(e.to_string())
    }
}

/// Reads up to and including the blank line ending the response head, one
/// byte at a time so nothing past it is consumed.
async fn read_head(tls: &mut TlsStream<TcpStream>) -> Result<Vec<u8>, EgressError> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if head.len() > MAX_HEAD {
            return Err(EgressError::Tunnel("CONNECT response head too long".into()));
        }
        match tls.read(&mut byte).await.map_err(classify_io)? {
            0 => {
                return Err(EgressError::Tunnel(
                    "reading CONNECT response: unexpected EOF".into(),
                ))
            }
            _ => head.push(byte[0]),
        }
    }
    Ok(head)
}

fn parse_status(head: &[u8]) -> Result<(u16, String), EgressError> {
    let text = String::from_utf8_lossy(head);
    let line = text.lines().next().unwrap_or_default();
    let mut parts = line.splitn(2, ' ');
    let version = parts.next().unwrap_or_default();
    let status = parts.next().unwrap_or_default().trim().to_string();
    if !version.starts_with("HTTP/") {
        return Err(EgressError::Tunnel(format!(
            "malformed CONNECT response {line:?}"
        )));
    }
    let code = status
        .split(' ')
        .next()
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| EgressError::Tunnel(format!("malformed CONNECT response {line:?}")))?;
    Ok((code, status))
}

async fn read_body(tls: &mut TlsStream<TcpStream>, head: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(head).to_ascii_lowercase();
    let length = text
        .lines()
        .find_map(|l| l.strip_prefix("content-length:"))
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(MAX_REJECTION_BODY)
        .min(MAX_REJECTION_BODY);
    let mut body = vec![0u8; length];
    let mut filled = 0;
    let read = async {
        while filled < length {
            match tls.read(&mut body[filled..]).await {
                Ok(0) | Err(_) => break,
                Ok(n) => filled += n,
            }
        }
    };
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), read).await;
    body.truncate(filled);
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_lines() {
        assert_eq!(
            parse_status(b"HTTP/1.1 403 Forbidden\r\n\r\n").unwrap(),
            (403, "403 Forbidden".into())
        );
        assert_eq!(
            parse_status(b"HTTP/1.1 200\r\n\r\n").unwrap(),
            (200, "200".into())
        );
        assert!(matches!(
            parse_status(b"garbage\r\n\r\n"),
            Err(EgressError::Tunnel(_))
        ));
    }

    #[test]
    fn config_errors() {
        let cfg = ClientConfig {
            gateway_address: "gate-egress".into(),
            server_name: "gate-egress".into(),
            credential_bundle: "/nonexistent".into(),
            trust_bundle_path: "/nonexistent".into(),
        };
        assert!(matches!(Client::new(&cfg), Err(EgressError::Config(_))));
        let cfg = ClientConfig {
            gateway_address: "gate-egress:443".into(),
            ..cfg
        };
        assert!(
            matches!(Client::new(&cfg), Err(EgressError::Config(m)) if m.contains("trust bundle"))
        );
    }
}
