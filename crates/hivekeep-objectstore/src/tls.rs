//! TLS for the plugin's TCP connections: clients that re-read their CA and
//! certificate files on every handshake, so rotation needs no restart, and a
//! server certificate resolver that does the same. Everything uses the ring
//! provider explicitly.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use hivekeep_identity::cred_bundle::{load_trust_anchors, CredentialBundle};
use hyper_util::rt::TokioIo;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tonic::transport::{Channel, Endpoint};

/// The crypto provider every TLS config here uses.
pub fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Installs ring as the process default, for binaries that pull more than
/// one provider. Safe to call twice.
pub fn install_default_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// A TLS setup error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct TlsError(pub String);

fn roots(ca_file: &Path) -> Result<rustls::RootCertStore, TlsError> {
    let anchors = load_trust_anchors(ca_file)
        .map_err(|e| TlsError(format!("while loading the CA bundle: {e}")))?;
    let mut roots = rustls::RootCertStore::empty();
    for der in anchors {
        roots
            .add(CertificateDer::from(der))
            .map_err(|e| TlsError(format!("CA bundle {}: {e}", ca_file.display())))?;
    }
    Ok(roots)
}

fn certified(
    bundle: &Path,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TlsError> {
    let b = CredentialBundle::load(bundle)
        .map_err(|e| TlsError(format!("while loading {}: {e}", bundle.display())))?;
    let chain = b.chain.into_iter().map(CertificateDer::from).collect();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(b.key.pkcs8_der().to_vec()));
    Ok((chain, key))
}

/// A client config verifying the server against `ca_file`, presenting the
/// credential bundle `client_bundle` when set, speaking HTTP/2.
pub fn client_config(
    ca_file: &Path,
    client_bundle: Option<&Path>,
) -> Result<rustls::ClientConfig, TlsError> {
    let builder = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| TlsError(e.to_string()))?
        .with_root_certificates(roots(ca_file)?);
    let mut cfg = match client_bundle {
        Some(b) => {
            let (chain, key) = certified(b)?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| TlsError(format!("client certificate: {e}")))?
        }
        None => builder.with_no_client_auth(),
    };
    cfg.alpn_protocols = vec![b"h2".to_vec()];
    Ok(cfg)
}

/// Where a TLS client reads its files from on each handshake.
#[derive(Clone)]
pub struct ClientFiles {
    /// The CA bundle verifying the server.
    pub ca_file: PathBuf,
    /// The client's credential bundle, for mTLS.
    pub client_bundle: Option<PathBuf>,
    /// The name expected on the server's certificate.
    pub server_name: String,
}

impl fmt::Debug for ClientFiles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientFiles")
            .field("ca_file", &self.ca_file)
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

/// A lazily connected gRPC channel to `address` over TLS per `files`. The
/// files are read on every connection, so certificate rotation needs no
/// restart; a handshake failure fails the call with UNAVAILABLE.
pub fn channel(address: &str, files: ClientFiles) -> Result<Channel, TlsError> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))
        .map_err(|e| TlsError(format!("address {address:?}: {e}")))?;
    let address = address.to_string();
    let files = Arc::new(files);
    Ok(
        endpoint.connect_with_connector_lazy(tower::service_fn(move |_| {
            let (address, files) = (address.clone(), files.clone());
            async move {
                let cfg = client_config(&files.ca_file, files.client_bundle.as_deref())
                    .map_err(|e| std::io::Error::other(e.0))?;
                let name = ServerName::try_from(files.server_name.clone())
                    .map_err(|e| std::io::Error::other(format!("server name: {e}")))?;
                let tcp = TcpStream::connect(&address).await?;
                let tls = TlsConnector::from(Arc::new(cfg)).connect(name, tcp).await?;
                Ok::<_, std::io::Error>(TokioIo::new(tls))
            }
        })),
    )
}

/// Serves the certificate in a credential bundle, re-read on every
/// handshake.
#[derive(Debug)]
pub struct ReloadingCert {
    bundle: PathBuf,
}

impl ReloadingCert {
    /// A resolver for `bundle`.
    pub fn new(bundle: &Path) -> Self {
        ReloadingCert {
            bundle: bundle.to_path_buf(),
        }
    }

    /// Loads the certificate now.
    pub fn load(&self) -> Result<Arc<CertifiedKey>, TlsError> {
        let (chain, key) = certified(&self.bundle)?;
        let signer = provider()
            .key_provider
            .load_private_key(key)
            .map_err(|e| TlsError(format!("serving key: {e}")))?;
        Ok(Arc::new(CertifiedKey::new(chain, signer)))
    }
}

impl ResolvesServerCert for ReloadingCert {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        match self.load() {
            Ok(k) => Some(k),
            Err(e) => {
                tracing::warn!(err = %e, "Failed to load the serving certificate");
                None
            }
        }
    }
}

/// A TLS 1.3 server config serving `bundle` without client certificates.
pub fn server_config(bundle: &Path) -> Result<rustls::ServerConfig, TlsError> {
    let resolver = ReloadingCert::new(bundle);
    // A wrong path fails the start rather than the first handshake.
    resolver.load()?;
    let mut cfg = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| TlsError(e.to_string()))?
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(resolver));
    cfg.alpn_protocols = vec![b"h2".to_vec()];
    Ok(cfg)
}

/// A TLS connection accepted by [`serve_incoming`].
pub struct TlsConn(pub tokio_rustls::server::TlsStream<TcpStream>);

impl tonic::transport::server::Connected for TlsConn {
    type ConnectInfo = Option<std::net::SocketAddr>;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.0.get_ref().0.peer_addr().ok()
    }
}

impl tokio::io::AsyncRead for TlsConn {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for TlsConn {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// Accepts TCP connections on `lis` and completes their TLS handshakes;
/// failed handshakes are dropped.
pub fn serve_incoming(
    lis: tokio::net::TcpListener,
    cfg: rustls::ServerConfig,
) -> impl futures::Stream<Item = std::io::Result<TlsConn>> {
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(cfg));
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = lis.accept().await else {
                continue;
            };
            let (acceptor, conn_tx) = (acceptor.clone(), tx.clone());
            tokio::spawn(async move {
                if let Ok(tls) = acceptor.accept(tcp).await {
                    let _ = conn_tx.send(Ok(TlsConn(tls))).await;
                }
            });
            if tx.is_closed() {
                return;
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}
