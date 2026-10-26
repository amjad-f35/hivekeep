//! The dialing side: present the pod-identity certificate and verify the
//! server against the trust bundle as it is at each handshake, either by
//! DNS name or, for peers dialed by pod IP whose certificates carry no DNS
//! or IP SAN, by chain plus SPIFFE ID.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use hivekeep_identity::cred_loader::{CredentialBundleLoader, TrustAnchorLoader};
use hyper_util::rt::TokioIo;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{ResolvesClientCert, WebPkiServerVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::ParsedCertificate;
use rustls::sign::CertifiedKey;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use tonic::transport::{Channel, Endpoint};

use super::peer_id::verify_peer_id;
use super::{certified_key, provider, root_store, TlsError};

/// How the server's certificate is checked beyond its chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerCheck {
    /// The certificate must be valid for this DNS name.
    ServerName(String),
    /// The certificate must carry this SPIFFE ID; no name is checked.
    SpiffeId(String),
}

struct ReloadingVerifier {
    roots: TrustAnchorLoader,
    check: PeerCheck,
    provider: Arc<CryptoProvider>,
}

impl fmt::Debug for ReloadingVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReloadingVerifier")
            .field("roots", &self.roots)
            .field("check", &self.check)
            .finish_non_exhaustive()
    }
}

fn general(e: impl fmt::Display) -> rustls::Error {
    rustls::Error::General(e.to_string())
}

impl ServerCertVerifier for ReloadingVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let anchors = self.roots.get().map_err(general)?;
        let roots = Arc::new(root_store(&anchors).map_err(general)?);
        match &self.check {
            PeerCheck::ServerName(name) => {
                let name = ServerName::try_from(name.clone()).map_err(general)?;
                WebPkiServerVerifier::builder_with_provider(roots, self.provider.clone())
                    .build()
                    .map_err(general)?
                    .verify_server_cert(end_entity, intermediates, &name, ocsp_response, now)
            }
            PeerCheck::SpiffeId(id) => {
                let cert = ParsedCertificate::try_from(end_entity)?;
                rustls::client::verify_server_cert_signed_by_trust_anchor(
                    &cert,
                    &roots,
                    intermediates,
                    now,
                    self.provider.signature_verification_algorithms.all,
                )?;
                verify_peer_id(&[end_entity.as_ref()], id).map_err(rustls::Error::General)?;
                Ok(ServerCertVerified::assertion())
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

struct BundleClientCert {
    bundle: CredentialBundleLoader,
    provider: Arc<CryptoProvider>,
}

impl fmt::Debug for BundleClientCert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BundleClientCert")
            .field("bundle", &self.bundle)
            .finish_non_exhaustive()
    }
}

impl ResolvesClientCert for BundleClientCert {
    fn resolve(&self, _hints: &[&[u8]], _schemes: &[SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        match self
            .bundle
            .get()
            .map_err(TlsError::from)
            .and_then(|b| certified_key(&b, &self.provider))
        {
            Ok(k) => Some(Arc::new(k)),
            Err(err) => {
                tracing::warn!(%err, "Client credential bundle unavailable");
                None
            }
        }
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// Builds a TLS 1.3 client configuration presenting the bundle at
/// `client_bundle` and checking the server against `trust_bundle` with
/// `check`. The trust bundle is read once now so a missing projection
/// fails startup; afterwards it is re-read per handshake.
pub fn client_config(
    client_bundle: &str,
    trust_bundle: &str,
    check: PeerCheck,
) -> Result<Arc<ClientConfig>, TlsError> {
    let roots = TrustAnchorLoader::trust_anchors(trust_bundle);
    roots.get()?;
    let provider = provider();
    let verifier = Arc::new(ReloadingVerifier {
        roots,
        check,
        provider: provider.clone(),
    });
    let mut cfg = ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| TlsError::Config(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_cert_resolver(Arc::new(BundleClientCert {
            bundle: CredentialBundleLoader::credential_bundle(client_bundle),
            provider,
        }));
    cfg.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(cfg))
}

/// A tonic connector dialing TCP and handshaking with a fixed rustls
/// configuration.
#[derive(Clone)]
pub struct Connector {
    tls: TlsConnector,
}

impl Connector {
    /// A connector over `config`.
    pub fn new(config: Arc<ClientConfig>) -> Connector {
        Connector {
            tls: TlsConnector::from(config),
        }
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl tower::Service<http::Uri> for Connector {
    type Response = TokioIo<TlsStream<TcpStream>>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let tls = self.tls.clone();
        Box::pin(async move {
            let host = uri
                .host()
                .ok_or("target has no host")?
                .trim_matches(['[', ']'])
                .to_string();
            let port = uri.port_u16().unwrap_or(443);
            let tcp = TcpStream::connect((host.as_str(), port)).await?;
            let name = match host.parse::<std::net::IpAddr>() {
                Ok(ip) => ServerName::IpAddress(ip.into()),
                Err(_) => ServerName::try_from(host.clone())?,
            };
            let stream = tls.connect(name, tcp).await?;
            Ok(TokioIo::new(stream))
        })
    }
}

/// A lazily connected channel to `addr` (`host:port`) over `config`. The
/// first call connects, so the peer need not be up yet.
pub fn lazy_channel(addr: &str, config: Arc<ClientConfig>) -> Result<Channel, TlsError> {
    let ep = Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| TlsError::Config(format!("{addr}: {e}")))?;
    Ok(ep.connect_with_connector_lazy(Connector::new(config)))
}
