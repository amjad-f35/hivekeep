//! Mutual TLS to a CSI controller service with the pod identity.
//!
//! The client presents its pod identity credential bundle and verifies the
//! driver's serving certificate against the service DNS trust bundle. Both
//! files are projected by the kubelet and rotate on disk, so neither is
//! read once: every handshake looks them up again through a cache that
//! re-parses a file only when it changed. A rotated trust bundle or client
//! certificate therefore takes effect on the next connection without
//! rebuilding the plugin.

use std::path::PathBuf;
use std::sync::Arc;

use hivekeep_identity::cred_bundle::CredentialBundle;
use hivekeep_identity::cred_loader::{CredentialBundleLoader, TrustAnchorLoader};
use parking_lot::Mutex;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{ResolvesClientCert, WebPkiServerVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::sign::CertifiedKey;
use rustls::{DigitallySignedStruct, ProtocolVersion, SignatureScheme};

use super::driver_config::CsiDriverConfig;

/// The pod identity credential bundle (key and chain) the client presents.
pub const DEFAULT_CLIENT_CERT_PATH: &str =
    "/run/podidentity.podcert.hivekeep.io/credential-bundle.pem";
/// The service DNS trust bundle the driver's certificate must chain to.
pub const DEFAULT_CA_CERT_PATH: &str = "/run/servicedns.podcert.hivekeep.io/trust-bundle.pem";

/// Where the client credentials and trust bundle are read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsPaths {
    /// The credential bundle presented to the driver.
    pub client_cert: PathBuf,
    /// The trust bundle the driver is verified against.
    pub ca_cert: PathBuf,
}

impl Default for TlsPaths {
    fn default() -> Self {
        TlsPaths {
            client_cert: DEFAULT_CLIENT_CERT_PATH.into(),
            ca_cert: DEFAULT_CA_CERT_PATH.into(),
        }
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// A resolved controller TLS configuration.
#[derive(Debug, Clone)]
pub struct ControllerTls {
    server_name: String,
    ca: Arc<TrustAnchorLoader>,
    client: Arc<CredentialBundleLoader>,
}

impl ControllerTls {
    /// The only protocol version offered.
    pub const MIN_VERSION: ProtocolVersion = ProtocolVersion::TLSv1_3;
    /// ALPN protocols: gRPC runs over HTTP/2.
    pub const ALPN: &'static [&'static [u8]] = &[b"h2"];

    /// The name the driver's certificate must carry.
    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    /// The trust bundle cache.
    pub fn ca_cache(&self) -> &TrustAnchorLoader {
        &self.ca
    }

    /// The rustls client configuration: TLS 1.3 only, ALPN h2, the pod
    /// identity presented and the driver verified against the trust bundle
    /// as it is on disk at each handshake.
    pub fn client_config(&self) -> Result<Arc<rustls::ClientConfig>, String> {
        let mut cfg = rustls::ClientConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| e.to_string())?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(TrustBundleVerifier {
                ca: self.ca.clone(),
                cached: Mutex::new(None),
            }))
            .with_client_cert_resolver(Arc::new(BundleResolver {
                loader: self.client.clone(),
                cached: Mutex::new(None),
            }));
        cfg.alpn_protocols = Self::ALPN.iter().map(|p| p.to_vec()).collect();
        Ok(Arc::new(cfg))
    }

    /// The TLS server name to send and verify.
    pub fn rustls_server_name(&self) -> Result<ServerName<'static>, String> {
        ServerName::try_from(self.server_name.clone())
            .map_err(|e| format!("invalid tls.serverName {:?}: {e}", self.server_name))
    }
}

/// The controller TLS configuration of `cfg`: `None` when TLS is off.
/// Only pod identity credentials are supported, and a server name is
/// required. The trust bundle must be readable now, though it is read again
/// on every handshake.
pub fn resolve_tls_config(
    cfg: Option<&CsiDriverConfig>,
    paths: &TlsPaths,
) -> Result<Option<ControllerTls>, String> {
    let Some(tls) = cfg.and_then(|c| c.tls.as_ref()).filter(|t| t.enabled) else {
        return Ok(None);
    };
    if !tls.use_pod_identity {
        return Err("only pod identity TLS is supported in this configuration".into());
    }
    if tls.server_name.is_empty() {
        return Err("tls.serverName is required when TLS is enabled".into());
    }
    let ca = Arc::new(TrustAnchorLoader::trust_anchors(&paths.ca_cert));
    ca.get()
        .map_err(|e| format!("failed to load CA cert pool from {:?}: {e}", paths.ca_cert))?;
    Ok(Some(ControllerTls {
        server_name: tls.server_name.clone(),
        ca,
        client: Arc::new(CredentialBundleLoader::credential_bundle(
            &paths.client_cert,
        )),
    }))
}

/// Trust anchors and the verifier built from them.
type VerifierCache = Option<(Arc<Vec<Vec<u8>>>, Arc<WebPkiServerVerifier>)>;

/// Verifies the server against the trust bundle as currently on disk. The
/// webpki verifier is rebuilt only when the cached anchors change.
#[derive(Debug)]
struct TrustBundleVerifier {
    ca: Arc<TrustAnchorLoader>,
    cached: Mutex<VerifierCache>,
}

impl TrustBundleVerifier {
    fn current(&self) -> Result<Arc<WebPkiServerVerifier>, rustls::Error> {
        let anchors = self.ca.get().map_err(|e| {
            rustls::Error::General(format!(
                "failed to load CA cert pool from {}: {e}",
                self.ca.path().display()
            ))
        })?;
        let mut cached = self.cached.lock();
        if let Some((a, v)) = cached.as_ref() {
            if Arc::ptr_eq(a, &anchors) {
                return Ok(v.clone());
            }
        }
        let mut roots = rustls::RootCertStore::empty();
        for der in anchors.iter() {
            roots.add(CertificateDer::from(der.clone())).map_err(|e| {
                rustls::Error::General(format!("trust bundle {}: {e}", self.ca.path().display()))
            })?;
        }
        let v = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider())
            .build()
            .map_err(|e| rustls::Error::General(e.to_string()))?;
        *cached = Some((anchors, v.clone()));
        Ok(v)
    }
}

impl ServerCertVerifier for TrustBundleVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.current()?
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
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
            &provider().signature_verification_algorithms,
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
            &provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Presents the credential bundle as currently on disk.
#[derive(Debug)]
struct BundleResolver {
    loader: Arc<CredentialBundleLoader>,
    cached: Mutex<Option<(Arc<CredentialBundle>, Arc<CertifiedKey>)>>,
}

impl ResolvesClientCert for BundleResolver {
    fn resolve(&self, _: &[&[u8]], _: &[SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        let bundle = match self.loader.get() {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(path = %self.loader.path().display(), err = %e, "Cannot load the CSI client credential bundle");
                return None;
            }
        };
        let mut cached = self.cached.lock();
        if let Some((b, k)) = cached.as_ref() {
            if Arc::ptr_eq(b, &bundle) {
                return Some(k.clone());
            }
        }
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(bundle.key.pkcs8_der().to_vec()));
        let signer = rustls::crypto::ring::sign::any_supported_type(&key).ok()?;
        let chain = bundle
            .chain
            .iter()
            .map(|d| CertificateDer::from(d.clone()))
            .collect();
        let certified = Arc::new(CertifiedKey::new(chain, signer));
        *cached = Some((bundle, certified.clone()));
        Some(certified)
    }

    fn has_certs(&self) -> bool {
        true
    }
}
