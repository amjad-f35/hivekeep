//! Connecting a worker to its node's keeper over the node-local socket.
//!
//! Both sides present Pod certificates. The keeper's certificate is
//! verified against the trust bundle without a host name (Pod certificates
//! carry SPIFFE IDs, not DNS names); instead it must name the keeper's
//! SPIFFE ID and run on the same node as this worker.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use hivekeep_identity::cred_bundle::CredentialBundle;
use hivekeep_identity::extension::{IdentityExtension, PodIdentity};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{ResolvesClientCert, WebPkiServerVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::sign::CertifiedKey;
use rustls::{CertificateError, DigitallySignedStruct, SignatureScheme};

/// The keeper's SPIFFE ID in a default install.
pub const DEFAULT_KEEPER_SPIFFE_ID: &str =
    "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-keeper";

/// Name used for the TLS session; never checked against the certificate.
const KEEPER_SESSION_NAME: &str = "keeper.local";

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// The Pod identity carried by a DER certificate, if any.
pub fn pod_identity(cert_der: &[u8]) -> Option<PodIdentity> {
    PodIdentity::from_certificate(cert_der).ok().flatten()
}

/// The URI SANs of a DER certificate.
pub fn uri_sans(cert_der: &[u8]) -> Vec<String> {
    let Ok((_, cert)) = x509_parser::parse_x509_certificate(cert_der) else {
        return Vec::new();
    };
    let Ok(Some(san)) = cert.subject_alternative_name() else {
        return Vec::new();
    };
    san.value
        .general_names
        .iter()
        .filter_map(|n| match n {
            x509_parser::extensions::GeneralName::URI(u) => Some(u.to_string()),
            _ => None,
        })
        .collect()
}

fn load_roots(path: &Path) -> Result<rustls::RootCertStore, String> {
    let anchors = hivekeep_identity::cred_bundle::load_trust_anchors(path)
        .map_err(|e| format!("load keeper trust bundle: {e}"))?;
    let mut roots = rustls::RootCertStore::empty();
    for der in anchors {
        roots
            .add(CertificateDer::from(der))
            .map_err(|e| format!("load keeper trust bundle: {e}"))?;
    }
    Ok(roots)
}

/// Presents the worker credential bundle, re-read on every handshake so a
/// rotated bundle is picked up.
#[derive(Debug)]
struct BundleResolver(PathBuf);

impl ResolvesClientCert for BundleResolver {
    fn resolve(&self, _: &[&[u8]], _: &[SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        let bundle = CredentialBundle::load(&self.0).ok()?;
        certified_key(&bundle).ok()
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// A rustls signing key for a credential bundle.
pub fn certified_key(bundle: &CredentialBundle) -> Result<Arc<CertifiedKey>, rustls::Error> {
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(bundle.key.pkcs8_der().to_vec()));
    let signer = rustls::crypto::ring::sign::any_supported_type(&key)?;
    let chain = bundle
        .chain
        .iter()
        .map(|d| CertificateDer::from(d.clone()))
        .collect();
    Ok(Arc::new(CertifiedKey::new(chain, signer)))
}

#[derive(Debug)]
struct KeeperVerifier {
    trust_bundle: PathBuf,
    keeper_spiffe_id: String,
    local: PodIdentity,
}

impl ServerCertVerifier for KeeperVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let roots = load_roots(&self.trust_bundle).map_err(rustls::Error::General)?;
        let webpki = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider())
            .build()
            .map_err(|e| rustls::Error::General(e.to_string()))?;
        match webpki.verify_server_cert(end_entity, intermediates, server_name, ocsp, now) {
            Ok(_)
            | Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => {}
            Err(e) => {
                return Err(rustls::Error::General(format!(
                    "verify keeper certificate: {e}"
                )))
            }
        }
        let uris = uri_sans(end_entity);
        if uris.len() != 1 || uris[0] != self.keeper_spiffe_id {
            return Err(rustls::Error::General(
                "node-local peer is not the keeper".into(),
            ));
        }
        match pod_identity(end_entity) {
            Some(id)
                if id.node_name == self.local.node_name && id.node_uid == self.local.node_uid =>
            {
                Ok(ServerCertVerified::assertion())
            }
            _ => Err(rustls::Error::General(format!(
                "keeper is not on worker node {:?} ({})",
                self.local.node_name, self.local.node_uid
            ))),
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

/// The TLS client config for talking to the keeper.
pub fn tls_config(
    credential_bundle: &Path,
    trust_bundle: &Path,
    keeper_spiffe_id: &str,
) -> Result<Arc<rustls::ClientConfig>, String> {
    if credential_bundle.as_os_str().is_empty()
        || trust_bundle.as_os_str().is_empty()
        || keeper_spiffe_id.is_empty()
    {
        return Err("worker credentials, trust bundle, and keeper SPIFFE ID are required".into());
    }
    let bundle = CredentialBundle::load(credential_bundle)
        .map_err(|e| format!("load worker identity: {e}"))?;
    let local = bundle
        .chain
        .first()
        .and_then(|leaf| pod_identity(leaf))
        .ok_or("worker certificate has no valid Pod identity")?;
    load_roots(trust_bundle)?;
    let verifier = Arc::new(KeeperVerifier {
        trust_bundle: trust_bundle.to_path_buf(),
        keeper_spiffe_id: keeper_spiffe_id.to_string(),
        local,
    });
    let mut cfg = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| e.to_string())?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_cert_resolver(Arc::new(BundleResolver(credential_bundle.to_path_buf())));
    cfg.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(cfg))
}

/// A gRPC channel to the keeper listening on `socket_path`.
pub async fn connect(
    socket_path: &Path,
    tls: Arc<rustls::ClientConfig>,
) -> Result<tonic::transport::Channel, tonic::transport::Error> {
    let path = socket_path.to_path_buf();
    tonic::transport::Endpoint::from_static("http://keeper.local")
        .connect_with_connector(tower::service_fn(move |_: http::Uri| {
            let (path, tls) = (path.clone(), tls.clone());
            async move {
                let stream = tokio::net::UnixStream::connect(&path).await?;
                let name = ServerName::try_from(KEEPER_SESSION_NAME).map_err(io::Error::other)?;
                let tls = tokio_rustls::TlsConnector::from(tls)
                    .connect(name, stream)
                    .await?;
                Ok::<_, io::Error>(hyper_util::rt::TokioIo::new(tls))
            }
        }))
        .await
}

/// The error and its sources, joined.
pub fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut out = err.to_string();
    let mut cur = err.source();
    while let Some(e) = cur {
        let s = e.to_string();
        if !out.contains(&s) {
            out.push_str(": ");
            out.push_str(&s);
        }
        cur = e.source();
    }
    out
}
