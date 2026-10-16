//! mTLS dials to node keepers.
//!
//! Keepers serve with a pod-identity certificate whose URI SAN is the
//! keeper's SPIFFE ID and which names no DNS or IP SAN, so the usual host
//! name check does not apply. [`KeeperServerVerifier`] instead verifies the
//! chain against the pod-identity CA, requires serverAuth, and requires the
//! expected SPIFFE ID. The control plane presents its own credential bundle,
//! re-read on every handshake so a rotated certificate is picked up without
//! a restart.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_identity::cred_bundle::{load_trust_anchors, CredentialBundle};
use hivekeep_proto::keeper::v1::keeper_client::KeeperClient;
use parking_lot::RwLock;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::ResolvesClientCert;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::sign::CertifiedKey;
use rustls::{DigitallySignedStruct, SignatureScheme};
use tonic::transport::{Channel, Endpoint};

use crate::error::{status, Result};
use crate::keeper_dialer::{Keeper, KeeperDialer};
use crate::peer_cert;

/// Verifies a keeper's serving certificate by SPIFFE ID.
#[derive(Debug)]
pub struct KeeperServerVerifier {
    roots: Vec<CertificateDer<'static>>,
    expected_id: String,
    expected_pod_uid: Option<String>,
    provider: Arc<CryptoProvider>,
}

impl KeeperServerVerifier {
    /// Also requires the certificate's PodIdentity to name pod `uid`.
    pub fn with_pod_uid(mut self, uid: impl Into<String>) -> Self {
        self.expected_pod_uid = Some(uid.into());
        self
    }

    /// A verifier trusting `roots` (DER) and accepting only `expected_id`.
    pub fn new(
        roots: Vec<Vec<u8>>,
        expected_id: impl Into<String>,
        provider: Arc<CryptoProvider>,
    ) -> Self {
        KeeperServerVerifier {
            roots: roots.into_iter().map(CertificateDer::from).collect(),
            expected_id: expected_id.into(),
            expected_pod_uid: None,
            provider,
        }
    }

    /// Checks a chain, leaf first, at `now`.
    pub fn verify_chain(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> std::result::Result<(), String> {
        let anchors: Vec<_> = self
            .roots
            .iter()
            .filter_map(|r| webpki::anchor_from_trusted_cert(r).ok())
            .collect();
        if anchors.is_empty() {
            return Err("no usable trust anchors".into());
        }
        let ee = webpki::EndEntityCert::try_from(end_entity)
            .map_err(|e| format!("parsing server certificate: {e}"))?;
        ee.verify_for_usage(
            self.provider.signature_verification_algorithms.all,
            &anchors,
            intermediates,
            now,
            webpki::KeyUsage::server_auth(),
            None,
            None,
        )
        .map_err(|e| format!("verifying server certificate chain: {e}"))?;
        match peer_cert::first_uri_san(end_entity) {
            Some(id) if id == self.expected_id => {}
            Some(id) => {
                return Err(format!(
                    "server SPIFFE ID {id:?} does not match expected {:?}",
                    self.expected_id
                ))
            }
            None => return Err("server certificate carries no SPIFFE ID".into()),
        }
        if let Some(want) = &self.expected_pod_uid {
            use hivekeep_identity::extension::{IdentityExtension, PodIdentity};
            match PodIdentity::from_certificate(end_entity) {
                Ok(Some(p)) if &p.pod_uid == want => {}
                Ok(Some(p)) => {
                    return Err(format!(
                        "pod UID {:?} does not match expected {want:?}",
                        p.pod_uid
                    ))
                }
                Ok(None) => {
                    return Err(format!(
                    "server certificate has no PodIdentity extension, expected pod UID {want:?}"
                ))
                }
                Err(e) => return Err(format!("failed to parse PodIdentity extension: {e}")),
            }
        }
        Ok(())
    }
}

impl ServerCertVerifier for KeeperServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        self.verify_chain(end_entity, intermediates, now)
            .map(|_| ServerCertVerified::assertion())
            .map_err(rustls::Error::General)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
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
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
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

/// A client certificate re-read from a credential bundle on every handshake.
#[derive(Debug)]
pub struct ReloadingClientCert {
    path: PathBuf,
    provider: Arc<CryptoProvider>,
}

impl ReloadingClientCert {
    /// Reads `path` on each use.
    pub fn new(path: impl Into<PathBuf>, provider: Arc<CryptoProvider>) -> Self {
        ReloadingClientCert {
            path: path.into(),
            provider,
        }
    }

    /// Loads the bundle now.
    pub fn load(&self) -> std::result::Result<Arc<CertifiedKey>, String> {
        let bundle = CredentialBundle::load(&self.path).map_err(|e| e.to_string())?;
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(bundle.key.pkcs8_der().to_vec()));
        let signing = self
            .provider
            .key_provider
            .load_private_key(key)
            .map_err(|e| format!("loading client key: {e}"))?;
        let chain = bundle.chain.into_iter().map(CertificateDer::from).collect();
        Ok(Arc::new(CertifiedKey::new(chain, signing)))
    }
}

impl ResolvesClientCert for ReloadingClientCert {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        match self.load() {
            Ok(k) => Some(k),
            Err(e) => {
                tracing::error!(path = %self.path.display(), error = %e, "cannot load keeper client credentials");
                None
            }
        }
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// Builds the TLS client config for keeper dials.
pub fn client_config(
    server_ca_path: &str,
    client_bundle_path: &str,
    keeper_spiffe_id: &str,
) -> std::result::Result<rustls::ClientConfig, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let roots = load_trust_anchors(server_ca_path).map_err(|e| e.to_string())?;
    let verifier = KeeperServerVerifier::new(roots, keeper_spiffe_id, provider.clone());
    let resolver = Arc::new(ReloadingClientCert::new(
        client_bundle_path,
        provider.clone(),
    ));
    // Fail at startup rather than on the first dial.
    resolver.load()?;
    config_with(provider, verifier, resolver)
}

/// A client config over a verifier and client certificate.
pub fn config_with(
    provider: Arc<CryptoProvider>,
    verifier: KeeperServerVerifier,
    resolver: Arc<ReloadingClientCert>,
) -> std::result::Result<rustls::ClientConfig, String> {
    let verifier = Arc::new(verifier);
    let mut cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| e.to_string())?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_cert_resolver(resolver);
    cfg.alpn_protocols = vec![b"h2".to_vec()];
    Ok(cfg)
}

/// Dials keepers over mTLS at an address derived from the node name.
pub struct TlsKeeperDialer {
    address_template: String,
    tls: Arc<rustls::ClientConfig>,
    channels: RwLock<HashMap<String, Channel>>,
}

impl TlsKeeperDialer {
    /// A dialer reaching node `n` at `address_template` with `{node}`
    /// replaced by `n` (e.g. `{node}:7443`).
    pub fn new(address_template: impl Into<String>, tls: rustls::ClientConfig) -> Self {
        TlsKeeperDialer {
            address_template: address_template.into(),
            tls: Arc::new(tls),
            channels: RwLock::new(HashMap::new()),
        }
    }

    fn address(&self, node: &str) -> String {
        self.address_template.replace("{node}", node)
    }
}

#[async_trait]
impl KeeperDialer for TlsKeeperDialer {
    async fn dial(&self, node: &str) -> Result<Keeper> {
        if node.is_empty() {
            return Err(crate::error::Error::NoKeeperOnNode(String::new()));
        }
        if let Some(ch) = self.channels.read().get(node) {
            return Ok(KeeperClient::new(ch.clone()));
        }
        let channel = tls_channel(&self.address(node), self.tls.clone())?;
        self.channels
            .write()
            .insert(node.to_string(), channel.clone());
        Ok(KeeperClient::new(channel))
    }

    async fn node_gone(&self, _node: &str) -> Result<bool> {
        Ok(false)
    }
}

/// A lazily connected channel to `addr` (host:port) over `tls`.
pub fn tls_channel(addr: &str, tls: Arc<rustls::ClientConfig>) -> Result<Channel> {
    {
        let connector = tower::service_fn(move |uri: http::Uri| {
            let tls = tls.clone();
            async move {
                let io_err = |m: String| std::io::Error::new(std::io::ErrorKind::InvalidInput, m);
                let host = uri
                    .host()
                    .ok_or_else(|| io_err("keeper address has no host".into()))?;
                let host = host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .to_string();
                let port = uri
                    .port_u16()
                    .ok_or_else(|| io_err("keeper address has no port".into()))?;
                let tcp = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
                let name = ServerName::try_from(host).map_err(|e| io_err(e.to_string()))?;
                let stream = tokio_rustls::TlsConnector::from(tls)
                    .connect(name, tcp)
                    .await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        });
        Ok(Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| status::internal(format!("invalid keeper address {addr:?}: {e}")))?
            .connect_with_connector_lazy(connector))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_identity::ca::{Ca, CaPool, CertificateAuthority, LeafSpec};
    use hivekeep_identity::key::{KeyAlgorithm, PrivateKey};
    use std::time::{Duration, SystemTime};

    fn leaf(pool: &CaPool, uri: &str, server: bool) -> Vec<u8> {
        let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
        let now = SystemTime::now();
        let mut spec = LeafSpec::new(
            now - Duration::from_secs(60),
            now + Duration::from_secs(3600),
        );
        spec.uris.push(uri.into());
        spec.server_auth = server;
        spec.client_auth = true;
        pool.sign(&spec, &key.public_key()).unwrap().remove(0)
    }

    #[test]
    fn verifies_spiffe_id_and_chain() {
        let ca = Ca::generate("a", KeyAlgorithm::EcdsaP256, Duration::from_secs(86400)).unwrap();
        let root = ca.root_der().to_vec();
        let pool = CaPool::new(vec![ca], "");
        let other = CaPool::new(
            vec![Ca::generate("b", KeyAlgorithm::EcdsaP256, Duration::from_secs(86400)).unwrap()],
            "",
        );
        let want = "spiffe://cluster.local/ns/hivekeep-system/sa/keeper";
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let v = KeeperServerVerifier::new(vec![root], want, provider);
        let now = UnixTime::now();
        let ok = leaf(&pool, want, true);
        assert!(v.verify_chain(&CertificateDer::from(ok), &[], now).is_ok());
        let wrong_id = leaf(&pool, "spiffe://cluster.local/ns/x/sa/y", true);
        assert!(v
            .verify_chain(&CertificateDer::from(wrong_id), &[], now)
            .unwrap_err()
            .contains("does not match"));
        let foreign = leaf(&other, want, true);
        assert!(v
            .verify_chain(&CertificateDer::from(foreign), &[], now)
            .is_err());
        let client_only = leaf(&pool, want, false);
        assert!(v
            .verify_chain(&CertificateDer::from(client_only), &[], now)
            .is_err());
    }
}
