//! Reaching the node-local keeper's `WorkerSupport` service: mutual TLS over
//! its unix socket, presenting the worker pod's certificate and accepting only
//! the keeper's SPIFFE identity on the same node.
//!
//! Pod certificates carry a SPIFFE URI and no DNS name, so the server name is
//! not checked; the chain is verified against the trust bundle and the leaf
//! must name the keeper and the worker's own node.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use hivekeep_proto::keeper::v1::worker_support_client::WorkerSupportClient;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, Error as TlsError, SignatureScheme};
use serde::Deserialize;
use tonic::transport::{Channel, Endpoint, Uri};
use x509_parser::prelude::*;

/// The keeper's identity in a default install.
pub const DEFAULT_KEEPER_SPIFFE_ID: &str =
    "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-keeper";
/// The worker pod's projected credential bundle (certificate chain and key).
pub const DEFAULT_CREDENTIAL_BUNDLE: &str =
    "/run/podidentity.podcert.hivekeep.io/credential-bundle.pem";
/// The bundle the keeper's certificate chains to.
pub const DEFAULT_TRUST_BUNDLE: &str = "/run/podidentity.podcert.hivekeep.io/trust-bundle.pem";
/// The X.509 extension carrying a certificate's pod identity.
pub const DEFAULT_POD_IDENTITY_OID: &str = "1.3.6.1.4.1.32473.1.1";
/// How long one dial may take.
pub const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// The node a pod certificate was issued on.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct PodNode {
    /// The node's name.
    #[serde(rename = "NodeName", default)]
    pub node_name: String,
    /// The node's UID.
    #[serde(rename = "NodeUID", default)]
    pub node_uid: String,
}

/// The pod identity extension `oid` of a DER certificate.
pub fn pod_node(der: &[u8], oid: &str) -> Result<Option<PodNode>, String> {
    let (_, cert) =
        X509Certificate::from_der(der).map_err(|e| format!("while parsing certificate: {e}"))?;
    let mut found = None;
    for ext in cert.extensions() {
        if ext.oid.to_id_string() == oid {
            if found.is_some() {
                return Err("certificate contains multiple pod identity extensions".into());
            }
            found = Some(ext.value.to_vec());
        }
    }
    let Some(v) = found else { return Ok(None) };
    let n: PodNode = serde_json::from_slice(&v)
        .map_err(|e| format!("while decoding the pod identity extension: {e}"))?;
    if n.node_name.is_empty() || n.node_uid.is_empty() {
        return Err("pod identity names no node".into());
    }
    Ok(Some(n))
}

/// The URI SANs of a DER certificate.
pub fn uri_sans(der: &[u8]) -> Result<Vec<String>, String> {
    let (_, cert) =
        X509Certificate::from_der(der).map_err(|e| format!("while parsing certificate: {e}"))?;
    let Some(san) = cert.subject_alternative_name().map_err(|e| e.to_string())? else {
        return Ok(vec![]);
    };
    Ok(san
        .value
        .general_names
        .iter()
        .filter_map(|n| match n {
            GeneralName::URI(u) => Some(u.to_string()),
            _ => None,
        })
        .collect())
}

/// Where the keeper is and how to prove and check identities.
#[derive(Debug, Clone)]
pub struct KeeperDialConfig {
    /// The keeper's worker-support socket.
    pub socket: PathBuf,
    /// The worker's credential bundle, re-read on every dial so rotation
    /// needs no restart.
    pub credential_bundle: PathBuf,
    /// The trust bundle.
    pub trust_bundle: PathBuf,
    /// The SPIFFE ID the keeper must present.
    pub keeper_spiffe_id: String,
    /// The pod identity extension.
    pub pod_identity_oid: String,
}

fn load_pem(
    path: &Path,
) -> io::Result<(Vec<CertificateDer<'static>>, Option<PrivateKeyDer<'static>>)> {
    let data = std::fs::read(path)
        .map_err(|e| io::Error::new(e.kind(), format!("reading {}: {e}", path.display())))?;
    let certs = rustls_pemfile::certs(&mut io::BufReader::new(data.as_slice()))
        .collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut io::BufReader::new(data.as_slice()))?;
    Ok((certs, key))
}

/// Checks the keeper's leaf: chained to the trust bundle, the keeper's SPIFFE
/// ID as its only URI, and issued on the worker's node.
#[derive(Debug)]
struct KeeperVerifier {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
    spiffe_id: String,
    node: PodNode,
    oid: String,
}

impl ServerCertVerifier for KeeperVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        // The chain is checked with a placeholder name; only a name mismatch
        // is forgiven, since pod certificates carry no DNS name.
        let name =
            ServerName::try_from("keeper.invalid").map_err(|e| TlsError::General(e.to_string()))?;
        match self
            .inner
            .verify_server_cert(end_entity, intermediates, &name, ocsp, now)
        {
            Ok(_)
            | Err(TlsError::InvalidCertificate(CertificateError::NotValidForName))
            | Err(TlsError::InvalidCertificate(CertificateError::NotValidForNameContext {
                ..
            })) => {}
            Err(e) => return Err(e),
        }
        let uris = uri_sans(end_entity).map_err(TlsError::General)?;
        if uris.len() != 1 || uris[0] != self.spiffe_id {
            return Err(TlsError::General(
                "node-local peer is not the keeper".into(),
            ));
        }
        match pod_node(end_entity, &self.oid) {
            Ok(Some(n)) if n == self.node => Ok(ServerCertVerified::assertion()),
            _ => Err(TlsError::General(format!(
                "keeper is not on worker node {:?} ({})",
                self.node.node_name, self.node.node_uid
            ))),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

impl KeeperDialConfig {
    /// Builds the TLS client config from the current credentials. Missing or
    /// unusable credentials are an error the caller can exit on.
    pub fn tls_config(&self) -> io::Result<rustls::ClientConfig> {
        if self.credential_bundle.as_os_str().is_empty()
            || self.trust_bundle.as_os_str().is_empty()
            || self.keeper_spiffe_id.is_empty()
        {
            return Err(io::Error::other(
                "worker credentials, trust bundle, and keeper SPIFFE ID are required",
            ));
        }
        let (chain, key) = load_pem(&self.credential_bundle)
            .map_err(|e| io::Error::new(e.kind(), format!("load worker identity: {e}")))?;
        let key = key.ok_or_else(|| io::Error::other("load worker identity: no private key"))?;
        let leaf = chain
            .first()
            .ok_or_else(|| io::Error::other("load worker identity: no certificate"))?;
        let node = match pod_node(leaf, &self.pod_identity_oid) {
            Ok(Some(n)) => n,
            _ => {
                return Err(io::Error::other(
                    "worker certificate has no valid Pod identity",
                ))
            }
        };
        let (roots_der, _) = load_pem(&self.trust_bundle)
            .map_err(|e| io::Error::new(e.kind(), format!("load keeper trust bundle: {e}")))?;
        let mut roots = rustls::RootCertStore::empty();
        for r in roots_der {
            roots
                .add(r)
                .map_err(|e| io::Error::other(format!("load keeper trust bundle: {e}")))?;
        }
        if roots.is_empty() {
            return Err(io::Error::other(
                "load keeper trust bundle: no certificates",
            ));
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            provider.clone(),
        )
        .build()
        .map_err(|e| io::Error::other(e.to_string()))?;
        let verifier = Arc::new(KeeperVerifier {
            inner,
            spiffe_id: self.keeper_spiffe_id.clone(),
            node,
            oid: self.pod_identity_oid.clone(),
        });
        let mut cfg = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| io::Error::other(e.to_string()))?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(chain, key)
            .map_err(|e| io::Error::other(format!("load worker identity: {e}")))?;
        cfg.alpn_protocols = vec![b"h2".to_vec()];
        Ok(cfg)
    }

    /// Dials the keeper and returns a `WorkerSupport` client over the
    /// connection.
    pub async fn connect(&self) -> io::Result<WorkerSupportClient<Channel>> {
        let tls = Arc::new(self.tls_config()?);
        let socket = self.socket.clone();
        let connector = tower::service_fn(move |_: Uri| {
            let (socket, tls) = (socket.clone(), tls.clone());
            async move {
                let s = tokio::net::UnixStream::connect(&socket).await?;
                let name = ServerName::try_from("keeper.invalid").map_err(io::Error::other)?;
                let s = tokio_rustls::TlsConnector::from(tls)
                    .connect(name, s)
                    .await?;
                Ok::<_, io::Error>(hyper_util::rt::TokioIo::new(s))
            }
        });
        let ch = Endpoint::from_static("http://keeper.invalid")
            .connect_timeout(DIAL_TIMEOUT)
            .connect_with_connector(connector)
            .await
            .map_err(|e| io::Error::other(format!("dial keeper: {e}")))?;
        Ok(WorkerSupportClient::new(ch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_credentials_fail_fast() {
        let d = tempfile::tempdir().unwrap();
        let cfg = KeeperDialConfig {
            socket: d.path().join("keeper.sock"),
            credential_bundle: d.path().join("does-not-exist.pem"),
            trust_bundle: d.path().join("also-missing.pem"),
            keeper_spiffe_id: DEFAULT_KEEPER_SPIFFE_ID.into(),
            pod_identity_oid: DEFAULT_POD_IDENTITY_OID.into(),
        };
        assert!(cfg.tls_config().is_err());
        let empty = KeeperDialConfig {
            keeper_spiffe_id: String::new(),
            ..cfg
        };
        assert!(empty.tls_config().is_err());
    }
}
