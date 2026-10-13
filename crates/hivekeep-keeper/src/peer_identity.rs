//! Who is calling: the pod identity and SPIFFE ID carried in a client's
//! TLS certificate, and the mTLS server configuration that verifies it.

use serde::{Deserialize, Serialize};
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo, UdsConnectInfo};
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic::{Request, Status};
use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};

/// The certificate extension holding a pod's identity, as the node's pod
/// certificate issuer writes it: the identity extension arc plus 1. It must
/// match the issuer's; overridable with `--pod-identity-oid`.
pub const DEFAULT_POD_IDENTITY_OID: &str = "1.3.6.1.4.1.32473.1.1";

/// The Kubernetes pod a certificate was issued to.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PodIdentity {
    /// The pod's namespace.
    pub namespace: String,
    /// The pod's service account.
    pub service_account_name: String,
    /// The service account's UID.
    #[serde(rename = "ServiceAccountUID")]
    pub service_account_uid: String,
    /// The pod's name.
    pub pod_name: String,
    /// The pod's UID.
    #[serde(rename = "PodUID")]
    pub pod_uid: String,
    /// The node the pod is bound to.
    pub node_name: String,
    /// The node's UID.
    #[serde(rename = "NodeUID")]
    pub node_uid: String,
}

impl PodIdentity {
    fn validate(&self) -> Result<(), String> {
        let empty: Vec<&str> = [
            ("Namespace", &self.namespace),
            ("ServiceAccountName", &self.service_account_name),
            ("ServiceAccountUID", &self.service_account_uid),
            ("PodName", &self.pod_name),
            ("PodUID", &self.pod_uid),
            ("NodeName", &self.node_name),
            ("NodeUID", &self.node_uid),
        ]
        .into_iter()
        .filter(|(_, v)| v.is_empty())
        .map(|(k, _)| k)
        .collect();
        if empty.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "pod identity has empty fields: {}",
                empty.join(", ")
            ))
        }
    }
}

/// Reads the pod identity extension `oid` of a DER certificate. `Ok(None)`
/// when the certificate has none; more than one is an error.
pub fn pod_identity_from_der(der: &[u8], oid: &str) -> Result<Option<PodIdentity>, String> {
    let (_, cert) =
        X509Certificate::from_der(der).map_err(|e| format!("while parsing certificate: {e}"))?;
    let mut found = None;
    for ext in cert.extensions() {
        if ext.oid.to_id_string() != oid {
            continue;
        }
        if found.is_some() {
            return Err("certificate contains multiple pod identity extensions".into());
        }
        found = Some(ext.value.to_vec());
    }
    let Some(v) = found else { return Ok(None) };
    let id: PodIdentity = serde_json::from_slice(&v)
        .map_err(|e| format!("while decoding the pod identity extension: {e}"))?;
    id.validate()?;
    Ok(Some(id))
}

/// The SPIFFE ID among a DER certificate's URI SANs.
pub fn spiffe_id_from_der(der: &[u8]) -> Result<String, String> {
    let (_, cert) =
        X509Certificate::from_der(der).map_err(|e| format!("while parsing certificate: {e}"))?;
    let san = cert
        .subject_alternative_name()
        .map_err(|e| e.to_string())?
        .ok_or("certificate has no subject alternative names")?;
    let uris: Vec<&str> = san
        .value
        .general_names
        .iter()
        .filter_map(|n| match n {
            GeneralName::URI(u) if u.starts_with("spiffe://") => Some(*u),
            _ => None,
        })
        .collect();
    match uris.as_slice() {
        [one] => Ok(one.to_string()),
        [] => Err("certificate has no SPIFFE ID".into()),
        _ => Err("certificate has more than one SPIFFE ID".into()),
    }
}

/// The DER leaf certificate the peer presented, over TCP or a Unix socket.
pub fn peer_leaf<T>(req: &Request<T>) -> Option<Vec<u8>> {
    let certs = req
        .extensions()
        .get::<TlsConnectInfo<TcpConnectInfo>>()
        .and_then(|i| i.peer_certs())
        .or_else(|| {
            req.extensions()
                .get::<TlsConnectInfo<UdsConnectInfo>>()
                .and_then(|i| i.peer_certs())
        })?;
    certs.first().map(|c| c.as_ref().to_vec())
}

/// Authenticates calls by their client certificate.
pub trait PeerAuthenticator: Send + Sync + 'static {
    /// The calling worker pod's identity.
    fn worker_identity(&self, leaf: Option<&[u8]>) -> Result<PodIdentity, Status>;
}

/// Reads the worker's pod identity from its certificate.
#[derive(Debug, Clone)]
pub struct CertPeerAuthenticator {
    oid: String,
}

impl CertPeerAuthenticator {
    /// An authenticator reading extension `oid`.
    pub fn new(oid: impl Into<String>) -> Self {
        CertPeerAuthenticator { oid: oid.into() }
    }
}

impl PeerAuthenticator for CertPeerAuthenticator {
    fn worker_identity(&self, leaf: Option<&[u8]>) -> Result<PodIdentity, Status> {
        let leaf = leaf.ok_or_else(|| Status::unauthenticated("missing peer certificate"))?;
        match pod_identity_from_der(leaf, &self.oid) {
            Ok(Some(id)) => Ok(id),
            _ => Err(Status::permission_denied("invalid worker identity")),
        }
    }
}

/// An interceptor admitting only clients whose verified certificate carries
/// exactly the SPIFFE ID `allowed`.
pub fn require_spiffe_id(
    allowed: String,
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |req: Request<()>| {
        let leaf = peer_leaf(&req)
            .ok_or_else(|| Status::unauthenticated("verified client certificate is required"))?;
        let got = spiffe_id_from_der(&leaf).map_err(|e| {
            Status::permission_denied(format!("client certificate has no valid SPIFFE ID: {e}"))
        })?;
        if got != allowed {
            return Err(Status::permission_denied(format!(
                "client SPIFFE ID {got:?} is not authorized, want {allowed:?}"
            )));
        }
        Ok(req)
    }
}

/// An interceptor admitting only workers on `node`: a pod certificate on
/// any other node, or none, is refused.
pub fn require_same_node(
    node: PodIdentity,
    oid: String,
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |req: Request<()>| {
        check_same_node(&node, peer_leaf(&req).as_deref(), &oid)?;
        Ok(req)
    }
}

/// Admits the DER client certificate `leaf` only if its pod identity names
/// `node`, by name and UID: a replacement node of the same name is another
/// node.
pub fn check_same_node(node: &PodIdentity, leaf: Option<&[u8]>, oid: &str) -> Result<(), Status> {
    let leaf = leaf.ok_or_else(|| Status::unauthenticated("worker certificate is required"))?;
    let id = pod_identity_from_der(leaf, oid)
        .map_err(|e| Status::permission_denied(format!("parse worker pod identity: {e}")))?
        .ok_or_else(|| Status::permission_denied("worker certificate has no pod identity"))?;
    if id.node_name != node.node_name || id.node_uid != node.node_uid {
        return Err(Status::permission_denied(format!(
            "worker is not on node {:?} ({})",
            node.node_name, node.node_uid
        )));
    }
    Ok(())
}

/// A TLS 1.3 server config presenting the credential bundle (certificate
/// chain and key in one PEM file) and requiring client certificates that
/// chain to `client_ca_pem`. The files are read once, at startup.
pub fn server_tls_config(
    cred_bundle_pem: &[u8],
    client_ca_pem: &[u8],
) -> Result<ServerTlsConfig, String> {
    if rustls_pemfile::certs(&mut std::io::BufReader::new(client_ca_pem))
        .flatten()
        .next()
        .is_none()
    {
        return Err("client CA bundle holds no certificate".into());
    }
    Ok(ServerTlsConfig::new()
        .identity(Identity::from_pem(cred_bundle_pem, cred_bundle_pem))
        .client_ca_root(Certificate::from_pem(client_ca_pem)))
}

/// The pod identity in the credential bundle's leaf certificate.
pub fn bundle_pod_identity(cred_bundle_pem: &[u8], oid: &str) -> Result<PodIdentity, String> {
    let leaf = rustls_pemfile::certs(&mut std::io::BufReader::new(cred_bundle_pem))
        .flatten()
        .next()
        .ok_or("credential bundle holds no certificate")?;
    pod_identity_from_der(leaf.as_ref(), oid)?
        .ok_or_else(|| "credential bundle has no pod identity".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_identity_json_names() {
        let id = PodIdentity {
            namespace: "ns".into(),
            service_account_name: "sa".into(),
            service_account_uid: "sau".into(),
            pod_name: "p".into(),
            pod_uid: "pu".into(),
            node_name: "n".into(),
            node_uid: "nu".into(),
        };
        let j = serde_json::to_string(&id).unwrap();
        for k in [
            "\"Namespace\"",
            "\"ServiceAccountUID\"",
            "\"PodUID\"",
            "\"NodeUID\"",
            "\"PodName\"",
        ] {
            assert!(j.contains(k), "{j}");
        }
        assert!(id.validate().is_ok());
        assert!(PodIdentity::default().validate().is_err());
    }

    #[test]
    fn garbage_certificates_are_refused() {
        assert!(pod_identity_from_der(b"junk", DEFAULT_POD_IDENTITY_OID).is_err());
        assert!(spiffe_id_from_der(b"junk").is_err());
        let a = CertPeerAuthenticator::new(DEFAULT_POD_IDENTITY_OID);
        assert_eq!(
            a.worker_identity(None).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            a.worker_identity(Some(b"junk")).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn interceptors_refuse_plaintext_callers() {
        let f = require_spiffe_id("spiffe://hivekeep.io/ns/hivekeep-system/sa/api".into());
        assert_eq!(
            f(Request::new(())).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        let g = require_same_node(PodIdentity::default(), DEFAULT_POD_IDENTITY_OID.into());
        assert_eq!(
            g(Request::new(())).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn tls_config_needs_a_ca() {
        assert!(server_tls_config(b"", b"").is_err());
    }
}

#[cfg(test)]
#[path = "peer_identity_tls_tests.rs"]
mod tls_tests;
