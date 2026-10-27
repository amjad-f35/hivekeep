//! The throwaway certificates the benchmark processes share, standing in for
//! the Pod identity and Service certificates a cluster projects. One CA signs
//! everything; the fake keeper keeps its key to sign Actor certificates.

use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hivekeep_gate::install;
use hivekeep_identity::ca::{Ca, LeafSpec};
use hivekeep_identity::cred_bundle::CredentialBundle;
use hivekeep_identity::extension::{IdentityExtension, PodIdentity};
use hivekeep_identity::key::{KeyAlgorithm, PrivateKey};
use hivekeep_identity::pem;
use rustls::pki_types::CertificateDer;
use rustls::sign::CertifiedKey;

use crate::{Error, Result};

/// The CA certificate; every peer verifies against it.
pub const CA_FILE: &str = "ca.pem";
/// The CA's private key, for the fake keeper.
pub const CA_KEY_FILE: &str = "ca-key.pem";
/// The worker Pod's credential bundle: the tunnel serves ingress with it
/// and authenticates to the keeper with it.
pub const WORKER_BUNDLE: &str = "worker.pem";
/// The fake keeper's serving certificate.
pub const KEEPER_BUNDLE: &str = "keeper.pem";
/// The client certificate the load generator presents to the tunnel's
/// ingress, carrying the router's SPIFFE ID.
pub const ROUTER_BUNDLE: &str = "router.pem";
/// The fake egress gateway's serving certificate.
pub const GATEWAY_BUNDLE: &str = "gateway.pem";

/// The node the worker and keeper share; the tunnel checks the keeper's
/// certificate names the same node as its own.
pub const NODE_NAME: &str = "bench-node";
/// The UID of [`NODE_NAME`].
pub const NODE_UID: &str = "00000000-0000-4000-8000-000000000001";

/// Shapes the certificates.
#[derive(Debug, Clone)]
pub struct Options {
    /// Names and IPs the tunnel's serving certificate is valid for.
    pub worker_hosts: Vec<String>,
    /// Names and IPs the gateway's certificate is valid for.
    pub gateway_hosts: Vec<String>,
    /// How long every certificate lasts.
    pub validity: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            worker_hosts: Vec::new(),
            gateway_hosts: Vec::new(),
            validity: Duration::from_secs(30 * 24 * 3600),
        }
    }
}

fn pod(namespace: &str, sa: &str, sa_uid: &str, pod: &str, pod_uid: &str) -> PodIdentity {
    PodIdentity {
        namespace: namespace.into(),
        service_account_name: sa.into(),
        service_account_uid: sa_uid.into(),
        pod_name: pod.into(),
        pod_uid: pod_uid.into(),
        node_name: NODE_NAME.into(),
        node_uid: NODE_UID.into(),
    }
}

struct Leaf {
    file: &'static str,
    name: &'static str,
    server: bool,
    client: bool,
    hosts: Vec<String>,
    uri: Option<String>,
    pod: Option<PodIdentity>,
}

/// Writes the CA and the bundles into `dir`, creating it if needed.
pub fn generate(dir: &Path, opts: &Options) -> Result<()> {
    if opts.worker_hosts.is_empty() || opts.gateway_hosts.is_empty() {
        return Err(Error::msg("pki: worker and gateway hosts are required"));
    }
    let validity = if opts.validity.is_zero() {
        Options::default().validity
    } else {
        opts.validity
    };
    std::fs::create_dir_all(dir)?;
    let not_before = SystemTime::now() - Duration::from_secs(60);
    let not_after = not_before + validity;

    let ca = Ca::generate(
        "bench",
        KeyAlgorithm::EcdsaP256,
        validity + Duration::from_secs(60),
    )?;
    std::fs::write(dir.join(CA_FILE), ca.root_pem())?;
    write_private(&dir.join(CA_KEY_FILE), ca.key_pem().as_bytes())?;

    let ns = install::SYSTEM_NAMESPACE;
    let leaves = [
        Leaf {
            file: WORKER_BUNDLE,
            name: "worker",
            server: true,
            client: true,
            hosts: opts.worker_hosts.clone(),
            uri: None,
            pod: Some(pod(
                "bench",
                "worker",
                "00000000-0000-4000-8000-000000000002",
                "worker-0",
                "00000000-0000-4000-8000-000000000003",
            )),
        },
        Leaf {
            file: KEEPER_BUNDLE,
            name: "keeper",
            server: true,
            client: false,
            hosts: Vec::new(),
            uri: Some(install::keeper_spiffe_id(ns)),
            pod: Some(pod(
                ns,
                install::KEEPER_SERVICE_ACCOUNT,
                "00000000-0000-4000-8000-000000000004",
                "keeper-0",
                "00000000-0000-4000-8000-000000000005",
            )),
        },
        Leaf {
            file: ROUTER_BUNDLE,
            name: "router",
            server: false,
            client: true,
            hosts: Vec::new(),
            uri: Some(install::router_spiffe_id(ns)),
            pod: None,
        },
        Leaf {
            file: GATEWAY_BUNDLE,
            name: "egress-gateway",
            server: true,
            client: false,
            hosts: opts.gateway_hosts.clone(),
            uri: None,
            pod: None,
        },
    ];
    for l in leaves {
        let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256)?;
        let mut spec = LeafSpec::new(not_before, not_after);
        spec.common_name = Some(l.name.into());
        spec.server_auth = l.server;
        spec.client_auth = l.client;
        for h in &l.hosts {
            match h.parse::<IpAddr>() {
                Ok(ip) => spec.ip_addresses.push(ip),
                Err(_) => spec.dns_names.push(h.clone()),
            }
        }
        spec.uris.extend(l.uri);
        if let Some(p) = &l.pod {
            spec.extensions.push(p.to_extension()?);
        }
        let der = ca.sign(&spec, &key.public_key())?;
        // Leaf, then CA, then key: the layout of a Pod certificate bundle.
        let mut bundle = pem::encode(pem::CERTIFICATE, &der);
        bundle.push_str(&ca.root_pem());
        bundle.push_str(&key.to_pem());
        std::fs::write(dir.join(l.file), bundle)?;
    }
    Ok(())
}

fn write_private(path: &Path, data: &[u8]) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(data)?;
    Ok(())
}

/// The CA [`generate`] wrote.
pub fn load_ca(dir: &Path) -> Result<Ca> {
    let root = std::fs::read(dir.join(CA_FILE))?;
    let root_der = pem::decode_certificates(&root)?
        .into_iter()
        .next()
        .ok_or_else(|| Error::msg(format!("pki: {CA_FILE} holds no certificate")))?;
    let key = PrivateKey::from_pem(&std::fs::read(dir.join(CA_KEY_FILE))?)?;
    Ok(Ca::from_parts("bench", key, root_der)?)
}

/// The TLS provider every benchmark connection uses.
pub fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// A credential bundle as a rustls signing key with its chain.
pub fn certified_key(path: &Path) -> Result<Arc<CertifiedKey>> {
    let bundle = CredentialBundle::load(path)?;
    Ok(hivekeep_gate::tunnel::keeper_dial::certified_key(&bundle)?)
}

/// A trust bundle as rustls roots.
pub fn roots(path: &Path) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for der in hivekeep_identity::cred_bundle::load_trust_anchors(path)? {
        roots.add(CertificateDer::from(der))?;
    }
    Ok(roots)
}

#[derive(Debug)]
struct FixedClientCert(Arc<CertifiedKey>);

impl rustls::client::ResolvesClientCert for FixedClientCert {
    fn resolve(&self, _: &[&[u8]], _: &[rustls::SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        Some(self.0.clone())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// A TLS client presenting the credential bundle at `bundle`, trusting
/// `ca`, offering `alpn`.
pub fn client_config(
    bundle: &Path,
    ca: &Path,
    alpn: &[&[u8]],
) -> Result<Arc<rustls::ClientConfig>> {
    let cert = certified_key(bundle)?;
    let mut cfg = rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots(ca)?)
        .with_client_cert_resolver(Arc::new(FixedClientCert(cert)));
    cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(Arc::new(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::client::danger::ServerCertVerifier;
    use rustls::client::WebPkiServerVerifier;
    use rustls::pki_types::{ServerName, UnixTime};
    use rustls::server::WebPkiClientVerifier;
    use rustls::CertificateError;

    fn leaf(dir: &Path, file: &str) -> CertificateDer<'static> {
        CertificateDer::from(CredentialBundle::load(dir.join(file)).unwrap().chain[0].clone())
    }

    #[test]
    fn generate_writes_verifiable_bundles() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        generate(
            d,
            &Options {
                worker_hosts: vec!["hk-tunnel-worker".into()],
                gateway_hosts: vec!["hk-tunnel-sink".into()],
                ..Default::default()
            },
        )
        .unwrap();
        let roots = Arc::new(roots(&d.join(CA_FILE)).unwrap());
        let servers = WebPkiServerVerifier::builder_with_provider(roots.clone(), provider())
            .build()
            .unwrap();
        let clients = WebPkiClientVerifier::builder_with_provider(roots, provider())
            .build()
            .unwrap();
        let now = UnixTime::now();
        let ns = install::SYSTEM_NAMESPACE;
        let server_ok = |file: &str, name: &str| {
            let name = ServerName::try_from(name.to_string()).unwrap();
            match servers.verify_server_cert(&leaf(d, file), &[], &name, &[], now) {
                // A wrong name is reported only after the chain and usage passed.
                Ok(_)
                | Err(rustls::Error::InvalidCertificate(
                    CertificateError::NotValidForName
                    | CertificateError::NotValidForNameContext { .. },
                )) => {}
                Err(e) => panic!("{file} does not verify as a server: {e}"),
            }
        };
        let client_ok = |file: &str| {
            clients
                .verify_client_cert(&leaf(d, file), &[], now)
                .unwrap_or_else(|e| panic!("{file} does not verify as a client: {e}"));
        };
        let uri = |file: &str| hivekeep_gate::tunnel::keeper_dial::uri_sans(&leaf(d, file));

        // Worker: serving for its host name, and a client.
        let name = ServerName::try_from("hk-tunnel-worker").unwrap();
        servers
            .verify_server_cert(&leaf(d, WORKER_BUNDLE), &[], &name, &[], now)
            .unwrap();
        client_ok(WORKER_BUNDLE);
        // Keeper: serving, with the keeper's SPIFFE ID.
        server_ok(KEEPER_BUNDLE, "keeper.invalid");
        assert_eq!(uri(KEEPER_BUNDLE), vec![install::keeper_spiffe_id(ns)]);
        // Router: a client with the router's SPIFFE ID.
        client_ok(ROUTER_BUNDLE);
        assert_eq!(uri(ROUTER_BUNDLE), vec![install::router_spiffe_id(ns)]);
        // Gateway: serving for its host name.
        let name = ServerName::try_from("hk-tunnel-sink").unwrap();
        servers
            .verify_server_cert(&leaf(d, GATEWAY_BUNDLE), &[], &name, &[], now)
            .unwrap();

        // The tunnel's own keeper client accepts the pair: the worker
        // certificate carries a Pod identity, and the keeper's names the
        // same node.
        hivekeep_gate::tunnel::keeper_dial::tls_config(
            &d.join(WORKER_BUNDLE),
            &d.join(CA_FILE),
            &install::keeper_spiffe_id(ns),
        )
        .expect("the keeper client rejects the worker certificate");
        let w = hivekeep_gate::tunnel::keeper_dial::pod_identity(&leaf(d, WORKER_BUNDLE)).unwrap();
        let k = hivekeep_gate::tunnel::keeper_dial::pod_identity(&leaf(d, KEEPER_BUNDLE)).unwrap();
        assert_eq!((w.node_name, w.node_uid), (k.node_name, k.node_uid));

        load_ca(d).unwrap();
    }

    #[test]
    fn generate_needs_hosts() {
        let dir = tempfile::tempdir().unwrap();
        assert!(generate(dir.path(), &Options::default()).is_err());
    }
}
