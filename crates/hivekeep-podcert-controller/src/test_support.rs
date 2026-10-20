//! Helpers shared by the unit tests.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use hivekeep_identity::ca::{Ca, CaPool};
use hivekeep_identity::key::KeyAlgorithm;
use hivekeep_k8s::cert_api::CertVersion;
use hivekeep_k8s::fake::FakeApiServer;
use hivekeep_k8s::informer::InformerEvent;
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::pod_certificate::PcrClient;
use crate::rendezvous::Hasher;

/// Plural of PodCertificateRequest.
pub const PCR_RESOURCE: &str = "podcertificaterequests";

/// The current time, whole seconds (certificates keep no more).
pub fn start_time() -> DateTime<Utc> {
    DateTime::from_timestamp(Utc::now().timestamp(), 0).unwrap()
}

/// A fresh Ed25519 key, as a workload would make.
pub fn subject_key() -> rcgen::KeyPair {
    rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap()
}

/// A stub CSR for `key` whose subject and names must be ignored by every
/// signer.
pub fn stub_csr(key: &rcgen::KeyPair) -> Vec<u8> {
    let mut params = rcgen::CertificateParams::new(vec!["attacker".to_string()]).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "attacker");
    params.serialize_request(key).unwrap().der().to_vec()
}

/// An Ed25519 CA valid for a year.
pub fn test_ca(id: &str) -> Ca {
    Ca::generate(id, KeyAlgorithm::Ed25519, Duration::from_secs(365 * 86400)).unwrap()
}

/// A pool signing with the first of `cas`.
pub fn pool(cas: &[Ca]) -> Arc<CaPool> {
    Arc::new(CaPool::new(cas.to_vec(), ""))
}

/// A request client against `fake`, which serves requests at v1beta1
/// only.
pub async fn beta_client(fake: &FakeApiServer) -> Arc<PcrClient> {
    fake.serve(
        &CertVersion::V1beta1.api_version(),
        &[(PCR_RESOURCE, "PodCertificateRequest", true)],
    );
    let client = PcrClient::new(fake.client()).await.unwrap();
    assert_eq!(client.version(), CertVersion::V1beta1);
    Arc::new(client)
}

/// The next informer event, failing after five seconds.
pub async fn next_event<K>(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<InformerEvent<K>>,
) -> InformerEvent<K> {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("informer event in time")
        .expect("informer open")
}

/// Waits (up to five seconds) for `cond`.
pub async fn wait_until(cond: impl Fn() -> bool) {
    for _ in 0..500 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached in time");
}

/// Checks that `leaf_der` is signed by `root_der` and returns the parsed
/// leaf.
pub fn verified_leaf<'a>(leaf_der: &'a [u8], root_der: &[u8]) -> X509Certificate<'a> {
    let (_, leaf) = X509Certificate::from_der(leaf_der).unwrap();
    let (_, root) = X509Certificate::from_der(root_der).unwrap();
    leaf.verify_signature(Some(root.public_key()))
        .expect("leaf verifies against the CA root");
    assert_eq!(leaf.issuer(), root.subject());
    assert!(!leaf.is_ca());
    leaf
}

/// A hasher that assigns everything to this replica, or nothing.
pub struct FixedHasher(pub bool);

impl Hasher for FixedHasher {
    fn assigned_to_this_replica(&self, _: &str) -> bool {
        self.0
    }
}

/// The SubjectPublicKeyInfo DER of `key`.
pub fn spki(key: &rcgen::KeyPair) -> Vec<u8> {
    rcgen::PublicKeyData::subject_public_key_info(key)
}
