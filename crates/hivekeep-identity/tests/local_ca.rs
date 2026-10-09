//! Self-signed CAs and the pools that hold them.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hivekeep_identity::ca::{Ca, CaPool, CertificateAuthority};
use hivekeep_identity::key::{KeyAlgorithm, KeyKind, PrivateKey};
use hivekeep_identity::pem;
use hivekeep_identity::state_file::RefreshingCaPool;
use x509_parser::prelude::{FromDer, X509Certificate};

const YEAR: Duration = Duration::from_secs(365 * 24 * 3600);

fn secs(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

#[test]
fn generate_ed25519_ca() {
    let before = secs(SystemTime::now());
    let ca = Ca::generate("test-ca", KeyAlgorithm::Ed25519, YEAR).unwrap();
    let after = secs(SystemTime::now()) + 1;

    assert_eq!(ca.id(), "test-ca");
    let key = PrivateKey::from_pem(ca.key_pem().as_bytes()).unwrap();
    assert_eq!(key.kind(), KeyKind::Ed25519);

    let (_, cert) = X509Certificate::from_der(ca.root_der()).unwrap();
    assert!(cert.is_ca());
    let bc = cert
        .basic_constraints()
        .unwrap()
        .expect("basic constraints");
    assert!(bc.value.ca);
    let ku = cert.key_usage().unwrap().expect("key usage").value;
    assert!(ku.digital_signature() && ku.key_cert_sign());
    assert_eq!(
        ku.flags,
        (1 << 0) | (1 << 5),
        "only digitalSignature and keyCertSign"
    );

    let not_before = cert.validity().not_before.timestamp();
    assert!(
        (before..=after).contains(&not_before),
        "not_before {not_before} outside [{before}, {after}]"
    );
    assert_eq!(
        cert.validity().not_after.timestamp() - not_before,
        YEAR.as_secs() as i64
    );
    cert.verify_signature(None)
        .expect("self-signed verification");
}

#[test]
fn tls_material_pem() {
    let ca = Ca::generate("test-ca", KeyAlgorithm::Ed25519, YEAR).unwrap();

    let blocks = pem::decode_all(ca.root_pem().as_bytes()).unwrap();
    assert_eq!(blocks.len(), 1, "unexpected additional data");
    assert_eq!(blocks[0].label, "CERTIFICATE");
    assert_eq!(blocks[0].der, ca.root_der());

    let blocks = pem::decode_all(ca.key_pem().as_bytes()).unwrap();
    assert_eq!(blocks.len(), 1, "unexpected additional data");
    assert_eq!(blocks[0].label, "PRIVATE KEY");
    let key = PrivateKey::from_pkcs8_der(&blocks[0].der).expect("PKCS#8");
    let (_, root) = X509Certificate::from_der(ca.root_der()).unwrap();
    assert_eq!(
        root.public_key().raw,
        key.public_key().spki_der(),
        "key does not match the issuing certificate"
    );
}

#[test]
fn marshal_unmarshal_round_trip() {
    let ca1 = Ca::generate("1", KeyAlgorithm::Ed25519, YEAR).unwrap();
    let ca2 = Ca::generate("2", KeyAlgorithm::Ed25519, YEAR).unwrap();
    let pool = CaPool::new(vec![ca1, ca2], "");
    let restored = CaPool::from_json(&pool.to_json().unwrap()).unwrap();
    let parts = |p: &CaPool| {
        p.cas()
            .iter()
            .map(|c| (c.id().to_string(), c.root_der().to_vec(), c.key_pem()))
            .collect::<Vec<_>>()
    };
    assert_eq!(parts(&restored), parts(&pool));
    assert_eq!(restored.to_json().unwrap(), pool.to_json().unwrap());
    // No designated CA: the first one signs.
    assert_eq!(restored.active().unwrap().id(), "1");
}

#[test]
fn refreshing_pool() {
    let pool1 = CaPool::new(
        vec![Ca::generate("1", KeyAlgorithm::Ed25519, YEAR).unwrap()],
        "1",
    );
    let pool2 = CaPool::new(
        vec![Ca::generate("2", KeyAlgorithm::Ed25519, YEAR).unwrap()],
        "2",
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pool.json");
    std::fs::write(&path, pool1.to_json().unwrap()).unwrap();

    let period = Duration::from_millis(100);
    let refreshing = RefreshingCaPool::with_period(&path, period).unwrap();
    assert_eq!(
        refreshing.trust_anchors().unwrap(),
        pool1.trust_anchors().unwrap()
    );

    // Write pool2; it is served once the cache period passes.
    std::fs::write(&path, pool2.to_json().unwrap()).unwrap();
    std::thread::sleep(period + Duration::from_millis(20));
    assert_eq!(
        refreshing.trust_anchors().unwrap(),
        pool2.trust_anchors().unwrap()
    );
}
