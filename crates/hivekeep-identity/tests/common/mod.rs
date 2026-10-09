//! Helpers shared by the integration tests: DER building, test keys, a
//! token minter independent of the crate's signer, and a fake issuer.

#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{
    EcdsaKeyPair, EcdsaSigningAlgorithm, KeyPair as _, RsaKeyPair, ECDSA_P256_SHA256_FIXED_SIGNING,
    ECDSA_P384_SHA384_FIXED_SIGNING, ECDSA_P521_SHA512_FIXED_SIGNING, RSA_PKCS1_SHA256,
    RSA_PKCS1_SHA384, RSA_PKCS1_SHA512,
};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hivekeep_identity::jwt::jwks::{parse_key_set, Jwk, JwkSet, KeyWithId};
use hivekeep_identity::jwt::verifier::KeySource;
use hivekeep_identity::key::{KeyAlgorithm, KeyComponents, PrivateKey, PublicKey};
use hivekeep_identity::{IdentityError, Result};
use parking_lot::Mutex;

pub fn b64(b: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(b)
}

pub fn unb64(s: &str) -> Vec<u8> {
    URL_SAFE_NO_PAD
        .decode(s)
        .unwrap_or_else(|e| panic!("decoding {s:?}: {e}"))
}

// ---- DER ----------------------------------------------------------------

pub fn tlv(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = content.len();
    if len < 0x80 {
        out.push(len as u8);
    } else {
        let bytes: Vec<u8> = len
            .to_be_bytes()
            .into_iter()
            .skip_while(|&b| b == 0)
            .collect();
        out.push(0x80 | bytes.len() as u8);
        out.extend(bytes);
    }
    out.extend_from_slice(content);
    out
}

fn der_uint(b: &[u8]) -> Vec<u8> {
    let mut v: Vec<u8> = b.iter().copied().skip_while(|&x| x == 0).collect();
    if v.is_empty() || v[0] & 0x80 != 0 {
        v.insert(0, 0);
    }
    tlv(0x02, &v)
}

const OID_RSA: &[u8] = &[
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01,
];
const OID_EC: &[u8] = &[0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01];
const OID_P256: &[u8] = &[0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07];

/// SubjectPublicKeyInfo of an RSA key, as `openssl pkey -pubin` encodes it.
pub fn rsa_spki(n: &[u8], e: &[u8]) -> PublicKey {
    let alg = tlv(0x30, &[OID_RSA, &[0x05, 0x00]].concat());
    let key = tlv(0x30, &[der_uint(n), der_uint(e)].concat());
    let bits = tlv(0x03, &[&[0u8][..], &key].concat());
    PublicKey::from_spki_der(tlv(0x30, &[alg, bits].concat())).unwrap()
}

/// SubjectPublicKeyInfo of a P-256 key.
pub fn p256_spki(x: &[u8], y: &[u8]) -> PublicKey {
    let alg = tlv(0x30, &[OID_EC, OID_P256].concat());
    let bits = tlv(0x03, &[&[0u8, 4][..], x, y].concat());
    PublicKey::from_spki_der(tlv(0x30, &[alg, bits].concat())).unwrap()
}

// ---- Example keys from the JOSE RFCs -------------------------------------

/// The example RSA key of RFC 7638 section 3.1.
pub const RFC_RSA_N: &str = "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw";
pub const RFC_RSA_E: &str = "AQAB";
/// The P-256 key of RFC 7517 appendix A.1.
pub const RFC_EC_X: &str = "MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4";
pub const RFC_EC_Y: &str = "4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM";

pub fn rfc_rsa_key() -> PublicKey {
    rsa_spki(&unb64(RFC_RSA_N), &unb64(RFC_RSA_E))
}

pub fn rfc_ec_key() -> PublicKey {
    p256_spki(&unb64(RFC_EC_X), &unb64(RFC_EC_Y))
}

/// A P-384 public key, a curve the crate does not publish.
pub fn p384_public_key() -> PublicKey {
    let pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P384_SHA384).unwrap();
    PublicKey::from_spki_der(rcgen::PublicKeyData::subject_public_key_info(&pair)).unwrap()
}

// ---- Token minting --------------------------------------------------------

/// A process-wide 2048-bit RSA key; key generation dominates otherwise.
pub fn rsa_key() -> &'static PrivateKey {
    static KEY: OnceLock<PrivateKey> = OnceLock::new();
    KEY.get_or_init(|| PrivateKey::generate(KeyAlgorithm::Rsa2048).unwrap())
}

/// A freshly generated RSA key.
pub fn new_rsa_key() -> PrivateKey {
    PrivateKey::generate(KeyAlgorithm::Rsa2048).unwrap()
}

/// An EC key on one of the JOSE curves.
pub struct EcKey {
    pub crv: &'static str,
    pkcs8: Vec<u8>,
    alg: &'static EcdsaSigningAlgorithm,
    /// Uncompressed SEC1 point.
    pub point: Vec<u8>,
}

impl EcKey {
    pub fn generate(crv: &'static str) -> Self {
        let alg = match crv {
            "P-256" => &ECDSA_P256_SHA256_FIXED_SIGNING,
            "P-384" => &ECDSA_P384_SHA384_FIXED_SIGNING,
            "P-521" => &ECDSA_P521_SHA512_FIXED_SIGNING,
            other => panic!("unsupported curve {other}"),
        };
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(alg, &SystemRandom::new())
            .unwrap()
            .as_ref()
            .to_vec();
        let point = EcdsaKeyPair::from_pkcs8(alg, &pkcs8)
            .unwrap()
            .public_key()
            .as_ref()
            .to_vec();
        EcKey {
            crv,
            pkcs8,
            alg,
            point,
        }
    }

    /// The JWK of this key; coordinates padded to the field size.
    pub fn jwk(&self, kid: &str) -> Jwk {
        let size = (self.point.len() - 1) / 2;
        Jwk {
            kty: "EC".into(),
            kid: kid.into(),
            crv: self.crv.into(),
            x: b64(&self.point[1..1 + size]),
            y: b64(&self.point[1 + size..]),
            ..Default::default()
        }
    }
}

pub fn rsa_jwk(kid: &str, key: &PublicKey) -> Jwk {
    let KeyComponents::Rsa { n, e } = key.components().unwrap() else {
        panic!("not an RSA key");
    };
    Jwk {
        kty: "RSA".into(),
        kid: kid.into(),
        n: b64(&n),
        e: b64(&e),
        ..Default::default()
    }
}

/// The key a token is signed with.
pub enum SigningKey<'a> {
    Rsa(&'a PrivateKey),
    Ec(&'a EcKey),
}

/// Signs a compact JWT the way a Kubernetes issuer does: RS* with PKCS#1
/// v1.5, ES* with a fixed-width `r || s`. `kid: None` omits the header field.
pub fn mint_jwt(
    alg: &str,
    kid: Option<&str>,
    key: SigningKey<'_>,
    claims: &serde_json::Value,
) -> String {
    let mut header = serde_json::json!({"alg": alg, "typ": "JWT"});
    if let Some(kid) = kid {
        header["kid"] = kid.into();
    }
    let input = format!(
        "{}.{}",
        b64(&serde_json::to_vec(&header).unwrap()),
        b64(&serde_json::to_vec(claims).unwrap())
    );
    let rng = SystemRandom::new();
    let sig = match key {
        SigningKey::Rsa(k) => {
            let pair = RsaKeyPair::from_pkcs8(k.pkcs8_der()).unwrap();
            let padding = match alg {
                "RS256" => &RSA_PKCS1_SHA256,
                "RS384" => &RSA_PKCS1_SHA384,
                "RS512" => &RSA_PKCS1_SHA512,
                // A deliberately mismatched header: sign with SHA-256.
                _ => &RSA_PKCS1_SHA256,
            };
            let mut sig = vec![0; pair.public_modulus_len()];
            pair.sign(padding, &rng, input.as_bytes(), &mut sig)
                .unwrap();
            sig
        }
        SigningKey::Ec(k) => EcdsaKeyPair::from_pkcs8(k.alg, &k.pkcs8)
            .unwrap()
            .sign(&rng, input.as_bytes())
            .unwrap()
            .as_ref()
            .to_vec(),
    };
    format!("{input}.{}", b64(&sig))
}

pub const TEST_AUDIENCE: &str = "hivekeep-api";

/// Claims a verifier for `issuer` accepts.
pub fn valid_claims(issuer: &str) -> serde_json::Value {
    let now = hivekeep_identity::jwt::claims::unix_seconds(std::time::SystemTime::now());
    serde_json::json!({
        "iss": issuer,
        "sub": "system:serviceaccount:hivekeep-system:keeper",
        "aud": [TEST_AUDIENCE],
        "exp": now + 3600,
        "nbf": now - 60,
        "iat": now - 60,
        "jti": "test-jti",
    })
}

// ---- Fake issuer ------------------------------------------------------------

/// Stands in for an issuer's key-set endpoint: serves the keys registered on
/// it, counts requests and fails a configurable number of them.
#[derive(Default)]
pub struct FakeIssuer {
    pub keys: Mutex<Vec<Jwk>>,
    pub requests: AtomicUsize,
    pub failures: AtomicUsize,
}

pub const FAKE_ISSUER: &str = "https://issuer.example.com";

impl FakeIssuer {
    pub fn add(&self, jwk: Jwk) {
        self.keys.lock().push(jwk);
    }

    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    pub fn fail_next(&self, n: usize) {
        self.failures.store(n, Ordering::SeqCst);
    }

    pub fn jwks_json(&self) -> Vec<u8> {
        serde_json::to_vec(&JwkSet {
            keys: self.keys.lock().clone(),
        })
        .unwrap()
    }
}

impl KeySource for FakeIssuer {
    fn keys(&self) -> Result<Vec<KeyWithId>> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |f| f.checked_sub(1))
            .is_ok()
        {
            return Err(IdentityError::Keys("503 temporary failure".into()));
        }
        parse_key_set(&self.jwks_json(), FAKE_ISSUER)
    }
}

// ---- Certificates ----------------------------------------------------------

/// A throwaway self-signed certificate, DER.
pub fn self_signed_der(cn: &str) -> Vec<u8> {
    rcgen::generate_simple_self_signed(vec![cn.to_string()])
        .unwrap()
        .cert
        .der()
        .to_vec()
}

/// A self-signed certificate for `key` with the given serial, DER.
pub fn self_signed_with_serial(key: &PrivateKey, serial: u8) -> Vec<u8> {
    let pair = rcgen::KeyPair::try_from(key.pkcs8_der()).unwrap();
    let mut p = rcgen::CertificateParams::new(vec!["api.hivekeep-system.svc".to_string()]).unwrap();
    p.serial_number = Some(rcgen::SerialNumber::from_slice(&[serial]));
    p.self_signed(&pair).unwrap().der().to_vec()
}
