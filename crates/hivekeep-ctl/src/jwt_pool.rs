//! JWT authority pools for the Actor identity broker: signing keys, which
//! one signs, and the JWK set that publishes their public halves.
//!
//! The pool is JSON: `{"Authorities": [{"ID", "Algorithm",
//! "SigningKeyPKCS8"}], "ActiveForSigning"}`, keys in standard base64.
//! Rotation is add (published, inactive), activate, then remove the old key
//! once the last token it signed has expired.

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine as _;
use rsa::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey};
use rsa::traits::PublicKeyParts;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Size of generated RSA keys; RS256 fixes none, and smaller ones are weak.
pub const RSA_KEY_BITS: usize = 4096;

/// One signing key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authority {
    /// Key ID, as published in the JWK set and token headers.
    #[serde(rename = "ID")]
    pub id: String,
    /// JOSE algorithm: ES256, RS256, RS384 or RS512.
    #[serde(rename = "Algorithm")]
    pub algorithm: String,
    /// PKCS#8 DER of the private key.
    #[serde(rename = "SigningKeyPKCS8", with = "b64")]
    pub key_pkcs8: Vec<u8>,
}

mod b64 {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = Option::<String>::deserialize(d)?.unwrap_or_default();
        STANDARD.decode(s).map_err(serde::de::Error::custom)
    }
}

fn null_as_empty<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<Authority>, D::Error> {
    Ok(Option::<Vec<Authority>>::deserialize(d)?.unwrap_or_default())
}

/// A pool of signing keys.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtPool {
    /// The keys, in the order they were added.
    #[serde(rename = "Authorities", default, deserialize_with = "null_as_empty")]
    pub authorities: Vec<Authority>,
    /// ID of the key that signs; the first key when empty.
    #[serde(rename = "ActiveForSigning", default)]
    pub active_for_signing: String,
}

/// The public half of a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PublicKey {
    /// P-256: the SubjectPublicKeyInfo DER and the uncompressed point.
    P256 { spki: Vec<u8>, point: Vec<u8> },
    /// RSA: the SubjectPublicKeyInfo DER, modulus and exponent (big-endian).
    Rsa {
        spki: Vec<u8>,
        n: Vec<u8>,
        e: Vec<u8>,
    },
}

impl PublicKey {
    pub(crate) fn spki(&self) -> &[u8] {
        match self {
            PublicKey::P256 { spki, .. } | PublicKey::Rsa { spki, .. } => spki,
        }
    }
}

fn is_rsa_algorithm(alg: &str) -> bool {
    matches!(alg, "RS256" | "RS384" | "RS512")
}

/// The public key of an authority, checking that it fits its algorithm.
pub(crate) fn public_key(a: &Authority) -> Result<PublicKey, String> {
    if a.algorithm == "ES256" {
        let kp = rcgen::KeyPair::try_from(a.key_pkcs8.as_slice())
            .map_err(|e| format!("key {:?}: {e}", a.id))?;
        if !kp.is_compatible(&rcgen::PKCS_ECDSA_P256_SHA256) {
            return Err(format!(
                "key {:?}: algorithm ES256 needs a P-256 EC key",
                a.id
            ));
        }
        return Ok(PublicKey::P256 {
            spki: rcgen::PublicKeyData::subject_public_key_info(&kp),
            point: kp.public_key_raw().to_vec(),
        });
    }
    if is_rsa_algorithm(&a.algorithm) {
        let key = rsa::RsaPrivateKey::from_pkcs8_der(&a.key_pkcs8).map_err(|e| {
            format!(
                "key {:?}: algorithm {} needs an RSA key: {e}",
                a.id, a.algorithm
            )
        })?;
        let public = key.to_public_key();
        let spki = public
            .to_public_key_der()
            .map_err(|e| format!("key {:?}: {e}", a.id))?
            .as_bytes()
            .to_vec();
        return Ok(PublicKey::Rsa {
            spki,
            n: public.n().to_bytes_be(),
            e: public.e().to_bytes_be(),
        });
    }
    Err(format!(
        "key {:?}: unsupported algorithm {:?}",
        a.id, a.algorithm
    ))
}

/// The base64url SHA-256 of a public key's SubjectPublicKeyInfo DER: the
/// default key ID.
pub fn thumbprint(spki_der: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(spki_der))
}

/// Generates a signing key for `algorithm` (ES256 or RS256), with ID `id`
/// or, when empty, the key's thumbprint.
pub fn generate_authority(algorithm: &str, id: &str) -> Result<Authority, String> {
    generate_authority_with_rsa_bits(algorithm, id, RSA_KEY_BITS)
}

pub(crate) fn generate_authority_with_rsa_bits(
    algorithm: &str,
    id: &str,
    rsa_bits: usize,
) -> Result<Authority, String> {
    let key_pkcs8 = match algorithm {
        "ES256" => rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
            .map_err(|e| format!("while generating key: {e}"))?
            .serialize_der(),
        "RS256" => rsa::RsaPrivateKey::new(&mut rand_core::OsRng, rsa_bits)
            .map_err(|e| format!("while generating key: {e}"))?
            .to_pkcs8_der()
            .map_err(|e| format!("while encoding key: {e}"))?
            .as_bytes()
            .to_vec(),
        other => {
            return Err(format!(
                "unsupported algorithm {other:?}, want RS256 or ES256"
            ))
        }
    };
    let mut authority = Authority {
        id: id.to_string(),
        algorithm: algorithm.to_string(),
        key_pkcs8,
    };
    if authority.id.is_empty() {
        authority.id = thumbprint(public_key(&authority)?.spki());
    }
    Ok(authority)
}

impl JwtPool {
    /// A pool holding `authority`, active for signing.
    pub fn with_active(authority: Authority) -> Self {
        JwtPool {
            active_for_signing: authority.id.clone(),
            authorities: vec![authority],
        }
    }

    fn index(&self, id: &str) -> Option<usize> {
        self.authorities.iter().position(|a| a.id == id)
    }

    /// The ID of the key that signs.
    pub fn active_id(&self) -> &str {
        if self.active_for_signing.is_empty() {
            self.authorities
                .first()
                .map(|a| a.id.as_str())
                .unwrap_or_default()
        } else {
            &self.active_for_signing
        }
    }

    /// Adds `authority` without activating it, so relying parties learn its
    /// key before it signs anything.
    pub fn add(&mut self, authority: Authority) -> Result<(), String> {
        if authority.id.is_empty() {
            return Err("authority has no ID".into());
        }
        if self.index(&authority.id).is_some() {
            return Err(format!("authority {:?} already present", authority.id));
        }
        self.authorities.push(authority);
        Ok(())
    }

    /// Makes the key `id` the one that signs.
    pub fn activate(&mut self, id: &str) -> Result<(), String> {
        if self.index(id).is_none() {
            return Err(format!("authority {id:?} not present"));
        }
        self.active_for_signing = id.to_string();
        Ok(())
    }

    /// Removes the key `id`, which must not be the one that signs.
    pub fn remove(&mut self, id: &str) -> Result<(), String> {
        let i = self
            .index(id)
            .ok_or_else(|| format!("authority {id:?} not present"))?;
        if id == self.active_id() {
            return Err(format!("authority {id:?} is active for signing"));
        }
        self.authorities.remove(i);
        Ok(())
    }

    /// Serializes the pool.
    pub fn marshal(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Parses a serialized pool, checking every key.
    pub fn unmarshal(bytes: &[u8]) -> Result<Self, String> {
        let pool: JwtPool =
            serde_json::from_slice(bytes).map_err(|e| format!("while unmarshaling JSON: {e}"))?;
        for a in &pool.authorities {
            public_key(a).map_err(|e| format!("while parsing signing key: {e}"))?;
        }
        Ok(pool)
    }

    /// The JWK set publishing every key's public half, sorted by key ID.
    pub fn jwks(&self) -> Result<Vec<u8>, String> {
        if self.authorities.is_empty() {
            return Err("no keys to publish".into());
        }
        let mut keys = Vec::with_capacity(self.authorities.len());
        for a in &self.authorities {
            keys.push(match public_key(a)? {
                PublicKey::P256 { point, .. } => Jwk {
                    kty: "EC",
                    kid: a.id.clone(),
                    use_: "sig",
                    alg: a.algorithm.clone(),
                    crv: Some("P-256"),
                    x: Some(URL_SAFE_NO_PAD.encode(&point[1..33])),
                    y: Some(URL_SAFE_NO_PAD.encode(&point[33..65])),
                    n: None,
                    e: None,
                },
                PublicKey::Rsa { n, e, .. } => Jwk {
                    kty: "RSA",
                    kid: a.id.clone(),
                    use_: "sig",
                    alg: a.algorithm.clone(),
                    crv: None,
                    x: None,
                    y: None,
                    n: Some(URL_SAFE_NO_PAD.encode(n)),
                    e: Some(URL_SAFE_NO_PAD.encode(e)),
                },
            });
        }
        keys.sort_by(|a, b| a.kid.cmp(&b.kid));
        serde_json::to_vec(&JwkSet { keys }).map_err(|e| e.to_string())
    }
}

#[derive(Serialize)]
struct JwkSet {
    keys: Vec<Jwk>,
}

#[derive(Serialize)]
struct Jwk {
    kty: &'static str,
    kid: String,
    #[serde(rename = "use")]
    use_: &'static str,
    alg: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    crv: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    x: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    y: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    n: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    e: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn es256_defaults_to_thumbprint_id() {
        let a = generate_authority("ES256", "").unwrap();
        let spki = public_key(&a).unwrap();
        assert_eq!(a.id, thumbprint(spki.spki()));
        assert_eq!(a.id.len(), 43);
        let a = generate_authority("ES256", "k1").unwrap();
        assert_eq!(a.id, "k1");
    }

    #[test]
    fn rejects_unsupported_algorithm() {
        let err = generate_authority("HS256", "").unwrap_err();
        assert!(err.contains("unsupported algorithm \"HS256\""), "{err}");
    }

    #[test]
    fn rotation() {
        let mut pool = JwtPool::with_active(generate_authority("ES256", "old").unwrap());
        pool.add(generate_authority("ES256", "new").unwrap())
            .unwrap();
        assert_eq!(pool.active_id(), "old");
        assert!(pool
            .add(generate_authority("ES256", "new").unwrap())
            .unwrap_err()
            .contains("already present"));
        assert!(pool
            .remove("old")
            .unwrap_err()
            .contains("active for signing"));
        pool.activate("new").unwrap();
        pool.remove("old").unwrap();
        assert_eq!(pool.authorities.len(), 1);
        assert!(pool.activate("gone").unwrap_err().contains("not present"));
        assert!(pool.remove("gone").unwrap_err().contains("not present"));
    }

    #[test]
    fn active_defaults_to_first() {
        let mut pool = JwtPool::with_active(generate_authority("ES256", "a").unwrap());
        pool.active_for_signing.clear();
        assert_eq!(pool.active_id(), "a");
        assert_eq!(JwtPool::default().active_id(), "");
    }

    #[test]
    fn marshal_round_trip() {
        let pool = JwtPool::with_active(generate_authority("ES256", "k1").unwrap());
        let bytes = pool.marshal();
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["ActiveForSigning"], "k1");
        assert_eq!(v["Authorities"][0]["Algorithm"], "ES256");
        assert_eq!(JwtPool::unmarshal(&bytes).unwrap(), pool);
        assert_eq!(
            JwtPool::unmarshal(br#"{"Authorities":null,"ActiveForSigning":""}"#).unwrap(),
            JwtPool::default()
        );
        assert!(JwtPool::unmarshal(b"nope")
            .unwrap_err()
            .contains("while unmarshaling JSON"));
    }

    #[test]
    fn jwks_publishes_public_halves_sorted() {
        let mut pool = JwtPool::with_active(generate_authority("ES256", "z").unwrap());
        pool.add(generate_authority_with_rsa_bits("RS256", "a", 1024).unwrap())
            .unwrap();
        let v: Value = serde_json::from_slice(&pool.jwks().unwrap()).unwrap();
        let keys = v["keys"].as_array().unwrap();
        assert_eq!(keys[0]["kid"], "a");
        assert_eq!(keys[0]["kty"], "RSA");
        assert_eq!(keys[0]["e"], "AQAB");
        assert!(keys[0].get("crv").is_none());
        assert_eq!(keys[1]["kid"], "z");
        assert_eq!(keys[1]["kty"], "EC");
        assert_eq!(keys[1]["crv"], "P-256");
        assert_eq!(keys[1]["use"], "sig");
        assert_eq!(
            URL_SAFE_NO_PAD
                .decode(keys[1]["x"].as_str().unwrap())
                .unwrap()
                .len(),
            32
        );
        assert!(keys[1].get("n").is_none());
        assert!(!String::from_utf8(pool.jwks().unwrap())
            .unwrap()
            .contains("SigningKey"));
        assert!(JwtPool::default().jwks().is_err());
    }

    #[test]
    fn mismatched_algorithm_is_rejected() {
        let mut a = generate_authority("ES256", "k").unwrap();
        a.algorithm = "RS256".into();
        assert!(public_key(&a).unwrap_err().contains("needs an RSA key"));
    }
}
