//! JWT signing authorities and the pool that rotates them.

use std::fmt;
use std::str::FromStr;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use jsonwebtoken::{Algorithm, EncodingKey};
use serde::{Deserialize, Serialize};

use super::claims::Claims;
use crate::error::{IdentityError, Result};
use crate::key::{KeyAlgorithm, KeyKind, PrivateKey, PublicKey};
use crate::state_file::{Refreshing, StateFile};

/// Signature algorithms authorities sign with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JwtAlgorithm {
    /// RSASSA-PKCS1-v1_5 with SHA-256.
    Rs256,
    /// RSASSA-PKCS1-v1_5 with SHA-384.
    Rs384,
    /// RSASSA-PKCS1-v1_5 with SHA-512.
    Rs512,
    /// ECDSA on P-256 with SHA-256.
    Es256,
}

impl JwtAlgorithm {
    /// The JOSE name, e.g. `"ES256"`.
    pub fn as_str(self) -> &'static str {
        match self {
            JwtAlgorithm::Rs256 => "RS256",
            JwtAlgorithm::Rs384 => "RS384",
            JwtAlgorithm::Rs512 => "RS512",
            JwtAlgorithm::Es256 => "ES256",
        }
    }

    /// Whether a key of `kind` can produce this algorithm's signatures.
    pub fn fits(self, kind: KeyKind) -> bool {
        match self {
            JwtAlgorithm::Es256 => kind == KeyKind::EcdsaP256,
            _ => kind == KeyKind::Rsa,
        }
    }

    pub(crate) fn jose(self) -> Algorithm {
        match self {
            JwtAlgorithm::Rs256 => Algorithm::RS256,
            JwtAlgorithm::Rs384 => Algorithm::RS384,
            JwtAlgorithm::Rs512 => Algorithm::RS512,
            JwtAlgorithm::Es256 => Algorithm::ES256,
        }
    }
}

impl fmt::Display for JwtAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for JwtAlgorithm {
    type Err = IdentityError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "RS256" => Ok(JwtAlgorithm::Rs256),
            "RS384" => Ok(JwtAlgorithm::Rs384),
            "RS512" => Ok(JwtAlgorithm::Rs512),
            "ES256" => Ok(JwtAlgorithm::Es256),
            other => Err(IdentityError::Key(format!(
                "unsupported algorithm {other:?}"
            ))),
        }
    }
}

/// A public key relying parties verify tokens with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationKey {
    /// `kid` of the tokens it verifies.
    pub key_id: String,
    /// Algorithm of those tokens.
    pub algorithm: JwtAlgorithm,
    /// The key.
    pub public_key: PublicKey,
}

/// One signing key with its id and algorithm.
#[derive(Debug, Clone)]
pub struct JwtAuthority {
    id: String,
    algorithm: JwtAlgorithm,
    key: PrivateKey,
}

/// RSA modulus size of generated RS256 authorities.
pub const RSA_KEY_ALGORITHM: KeyAlgorithm = KeyAlgorithm::Rsa4096;

impl JwtAuthority {
    /// An authority signing with `key`.
    pub fn new(id: impl Into<String>, algorithm: JwtAlgorithm, key: PrivateKey) -> Result<Self> {
        let id = id.into();
        if id.is_empty() {
            return Err(IdentityError::Pool("authority has no ID".into()));
        }
        if !algorithm.fits(key.kind()) {
            return Err(IdentityError::Key(format!(
                "{algorithm} does not fit a {:?} key",
                key.kind()
            )));
        }
        Ok(JwtAuthority { id, algorithm, key })
    }

    /// Generates an RS256 (4096-bit) or ES256 authority. Without an `id`, the
    /// key's thumbprint is used, so the id is stable across restarts.
    pub fn generate(algorithm: JwtAlgorithm, id: Option<&str>) -> Result<Self> {
        let key = match algorithm {
            JwtAlgorithm::Rs256 => PrivateKey::generate(RSA_KEY_ALGORITHM)?,
            JwtAlgorithm::Es256 => PrivateKey::generate(KeyAlgorithm::EcdsaP256)?,
            other => {
                return Err(IdentityError::Key(format!(
                    "unsupported algorithm {other}, want RS256 or ES256"
                )));
            }
        };
        let id = match id {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => key.public_key().thumbprint()?,
        };
        Self::new(id, algorithm, key)
    }

    /// The key id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The algorithm.
    pub fn algorithm(&self) -> JwtAlgorithm {
        self.algorithm
    }

    /// The key relying parties verify with.
    pub fn verification_key(&self) -> VerificationKey {
        VerificationKey {
            key_id: self.id.clone(),
            algorithm: self.algorithm,
            public_key: self.key.public_key(),
        }
    }

    fn encoding_key(&self) -> Result<EncodingKey> {
        match self.key.kind() {
            KeyKind::EcdsaP256 => Ok(EncodingKey::from_ec_der(self.key.pkcs8_der())),
            KeyKind::Rsa => Ok(EncodingKey::from_rsa_der(&pkcs1_from_pkcs8(
                self.key.pkcs8_der(),
            )?)),
            other => Err(IdentityError::Sign(format!(
                "cannot sign JWTs with a {other:?} key"
            ))),
        }
    }

    /// Signs `claims`: header `{"typ":"JWT","alg":...,"kid":...}`, then the
    /// wire claims.
    pub fn sign(&self, claims: &Claims) -> Result<String> {
        #[derive(Serialize)]
        struct Header<'a> {
            typ: &'static str,
            alg: &'static str,
            kid: &'a str,
        }
        let header = serde_json::to_vec(&Header {
            typ: "JWT",
            alg: self.algorithm.as_str(),
            kid: &self.id,
        })
        .map_err(|e| IdentityError::Sign(e.to_string()))?;
        let payload = serde_json::to_vec(&claims.to_wire()?)
            .map_err(|e| IdentityError::Sign(e.to_string()))?;
        let signing_input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(header),
            URL_SAFE_NO_PAD.encode(payload)
        );
        let signature = jsonwebtoken::crypto::sign(
            signing_input.as_bytes(),
            &self.encoding_key()?,
            self.algorithm.jose(),
        )
        .map_err(|e| IdentityError::Sign(e.to_string()))?;
        Ok(format!("{signing_input}.{signature}"))
    }
}

/// Reads one DER element: `(tag, contents, rest)`.
fn der_element(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    let (&first, mut rest) = rest.split_first()?;
    let len = if first < 0x80 {
        first as usize
    } else {
        let n = (first & 0x7f) as usize;
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        let len = rest[..n]
            .iter()
            .fold(0usize, |acc, &b| (acc << 8) | b as usize);
        rest = &rest[n..];
        len
    };
    (rest.len() >= len).then(|| (tag, &rest[..len], &rest[len..]))
}

/// Extracts the PKCS#1 RSAPrivateKey from a PKCS#8 PrivateKeyInfo
/// (`SEQUENCE { version, algorithm, OCTET STRING privateKey, ... }`).
fn pkcs1_from_pkcs8(pkcs8: &[u8]) -> Result<Vec<u8>> {
    let bad = || IdentityError::Key("malformed PKCS#8 RSA key".into());
    let (tag, info, _) = der_element(pkcs8).ok_or_else(bad)?;
    if tag != 0x30 {
        return Err(bad());
    }
    let (_, _version, rest) = der_element(info).ok_or_else(bad)?;
    let (_, _algorithm, rest) = der_element(rest).ok_or_else(bad)?;
    let (tag, key, _) = der_element(rest).ok_or_else(bad)?;
    if tag != 0x04 {
        return Err(bad());
    }
    Ok(key.to_vec())
}

/// Mints and publishes tokens.
pub trait JwtSigner: Send + Sync {
    /// Signs `claims` with the active authority.
    fn sign_jwt(&self, claims: &Claims) -> Result<String>;

    /// The keys of every authority, active or not.
    fn verification_keys(&self) -> Result<Vec<VerificationKey>>;
}

/// A set of authorities, one active for signing. Every authority's key is
/// published, so a new one can be added (published first), activated, and the
/// old one removed once its tokens have expired.
#[derive(Debug, Clone, Default)]
pub struct JwtPool {
    authorities: Vec<JwtAuthority>,
    active_for_signing: String,
}

#[derive(Serialize, Deserialize)]
struct WirePool {
    #[serde(rename = "Authorities", default)]
    authorities: Vec<WireAuthority>,
    #[serde(rename = "ActiveForSigning", default)]
    active_for_signing: String,
}

#[derive(Serialize, Deserialize)]
struct WireAuthority {
    #[serde(rename = "ID")]
    id: String,
    #[serde(rename = "Algorithm")]
    algorithm: String,
    #[serde(rename = "SigningKeyPKCS8", with = "crate::ca::b64")]
    signing_key_pkcs8: Vec<u8>,
}

impl JwtPool {
    /// A pool of `authorities` signing with `active_for_signing` (the first
    /// authority when empty).
    pub fn new(authorities: Vec<JwtAuthority>, active_for_signing: impl Into<String>) -> Self {
        JwtPool {
            authorities,
            active_for_signing: active_for_signing.into(),
        }
    }

    /// A pool of one freshly generated authority, active.
    pub fn generate(algorithm: JwtAlgorithm, id: Option<&str>) -> Result<Self> {
        let a = JwtAuthority::generate(algorithm, id)?;
        let id = a.id().to_string();
        Ok(Self::new(vec![a], id))
    }

    /// The authorities.
    pub fn authorities(&self) -> &[JwtAuthority] {
        &self.authorities
    }

    /// Id of the authority that signs: the configured one, or the first.
    pub fn active_id(&self) -> &str {
        if self.active_for_signing.is_empty() {
            self.authorities.first().map(|a| a.id()).unwrap_or("")
        } else {
            &self.active_for_signing
        }
    }

    fn index(&self, id: &str) -> Option<usize> {
        self.authorities.iter().position(|a| a.id == id)
    }

    /// Adds an authority without activating it.
    pub fn add(&mut self, authority: JwtAuthority) -> Result<()> {
        if self.index(&authority.id).is_some() {
            return Err(IdentityError::Pool(format!(
                "authority {:?} already present",
                authority.id
            )));
        }
        self.authorities.push(authority);
        Ok(())
    }

    /// Makes `id` the signing authority.
    pub fn activate(&mut self, id: &str) -> Result<()> {
        if self.index(id).is_none() {
            return Err(IdentityError::Pool(format!("authority {id:?} not present")));
        }
        self.active_for_signing = id.to_string();
        Ok(())
    }

    /// Removes an inactive authority.
    pub fn remove(&mut self, id: &str) -> Result<()> {
        let i = self
            .index(id)
            .ok_or_else(|| IdentityError::Pool(format!("authority {id:?} not present")))?;
        if id == self.active_id() {
            return Err(IdentityError::Pool(format!(
                "authority {id:?} is active for signing"
            )));
        }
        self.authorities.remove(i);
        Ok(())
    }

    /// The signing authority.
    pub fn active(&self) -> Result<&JwtAuthority> {
        if self.authorities.is_empty() {
            return Err(IdentityError::Pool(
                "pool has no authorities defined".into(),
            ));
        }
        let id = self.active_id();
        self.index(id)
            .map(|i| &self.authorities[i])
            .ok_or_else(|| IdentityError::Pool(format!("selected authority {id:?} not present")))
    }

    /// Serializes the pool, keys included, as its JSON state file.
    pub fn to_json(&self) -> Result<Vec<u8>> {
        let wire = WirePool {
            authorities: self
                .authorities
                .iter()
                .map(|a| WireAuthority {
                    id: a.id.clone(),
                    algorithm: a.algorithm.as_str().to_string(),
                    signing_key_pkcs8: a.key.pkcs8_der().to_vec(),
                })
                .collect(),
            active_for_signing: self.active_for_signing.clone(),
        };
        serde_json::to_vec(&wire).map_err(|e| IdentityError::Pool(e.to_string()))
    }

    /// Parses a JSON state file.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        let wire: WirePool = serde_json::from_slice(bytes)
            .map_err(|e| IdentityError::Pool(format!("parsing JWT pool: {e}")))?;
        let authorities = wire
            .authorities
            .into_iter()
            .map(|a| {
                JwtAuthority::new(
                    a.id,
                    a.algorithm.parse()?,
                    PrivateKey::from_pkcs8_der(&a.signing_key_pkcs8)?,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JwtPool {
            authorities,
            active_for_signing: wire.active_for_signing,
        })
    }
}

impl JwtSigner for JwtPool {
    fn sign_jwt(&self, claims: &Claims) -> Result<String> {
        self.active()?.sign(claims)
    }

    fn verification_keys(&self) -> Result<Vec<VerificationKey>> {
        Ok(self
            .authorities
            .iter()
            .map(JwtAuthority::verification_key)
            .collect())
    }
}

impl StateFile for JwtPool {
    fn parse(bytes: &[u8]) -> Result<Self> {
        JwtPool::from_json(bytes)
    }
}

/// A [`JwtPool`] backed by its state file.
pub type RefreshingJwtPool = Refreshing<JwtPool>;

impl JwtSigner for Refreshing<JwtPool> {
    fn sign_jwt(&self, claims: &Claims) -> Result<String> {
        self.current()?.sign_jwt(claims)
    }

    fn verification_keys(&self) -> Result<Vec<VerificationKey>> {
        self.current()?.verification_keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    pub(crate) fn claims() -> Claims {
        Claims::for_actor(
            "https://idp.example.com",
            "team-a",
            "agent",
            "uid-1",
            vec!["aud".into()],
            Duration::from_secs(600),
            SystemTime::now(),
        )
        .unwrap()
    }

    #[test]
    fn pool_rotation() {
        let mut pool = JwtPool::generate(JwtAlgorithm::Es256, Some("one")).unwrap();
        assert_eq!(pool.active_id(), "one");
        let two = JwtAuthority::generate(JwtAlgorithm::Es256, None).unwrap();
        let two_id = two.id().to_string();
        assert_eq!(
            two_id,
            two.verification_key().public_key.thumbprint().unwrap()
        );
        pool.add(two.clone()).unwrap();
        assert!(pool.add(two).is_err());
        assert!(pool.remove("one").is_err(), "removed the active authority");
        pool.activate(&two_id).unwrap();
        assert!(pool.activate("missing").is_err());
        pool.remove("one").unwrap();
        assert!(pool.remove("one").is_err());
        assert_eq!(pool.verification_keys().unwrap().len(), 1);

        let again = JwtPool::from_json(&pool.to_json().unwrap()).unwrap();
        assert_eq!(again.active_id(), two_id);
        assert!(JwtPool::default().sign_jwt(&claims()).is_err());
        assert!(JwtPool::new(again.authorities().to_vec(), "missing")
            .sign_jwt(&claims())
            .is_err());
    }

    #[test]
    fn algorithm_must_fit_the_key() {
        let ec = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
        assert!(JwtAuthority::new("a", JwtAlgorithm::Rs256, ec.clone()).is_err());
        assert!(JwtAuthority::new("", JwtAlgorithm::Es256, ec.clone()).is_err());
        assert!(JwtAuthority::new("a", JwtAlgorithm::Es256, ec).is_ok());
        assert!(JwtAuthority::generate(JwtAlgorithm::Rs384, None).is_err());
        assert!("HS256".parse::<JwtAlgorithm>().is_err());
    }

    #[test]
    fn header_shape() {
        let a = JwtAuthority::generate(JwtAlgorithm::Es256, Some("kid-1")).unwrap();
        let token = a.sign(&claims()).unwrap();
        let header = token.split('.').next().unwrap();
        let header: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header).unwrap()).unwrap();
        assert_eq!(
            header,
            serde_json::json!({"typ": "JWT", "alg": "ES256", "kid": "kid-1"})
        );
    }

    #[test]
    fn pkcs1_extraction() {
        let rsa = PrivateKey::generate(KeyAlgorithm::Rsa2048).unwrap();
        let pkcs1 = pkcs1_from_pkcs8(rsa.pkcs8_der()).unwrap();
        assert_eq!(pkcs1[0], 0x30);
        assert!(pkcs1_from_pkcs8(b"\x30\x01").is_err());
        assert!(pkcs1_from_pkcs8(b"").is_err());
    }
}
