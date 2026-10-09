//! JSON Web Key Sets: publishing an issuer's keys and reading them back.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use super::authority::VerificationKey;
use crate::error::{IdentityError, Result};
use crate::key::KeyComponents;

/// One key of a set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jwk {
    /// `RSA` or `EC`.
    pub kty: String,
    /// Key id.
    #[serde(default)]
    pub kid: String,
    /// Always `sig` when published here.
    #[serde(rename = "use", default, skip_serializing_if = "String::is_empty")]
    pub key_use: String,
    /// Algorithm, e.g. `ES256`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub alg: String,
    /// Curve of an EC key.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub crv: String,
    /// X coordinate of an EC key, base64url.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub x: String,
    /// Y coordinate of an EC key, base64url.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub y: String,
    /// Modulus of an RSA key, base64url.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub n: String,
    /// Exponent of an RSA key, base64url.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub e: String,
}

/// A key set document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwkSet {
    /// The keys.
    #[serde(default)]
    pub keys: Vec<Jwk>,
}

fn to_jwk(key: &VerificationKey) -> Result<Jwk> {
    if !key.algorithm.fits(key.public_key.kind()) {
        return Err(IdentityError::Keys(format!(
            "key {:?}: algorithm {} does not fit a {:?} key",
            key.key_id,
            key.algorithm,
            key.public_key.kind()
        )));
    }
    let mut j = Jwk {
        kid: key.key_id.clone(),
        key_use: "sig".into(),
        alg: key.algorithm.as_str().into(),
        ..Default::default()
    };
    match key.public_key.components()? {
        KeyComponents::Rsa { n, e } => {
            j.kty = "RSA".into();
            j.n = URL_SAFE_NO_PAD.encode(n);
            j.e = URL_SAFE_NO_PAD.encode(e);
        }
        KeyComponents::P256 { x, y } => {
            j.kty = "EC".into();
            j.crv = "P-256".into();
            j.x = URL_SAFE_NO_PAD.encode(x);
            j.y = URL_SAFE_NO_PAD.encode(y);
        }
        KeyComponents::Ed25519(_) => {
            return Err(IdentityError::Keys("Ed25519 keys are not published".into()))
        }
    }
    Ok(j)
}

/// The key set publishing `keys`, sorted by key id. Fails on an empty set,
/// a missing or repeated key id, or an algorithm that does not fit its key.
pub fn key_set(keys: &[VerificationKey]) -> Result<JwkSet> {
    if keys.is_empty() {
        return Err(IdentityError::Keys("no keys to publish".into()));
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::with_capacity(keys.len());
    for k in keys {
        if k.key_id.is_empty() {
            return Err(IdentityError::Keys("key has no ID".into()));
        }
        if !seen.insert(k.key_id.as_str()) {
            return Err(IdentityError::Keys(format!(
                "duplicate key ID {:?}",
                k.key_id
            )));
        }
        out.push(to_jwk(k)?);
    }
    out.sort_by(|a, b| a.kid.cmp(&b.kid));
    Ok(JwkSet { keys: out })
}

/// [`key_set`] as JSON.
pub fn key_set_json(keys: &[VerificationKey]) -> Result<Vec<u8>> {
    serde_json::to_vec(&key_set(keys)?).map_err(|e| IdentityError::Keys(e.to_string()))
}

/// An elliptic curve a verifier accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Curve {
    /// P-256, for ES256.
    P256,
    /// P-384, for ES384.
    P384,
    /// P-521, for ES512.
    P521,
}

/// A verification key read from a key set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JwkKey {
    /// RSA, for RS256/384/512.
    Rsa {
        /// Modulus, big-endian.
        n: Vec<u8>,
        /// Exponent, big-endian.
        e: Vec<u8>,
    },
    /// EC, for ES256/ES384/ES512.
    Ec {
        /// The curve.
        curve: Curve,
        /// X coordinate, base64url as published.
        x: String,
        /// Y coordinate, base64url as published.
        y: String,
    },
}

/// A verification key with its id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyWithId {
    /// Key id.
    pub key_id: String,
    /// The key.
    pub key: JwkKey,
}

fn parse_jwk(j: &Jwk) -> Result<KeyWithId> {
    let bad = |m: String| IdentityError::Keys(m);
    if j.kid.is_empty() {
        return Err(bad("JWK has no key ID".into()));
    }
    let decode = |what: &str, v: &str| {
        URL_SAFE_NO_PAD
            .decode(v)
            .map_err(|e| bad(format!("base64-decoding {what}: {e}")))
    };
    let key = match j.kty.as_str() {
        "EC" => {
            let (curve, size) = match j.crv.as_str() {
                "P-256" => (Curve::P256, 32),
                "P-384" => (Curve::P384, 48),
                "P-521" => (Curve::P521, 66),
                other => return Err(bad(format!("unsupported curve {other:?}"))),
            };
            if j.x.is_empty() || j.y.is_empty() {
                return Err(bad("EC JWK is missing the x or y coordinate".into()));
            }
            if decode("x", &j.x)?.len() != size || decode("y", &j.y)?.len() != size {
                return Err(bad(format!(
                    "EC JWK coordinates do not fit curve {:?}",
                    j.crv
                )));
            }
            JwkKey::Ec {
                curve,
                x: j.x.clone(),
                y: j.y.clone(),
            }
        }
        "RSA" => {
            let n = decode("n", &j.n)?;
            let e = decode("e", &j.e)?;
            let e_val = e.iter().try_fold(0u64, |acc, &b| {
                (acc < (1 << 55)).then_some((acc << 8) | b as u64)
            });
            if !matches!(e_val, Some(v) if (2..=u32::MAX as u64).contains(&v)) || n.is_empty() {
                return Err(bad(
                    "RSA JWK exponent is outside the supported integer range".into(),
                ));
            }
            JwkKey::Rsa { n, e }
        }
        other => return Err(bad(format!("unhandled key type {other:?}"))),
    };
    Ok(KeyWithId {
        key_id: j.kid.clone(),
        key,
    })
}

/// The usable keys of a key set, skipping ones this verifier cannot use.
/// Fails if none are usable.
pub fn parse_key_set(json: &[u8], origin: &str) -> Result<Vec<KeyWithId>> {
    let set: JwkSet = serde_json::from_slice(json)
        .map_err(|e| IdentityError::Keys(format!("parsing JWKS from {origin}: {e}")))?;
    if set.keys.is_empty() {
        return Err(IdentityError::Keys(format!(
            "{origin} published an empty JWKS"
        )));
    }
    let mut skipped = 0;
    let mut out = Vec::new();
    for j in &set.keys {
        match parse_jwk(j) {
            Ok(k) => out.push(k),
            Err(e) => {
                tracing::debug!(origin, kid = j.kid, error = %e, "skipping unusable JWK");
                skipped += 1;
            }
        }
    }
    if out.is_empty() {
        return Err(IdentityError::Keys(format!(
            "no usable keys in JWKS from {origin} ({skipped} skipped)"
        )));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jwt::authority::{JwtAlgorithm, JwtAuthority};
    use crate::key::{KeyAlgorithm, PrivateKey};

    #[test]
    fn publishes_sorted_and_round_trips() {
        let a = JwtAuthority::generate(JwtAlgorithm::Es256, Some("b-key")).unwrap();
        let rsa = JwtAuthority::new(
            "a-key",
            JwtAlgorithm::Rs256,
            PrivateKey::generate(KeyAlgorithm::Rsa2048).unwrap(),
        )
        .unwrap();
        let json = key_set_json(&[a.verification_key(), rsa.verification_key()]).unwrap();
        let set: JwkSet = serde_json::from_slice(&json).unwrap();
        assert_eq!(
            set.keys.iter().map(|k| k.kid.as_str()).collect::<Vec<_>>(),
            ["a-key", "b-key"]
        );
        assert_eq!(
            (set.keys[0].kty.as_str(), set.keys[0].e.as_str()),
            ("RSA", "AQAB")
        );
        assert_eq!(
            (set.keys[1].kty.as_str(), set.keys[1].crv.as_str()),
            ("EC", "P-256")
        );
        assert!(set.keys.iter().all(|k| k.key_use == "sig"));
        let parsed = parse_key_set(&json, "test").unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn refuses_bad_sets() {
        let a = JwtAuthority::generate(JwtAlgorithm::Es256, Some("k"))
            .unwrap()
            .verification_key();
        assert!(key_set(&[]).is_err());
        assert!(key_set(&[a.clone(), a.clone()]).is_err());
        let mut no_id = a.clone();
        no_id.key_id.clear();
        assert!(key_set(&[no_id]).is_err());
        let mut misfit = a;
        misfit.algorithm = JwtAlgorithm::Rs256;
        assert!(key_set(&[misfit]).is_err());
    }

    #[test]
    fn skips_unusable_keys() {
        let json = br#"{"keys":[
            {"kty":"oct","kid":"x"},
            {"kty":"EC","kid":"","crv":"P-256","x":"AA","y":"AA"},
            {"kty":"EC","kid":"short","crv":"P-256","x":"AA","y":"AA"},
            {"kty":"RSA","kid":"e1","n":"AQAB","e":"AQ"},
            {"kty":"RSA","kid":"ok","n":"AQAB","e":"AQAB"}
        ]}"#;
        let keys = parse_key_set(json, "test").unwrap();
        assert_eq!(
            keys.iter().map(|k| k.key_id.as_str()).collect::<Vec<_>>(),
            ["ok"]
        );
        assert!(parse_key_set(br#"{"keys":[]}"#, "t").is_err());
        assert!(parse_key_set(br#"{"keys":[{"kty":"oct","kid":"x"}]}"#, "t").is_err());
        assert!(parse_key_set(b"nope", "t").is_err());
    }

    fn ec(crv: &str, x: &str, y: &str) -> Jwk {
        Jwk {
            kty: "EC".into(),
            kid: "k".into(),
            crv: crv.into(),
            x: x.into(),
            y: y.into(),
            ..Default::default()
        }
    }

    #[test]
    fn parse_jwk_rejects() {
        let rsa = |n: &str, e: &str| Jwk {
            kty: "RSA".into(),
            kid: "k".into(),
            n: n.into(),
            e: e.into(),
            ..Default::default()
        };
        let mut oversized = vec![1u8];
        oversized.extend([0u8; 10]); // 2^80
        let cases = [
            (
                "no key ID",
                Jwk {
                    kty: "RSA".into(),
                    ..Default::default()
                },
            ),
            (
                "unknown key type",
                Jwk {
                    kty: "OKP".into(),
                    kid: "k".into(),
                    ..Default::default()
                },
            ),
            ("unsupported EC curve", ec("P-192", "AA", "AA")),
            ("EC missing coordinate", ec("P-256", "AA", "")),
            ("EC malformed x", ec("P-256", "!!!", "AA")),
            ("RSA malformed n", rsa("!!!", "AQAB")),
            (
                "RSA oversized exponent",
                rsa("Aw", &URL_SAFE_NO_PAD.encode(&oversized)),
            ),
        ];
        for (name, j) in cases {
            assert!(parse_jwk(&j).is_err(), "{name}: parsed");
        }
    }

    #[test]
    fn elliptic_curve_for_jwk() {
        for (crv, size, curve) in [
            ("P-256", 32, Curve::P256),
            ("P-384", 48, Curve::P384),
            ("P-521", 66, Curve::P521),
        ] {
            let c = URL_SAFE_NO_PAD.encode(vec![1u8; size]);
            match parse_jwk(&ec(crv, &c, &c)) {
                Ok(KeyWithId {
                    key: JwkKey::Ec { curve: got, .. },
                    ..
                }) => assert_eq!(got, curve, "{crv}"),
                other => panic!("{crv}: {other:?}"),
            }
        }
        let err = parse_jwk(&ec("P-192", "AA", "AA")).unwrap_err().to_string();
        assert!(err.contains("unsupported curve"), "{err}");
    }
}
