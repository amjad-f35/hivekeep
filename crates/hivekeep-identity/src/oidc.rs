//! The documents an OpenID Connect identity provider serves so relying
//! parties can verify Actor tokens: discovery and the key set. Serving them
//! over HTTP is left to the binary.

use serde::{Deserialize, Serialize};

use crate::error::{IdentityError, Result};
use crate::jwt::authority::JwtSigner;
use crate::jwt::claims::{validate_issuer, ACTOR_CLAIM, WORKER_FOR_ACTOR_CLAIM};
use crate::jwt::jwks::key_set_json;

/// Path of the discovery document.
pub const DISCOVERY_PATH: &str = "/.well-known/openid-configuration";
/// Path of the key set.
pub const JWKS_PATH: &str = "/openid/v1/jwks";

/// An OpenID Connect discovery document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryDocument {
    /// The issuer.
    pub issuer: String,
    /// Where the key set is.
    pub jwks_uri: String,
    /// Always `["id_token"]`.
    pub response_types_supported: Vec<String>,
    /// Always `["public"]`.
    pub subject_types_supported: Vec<String>,
    /// Algorithms of the published keys.
    pub id_token_signing_alg_values_supported: Vec<String>,
    /// Claims tokens may carry.
    pub claims_supported: Vec<String>,
}

/// Builds the documents for one issuer from its signer's keys.
pub struct IdentityProvider<S> {
    issuer: String,
    keys: S,
}

impl<S: JwtSigner> IdentityProvider<S> {
    /// A provider for `issuer`, which must be `https://<host>`. Fails if the
    /// keys cannot be published, so a misconfiguration surfaces at startup.
    pub fn new(issuer: impl Into<String>, keys: S) -> Result<Self> {
        let issuer = issuer.into();
        validate_issuer(&issuer)?;
        let p = IdentityProvider { issuer, keys };
        p.documents()?;
        Ok(p)
    }

    /// The discovery document and key set, as JSON. Built afresh on each
    /// call so key rotation is reflected immediately.
    pub fn documents(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        let keys = self.keys.verification_keys()?;
        let jwks = key_set_json(&keys)?;
        let mut algorithms: Vec<String> = keys
            .iter()
            .map(|k| k.algorithm.as_str().to_string())
            .collect();
        algorithms.sort();
        algorithms.dedup();
        let discovery = DiscoveryDocument {
            issuer: self.issuer.clone(),
            jwks_uri: format!("{}{JWKS_PATH}", self.issuer),
            response_types_supported: vec!["id_token".into()],
            subject_types_supported: vec!["public".into()],
            id_token_signing_alg_values_supported: algorithms,
            claims_supported: [
                "iss",
                "sub",
                "aud",
                "exp",
                "nbf",
                "iat",
                "jti",
                ACTOR_CLAIM,
                WORKER_FOR_ACTOR_CLAIM,
            ]
            .map(String::from)
            .to_vec(),
        };
        let discovery =
            serde_json::to_vec(&discovery).map_err(|e| IdentityError::Keys(e.to_string()))?;
        Ok((discovery, jwks))
    }

    /// The discovery document, JSON.
    pub fn discovery(&self) -> Result<Vec<u8>> {
        Ok(self.documents()?.0)
    }

    /// The key set, JSON.
    pub fn jwks(&self) -> Result<Vec<u8>> {
        Ok(self.documents()?.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jwt::authority::{JwtAlgorithm, JwtPool};

    #[test]
    fn documents() {
        let pool = JwtPool::generate(JwtAlgorithm::Es256, Some("k1")).unwrap();
        let idp = IdentityProvider::new("https://idp.example.com", pool).unwrap();
        let doc: DiscoveryDocument = serde_json::from_slice(&idp.discovery().unwrap()).unwrap();
        assert_eq!(doc.jwks_uri, "https://idp.example.com/openid/v1/jwks");
        assert_eq!(doc.id_token_signing_alg_values_supported, vec!["ES256"]);
        assert!(doc.claims_supported.contains(&ACTOR_CLAIM.to_string()));
        let set: serde_json::Value = serde_json::from_slice(&idp.jwks().unwrap()).unwrap();
        assert_eq!(set["keys"][0]["kid"], "k1");
    }

    #[test]
    fn refuses_bad_configuration() {
        let pool = JwtPool::generate(JwtAlgorithm::Es256, None).unwrap();
        assert!(IdentityProvider::new("https://idp.example.com/path", pool).is_err());
        assert!(IdentityProvider::new("https://idp.example.com", JwtPool::default()).is_err());
    }
}
