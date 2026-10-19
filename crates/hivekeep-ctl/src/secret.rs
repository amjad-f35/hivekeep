//! Kubernetes Secrets holding key pools, read and written through kubectl.
//! Writes are conditional on the Secret's resourceVersion.

use std::collections::BTreeMap;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde_json::{json, Map, Value};

use crate::kubectl::Kubectl;

/// The Secret type of a TLS key pair.
pub const TLS_SECRET_TYPE: &str = "kubernetes.io/tls";
/// Key of a TLS Secret's certificate chain.
pub const TLS_CERT_KEY: &str = "tls.crt";
/// Key of a TLS Secret's private key.
pub const TLS_KEY_KEY: &str = "tls.key";

/// A Secret, reduced to what the pool commands use.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Secret {
    /// Namespace.
    pub namespace: String,
    /// Name.
    pub name: String,
    /// Secret type; `Opaque` when unset.
    pub secret_type: Option<String>,
    /// Data, decoded.
    pub data: BTreeMap<String, Vec<u8>>,
    /// The version read, which guards an update.
    pub resource_version: Option<String>,
}

impl Secret {
    /// The Secret as a Kubernetes object.
    pub fn to_json(&self) -> Value {
        let mut metadata = Map::new();
        metadata.insert("namespace".into(), json!(self.namespace));
        metadata.insert("name".into(), json!(self.name));
        if let Some(rv) = &self.resource_version {
            metadata.insert("resourceVersion".into(), json!(rv));
        }
        let data: Map<String, Value> = self
            .data
            .iter()
            .map(|(k, v)| (k.clone(), json!(STANDARD.encode(v))))
            .collect();
        let mut obj = json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": metadata,
            "data": data,
        });
        if let Some(t) = &self.secret_type {
            obj["type"] = json!(t);
        }
        obj
    }

    /// Parses a Secret object.
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let v: Value = serde_json::from_slice(bytes).map_err(|e| format!("decode secret: {e}"))?;
        let meta = &v["metadata"];
        let s = |x: &Value| x.as_str().map(String::from);
        let mut data = BTreeMap::new();
        if let Some(d) = v["data"].as_object() {
            for (k, val) in d {
                let decoded = STANDARD
                    .decode(val.as_str().unwrap_or_default())
                    .map_err(|e| format!("decode secret key {k:?}: {e}"))?;
                data.insert(k.clone(), decoded);
            }
        }
        Ok(Secret {
            namespace: s(&meta["namespace"]).unwrap_or_default(),
            name: s(&meta["name"]).unwrap_or_default(),
            secret_type: s(&v["type"]),
            data,
            resource_version: s(&meta["resourceVersion"]),
        })
    }
}

/// A failed Secret operation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SecretError {
    /// The Secret does not exist.
    #[error("{0}")]
    NotFound(String),
    /// The Secret changed since it was read.
    #[error("{0}")]
    Conflict(String),
    /// Anything else.
    #[error("{0}")]
    Other(String),
}

/// Where Secrets are kept.
pub trait SecretStore {
    /// Reads a Secret.
    fn get(&self, namespace: &str, name: &str) -> Result<Secret, SecretError>;
    /// Creates a Secret; fails if it exists.
    fn create(&self, secret: &Secret) -> Result<(), SecretError>;
    /// Replaces a Secret, provided it is still at the version read.
    fn update(&self, secret: &Secret) -> Result<(), SecretError>;
}

/// Secrets in the cluster, through kubectl.
pub struct KubectlSecrets(pub Kubectl);

fn classify(e: crate::kubectl::KubectlError) -> SecretError {
    if e.is_conflict() {
        SecretError::Conflict(e.message)
    } else if e.is_not_found() {
        SecretError::NotFound(e.message)
    } else {
        SecretError::Other(e.message)
    }
}

impl SecretStore for KubectlSecrets {
    fn get(&self, namespace: &str, name: &str) -> Result<Secret, SecretError> {
        let out = self
            .0
            .run(
                &["get", "secret", name, "-n", namespace, "-o", "json"],
                None,
            )
            .map_err(classify)?;
        Secret::from_json(&out).map_err(SecretError::Other)
    }

    fn create(&self, secret: &Secret) -> Result<(), SecretError> {
        let body = secret.to_json().to_string();
        self.0
            .run(&["create", "-f", "-"], Some(body.as_bytes()))
            .map(drop)
            .map_err(classify)
    }

    fn update(&self, secret: &Secret) -> Result<(), SecretError> {
        let body = secret.to_json().to_string();
        self.0
            .run(&["replace", "-f", "-"], Some(body.as_bytes()))
            .map(drop)
            .map_err(classify)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_round_trip() {
        let s = Secret {
            namespace: "ns".into(),
            name: "pool".into(),
            secret_type: Some(TLS_SECRET_TYPE.into()),
            data: [("pool".to_string(), b"{}".to_vec())].into(),
            resource_version: Some("42".into()),
        };
        let v = s.to_json();
        assert_eq!(v["data"]["pool"], "e30=");
        assert_eq!(v["metadata"]["resourceVersion"], "42");
        assert_eq!(Secret::from_json(v.to_string().as_bytes()).unwrap(), s);
    }

    #[test]
    fn new_secret_has_no_version() {
        let v = Secret {
            namespace: "ns".into(),
            name: "p".into(),
            ..Default::default()
        }
        .to_json();
        assert!(v["metadata"].get("resourceVersion").is_none());
        assert!(v.get("type").is_none());
    }
}
