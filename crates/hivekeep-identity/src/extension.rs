//! Hivekeep's private X.509 extensions, carrying the identity a certificate
//! was minted for in machine-readable form (as JSON) next to its SPIFFE URI.

use rcgen::CustomExtension;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::error::{IdentityError, Result};

/// Object-identifier arc under which Hivekeep's extensions are numbered.
///
/// Relying parties match extensions by these numbers, so production builds
/// must use an arc the project controls (a registered Private Enterprise
/// Number). Until one is assigned this sits under 32473, the enterprise
/// number reserved for documentation use (RFC 5612).
pub const OID_ARC: &[u64] = &[1, 3, 6, 1, 4, 1, 32473, 1];

/// Identifier of the [`PodIdentity`] extension.
pub fn pod_identity_oid() -> Vec<u64> {
    [OID_ARC, &[1]].concat()
}

/// Identifier of the [`ActorIdentity`] extension.
pub fn actor_identity_oid() -> Vec<u64> {
    [OID_ARC, &[2]].concat()
}

/// The Kubernetes Pod a certificate was issued to.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PodIdentity {
    /// Namespace of the Pod.
    pub namespace: String,
    /// Service account the Pod runs as.
    pub service_account_name: String,
    /// UID of that service account.
    #[serde(rename = "ServiceAccountUID")]
    pub service_account_uid: String,
    /// Pod name.
    pub pod_name: String,
    /// Pod UID.
    #[serde(rename = "PodUID")]
    pub pod_uid: String,
    /// Node the Pod is bound to.
    pub node_name: String,
    /// UID of that Node.
    #[serde(rename = "NodeUID")]
    pub node_uid: String,
}

/// The Actor a certificate was issued for.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ActorIdentity {
    /// Hive of the Actor.
    pub hive: String,
    /// Actor name.
    pub actor_name: String,
    /// Actor UID, which distinguishes incarnations of the same name.
    pub actor_uid: String,
}

/// An identity that travels in a certificate extension.
pub trait IdentityExtension: Serialize + DeserializeOwned {
    /// The extension's identifier.
    fn oid() -> Vec<u64>;

    /// Names of required fields that are empty.
    fn empty_fields(&self) -> Vec<&'static str>;

    /// Checks that every field is set.
    fn validate(&self) -> Result<()> {
        let empty = self.empty_fields();
        if empty.is_empty() {
            Ok(())
        } else {
            Err(IdentityError::Certificate(format!(
                "empty fields: {}",
                empty.join(", ")
            )))
        }
    }

    /// The extension to add to a certificate.
    fn to_extension(&self) -> Result<CustomExtension> {
        self.validate()?;
        let json =
            serde_json::to_vec(self).map_err(|e| IdentityError::Certificate(e.to_string()))?;
        Ok(CustomExtension::from_oid_content(&Self::oid(), json))
    }

    /// Reads the extension from a DER certificate: `None` if absent, an
    /// error if repeated, malformed or incomplete.
    fn from_certificate(cert_der: &[u8]) -> Result<Option<Self>> {
        let (_, cert) = X509Certificate::from_der(cert_der)
            .map_err(|e| IdentityError::Certificate(format!("parsing certificate: {e}")))?;
        let oid = Self::oid();
        let mut found = cert.extensions().iter().filter(|ext| {
            ext.oid
                .iter()
                .is_some_and(|arcs| arcs.eq(oid.iter().copied()))
        });
        let Some(ext) = found.next() else {
            return Ok(None);
        };
        if found.next().is_some() {
            return Err(IdentityError::Certificate(
                "certificate repeats an identity extension".into(),
            ));
        }
        let id: Self = serde_json::from_slice(ext.value)
            .map_err(|e| IdentityError::Certificate(format!("decoding identity extension: {e}")))?;
        id.validate()?;
        Ok(Some(id))
    }
}

impl IdentityExtension for PodIdentity {
    fn oid() -> Vec<u64> {
        pod_identity_oid()
    }

    fn empty_fields(&self) -> Vec<&'static str> {
        [
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
        .collect()
    }
}

impl IdentityExtension for ActorIdentity {
    fn oid() -> Vec<u64> {
        actor_identity_oid()
    }

    fn empty_fields(&self) -> Vec<&'static str> {
        [
            ("Hive", &self.hive),
            ("ActorName", &self.actor_name),
            ("ActorUid", &self.actor_uid),
        ]
        .into_iter()
        .filter(|(_, v)| v.is_empty())
        .map(|(k, _)| k)
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_field_names() {
        let a = ActorIdentity {
            hive: "h".into(),
            actor_name: "a".into(),
            actor_uid: "u".into(),
        };
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            r#"{"Hive":"h","ActorName":"a","ActorUid":"u"}"#
        );
        let p: PodIdentity = serde_json::from_str(
            r#"{"Namespace":"n","ServiceAccountName":"s","ServiceAccountUID":"su","PodName":"p","PodUID":"pu","NodeName":"nn","NodeUID":"nu"}"#,
        )
        .unwrap();
        assert!(p.validate().is_ok());
        assert_eq!(p.node_uid, "nu");
    }

    #[test]
    fn incomplete_identities_are_refused() {
        let err = ActorIdentity {
            hive: "h".into(),
            ..Default::default()
        }
        .to_extension()
        .unwrap_err()
        .to_string();
        assert!(err.contains("ActorName, ActorUid"), "{err}");
        assert_eq!(PodIdentity::default().empty_fields().len(), 7);
    }
}
