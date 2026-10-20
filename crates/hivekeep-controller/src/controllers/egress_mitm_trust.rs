//! Publishes the roots of the egress MITM CA pool as a ClusterTrustBundle,
//! so actors can trust the leaves the egress gateway signs per SNI.

use async_trait::async_trait;
use hivekeep_identity::ca::{CaPool, CertificateAuthority};
use hivekeep_identity::trust_bundle::EGRESS_CLUSTER_TRUST_BUNDLE;
use hivekeep_k8s::cert_api::VersionedApi;
use hivekeep_k8s::cluster_trust_bundle::{ClusterTrustBundle, ClusterTrustBundleClient};
use k8s_openapi::api::core::v1::Secret;
use kube::api::{DeleteParams, Patch, PatchParams, Preconditions};
use kube::Client;
use serde_json::json;

use super::error::{api_err, Error, Result};
use super::queue::ObjectKey;
use super::reader::Reader;
use super::runner::{Action, Reconcile};
use super::worker_env::CANARYING_LABEL;

/// Name of the Secret holding the CA pool the egress gateway signs per-SNI
/// leaves with.
pub const EGRESS_MITM_CA_POOL_SECRET: &str = "egress-mitm-ca-pool";
/// The Secret key the marshaled pool is stored under.
pub const EGRESS_MITM_CA_POOL_KEY: &str = "pool";
/// Signer of the MITM CA's trust domain.
pub const EGRESS_MITM_SIGNER_NAME: &str = "egress-mitm.hivekeep.io/mitm";
/// The ClusterTrustBundle the roots are published as.
pub const EGRESS_MITM_TRUST_BUNDLE_NAME: &str = EGRESS_CLUSTER_TRUST_BUNDLE;
/// Field manager of the ClusterTrustBundle.
pub const EGRESS_MITM_TRUST_FIELD_OWNER: &str = "hivekeep-egress-mitm-trust";

/// The (namespace, name) of the egress MITM CA pool Secret.
pub fn egress_mitm_ca_pool_ref(system_namespace: &str) -> (String, String) {
    (
        system_namespace.to_string(),
        EGRESS_MITM_CA_POOL_SECRET.to_string(),
    )
}

/// Derives the ClusterTrustBundle from the CA pool Secret.
pub struct EgressMitmTrustReconciler {
    secrets: Reader<Secret>,
    bundles: ClusterTrustBundleClient,
    bundle_api: VersionedApi<ClusterTrustBundle>,
}

impl EgressMitmTrustReconciler {
    /// A reconciler reading the pool Secret from `secrets` and writing
    /// bundles through `bundles` (at its discovered version).
    pub fn new(client: Client, secrets: Reader<Secret>, bundles: ClusterTrustBundleClient) -> Self {
        let bundle_api = VersionedApi::all(client, bundles.version());
        EgressMitmTrustReconciler {
            secrets,
            bundles,
            bundle_api,
        }
    }

    /// The version bundles are written at.
    pub fn bundle_version(&self) -> hivekeep_k8s::cert_api::CertVersion {
        self.bundles.version()
    }

    async fn apply_trust_bundle(&self, trust_bundle: String) -> Result<()> {
        let body = json!({
            "apiVersion": self.bundles.version().api_version(),
            "kind": "ClusterTrustBundle",
            "metadata": {
                "name": EGRESS_MITM_TRUST_BUNDLE_NAME,
                "labels": {CANARYING_LABEL: "live"},
            },
            "spec": {
                "signerName": EGRESS_MITM_SIGNER_NAME,
                "trustBundle": trust_bundle,
            },
        });
        // Server-side apply creates and updates in one call, and reverts
        // hand edits to the fields owned here without clobbering another
        // manager's.
        self.bundle_api
            .patch(
                EGRESS_MITM_TRUST_BUNDLE_NAME,
                &PatchParams::apply(EGRESS_MITM_TRUST_FIELD_OWNER).force(),
                &Patch::Apply(&body),
            )
            .await
            .map_err(api_err(format!(
                "failed to apply ClusterTrustBundle {EGRESS_MITM_TRUST_BUNDLE_NAME:?}"
            )))?;
        Ok(())
    }

    /// Deletes the bundle, if it is the MITM signer's. A ClusterTrustBundle
    /// cannot be owned by the namespaced Secret, so nothing else collects
    /// it, and a bundle left behind keeps consumers trusting a CA whose key
    /// may still exist somewhere.
    async fn delete_trust_bundle(&self) -> Result<()> {
        let ctb = match self.bundles.get(EGRESS_MITM_TRUST_BUNDLE_NAME).await {
            Ok(b) => b,
            Err(e) if hivekeep_k8s::api_errors::is_not_found(&e) => return Ok(()),
            Err(e) => {
                return Err(api_err(format!(
                    "failed to get ClusterTrustBundle {EGRESS_MITM_TRUST_BUNDLE_NAME:?}"
                ))(e))
            }
        };
        let signer = ctb.spec.signer_name.clone().unwrap_or_default();
        if signer != EGRESS_MITM_SIGNER_NAME {
            return Err(Error::Refused(format!(
                "refusing to delete ClusterTrustBundle {EGRESS_MITM_TRUST_BUNDLE_NAME:?}: signer is {signer:?}, not {EGRESS_MITM_SIGNER_NAME:?}"
            )));
        }
        let dp = DeleteParams {
            preconditions: Some(Preconditions {
                uid: ctb.metadata.uid.clone(),
                resource_version: None,
            }),
            ..Default::default()
        };
        match self
            .bundles
            .delete(EGRESS_MITM_TRUST_BUNDLE_NAME, &dp)
            .await
        {
            Ok(()) => {}
            Err(e) if hivekeep_k8s::api_errors::is_not_found(&e) => {}
            Err(e) => {
                return Err(api_err(format!(
                    "failed to delete ClusterTrustBundle {EGRESS_MITM_TRUST_BUNDLE_NAME:?}"
                ))(e))
            }
        }
        tracing::info!(
            name = EGRESS_MITM_TRUST_BUNDLE_NAME,
            "Deleted the egress MITM trust bundle; its CA pool is gone"
        );
        Ok(())
    }
}

/// The pool's root certificates, and nothing else, as the PEM a
/// ClusterTrustBundle carries.
pub fn egress_mitm_trust_bundle_pem(secret: &Secret) -> std::result::Result<String, String> {
    let wire = secret
        .data
        .as_ref()
        .and_then(|d| d.get(EGRESS_MITM_CA_POOL_KEY))
        .ok_or_else(|| format!("secret has no {EGRESS_MITM_CA_POOL_KEY:?} key"))?;
    let pool = CaPool::from_json(&wire.0).map_err(|e| format!("parsing the CA pool: {e}"))?;
    if pool.cas().is_empty() {
        return Err("the CA pool contains no CAs".into());
    }
    pool.trust_bundle_pem()
        .map_err(|e| format!("encoding the CA pool's roots: {e}"))
}

#[async_trait]
impl Reconcile for EgressMitmTrustReconciler {
    async fn reconcile(&self, key: &ObjectKey) -> Result<Action> {
        let secret = self
            .secrets
            .get(&key.namespace, &key.name)
            .await
            .map_err(api_err(format!(
                "failed to get egress MITM CA pool {key:?}"
            )))?;
        let Some(secret) = secret.filter(|s| s.metadata.deletion_timestamp.is_none()) else {
            self.delete_trust_bundle().await?;
            return Ok(Action::Done);
        };
        // An unreadable pool says nothing about what the anchor should be;
        // the last good bundle stays and the key is retried.
        let pem = egress_mitm_trust_bundle_pem(&secret).map_err(|e| {
            Error::Invalid(format!(
                "failed to derive the egress MITM trust bundle from {key}: {e}"
            ))
        })?;
        self.apply_trust_bundle(pem).await?;
        tracing::info!(name = EGRESS_MITM_TRUST_BUNDLE_NAME, secret = %key, "Reconciled the egress MITM trust bundle");
        Ok(Action::Done)
    }
}

#[cfg(test)]
#[path = "egress_mitm_trust_tests.rs"]
mod tests;
