//! `servicedns.podcert.hivekeep.io/identity`: serving certificates for the
//! Services that select the requesting Pod, backed by a local CA pool.

use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_identity::ca::CertificateAuthority;
use hivekeep_k8s::cluster_trust_bundle::ClusterTrustBundle;
use hivekeep_node::clock::SharedClock;
use k8s_openapi::api::core::v1::{Pod, Service};
use kube::api::{Api, ListParams};
use kube::{Client, ResourceExt};

use crate::error::SignerError;
use crate::pod_certificate::{public_key, PcrClient, PodCertificateRequest};
use crate::signer::issue::{capped_lifetime, sign_leaf};
use crate::signer::status::with_issued;
use crate::signer::trust_bundle::primary_bundle;
use crate::signer::SignerImpl;

/// The signer name.
pub const NAME: &str = "servicedns.podcert.hivekeep.io/identity";

/// Prefix of the signer's ClusterTrustBundles.
pub const CTB_PREFIX: &str = "servicedns.podcert.hivekeep.io:identity:";

/// Issues certificates naming `<service>.<namespace>.svc` for every Service
/// selecting the requesting Pod.
pub struct ServiceDnsSigner {
    client: Client,
    ca: Arc<dyn CertificateAuthority>,
    pcrs: Arc<PcrClient>,
    clock: SharedClock,
}

impl ServiceDnsSigner {
    /// A signer reading Services and Pods through `client`, issuing from
    /// `ca` and answering through `pcrs`.
    pub fn new(
        client: Client,
        ca: Arc<dyn CertificateAuthority>,
        pcrs: Arc<PcrClient>,
        clock: SharedClock,
    ) -> Self {
        ServiceDnsSigner {
            client,
            ca,
            pcrs,
            clock,
        }
    }

    /// The DNS names of the Services in the request's namespace that select
    /// the requesting Pod (matched by name and UID). Services that select
    /// no Pods by label (ExternalName, selector-less) are skipped.
    pub async fn dns_names(&self, pcr: &PodCertificateRequest) -> Result<Vec<String>, SignerError> {
        // Every Service is checked live; an index from Pods to covering
        // Services would avoid the scan on large namespaces.
        let namespace = pcr.metadata.namespace.as_deref().unwrap_or_default();
        let services: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let mut names = Vec::new();
        for svc in services
            .list(&ListParams::default())
            .await
            .map_err(|e| SignerError::ListServices(Box::new(e)))?
        {
            let Some(spec) = svc.spec.as_ref() else {
                continue;
            };
            if !matches!(
                spec.type_.as_deref().unwrap_or("ClusterIP"),
                "ClusterIP" | "NodePort" | "LoadBalancer"
            ) {
                continue;
            }
            let Some(selector) = spec.selector.as_ref().filter(|s| !s.is_empty()) else {
                continue;
            };
            let selector: Vec<String> = selector.iter().map(|(k, v)| format!("{k}={v}")).collect();
            let matched = pods
                .list(&ListParams::default().labels(&selector.join(",")))
                .await
                .map_err(|source| SignerError::SelectPods {
                    service: format!("{namespace}/{}", svc.name_any()),
                    source: Box::new(source),
                })?;
            if matched.items.iter().any(|p| {
                p.metadata.name.as_deref() == Some(pcr.spec.pod_name.as_str())
                    && p.metadata.uid.as_deref() == Some(pcr.spec.pod_uid.as_str())
            }) {
                // Assumes the cluster's default Service DNS layout.
                names.push(format!("{}.{namespace}.svc", svc.name_any()));
            }
        }
        Ok(names)
    }
}

#[async_trait]
impl SignerImpl for ServiceDnsSigner {
    fn signer_name(&self) -> &str {
        NAME
    }

    fn desired_cluster_trust_bundles(&self) -> Result<Vec<ClusterTrustBundle>, SignerError> {
        Ok(vec![primary_bundle(CTB_PREFIX, NAME, self.ca.as_ref())?])
    }

    async fn make_cert(&self, pcr: &PodCertificateRequest) -> Result<(), SignerError> {
        let dns_names = self.dns_names(pcr).await?;
        if dns_names.is_empty() {
            return Err(SignerError::NotSelected {
                namespace: pcr.metadata.namespace.clone().unwrap_or_default(),
                pod: pcr.spec.pod_name.clone(),
            });
        }
        let subject = public_key(pcr)?;
        let now = self.clock.now();
        let issued = sign_leaf(
            self.ca.as_ref(),
            &subject,
            capped_lifetime(pcr),
            now.into(),
            |leaf| {
                leaf.dns_names = dns_names;
                leaf.client_auth = true;
                leaf.server_auth = true;
            },
        )?;
        self.pcrs
            .update_status(&with_issued(pcr, &issued, now))
            .await
            .map_err(|e| SignerError::UpdateStatus(Box::new(e)))?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "service_dns_signer_tests.rs"]
mod tests;
