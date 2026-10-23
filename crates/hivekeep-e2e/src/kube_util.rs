//! Kubernetes plumbing the harness needs beyond plain CRUD: loading the
//! cluster config, pod readiness, ClusterTrustBundles and pod-identity
//! certificates for processes outside the cluster.

use std::path::Path;
use std::time::Duration;

use base64::Engine as _;
use hivekeep_k8s::cluster_trust_bundle::{ClusterTrustBundle, ClusterTrustBundleClient};
use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::Client;

/// Builds a client for the kubeconfig at `path`, using `context` if given.
pub async fn load_client(path: &Path, context: Option<&str>) -> Result<Client, String> {
    let kubeconfig = Kubeconfig::read_from(path)
        .map_err(|e| format!("reading kubeconfig {}: {e}", path.display()))?;
    let options = KubeConfigOptions {
        context: context.map(str::to_string),
        ..Default::default()
    };
    let config = kube::Config::from_custom_kubeconfig(kubeconfig, &options)
        .await
        .map_err(|e| {
            format!(
                "loading kubeconfig {} (context {context:?}): {e}",
                path.display()
            )
        })?;
    Client::try_from(config).map_err(|e| format!("building the Kubernetes client: {e}"))
}

pub use hivekeep_k8s::api_errors::{is_already_exists, is_conflict, is_not_found};

/// Whether `pod` is running, not terminating, and Ready.
pub fn is_pod_ready(pod: &Pod) -> bool {
    let Some(status) = &pod.status else {
        return false;
    };
    if status.phase.as_deref() != Some("Running") || pod.metadata.deletion_timestamp.is_some() {
        return false;
    }
    status
        .conditions
        .iter()
        .flatten()
        .find(|c| c.type_ == "Ready")
        .map(|c| c.status == "True")
        .unwrap_or(false)
}

/// Why a pod is not ready yet, one clause per container, for a timeout
/// message.
pub fn describe_pod_state(pod: &Pod) -> String {
    let status = pod.status.clone().unwrap_or_default();
    let mut parts = vec![format!("phase={}", status.phase.unwrap_or_default())];
    for cs in status.container_statuses.unwrap_or_default() {
        let state = cs.state.unwrap_or_default();
        if let Some(w) = state.waiting {
            parts.push(format!(
                "{} waiting: {}: {}",
                cs.name,
                w.reason.unwrap_or_default(),
                w.message.unwrap_or_default()
            ));
        } else if let Some(term) = state.terminated {
            parts.push(format!(
                "{} terminated: {}: {}",
                cs.name,
                term.reason.unwrap_or_default(),
                term.message.unwrap_or_default()
            ));
        } else {
            parts.push(format!("{} running, ready={}", cs.name, cs.ready));
        }
    }
    parts.join("; ")
}

/// A ClusterTrustBundle, reduced to what the harness reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustBundle {
    /// Object name.
    pub name: String,
    /// The signer it belongs to, if any.
    pub signer_name: Option<String>,
    /// The PEM bundle.
    pub trust_bundle: String,
}

fn bundle_of(ctb: ClusterTrustBundle) -> TrustBundle {
    TrustBundle {
        name: ctb.metadata.name.unwrap_or_default(),
        signer_name: ctb.spec.signer_name,
        trust_bundle: ctb.spec.trust_bundle,
    }
}

/// The ClusterTrustBundle API the cluster serves (v1 or v1beta1).
async fn trust_bundle_client(client: &Client) -> Result<ClusterTrustBundleClient, String> {
    ClusterTrustBundleClient::new(client.clone(), None)
        .await
        .map_err(|e| format!("discovering the ClusterTrustBundle API: {e}"))
}

/// Lists ClusterTrustBundles matching `label_selector`.
pub async fn list_trust_bundles(
    client: &Client,
    label_selector: &str,
) -> Result<Vec<TrustBundle>, String> {
    let list = trust_bundle_client(client)
        .await?
        .list(&ListParams::default().labels(label_selector))
        .await
        .map_err(|e| format!("failed to list ClusterTrustBundles: {e}"))?;
    Ok(list.into_iter().map(bundle_of).collect())
}

/// Gets the ClusterTrustBundle `name`.
pub async fn get_trust_bundle(client: &Client, name: &str) -> Result<TrustBundle, String> {
    trust_bundle_client(client)
        .await?
        .get(name)
        .await
        .map(bundle_of)
        .map_err(|e| format!("getting ClusterTrustBundle {name}: {e}"))
}

/// The concatenated live trust bundles of `signer`: the roots in-cluster
/// clients of the signer's serving certificates trust.
pub async fn live_signer_roots(client: &Client, signer: &str) -> Result<String, String> {
    let bundles = list_trust_bundles(client, crate::install::LIVE_BUNDLE_SELECTOR).await?;
    let mut pem = String::new();
    for b in bundles
        .into_iter()
        .filter(|b| b.signer_name.as_deref() == Some(signer))
    {
        if crate::trustbundle::certificate_blocks(&b.trust_bundle)
            .is_none_or(|blocks| blocks.is_empty())
        {
            return Err(format!(
                "ClusterTrustBundle {:?} contains no valid certificates",
                b.name
            ));
        }
        pem.push_str(&b.trust_bundle);
        if !pem.ends_with('\n') {
            pem.push('\n');
        }
    }
    if pem.is_empty() {
        return Err(format!(
            "no live ClusterTrustBundle found for signer {signer:?}"
        ));
    }
    Ok(pem)
}

/// A pod-identity certificate and its key, both PEM.
#[derive(Debug, Clone)]
pub struct PodCertificate {
    /// The chain, leaf first.
    pub chain_pem: String,
    /// The PKCS#8 private key.
    pub key_pem: String,
}

/// Requests a pod-identity certificate for `service_account` in `namespace`
/// through a PodCertificateRequest (see
/// [`hivekeep_k8s::pod_identity::request_certificate`]), waiting up to
/// `timeout` for the signer to issue it. `requester` names the node and
/// prefixes the request name.
pub async fn request_pod_certificate(
    client: &Client,
    requester: &str,
    namespace: &str,
    service_account: &str,
    timeout: Duration,
) -> Result<PodCertificate, String> {
    let cred = hivekeep_k8s::pod_identity::request_certificate(
        client.clone(),
        requester,
        namespace,
        service_account,
        timeout,
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(PodCertificate {
        chain_pem: hivekeep_identity::pem::encode_certificates(
            cred.chain.iter().map(Vec::as_slice),
        ),
        key_pem: cred.key.to_pem(),
    })
}

/// Base64 of `data`, as the Kubernetes API carries bytes.
pub fn b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        ContainerState, ContainerStateWaiting, ContainerStatus, PodCondition, PodStatus,
    };

    fn pod(phase: &str, ready: &str) -> Pod {
        Pod {
            status: Some(PodStatus {
                phase: Some(phase.into()),
                conditions: Some(vec![PodCondition {
                    type_: "Ready".into(),
                    status: ready.into(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn pod_readiness() {
        assert!(is_pod_ready(&pod("Running", "True")));
        assert!(!is_pod_ready(&pod("Running", "False")));
        assert!(!is_pod_ready(&pod("Pending", "True")));
        let mut deleting = pod("Running", "True");
        deleting.metadata.deletion_timestamp =
            Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                k8s_openapi::jiff::Timestamp::UNIX_EPOCH,
            ));
        assert!(!is_pod_ready(&deleting));
        assert!(!is_pod_ready(&Pod::default()));
    }

    #[test]
    fn pod_state_description() {
        let mut p = pod("Pending", "False");
        p.status.as_mut().unwrap().container_statuses = Some(vec![ContainerStatus {
            name: "server".into(),
            state: Some(ContainerState {
                waiting: Some(ContainerStateWaiting {
                    reason: Some("ImagePullBackOff".into()),
                    message: Some("no such image".into()),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }]);
        assert_eq!(
            describe_pod_state(&p),
            "phase=Pending; server waiting: ImagePullBackOff: no such image"
        );
    }
}
