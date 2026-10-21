//! The bundled Kubernetes Secrets credential provider the egress gateway
//! injects credentials from.

use super::{Env, Result};
use crate::log;
use std::path::PathBuf;

/// Directory of the provider's manifests, under the manifests root. They sit
/// outside `install/` because only installs that select the provider apply
/// them; `sample-secret.yaml` there is never applied.
pub const K8S_CREDENTIAL_PROVIDER_DIR: &str = "egress-credential-injection";
pub const K8S_CREDENTIAL_PROVIDER_MANIFEST: &str = "k8s-credential-provider.yaml";
pub const K8S_CREDENTIAL_PROVIDER_POLICY_MANIFEST: &str = "namespace-policy.yaml";
/// Object names from the manifests above; the tests check them.
pub const K8S_CREDENTIAL_PROVIDER_DEPLOYMENT: &str = "k8s-credential-provider";
pub const K8S_CREDENTIAL_PROVIDER_POLICY_CONFIG_MAP: &str =
    "k8s-credential-provider-namespace-policy";

impl Env {
    pub(crate) fn k8s_credential_provider_path(&self, manifest: &str) -> PathBuf {
        self.cfg
            .manifests_path(&[K8S_CREDENTIAL_PROVIDER_DIR, manifest])
    }

    /// Deploys the bundled provider. The namespace policy ConfigMap is
    /// applied only when absent: the provider needs it to start, but it holds
    /// the operator's hive allow-list, which a redeploy must keep.
    pub(crate) async fn deploy_k8s_credential_provider(&self) -> Result<()> {
        log::step("deploy_k8s_credential_provider");
        let k = self.kube()?;
        if !k
            .config_map_exists(self.namespace(), K8S_CREDENTIAL_PROVIDER_POLICY_CONFIG_MAP)
            .await?
        {
            k.apply_path(
                &self.k8s_credential_provider_path(K8S_CREDENTIAL_PROVIDER_POLICY_MANIFEST),
            )
            .await?;
        }
        self.render_resolve_apply(
            &self.k8s_credential_provider_path(K8S_CREDENTIAL_PROVIDER_MANIFEST),
        )
        .await
    }

    /// Removes a bundled provider an earlier install left, so it does not
    /// keep its cluster-wide Secret read once the gateway no longer uses it.
    pub(crate) async fn remove_k8s_credential_provider(&self) -> Result<()> {
        let k = self.kube()?;
        if !k
            .deployment_exists(self.namespace(), K8S_CREDENTIAL_PROVIDER_DEPLOYMENT)
            .await?
        {
            return Ok(());
        }
        log::step("remove_k8s_credential_provider");
        Ok(
            k.delete_path(&self.k8s_credential_provider_path(K8S_CREDENTIAL_PROVIDER_MANIFEST))
                .await?,
        )
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::{test_config, K8S_CREDENTIAL_PROVIDER_ADDRESS};
    use crate::kube::{load_path, Object, ObjectExt};
    use crate::steps::env::NAMESPACE_SYSTEM;
    use serde_json::Value;
    use std::collections::BTreeMap;

    pub(crate) fn find_object<'a>(
        objs: &'a [Object],
        kind: &str,
        name: &str,
    ) -> Option<&'a Object> {
        objs.iter().find(|o| o.kind() == kind && o.name() == name)
    }

    fn string_map(v: Option<&Value>) -> BTreeMap<String, String> {
        v.and_then(Value::as_object)
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn pod_labels(dep: &Object) -> BTreeMap<String, String> {
        string_map(dep.pointer("/spec/template/metadata/labels"))
    }

    fn selects(selector: &BTreeMap<String, String>, labels: &BTreeMap<String, String>) -> bool {
        !selector.is_empty() && selector.iter().all(|(k, v)| labels.get(k) == Some(v))
    }

    fn container_port(dep: &Object, name: &str) -> i64 {
        for c in dep
            .pointer("/spec/template/spec/containers")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            for p in c
                .get("ports")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if p.get("name").and_then(Value::as_str) == Some(name) {
                    return p
                        .get("containerPort")
                        .and_then(Value::as_i64)
                        .expect("numeric containerPort");
                }
            }
        }
        panic!(
            "deployment/{} has no container port named {name:?}",
            dep.name()
        );
    }

    fn mounted_config_maps(dep: &Object) -> Vec<String> {
        dep.pointer("/spec/template/spec/volumes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|v| {
                v.pointer("/configMap/name")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect()
    }

    #[test]
    fn k8s_credential_provider_manifests_agree() {
        let e = Env::local(test_config());
        let provider =
            load_path(&e.k8s_credential_provider_path(K8S_CREDENTIAL_PROVIDER_MANIFEST)).unwrap();
        let dep = find_object(&provider, "Deployment", K8S_CREDENTIAL_PROVIDER_DEPLOYMENT)
            .expect("deployment");
        assert_eq!(dep.obj_namespace(), NAMESPACE_SYSTEM);
        assert!(find_object(
            &provider,
            "ConfigMap",
            K8S_CREDENTIAL_PROVIDER_POLICY_CONFIG_MAP
        )
        .is_none());
        assert_eq!(
            mounted_config_maps(dep),
            vec![K8S_CREDENTIAL_PROVIDER_POLICY_CONFIG_MAP.to_string()]
        );
        let svc =
            find_object(&provider, "Service", K8S_CREDENTIAL_PROVIDER_DEPLOYMENT).expect("service");
        assert_eq!(
            format!("{}.{}.svc:50051", svc.name(), svc.obj_namespace()),
            K8S_CREDENTIAL_PROVIDER_ADDRESS
        );

        let policy =
            load_path(&e.k8s_credential_provider_path(K8S_CREDENTIAL_PROVIDER_POLICY_MANIFEST))
                .unwrap();
        let cm = find_object(
            &policy,
            "ConfigMap",
            K8S_CREDENTIAL_PROVIDER_POLICY_CONFIG_MAP,
        )
        .expect("configmap");
        assert_eq!(cm.obj_namespace(), NAMESPACE_SYSTEM);
    }

    #[test]
    fn k8s_credential_provider_network_policy() {
        let e = Env::local(test_config());
        let provider =
            load_path(&e.k8s_credential_provider_path(K8S_CREDENTIAL_PROVIDER_MANIFEST)).unwrap();
        let dep = find_object(&provider, "Deployment", K8S_CREDENTIAL_PROVIDER_DEPLOYMENT)
            .expect("deployment");
        let np = find_object(
            &provider,
            "NetworkPolicy",
            K8S_CREDENTIAL_PROVIDER_DEPLOYMENT,
        )
        .expect("networkpolicy");
        assert_eq!(np.obj_namespace(), NAMESPACE_SYSTEM);
        let selector = string_map(np.pointer("/spec/podSelector/matchLabels"));
        assert!(
            selects(&selector, &pod_labels(dep)),
            "{selector:?} vs {:?}",
            pod_labels(dep)
        );
        assert_eq!(
            np.pointer("/spec/policyTypes"),
            Some(&serde_json::json!(["Ingress"]))
        );

        let ingress = np
            .pointer("/spec/ingress")
            .and_then(Value::as_array)
            .unwrap();
        assert_eq!(ingress.len(), 1);
        let rule = &ingress[0];
        let egress_objs = load_path(&e.cfg.manifest(&["gate-egress.yaml"])).unwrap();
        let gateway = find_object(&egress_objs, "Deployment", "gate-egress").expect("gate-egress");
        assert_eq!(gateway.obj_namespace(), np.obj_namespace());
        let from = rule.get("from").and_then(Value::as_array).unwrap();
        assert_eq!(from.len(), 1, "want only the egress gateway");
        let peer = from[0].as_object().unwrap();
        assert_eq!(peer.len(), 1, "want a podSelector alone: {peer:?}");
        let peer_labels = string_map(from[0].pointer("/podSelector/matchLabels"));
        assert!(selects(&peer_labels, &pod_labels(gateway)));

        let ports = rule.get("ports").and_then(Value::as_array).unwrap();
        let want = container_port(dep, "grpc");
        assert_eq!(ports.len(), 1);
        assert_eq!(
            ports[0].get("protocol").and_then(Value::as_str),
            Some("TCP")
        );
        assert_eq!(ports[0].get("port").and_then(Value::as_i64), Some(want));
    }
}
