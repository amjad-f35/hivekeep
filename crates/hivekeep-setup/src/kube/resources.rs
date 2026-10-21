//! Helpers for the core objects the installer manages directly: namespaces,
//! Secrets, ConfigMaps, Deployments and ServiceAccount annotations.

use super::{Client, Gvk, KubeError, Patch, Propagation};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, ServiceAccount};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

impl Client {
    /// Server-side applies a bare namespace.
    pub async fn ensure_namespace(&self, name: &str) -> Result<(), KubeError> {
        let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}});
        self.apply_one(&ns)
            .await
            .map_err(|e| e.context(format!("while applying namespace {name}")))
    }

    /// Server-side applies an Opaque Secret from string data.
    pub async fn apply_secret(
        &self,
        namespace: &str,
        name: &str,
        data: &BTreeMap<String, String>,
    ) -> Result<(), KubeError> {
        let secret = json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": name, "namespace": namespace},
            "type": "Opaque",
            "stringData": data,
        });
        self.apply_one(&secret)
            .await
            .map_err(|e| e.context(format!("while applying secret {namespace}/{name}")))
    }

    /// Server-side applies a ConfigMap's data.
    pub async fn apply_config_map(
        &self,
        namespace: &str,
        name: &str,
        data: &BTreeMap<String, String>,
    ) -> Result<(), KubeError> {
        let cm = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": name, "namespace": namespace},
            "data": data,
        });
        self.apply_one(&cm)
            .await
            .map_err(|e| e.context(format!("while applying configmap {namespace}/{name}")))
    }

    /// The ConfigMap, or `None` when it does not exist.
    pub async fn get_config_map(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<ConfigMap>, KubeError> {
        self.get_typed::<ConfigMap>(namespace, name)
            .await
            .map_err(|e| e.context(format!("while getting configmap {namespace}/{name}")))
    }

    /// Merge-patches keys into a ConfigMap's data, leaving the others alone.
    pub async fn merge_patch_config_map(
        &self,
        namespace: &str,
        name: &str,
        data: &BTreeMap<String, String>,
    ) -> Result<(), KubeError> {
        self.patch(
            &Gvk::of::<ConfigMap>(),
            namespace,
            name,
            &Patch::Merge(json!({"data": data})),
        )
        .await
        .map(|_| ())
        .map_err(|e| e.context(format!("while patching configmap {namespace}/{name}")))
    }

    /// The Secret, or `None` when it does not exist.
    pub async fn get_secret(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Secret>, KubeError> {
        self.get_typed::<Secret>(namespace, name)
            .await
            .map_err(|e| e.context(format!("while getting secret {namespace}/{name}")))
    }

    pub async fn secret_exists(&self, namespace: &str, name: &str) -> Result<bool, KubeError> {
        Ok(self.get_secret(namespace, name).await?.is_some())
    }

    pub async fn config_map_exists(&self, namespace: &str, name: &str) -> Result<bool, KubeError> {
        Ok(self.get_config_map(namespace, name).await?.is_some())
    }

    /// The Deployment, or `None` when it does not exist.
    pub async fn get_deployment(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Deployment>, KubeError> {
        self.get_typed::<Deployment>(namespace, name)
            .await
            .map_err(|e| e.context(format!("while getting deployment {namespace}/{name}")))
    }

    pub async fn deployment_exists(&self, namespace: &str, name: &str) -> Result<bool, KubeError> {
        Ok(self.get_deployment(namespace, name).await?.is_some())
    }

    /// Deletes a Deployment in the foreground and waits for it to be gone.
    /// Foreground deletion keeps the Deployment until its pods are gone, so
    /// its absence means theirs.
    pub async fn delete_deployment_and_wait(
        &self,
        namespace: &str,
        name: &str,
        timeout: Duration,
    ) -> Result<(), KubeError> {
        self.delete_named(
            &Gvk::of::<Deployment>(),
            namespace,
            name,
            Some(Propagation::Foreground),
        )
        .await?;
        self.poll(timeout, || async {
            Ok(self.get_deployment(namespace, name).await?.is_none())
        })
        .await
        .map_err(|e| {
            e.context(format!(
                "waiting for deployment {namespace}/{name} to be deleted"
            ))
        })
    }

    /// Strategic-merge-patches a Deployment with a YAML (or JSON) patch.
    pub async fn patch_deployment(
        &self,
        namespace: &str,
        name: &str,
        patch: &str,
    ) -> Result<(), KubeError> {
        let body: Value = serde_yaml::from_str(patch).map_err(|e| {
            KubeError::Decode(format!(
                "while parsing the patch for deployment {namespace}/{name}: {e}"
            ))
        })?;
        self.patch(
            &Gvk::of::<Deployment>(),
            namespace,
            name,
            &Patch::Strategic(body),
        )
        .await
        .map(|_| ())
        .map_err(|e| e.context(format!("while patching deployment {namespace}/{name}")))
    }

    /// Sets (or, with an empty value, removes) a ServiceAccount annotation.
    /// A missing ServiceAccount is not an error.
    pub async fn set_service_account_annotation(
        &self,
        namespace: &str,
        name: &str,
        key: &str,
        value: &str,
    ) -> Result<(), KubeError> {
        let annotation = if value.is_empty() {
            Value::Null
        } else {
            Value::String(value.into())
        };
        let patch = json!({"metadata": {"annotations": {key: annotation}}});
        match self
            .patch(
                &Gvk::of::<ServiceAccount>(),
                namespace,
                name,
                &Patch::Merge(patch),
            )
            .await
        {
            Err(e) if !e.is_not_found() => Err(e.context(format!(
                "while annotating serviceaccount {namespace}/{name}"
            ))),
            _ => Ok(()),
        }
    }

    /// A ServiceAccount annotation; empty when it or the account is absent.
    pub async fn service_account_annotation(
        &self,
        namespace: &str,
        name: &str,
        key: &str,
    ) -> Result<String, KubeError> {
        let sa = self
            .get_typed::<ServiceAccount>(namespace, name)
            .await
            .map_err(|e| e.context(format!("while getting serviceaccount {namespace}/{name}")))?;
        Ok(sa
            .and_then(|sa| sa.metadata.annotations)
            .and_then(|a| a.get(key).cloned())
            .unwrap_or_default())
    }

    /// The service account token issuer the API server advertises through
    /// OpenID discovery; empty when it advertises none.
    pub async fn oidc_issuer(&self) -> String {
        let Ok(raw) = self.raw_get("/.well-known/openid-configuration").await else {
            return String::new();
        };
        serde_json::from_slice::<Value>(&raw)
            .ok()
            .and_then(|v| v.get("issuer").and_then(|i| i.as_str()).map(str::to_string))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube::fake::FakeCluster;
    use crate::kube::stub;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn delete_deployment_and_wait() {
        let gvk = Gvk::of::<Deployment>();
        let dep = stub(&gvk, "hivekeep-system", "fake-workersync");
        let f = FakeCluster::with_objects([dep.clone()]);
        // The Deployment lingers for the first few reads after the delete,
        // as while the garbage collector removes its pods.
        let lingering = 3;
        let gets = Arc::new(AtomicUsize::new(0));
        let g = gets.clone();
        f.prepend_reactor("get", "Deployment", move |_| {
            let n = g.fetch_add(1, Ordering::SeqCst) + 1;
            if n > lingering {
                return None;
            }
            let mut terminating = dep.clone();
            terminating["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
            Some(Ok(terminating))
        });
        let c = Client::new(Arc::new(f.clone())).with_fast_timings();

        c.delete_deployment_and_wait("hivekeep-system", "fake-workersync", Duration::from_secs(1))
            .await
            .unwrap();
        assert!(
            gets.load(Ordering::SeqCst) > lingering,
            "returned while the Deployment still existed"
        );
        assert!(c
            .get_deployment("hivekeep-system", "fake-workersync")
            .await
            .unwrap()
            .is_none());
        let policy = f
            .actions()
            .iter()
            .rev()
            .find(|a| a.verb == "delete")
            .and_then(|a| a.propagation);
        assert_eq!(policy, Some(Propagation::Foreground));

        c.delete_deployment_and_wait("hivekeep-system", "fake-workersync", Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn service_account_annotation_round_trip() {
        let f = FakeCluster::with_objects([stub(&Gvk::of::<ServiceAccount>(), "ns", "sa")]);
        let c = Client::new(Arc::new(f));
        c.set_service_account_annotation("ns", "sa", "k", "v")
            .await
            .unwrap();
        assert_eq!(
            c.service_account_annotation("ns", "sa", "k").await.unwrap(),
            "v"
        );
        c.set_service_account_annotation("ns", "sa", "k", "")
            .await
            .unwrap();
        assert_eq!(
            c.service_account_annotation("ns", "sa", "k").await.unwrap(),
            ""
        );
        // Absent accounts are tolerated both ways.
        c.set_service_account_annotation("ns", "missing", "k", "v")
            .await
            .unwrap();
        assert_eq!(
            c.service_account_annotation("ns", "missing", "k")
                .await
                .unwrap(),
            ""
        );
    }
}
