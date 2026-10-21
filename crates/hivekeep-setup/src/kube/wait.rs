//! Rollout and condition waits, and rollout restarts.

use super::{Client, Gvk, KubeError, Patch};
use crate::log;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::core::v1::Namespace;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Workload kinds a rollout can be waited on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkloadKind {
    Deployment,
    DaemonSet,
    StatefulSet,
}

impl WorkloadKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deployment => "deployment",
            Self::DaemonSet => "daemonset",
            Self::StatefulSet => "statefulset",
        }
    }
}

impl std::fmt::Display for WorkloadKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How many consecutive not-found reads a rollout wait tolerates: a workload
/// missing right after its manifest was applied is worth a few more probes,
/// one that never appears is a failure rather than a bare timeout.
const MAX_NOT_FOUND: u32 = 5;

/// Progress of a rollout: done, or the status line to show while waiting.
type Progress = (bool, String);

fn deployment_progress(d: &Deployment) -> Result<Progress, String> {
    let generation = d.metadata.generation.unwrap_or(0);
    let status = d.status.clone().unwrap_or_default();
    if generation > status.observed_generation.unwrap_or(0) {
        return Ok((
            false,
            "waiting for the controller to observe the update".into(),
        ));
    }
    for cond in status.conditions.iter().flatten() {
        if cond.type_ == "Progressing" && cond.reason.as_deref() == Some("ProgressDeadlineExceeded")
        {
            return Err("exceeded its progress deadline".into());
        }
    }
    let desired = d.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
    let updated = status.updated_replicas.unwrap_or(0);
    let replicas = status.replicas.unwrap_or(0);
    let available = status.available_replicas.unwrap_or(0);
    if updated < desired {
        return Ok((false, format!("{updated}/{desired} replicas updated")));
    }
    if replicas > updated {
        return Ok((
            false,
            format!("{} old replicas pending termination", replicas - updated),
        ));
    }
    if available < updated {
        return Ok((false, format!("{available}/{updated} replicas available")));
    }
    Ok((true, "rolled out".into()))
}

fn daemonset_progress(ds: &DaemonSet) -> Progress {
    let strategy = ds
        .spec
        .as_ref()
        .and_then(|s| s.update_strategy.as_ref())
        .and_then(|u| u.type_.clone());
    if strategy.as_deref().unwrap_or("RollingUpdate") != "RollingUpdate" {
        return (true, "not a rolling update strategy".into());
    }
    let generation = ds.metadata.generation.unwrap_or(0);
    let status = ds.status.clone().unwrap_or_default();
    if generation > status.observed_generation.unwrap_or(0) {
        return (
            false,
            "waiting for the controller to observe the update".into(),
        );
    }
    let desired = status.desired_number_scheduled;
    let updated = status.updated_number_scheduled.unwrap_or(0);
    let available = status.number_available.unwrap_or(0);
    if updated < desired {
        return (false, format!("{updated}/{desired} nodes updated"));
    }
    if available < desired {
        return (false, format!("{available}/{desired} nodes available"));
    }
    (true, "rolled out".into())
}

fn statefulset_progress(ss: &StatefulSet) -> Progress {
    let spec = ss.spec.clone().unwrap_or_default();
    let strategy = spec.update_strategy.as_ref().and_then(|u| u.type_.clone());
    if strategy.as_deref().unwrap_or("RollingUpdate") != "RollingUpdate" {
        return (true, "not a rolling update strategy".into());
    }
    let status = ss.status.clone().unwrap_or_default();
    if status.observed_generation.unwrap_or(0) < ss.metadata.generation.unwrap_or(0) {
        return (
            false,
            "waiting for the controller to observe the update".into(),
        );
    }
    let desired = spec.replicas.unwrap_or(1);
    let ready = status.ready_replicas.unwrap_or(0);
    if ready < desired {
        return (false, format!("{ready}/{desired} replicas ready"));
    }
    if let Some(partition) = spec
        .update_strategy
        .and_then(|u| u.rolling_update)
        .and_then(|r| r.partition)
    {
        let expected = desired - partition;
        let updated = status.updated_replicas.unwrap_or(0);
        if updated < expected {
            return (false, format!("{updated}/{expected} replicas updated"));
        }
        return (true, "partitioned rollout complete".into());
    }
    if status.update_revision != status.current_revision {
        return (
            false,
            "waiting for the update revision to become current".into(),
        );
    }
    (true, "rolled out".into())
}

impl Client {
    /// Waits for a workload's rollout to complete, in the manner of
    /// `kubectl rollout status`.
    pub async fn rollout_status(
        &self,
        kind: WorkloadKind,
        namespace: &str,
        name: &str,
        timeout: Duration,
    ) -> Result<(), KubeError> {
        let _t = log::Timer::new(format!("rollout status {kind}/{name}"));
        let last_msg = Arc::new(Mutex::new(String::new()));
        let not_found = Arc::new(Mutex::new(0u32));
        let result = self
            .poll(timeout, || {
                let last_msg = last_msg.clone();
                let not_found = not_found.clone();
                async move {
                    let progress: Result<Option<Progress>, KubeError> = match kind {
                        WorkloadKind::Deployment => {
                            match self.get_typed::<Deployment>(namespace, name).await {
                                Ok(Some(d)) => deployment_progress(&d).map(Some).map_err(|why| {
                                    KubeError::Other(format!("deployment {namespace}/{name} {why}"))
                                }),
                                Ok(None) => Ok(None),
                                Err(e) => Err(e),
                            }
                        }
                        WorkloadKind::DaemonSet => self
                            .get_typed::<DaemonSet>(namespace, name)
                            .await
                            .map(|o| o.map(|d| daemonset_progress(&d))),
                        WorkloadKind::StatefulSet => self
                            .get_typed::<StatefulSet>(namespace, name)
                            .await
                            .map(|o| o.map(|s| statefulset_progress(&s))),
                    };
                    match progress? {
                        None => {
                            let mut n = not_found.lock();
                            *n += 1;
                            if *n > MAX_NOT_FOUND {
                                return Err(KubeError::Other(format!(
                                    "{kind}/{name} not found in namespace {namespace}"
                                )));
                            }
                            *last_msg.lock() = format!("{kind}/{name} not created yet");
                            Ok(false)
                        }
                        Some((done, msg)) => {
                            *last_msg.lock() = msg;
                            Ok(done)
                        }
                    }
                }
            })
            .await;
        result.map_err(|e| {
            KubeError::Other(format!(
                "waiting for {kind}/{name} in {namespace}: {e} (last status: {})",
                last_msg.lock()
            ))
        })
    }

    /// Waits until the namespace exists and is Active.
    pub async fn wait_namespace_active(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<(), KubeError> {
        self.poll(timeout, || async {
            Ok(self
                .get_typed::<Namespace>("", name)
                .await?
                .and_then(|ns| ns.status)
                .and_then(|s| s.phase)
                .as_deref()
                == Some("Active"))
        })
        .await
        .map_err(|e| e.context(format!("waiting for namespace {name} to become Active")))
    }

    /// Waits until the object reports `condition=True` in
    /// `status.conditions`.
    pub async fn wait_condition(
        &self,
        gvk: &Gvk,
        namespace: &str,
        name: &str,
        condition: &str,
        timeout: Duration,
    ) -> Result<(), KubeError> {
        let res = self.resource_for(gvk).await?;
        self.poll(timeout, || {
            let res = res.clone();
            async move {
                let current = match self.cluster().get(&res, namespace, name).await {
                    Ok(o) => o,
                    Err(e) if e.is_not_found() => return Ok(false),
                    Err(e) => return Err(e),
                };
                let Some(conds) = current
                    .pointer("/status/conditions")
                    .and_then(|c| c.as_array())
                else {
                    return Ok(false);
                };
                for cond in conds {
                    if cond.get("type").and_then(|t| t.as_str()) == Some(condition) {
                        return Ok(cond.get("status").and_then(|s| s.as_str()) == Some("True"));
                    }
                }
                Ok(false)
            }
        })
        .await
        .map_err(|e| {
            e.context(format!(
                "waiting for {} {namespace}/{name} to report {condition}=True",
                gvk.kind
            ))
        })
    }

    /// The certificates.k8s.io version serving ClusterTrustBundle, preferring
    /// the most stable.
    pub async fn cluster_trust_bundle_gvk(&self) -> Result<Gvk, KubeError> {
        let versions = self.cluster().group_versions("certificates.k8s.io").await?;
        for v in ["v1", "v1beta1", "v1alpha1"] {
            if !versions.iter().any(|x| x == v) {
                continue;
            }
            let gvk = Gvk::new("certificates.k8s.io", v, "ClusterTrustBundle");
            if self.resource_for(&gvk).await.is_ok() {
                return Ok(gvk);
            }
        }
        Err(KubeError::Other(
            "the cluster serves no ClusterTrustBundle API".into(),
        ))
    }

    /// Waits for every named ClusterTrustBundle to exist, sharing one
    /// timeout across them.
    pub async fn wait_cluster_trust_bundles(
        &self,
        names: &[&str],
        timeout: Duration,
    ) -> Result<(), KubeError> {
        let _t = log::Timer::new("wait for clustertrustbundles");
        let deadline = Instant::now() + timeout;
        for name in names {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(KubeError::Deadline(format!(
                    "waiting for ClusterTrustBundle {name}: the {timeout:?} budget was spent on the bundles before it"
                )));
            }
            let last_err: Arc<Mutex<Option<KubeError>>> = Arc::default();
            let result = self
                .poll(remaining, || {
                    let last_err = last_err.clone();
                    async move {
                        let gvk = match self.cluster_trust_bundle_gvk().await {
                            Ok(g) => g,
                            Err(e) => {
                                *last_err.lock() = Some(e);
                                return Ok(false);
                            }
                        };
                        match self.exists(&gvk, "", name).await {
                            Ok(ok) => Ok(ok),
                            Err(e) => {
                                *last_err.lock() = Some(e);
                                Ok(false)
                            }
                        }
                    }
                })
                .await;
            if let Err(e) = result {
                return Err(match last_err.lock().take() {
                    Some(last) => KubeError::Other(format!(
                        "waiting for ClusterTrustBundle {name}: {e} (last discovery error: {last})"
                    )),
                    None => e.context(format!("waiting for ClusterTrustBundle {name}")),
                });
            }
        }
        Ok(())
    }

    /// Waits until the object no longer exists.
    pub async fn wait_deleted(
        &self,
        gvk: &Gvk,
        namespace: &str,
        name: &str,
        timeout: Duration,
    ) -> Result<(), KubeError> {
        self.poll(timeout, || async {
            Ok(!self.exists(gvk, namespace, name).await?)
        })
        .await
        .map_err(|e| {
            e.context(format!(
                "waiting for {} {namespace}/{name} to be deleted",
                gvk.kind
            ))
        })
    }

    fn restart_patch(now: chrono::DateTime<chrono::Utc>) -> Patch {
        Patch::Strategic(serde_json::json!({
            "spec": {"template": {"metadata": {"annotations": {
                "kubectl.kubernetes.io/restartedAt": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            }}}}
        }))
    }

    /// `kubectl rollout restart deployment`. A missing Deployment is not an
    /// error.
    pub async fn rollout_restart_deployment(
        &self,
        namespace: &str,
        name: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), KubeError> {
        match self
            .patch(
                &Gvk::of::<Deployment>(),
                namespace,
                name,
                &Self::restart_patch(now),
            )
            .await
        {
            Err(e) if !e.is_not_found() => {
                Err(e.context(format!("while restarting deployment/{name} in {namespace}")))
            }
            _ => Ok(()),
        }
    }

    /// `kubectl rollout restart daemonset`. A missing DaemonSet is not an
    /// error.
    pub async fn rollout_restart(
        &self,
        namespace: &str,
        name: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), KubeError> {
        match self
            .patch(
                &Gvk::of::<DaemonSet>(),
                namespace,
                name,
                &Self::restart_patch(now),
            )
            .await
        {
            Err(e) if !e.is_not_found() => {
                Err(e.context(format!("while restarting daemonset/{name} in {namespace}")))
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube::fake::FakeCluster;
    use crate::kube::ResourceInfo;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn client(f: &FakeCluster) -> Client {
        Client::new(Arc::new(f.clone())).with_fast_timings()
    }

    #[tokio::test]
    async fn rollout_status_gives_up_on_a_persistently_missing_workload() {
        let c = client(&FakeCluster::new());
        let err = c
            .rollout_status(
                WorkloadKind::Deployment,
                "hivekeep-system",
                "hivekeep-api",
                Duration::from_secs(60),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not found"), "{err}");
    }

    #[tokio::test]
    async fn wait_cluster_trust_bundles_versions() {
        for version in ["v1", "v1beta1"] {
            let f = FakeCluster::new();
            let gvk = Gvk::new("certificates.k8s.io", version, "ClusterTrustBundle");
            f.set_resources(vec![ResourceInfo {
                gvk: gvk.clone(),
                plural: "clustertrustbundles".into(),
                namespaced: false,
            }]);
            f.insert(crate::kube::stub(&gvk, "", "bundle"));
            let gets = Arc::new(AtomicUsize::new(0));
            let g = gets.clone();
            let want_version = version.to_string();
            f.prepend_reactor("get", "ClusterTrustBundle", move |a| {
                assert_eq!(a.gvk.version, want_version);
                assert_eq!(a.name, "bundle");
                g.fetch_add(1, Ordering::SeqCst);
                None
            });
            client(&f)
                .wait_cluster_trust_bundles(&["bundle"], Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(gets.load(Ordering::SeqCst), 1, "{version}");
        }
    }

    #[test]
    fn deployment_progress_cases() {
        let mut d: Deployment = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "x", "generation": 2},
            "spec": {"replicas": 2, "selector": {}, "template": {}},
            "status": {"observedGeneration": 1}
        }))
        .unwrap();
        assert!(!deployment_progress(&d).unwrap().0);
        d.status = serde_json::from_value(serde_json::json!({
            "observedGeneration": 2, "replicas": 2, "updatedReplicas": 2, "availableReplicas": 2
        }))
        .unwrap();
        assert_eq!(
            deployment_progress(&d).unwrap(),
            (true, "rolled out".into())
        );
    }

    #[test]
    fn statefulset_partitioned_rollout() {
        let ss: StatefulSet = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "pg", "generation": 1},
            "spec": {"replicas": 3, "selector": {}, "template": {}, "serviceName": "pg",
                     "updateStrategy": {"type": "RollingUpdate", "rollingUpdate": {"partition": 1}}},
            "status": {"replicas": 3, "observedGeneration": 1, "readyReplicas": 3, "updatedReplicas": 2}
        }))
        .unwrap();
        assert_eq!(
            statefulset_progress(&ss),
            (true, "partitioned rollout complete".into())
        );
    }
}
