//! Teardown: the control plane, the gate dataplane, the cluster-scoped
//! trust bundles, and the demos ahead of them.

use super::benchmarks::INSTALL_MICROVM_DEP_SCRIPT;
use super::credprovider::K8S_CREDENTIAL_PROVIDER_MANIFEST;
use super::env::{BOOTSTRAP_TIMEOUT, NAMESPACE_POD_CERT};
use super::{Env, Result, StepError};
use crate::kube::{Client, Gvk, KubeError, Object, ObjectExt, Patch};
use crate::log;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::time::Duration;

/// The finalizer the controller puts on a SandboxConfig while a WorkerPool
/// uses it.
pub const SANDBOX_CONFIG_WORKER_POOL_PROTECTION_FINALIZER: &str =
    "sandboxconfig.hivekeep.io/workerpool-protection";

/// Each ClusterTrustBundle Hivekeep publishes, by name, with its signer:
/// the podcertificate controller's and the controller's egress MITM bundle.
pub const HIVEKEEP_TRUST_BUNDLES: [(&str, &str); 4] = [
    (
        "podidentity.podcert.hivekeep.io:identity:primary-bundle",
        "podidentity.podcert.hivekeep.io/identity",
    ),
    (
        "servicedns.podcert.hivekeep.io:identity:primary-bundle",
        "servicedns.podcert.hivekeep.io/identity",
    ),
    (
        "postgres.podcert.hivekeep.io:identity:primary-bundle",
        "postgres.podcert.hivekeep.io/identity",
    ),
    (
        "egress-mitm.hivekeep.io:mitm:primary-bundle",
        "egress-mitm.hivekeep.io/mitm",
    ),
];

/// Attempts at a conflicting finalizer update before giving up.
const RETRY_STEPS: usize = 5;
const RETRY_DELAY: Duration = Duration::from_millis(10);

/// Something [`Env::delete_all`] removes before the control plane: a demo.
/// It must succeed on a cluster where it was never installed.
#[async_trait]
pub trait Deleter: Send + Sync {
    async fn delete(&self, env: &Env) -> Result<()>;
}

/// The SandboxConfig calls finalizer release makes, so tests can fake them.
#[async_trait]
pub trait SandboxConfigStore: Send + Sync {
    async fn list(&self) -> Result<Vec<Object>, KubeError>;
    async fn get(&self, name: &str) -> Result<Object, KubeError>;
    /// Writes the object back, failing with a conflict when it changed since
    /// it was read.
    async fn update(&self, obj: &Object) -> Result<(), KubeError>;
}

/// SandboxConfigs through the cluster client. The update is a merge patch
/// carrying the resourceVersion read, so a concurrent change conflicts
/// rather than being overwritten.
pub struct ClusterSandboxConfigs<'a>(pub &'a Client);

fn sandbox_config_gvk() -> Gvk {
    Gvk::new("hivekeep.io", "v1", "SandboxConfig")
}

#[async_trait]
impl SandboxConfigStore for ClusterSandboxConfigs<'_> {
    async fn list(&self) -> Result<Vec<Object>, KubeError> {
        let res = self
            .0
            .resource_for(&sandbox_config_gvk())
            .await
            .map_err(|e| {
                if e.is_no_match() {
                    KubeError::not_found("sandboxconfigs.hivekeep.io", "")
                } else {
                    e
                }
            })?;
        self.0.cluster().list(&res, "", None).await
    }
    async fn get(&self, name: &str) -> Result<Object, KubeError> {
        let res = self.0.resource_for(&sandbox_config_gvk()).await?;
        self.0.cluster().get(&res, "", name).await
    }
    async fn update(&self, obj: &Object) -> Result<(), KubeError> {
        let mut meta = json!({"finalizers": obj.at(&["metadata", "finalizers"]).cloned().unwrap_or(json!([]))});
        if let Some(rv) = obj.at(&["metadata", "resourceVersion"]) {
            meta["resourceVersion"] = rv.clone();
        }
        self.0
            .patch(
                &sandbox_config_gvk(),
                "",
                obj.name(),
                &Patch::Merge(json!({"metadata": meta})),
            )
            .await
            .map(|_| ())
    }
}

fn finalizers(obj: &Object) -> Vec<String> {
    obj.at(&["metadata", "finalizers"])
        .and_then(|f| f.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Removes the WorkerPool protection finalizer from every SandboxConfig.
/// Only the controller manages it, and it is gone once its namespace is;
/// without this no SandboxConfig could finish deleting.
pub async fn release_sandbox_configs(store: &dyn SandboxConfigStore) -> Result<()> {
    let list = match store.list().await {
        Err(e) if e.is_not_found() => return Ok(()),
        Err(e) => return Err(StepError::msg(format!("while listing SandboxConfigs: {e}"))),
        Ok(l) => l,
    };
    for item in list {
        let name = item.name().to_string();
        let mut attempt = 0;
        let result: Result<(), KubeError> = loop {
            let mut sc = match store.get(&name).await {
                Ok(sc) => sc,
                Err(e) => break Err(e),
            };
            let before = finalizers(&sc);
            let after: Vec<String> = before
                .iter()
                .filter(|f| *f != SANDBOX_CONFIG_WORKER_POOL_PROTECTION_FINALIZER)
                .cloned()
                .collect();
            if after.len() == before.len() {
                break Ok(());
            }
            *sc.at_mut(&["metadata", "finalizers"]) =
                Value::Array(after.into_iter().map(Value::String).collect());
            match store.update(&sc).await {
                Err(e) if e.is_conflict() && attempt + 1 < RETRY_STEPS => {
                    attempt += 1;
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                other => break other,
            }
        };
        match result {
            Err(e) if !e.is_not_found() => {
                return Err(StepError::msg(format!(
                    "while releasing SandboxConfig {name}: {e}"
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

impl Env {
    /// Removes the control plane. PostgreSQL, the bundled credential
    /// provider, fake-workersync and the CRDs are deleted explicitly because
    /// not every rendered bundle carries them; the trust bundles go last,
    /// once the controllers publishing them are gone.
    pub async fn delete_hivekeep_system(&self) -> Result<()> {
        log::step("delete_hivekeep_system");
        let k = self.kube()?;
        k.delete_path(&self.k8s_credential_provider_path(K8S_CREDENTIAL_PROVIDER_MANIFEST))
            .await?;
        if self.cfg.kind {
            let manifest = self.kustomize("install/kind")?;
            k.delete_bytes(&manifest).await?;
            // Not part of the kind bundle, so it goes by name.
            k.delete_path(&self.cfg.manifest(&["pod-certificate-controller.yaml"]))
                .await?;
        } else {
            k.delete_path(&self.cfg.manifest(&[])).await?;
        }
        for path in [
            &["components", "fake-data-plane", "fake-workersync.yaml"][..],
            &["postgres", "postgres.yaml"],
            &["generated"],
        ] {
            k.delete_path(&self.cfg.manifest(path)).await?;
        }
        let namespace = Gvk::namespace();
        k.wait_deleted(
            &namespace,
            "",
            self.namespace(),
            self.cfg.wait_timeout(BOOTSTRAP_TIMEOUT),
        )
        .await?;
        // Safe by hand now: the controller that would remove the finalizer
        // went with the namespace, and deleting the CRDs deleted every pool.
        release_sandbox_configs(&ClusterSandboxConfigs(k)).await?;
        // The podcertificate controller republishes its bundles every few
        // seconds, so they can only go once it has.
        k.wait_deleted(
            &namespace,
            "",
            NAMESPACE_POD_CERT,
            self.cfg.wait_timeout(BOOTSTRAP_TIMEOUT),
        )
        .await?;
        self.delete_trust_bundles().await
    }

    /// Deletes the ClusterTrustBundles in [`HIVEKEEP_TRUST_BUNDLES`], which
    /// outlive the namespaces of the controllers publishing them. A bundle
    /// goes only when both its name and signer match: anyone allowed to
    /// attest for these signers may publish under another name.
    pub async fn delete_trust_bundles(&self) -> Result<()> {
        let k = self.kube()?;
        let versions = k
            .cluster()
            .group_versions("certificates.k8s.io")
            .await
            .map_err(|e| {
                StepError::msg(format!("while discovering the ClusterTrustBundle API: {e}"))
            })?;
        let mut gvk = None;
        for v in ["v1", "v1beta1", "v1alpha1"] {
            if !versions.iter().any(|x| x == v) {
                continue;
            }
            let candidate = Gvk::new("certificates.k8s.io", v, "ClusterTrustBundle");
            match k.resource_for(&candidate).await {
                Ok(_) => {
                    gvk = Some(candidate);
                    break;
                }
                Err(e) if e.is_no_match() => continue,
                Err(e) => {
                    return Err(StepError::msg(format!(
                        "while discovering the ClusterTrustBundle API: {e}"
                    )))
                }
            }
        }
        let Some(gvk) = gvk else {
            // Not served: the cluster cannot hold any of ours.
            return Ok(());
        };
        let list = k
            .list(&gvk, "", None)
            .await
            .map_err(|e| StepError::msg(format!("while listing ClusterTrustBundles: {e}")))?;
        for bundle in list {
            let signer = bundle
                .at(&["spec", "signerName"])
                .and_then(|s| s.as_str())
                .unwrap_or("");
            if !HIVEKEEP_TRUST_BUNDLES
                .iter()
                .any(|(n, s)| *n == bundle.name() && *s == signer)
            {
                continue;
            }
            k.delete_named(&gvk, "", bundle.name(), None)
                .await
                .map_err(|e| {
                    StepError::msg(format!(
                        "while deleting ClusterTrustBundle {}: {e}",
                        bundle.name()
                    ))
                })?;
            log::info(&format!("Deleted ClusterTrustBundle {}", bundle.name()));
        }
        Ok(())
    }

    /// Removes the gate dataplane and the bundled credential provider. The
    /// provider's policy ConfigMap stays: it holds the operator's allow-list.
    pub async fn delete_gate(&self) -> Result<()> {
        log::step("delete_gate");
        let k = self.kube()?;
        k.delete_path(&self.k8s_credential_provider_path(K8S_CREDENTIAL_PROVIDER_MANIFEST))
            .await?;
        for path in [&["gate-router.yaml"][..], &["gate-egress.yaml"]] {
            k.delete_path(&self.cfg.manifest(path)).await?;
        }
        Ok(())
    }

    /// Removes every demo, then the microvm SandboxConfig, then the control
    /// plane.
    pub async fn delete_all(&self, demos: &[&dyn Deleter]) -> Result<()> {
        log::step("delete_all");
        self.delete_demos(demos).await?;
        self.delete_hivekeep_system().await
    }

    /// Removes every demo, then tries to delete the microvm SandboxConfig an
    /// earlier delete left because a pool still used it. Best effort: the
    /// control plane teardown deletes it with the CRD anyway.
    pub(crate) async fn delete_demos(&self, demos: &[&dyn Deleter]) -> Result<()> {
        for demo in demos {
            demo.delete(self).await?;
        }
        if let Err(e) = self
            .run_script(INSTALL_MICROVM_DEP_SCRIPT, &["--delete"])
            .await
        {
            log::warn(&format!("while deleting the microvm SandboxConfig: {e}"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::kube::fake::FakeCluster;
    use crate::kube::ResourceInfo;
    use crate::steps::cluster_fixture::fake_env;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    const IN_USE: &str = SANDBOX_CONFIG_WORKER_POOL_PROTECTION_FINALIZER;

    fn new_sandbox_config(name: &str, finalizers: &[&str]) -> Object {
        json!({"apiVersion": "hivekeep.io/v1", "kind": "SandboxConfig", "metadata": {"name": name, "finalizers": finalizers}})
    }

    #[derive(Default)]
    struct FakeStore {
        objs: Mutex<BTreeMap<String, Object>>,
        list_err: Option<KubeError>,
        gone: Vec<String>,
        conflicts: Mutex<usize>,
        updates: Mutex<usize>,
    }

    #[async_trait]
    impl SandboxConfigStore for FakeStore {
        async fn list(&self) -> Result<Vec<Object>, KubeError> {
            if let Some(e) = &self.list_err {
                return Err(e.clone());
            }
            let mut out: Vec<Object> = self.objs.lock().values().cloned().collect();
            out.extend(self.gone.iter().map(|n| new_sandbox_config(n, &[IN_USE])));
            Ok(out)
        }
        async fn get(&self, name: &str) -> Result<Object, KubeError> {
            self.objs
                .lock()
                .get(name)
                .cloned()
                .ok_or_else(|| KubeError::not_found("sandboxconfigs", name))
        }
        async fn update(&self, obj: &Object) -> Result<(), KubeError> {
            *self.updates.lock() += 1;
            let mut c = self.conflicts.lock();
            if *c > 0 {
                *c -= 1;
                return Err(KubeError::conflict(
                    "sandboxconfigs",
                    obj.name(),
                    "stale resourceVersion",
                ));
            }
            self.objs.lock().insert(obj.name().to_string(), obj.clone());
            Ok(())
        }
    }

    fn store(objs: &[Object]) -> FakeStore {
        FakeStore {
            objs: Mutex::new(
                objs.iter()
                    .map(|o| (o.name().to_string(), o.clone()))
                    .collect(),
            ),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn release_sandbox_configs_cases() {
        struct Case {
            name: &'static str,
            store: FakeStore,
            want: Vec<(&'static str, Vec<&'static str>)>,
            updates: usize,
            err: bool,
        }
        let cases = vec![
            Case {
                name: "strips only the protection finalizer and skips configs without it",
                store: store(&[
                    new_sandbox_config("in-use", &[IN_USE, "example.com/other"]),
                    new_sandbox_config("unused", &["example.com/other"]),
                ]),
                want: vec![
                    ("in-use", vec!["example.com/other"]),
                    ("unused", vec!["example.com/other"]),
                ],
                updates: 1,
                err: false,
            },
            Case {
                name: "List NotFound means the CRD is gone and is not an error",
                store: FakeStore {
                    list_err: Some(KubeError::not_found("sandboxconfigs", "")),
                    ..Default::default()
                },
                want: vec![],
                updates: 0,
                err: false,
            },
            Case {
                name: "List failure other than NotFound is returned",
                store: FakeStore {
                    list_err: Some(KubeError::service_unavailable("apiserver down")),
                    ..Default::default()
                },
                want: vec![],
                updates: 0,
                err: true,
            },
            Case {
                name: "config deleted between List and Get is skipped and the rest are released",
                store: FakeStore {
                    gone: vec!["vanished".into()],
                    ..store(&[new_sandbox_config("in-use", &[IN_USE])])
                },
                want: vec![("in-use", vec![])],
                updates: 1,
                err: false,
            },
            Case {
                name: "Update conflict is retried until the write lands",
                store: FakeStore {
                    conflicts: Mutex::new(2),
                    ..store(&[new_sandbox_config("in-use", &[IN_USE, "example.com/other"])])
                },
                want: vec![("in-use", vec!["example.com/other"])],
                updates: 3,
                err: false,
            },
            Case {
                name: "Update conflicts beyond the retry budget are returned",
                store: FakeStore {
                    conflicts: Mutex::new(RETRY_STEPS),
                    ..store(&[new_sandbox_config("in-use", &[IN_USE])])
                },
                want: vec![("in-use", vec![IN_USE])],
                updates: RETRY_STEPS,
                err: true,
            },
        ];
        for c in cases {
            let res = release_sandbox_configs(&c.store).await;
            assert_eq!(res.is_err(), c.err, "{}: {res:?}", c.name);
            for (name, want) in &c.want {
                let obj = c.store.objs.lock().get(*name).cloned().unwrap();
                assert_eq!(finalizers(&obj), *want, "{}: {name}", c.name);
            }
            assert_eq!(*c.store.updates.lock(), c.updates, "{}", c.name);
        }
    }

    struct RecordingDeleter {
        name: &'static str,
        log: std::path::PathBuf,
        err: bool,
    }

    fn append_line(path: &std::path::Path, line: &str) {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        writeln!(f, "{line}").unwrap();
    }

    #[async_trait]
    impl Deleter for RecordingDeleter {
        async fn delete(&self, _env: &Env) -> Result<()> {
            append_line(&self.log, self.name);
            if self.err {
                return Err(StepError::msg("demo failed"));
            }
            Ok(())
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_demos_cases() {
        use std::os::unix::fs::PermissionsExt;
        for (name, demos, exit, want, want_err) in [
            (
                "microvm SandboxConfig is deleted after every demo",
                vec![("demo-a", false), ("demo-b", false)],
                0,
                vec!["demo-a", "demo-b", "script --delete"],
                false,
            ),
            (
                "script failure is logged, not returned",
                vec![("demo-a", false)],
                1,
                vec!["demo-a", "script --delete"],
                false,
            ),
            (
                "a failing demo stops before the script",
                vec![("demo-a", true), ("demo-b", false)],
                0,
                vec!["demo-a"],
                true,
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            let calls = root.path().join("calls.log");
            let script = root.path().join(INSTALL_MICROVM_DEP_SCRIPT);
            std::fs::create_dir_all(script.parent().unwrap()).unwrap();
            std::fs::write(
                &script,
                format!(
                    "#!/bin/sh\necho \"script $*\" >> {:?}\nexit {exit}\n",
                    calls
                ),
            )
            .unwrap();
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
            let deleters: Vec<RecordingDeleter> = demos
                .iter()
                .map(|(n, err)| RecordingDeleter {
                    name: n,
                    log: calls.clone(),
                    err: *err,
                })
                .collect();
            let refs: Vec<&dyn Deleter> = deleters.iter().map(|d| d as &dyn Deleter).collect();
            let e = Env::local(Config {
                root: root.path().to_path_buf(),
                ..Default::default()
            });
            let res = e.delete_demos(&refs).await;
            assert_eq!(res.is_err(), want_err, "{name}: {res:?}");
            if let Err(err) = res {
                assert_eq!(err.to_string(), "demo failed", "{name}");
            }
            let got = std::fs::read_to_string(&calls).unwrap_or_default();
            let got: Vec<&str> = got.lines().collect();
            assert_eq!(got, want, "{name}");
        }
    }

    fn trust_bundle(signer: &str, suffix: &str) -> Object {
        let name = if signer.is_empty() {
            suffix.to_string()
        } else {
            format!("{}:{suffix}", signer.replace('/', ":"))
        };
        json!({
            "apiVersion": "certificates.k8s.io/v1", "kind": "ClusterTrustBundle",
            "metadata": {"name": name, "uid": format!("uid-{name}"), "labels": {"podcert.hivekeep.io/canarying": "live"}},
            "spec": {"signerName": signer, "trustBundle": "pem"}
        })
    }

    /// A fake serving ClusterTrustBundles at `version` (none when empty).
    fn trust_bundle_cluster(version: &str, bundles: &[Object]) -> FakeCluster {
        let f = FakeCluster::new();
        let mut resources = vec![ResourceInfo {
            gvk: Gvk::new("", "v1", "Namespace"),
            plural: "namespaces".into(),
            namespaced: false,
        }];
        if !version.is_empty() {
            let gvk = Gvk::new("certificates.k8s.io", version, "ClusterTrustBundle");
            resources.push(ResourceInfo {
                gvk: gvk.clone(),
                plural: "clustertrustbundles".into(),
                namespaced: false,
            });
            for b in bundles {
                let mut b = b.clone();
                b["apiVersion"] = json!(gvk.api_version());
                f.insert(b);
            }
        }
        f.set_resources(resources);
        f
    }

    fn remaining(f: &FakeCluster) -> Vec<String> {
        let mut out: Vec<String> = f
            .objects_of("ClusterTrustBundle")
            .iter()
            .map(|o| o.name().to_string())
            .collect();
        out.sort();
        out
    }

    fn env_over(f: &FakeCluster) -> Env {
        Env::with_client(
            Config::default(),
            Client::new(Arc::new(f.clone())).with_fast_timings(),
        )
    }

    #[tokio::test]
    async fn delete_trust_bundles_cases() {
        for version in ["v1", "v1beta1"] {
            let ours: Vec<Object> = HIVEKEEP_TRUST_BUNDLES
                .iter()
                .map(|(name, signer)| {
                    let b = trust_bundle(signer, "primary-bundle");
                    assert_eq!(b.name(), *name);
                    b
                })
                .collect();
            let others = vec![
                trust_bundle("podidentity.podcert.hivekeep.io/identity", "next-bundle"),
                trust_bundle("egress-mitm.hivekeep.io/mitm", "foreign-bundle"),
                trust_bundle("example.com/signer", "primary-bundle"),
                trust_bundle("other.podcert.hivekeep.io/identity", "primary-bundle"),
                trust_bundle("podidentity.podcert.hivekeep.io/other", "primary-bundle"),
                trust_bundle("", "unsigned-bundle"),
            ];
            let mut all = ours.clone();
            all.extend(others.clone());
            let f = trust_bundle_cluster(version, &all);
            let e = env_over(&f);
            e.delete_trust_bundles().await.unwrap();
            let mut want: Vec<String> = others.iter().map(|o| o.name().to_string()).collect();
            want.sort();
            assert_eq!(remaining(&f), want, "{version}");
            let deletes: Vec<_> = f
                .actions()
                .into_iter()
                .filter(|a| a.verb == "delete")
                .collect();
            assert_eq!(deletes.len(), ours.len(), "{version}");
            for d in &deletes {
                assert_eq!(
                    d.gvk.version, version,
                    "delete of {} went to the wrong version",
                    d.name
                );
            }
            // A rerun finds nothing left of ours and changes nothing.
            f.clear_actions();
            e.delete_trust_bundles().await.unwrap();
            assert!(
                f.actions().iter().all(|a| a.verb != "delete"),
                "{version}: second run deleted"
            );
            assert_eq!(remaining(&f), want, "{version}");
        }
    }

    #[tokio::test]
    async fn delete_trust_bundles_name_with_wrong_signer() {
        let mut foreign = trust_bundle("example.com/signer", "primary-bundle");
        foreign["metadata"]["name"] = json!("postgres.podcert.hivekeep.io:identity:primary-bundle");
        let f = trust_bundle_cluster("v1", &[foreign]);
        env_over(&f).delete_trust_bundles().await.unwrap();
        assert_eq!(
            remaining(&f),
            ["postgres.podcert.hivekeep.io:identity:primary-bundle"]
        );
    }

    #[tokio::test]
    async fn delete_trust_bundles_none_of_ours() {
        let foreign = trust_bundle("example.com/signer", "primary-bundle");
        let f = trust_bundle_cluster("v1", std::slice::from_ref(&foreign));
        env_over(&f).delete_trust_bundles().await.unwrap();
        assert!(f.actions().iter().all(|a| a.verb != "delete"));
        assert_eq!(remaining(&f), [foreign.name().to_string()]);
    }

    #[tokio::test]
    async fn delete_trust_bundles_api_not_served() {
        let f = trust_bundle_cluster("", &[]);
        env_over(&f).delete_trust_bundles().await.unwrap();
        assert!(f
            .actions()
            .iter()
            .all(|a| a.gvk.kind != "ClusterTrustBundle"));
    }

    /// A cluster whose discovery fails, delegating everything else.
    struct FailingDiscovery(FakeCluster);

    #[async_trait]
    impl crate::kube::Cluster for FailingDiscovery {
        async fn resolve(&self, gvk: &Gvk) -> Result<ResourceInfo, KubeError> {
            self.0.resolve(gvk).await
        }
        async fn group_versions(&self, _group: &str) -> Result<Vec<String>, KubeError> {
            Err(KubeError::service_unavailable("unavailable"))
        }
        async fn server_version(&self) -> Result<String, KubeError> {
            self.0.server_version().await
        }
        async fn apply(&self, r: &ResourceInfo, ns: &str, o: &Object) -> Result<Object, KubeError> {
            self.0.apply(r, ns, o).await
        }
        async fn get(&self, r: &ResourceInfo, ns: &str, n: &str) -> Result<Object, KubeError> {
            self.0.get(r, ns, n).await
        }
        async fn list(
            &self,
            r: &ResourceInfo,
            ns: &str,
            s: Option<&str>,
        ) -> Result<Vec<Object>, KubeError> {
            self.0.list(r, ns, s).await
        }
        async fn create(
            &self,
            r: &ResourceInfo,
            ns: &str,
            o: &Object,
        ) -> Result<Object, KubeError> {
            self.0.create(r, ns, o).await
        }
        async fn delete(
            &self,
            r: &ResourceInfo,
            ns: &str,
            n: &str,
            p: Option<crate::kube::Propagation>,
        ) -> Result<(), KubeError> {
            self.0.delete(r, ns, n, p).await
        }
        async fn patch(
            &self,
            r: &ResourceInfo,
            ns: &str,
            n: &str,
            p: &Patch,
        ) -> Result<Object, KubeError> {
            self.0.patch(r, ns, n, p).await
        }
        async fn raw_get(&self, path: &str) -> Result<Vec<u8>, KubeError> {
            self.0.raw_get(path).await
        }
        async fn exec(
            &self,
            ns: &str,
            pod: &str,
            c: &str,
            cmd: &[String],
            stdin: Option<Vec<u8>>,
        ) -> Result<crate::kube::ExecOutput, KubeError> {
            self.0.exec(ns, pod, c, cmd, stdin).await
        }
    }

    #[tokio::test]
    async fn delete_trust_bundles_errors() {
        let ours = || trust_bundle("podidentity.podcert.hivekeep.io/identity", "primary-bundle");
        for (name, verb, err, want_err) in [
            (
                "already gone",
                "delete",
                KubeError::not_found("clustertrustbundles", ""),
                false,
            ),
            (
                "delete denied",
                "delete",
                KubeError::forbidden("clustertrustbundles", "", "denied"),
                true,
            ),
            (
                "list denied",
                "list",
                KubeError::forbidden("clustertrustbundles", "", "denied"),
                true,
            ),
        ] {
            let f = trust_bundle_cluster("v1", &[ours()]);
            f.prepend_reactor(verb, "ClusterTrustBundle", move |_| Some(Err(err.clone())));
            let res = env_over(&f).delete_trust_bundles().await;
            assert_eq!(res.is_err(), want_err, "{name}: {res:?}");
        }
        let f = trust_bundle_cluster("v1", &[ours()]);
        let e = Env::with_client(
            Config::default(),
            Client::new(Arc::new(FailingDiscovery(f))).with_fast_timings(),
        );
        assert!(e.delete_trust_bundles().await.is_err(), "discovery fails");
    }

    #[test]
    fn hivekeep_trust_bundles_cover_installed_bundles() {
        for name in crate::steps::prereqs::TRUST_BUNDLE_NAMES {
            assert!(
                HIVEKEEP_TRUST_BUNDLES.iter().any(|(n, _)| *n == name),
                "teardown does not delete {name}"
            );
        }
    }

    #[tokio::test]
    async fn cluster_sandbox_configs_release_through_the_client() {
        let (e, f) = fake_env(
            Config::default(),
            vec![new_sandbox_config("in-use", &[IN_USE, "example.com/other"])],
        );
        release_sandbox_configs(&ClusterSandboxConfigs(e.kube().unwrap()))
            .await
            .unwrap();
        let sc = f.object(&sandbox_config_gvk(), "", "in-use").unwrap();
        assert_eq!(finalizers(&sc), ["example.com/other"]);
    }
}
