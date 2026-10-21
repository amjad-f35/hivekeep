//! Deploying the control plane, whole or one component at a time.

use super::credprovider::K8S_CREDENTIAL_PROVIDER_DEPLOYMENT;
use super::env::{BOOTSTRAP_TIMEOUT, NAMESPACE_POD_CERT};
use super::overlay::{GATE_EGRESS, GATE_ROUTER};
use super::{Env, Result, StepError};
use crate::kube::{
    decode_manifest_bytes, describe, load_path, Gvk, Object, ObjectExt, WorkloadKind,
};
use crate::log;
use k8s_openapi::api::core::v1::Pod;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

/// The kind of a CustomResourceDefinition.
pub fn crd_gvk() -> Gvk {
    Gvk::new("apiextensions.k8s.io", "v1", "CustomResourceDefinition")
}

/// How long a CRD gets to become Established.
const CRD_ESTABLISHED_TIMEOUT: Duration = Duration::from_secs(30);

/// The Deployment the fake data plane component adds.
pub const FAKE_WORKERSYNC_DEPLOYMENT: &str = "fake-workersync";

/// The label hivekeep-controller puts on each WorkerPool's worker pods,
/// naming the pool.
pub const WORKER_POOL_LABEL: &str = "hivekeep.io/worker-pool";

/// The control plane's Deployments and DaemonSet.
pub const API_SERVER: &str = "hivekeep-api";
pub const CONTROLLER: &str = "hivekeep-controller";
pub const KEEPER: &str = "keeper";
pub const PODCERT_CONTROLLER: &str = "podcertificate-controller";

/// Per-invocation choices of [`Env::deploy_hivekeep_system`].
#[derive(Clone, Debug, Default)]
pub struct DeployOptions {
    /// Additionally installs a CSI driver: nfs, hostpath, both, none. Kind
    /// only; hostpath is Kind only.
    pub setup_csi: String,
}

impl DeployOptions {
    /// Checks what can be checked without configuration or a cluster.
    pub fn validate(&self) -> Result<()> {
        super::csi::validate_csi_driver(&self.setup_csi)
    }
}

/// Sets WORKERS_PER_SIGNER, which `--workers-per-signer` expands, on the
/// podcertificate-controller container.
pub fn set_podcert_workers_per_signer(objs: &mut [Object], workers: i64) -> Result<()> {
    let dep = objs
        .iter_mut()
        .find(|o| o.kind() == "Deployment" && o.obj_namespace() == NAMESPACE_POD_CERT && o.name() == PODCERT_CONTROLLER)
        .ok_or_else(|| StepError::msg("the podcertificate-controller manifest has no deployment/podcertificate-controller"))?;
    let what = describe(dep);
    let container = dep
        .pointer_mut("/spec/template/spec/containers/0")
        .filter(|c| c.is_object())
        .ok_or_else(|| StepError::msg(format!("{what} has no containers")))?;
    let value = workers.to_string();
    let entry = json!({"name": "WORKERS_PER_SIGNER", "value": value});
    let env = container
        .as_object_mut()
        .expect("object")
        .entry("env")
        .or_insert_with(|| json!([]));
    let list = env
        .as_array_mut()
        .ok_or_else(|| StepError::msg(format!("{what}: env is not a list")))?;
    match list
        .iter_mut()
        .find(|v| v.get("name").and_then(Value::as_str) == Some("WORKERS_PER_SIGNER"))
    {
        Some(existing) => *existing = entry,
        None => list.push(entry),
    }
    Ok(())
}

/// The CRDs `exists` reports absent.
pub fn missing_crds(
    crds: &[Object],
    mut exists: impl FnMut(&str) -> Result<bool>,
) -> Result<Vec<Object>> {
    let mut out = Vec::new();
    for crd in crds {
        if !exists(crd.name())? {
            out.push(crd.clone());
        }
    }
    Ok(out)
}

impl Env {
    /// Installs the whole control plane: CRDs, RBAC, the podcertificate
    /// controller, the store, hivekeep-api, the controller, the gate, and
    /// keeper.
    pub async fn deploy_hivekeep_system(&self, opts: &DeployOptions) -> Result<()> {
        log::step("deploy_hivekeep_system");
        // Everything that can be rejected without the cluster is, before the
        // cluster is touched.
        self.require_canonical_namespace("deploy hivekeep-system")?;
        let provider = self.cfg.credential_provider()?;
        self.cfg.require_transfer_service_account()?;
        self.check_csi_driver(&opts.setup_csi)?;
        super::apienv::cluster_snapshot_location(
            &self.cfg.snapshot_location,
            &self.cfg.preview_gates,
        )?;
        self.require_no_worker_pods().await?;

        // The namespace has to exist before RBAC or CRDs.
        self.ensure_system_namespace().await?;
        // deploy, not ensure: an existence check would skip upgrades of CRD
        // schemas and RBAC, which have no other apply path.
        self.deploy_crds().await?;
        self.ensure_api_server_prerequisites().await?;
        // First, so it starts signing and publishing trust bundles at once.
        self.deploy_pod_certificate_controller().await?;
        self.setup_csi(&opts.setup_csi).await?;
        self.deploy_sandbox_config().await?;
        // Every workload pulls this ConfigMap in through envFrom, and a
        // container whose envFrom target is missing does not start.
        self.apply_otel_config().await?;

        // Planned before the bundle: adopting Cloud SQL reads the ConfigMap
        // the prerequisites just rewrote, and the answer decides both the
        // apply and the rollout wait.
        let postgres = self.plan_postgres().await?;
        self.deploy_postgres_plan(&postgres).await?;

        let manifests = self.render_system_manifests().await?;
        // Before the bundle, which starts hivekeep-controller on the real
        // data plane.
        self.remove_fake_workersync().await?;
        self.apply_with_api_server_env_hash(&manifests).await?;
        // After the bundle, which resets the pod template to the base.
        self.reconcile_cloud_sql_proxy_sidecar().await?;

        self.ensure_egress_mitm_ca_pool_secret().await?;
        // After the podcertificate controller: the provider serves with a
        // projected pod certificate.
        if provider.kubernetes() {
            self.deploy_k8s_credential_provider().await?;
        } else {
            self.remove_k8s_credential_provider().await?;
        }
        self.apply_gate_egress(&provider).await?;

        log::step("Waiting for Hivekeep system components to be ready...");
        let mut waits = vec![
            (WorkloadKind::Deployment, API_SERVER),
            (WorkloadKind::Deployment, super::idpserver::IDP_SERVER),
            (WorkloadKind::Deployment, CONTROLLER),
            (WorkloadKind::Deployment, GATE_ROUTER),
            (WorkloadKind::Deployment, GATE_EGRESS),
            (WorkloadKind::DaemonSet, KEEPER),
        ];
        if provider.kubernetes() {
            waits.push((WorkloadKind::Deployment, K8S_CREDENTIAL_PROVIDER_DEPLOYMENT));
        }
        let k = self.kube()?;
        for (kind, name) in waits {
            k.rollout_status(kind, self.namespace(), name, self.cfg.rollout_timeout)
                .await?;
        }
        self.wait_fake_workersync().await?;
        self.apply_otel_endpoint_override().await
    }

    /// Creates missing signer CA pools, applies the podcertificate controller
    /// and waits for it and its trust bundles.
    ///
    /// size10 renders the podcert-size10 overlay (raised API rate limits) in
    /// place of the base file, and WORKERS_PER_SIGNER is set on the rendered
    /// objects, so one apply means one rollout and the wait sees the pod that
    /// will serve.
    pub async fn deploy_pod_certificate_controller(&self) -> Result<()> {
        // The controller mounts every pool; one an older release did not
        // create keeps the new pod from starting.
        self.ensure_pod_certificate_cas().await?;
        let path = if self.cfg.size10() {
            self.cfg.manifest(&["podcert-size10"])
        } else {
            self.cfg.manifest(&["pod-certificate-controller.yaml"])
        };
        let manifest = self.render_resolve(&path).await?;
        let mut objs = decode_manifest_bytes(&manifest)?;
        if self.cfg.podcert_workers_per_signer > 0 {
            log::info(&format!(
                "Setting WORKERS_PER_SIGNER to {}",
                self.cfg.podcert_workers_per_signer
            ));
            set_podcert_workers_per_signer(&mut objs, self.cfg.podcert_workers_per_signer)?;
        }
        let k = self.kube()?;
        k.apply(&objs).await?;
        k.rollout_status(
            WorkloadKind::Deployment,
            NAMESPACE_POD_CERT,
            PODCERT_CONTROLLER,
            self.cfg.wait_timeout(BOOTSTRAP_TIMEOUT),
        )
        .await?;
        self.wait_for_pod_certificate_trust_bundles().await
    }

    /// Redeploys hivekeep-api and hivekeep-idp-server.
    pub async fn deploy_api_server(&self) -> Result<()> {
        log::step("deploy_api_server");
        super::apienv::cluster_snapshot_location(
            &self.cfg.snapshot_location,
            &self.cfg.preview_gates,
        )?;
        self.cfg.require_transfer_service_account()?;
        self.ensure_crds().await?;
        self.ensure_system_namespace().await?;
        self.ensure_api_server_prerequisites().await?;
        self.apply_otel_config().await?;
        self.apply_otel_endpoint_override().await?;
        let postgres = self.plan_postgres().await?;
        if postgres.bundled {
            self.wait_and_setup_bundled_postgres().await?;
        }
        let manifest = self
            .render_resolve(&self.api_server_manifest_path())
            .await?;
        self.apply_with_api_server_env_hash(&manifest).await?;
        // After the manifest, which resets the pod template to the base.
        self.reconcile_cloud_sql_proxy_sidecar().await?;
        // The identity provider shares only the actor JWT pool with the API
        // server; it comes along so redeploying an older release rolls it
        // back too.
        self.deploy_idp_server().await?;
        let k = self.kube()?;
        for name in [API_SERVER, super::idpserver::IDP_SERVER] {
            k.rollout_status(
                WorkloadKind::Deployment,
                self.namespace(),
                name,
                self.cfg.rollout_timeout,
            )
            .await?;
        }
        Ok(())
    }

    /// The hivekeep-api manifest for the environment.
    pub(crate) fn api_server_manifest_path(&self) -> PathBuf {
        if self.cfg.kind {
            self.cfg.manifest(&["kind", "hivekeep-api"])
        } else {
            self.cfg.manifest(&["hivekeep-api.yaml"])
        }
    }

    /// Redeploys only hivekeep-controller.
    pub async fn deploy_controller(&self) -> Result<()> {
        log::step("deploy_controller");
        self.require_installed_data_plane("deploy hivekeep-controller")
            .await?;
        self.deploy_crds().await?;
        self.ensure_system_namespace().await?;
        self.apply_otel_config().await?;
        let manifest = self.render(&self.cfg.manifest(&["hivekeep-controller.yaml"]))?;
        self.resolve_and_apply_bytes(&manifest).await?;
        self.kube()?
            .rollout_status(
                WorkloadKind::Deployment,
                self.namespace(),
                CONTROLLER,
                self.cfg.rollout_timeout,
            )
            .await?;
        self.wait_fake_workersync().await
    }

    /// The fake data plane component's fake-workersync manifest.
    pub(crate) fn fake_workersync_path(&self) -> PathBuf {
        self.cfg
            .manifest(&["components", "fake-data-plane", "fake-workersync.yaml"])
    }

    /// Refuses a single-component redeploy that asks for a data plane other
    /// than the installed one: keeper and hivekeep-controller switch
    /// together, and only `deploy hivekeep-system` switches both.
    pub(crate) async fn require_installed_data_plane(&self, step: &str) -> Result<()> {
        let installed = self
            .kube()?
            .deployment_exists(self.namespace(), FAKE_WORKERSYNC_DEPLOYMENT)
            .await?;
        match (self.cfg.fake_data_plane, installed) {
            (true, false) => Err(StepError::msg(format!(
                "{step} --fake-data-plane needs the fake data plane installed; install it with deploy \
                 hivekeep-system --fake-data-plane"
            ))),
            (false, true) => Err(StepError::msg(format!(
                "the cluster has the fake data plane, which {step} alone would half replace; pass \
                 --fake-data-plane, or switch back with deploy hivekeep-system"
            ))),
            _ => Ok(()),
        }
    }

    /// Refuses to install the fake data plane while worker pods exist,
    /// terminating ones included: stopping the controller would leave them
    /// registered beside a fake keeper. Finished pods (an evicted one) hold
    /// no Worker and do not count.
    pub(crate) async fn require_no_worker_pods(&self) -> Result<()> {
        if !self.cfg.fake_data_plane {
            return Ok(());
        }
        let pods = self
            .kube()?
            .list(&Gvk::of::<Pod>(), "", Some(WORKER_POOL_LABEL))
            .await
            .map_err(|e| StepError::from(e).context("while listing worker pods"))?;
        let mut names: Vec<String> = pods
            .iter()
            .filter(|p| {
                !matches!(
                    p.pointer("/status/phase").and_then(Value::as_str),
                    Some("Succeeded") | Some("Failed")
                )
            })
            .map(|p| {
                let pool = p
                    .labels()
                    .get(WORKER_POOL_LABEL)
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                format!("{}/{pool}", p.obj_namespace())
            })
            .collect();
        if names.is_empty() {
            return Ok(());
        }
        names.sort();
        names.dedup();
        Err(StepError::msg(format!(
            "--fake-data-plane cannot replace the real data plane while worker pods exist; delete these \
             WorkerPools and wait for their pods to go: {}",
            names.join(", ")
        )))
    }

    /// Deletes fake-workersync's Deployment, if any, and waits for its pod.
    pub(crate) async fn stop_fake_workersync(&self) -> Result<()> {
        let k = self.kube()?;
        if !k
            .deployment_exists(self.namespace(), FAKE_WORKERSYNC_DEPLOYMENT)
            .await?
        {
            return Ok(());
        }
        log::step("remove_fake_workersync");
        Ok(k.delete_deployment_and_wait(
            self.namespace(),
            FAKE_WORKERSYNC_DEPLOYMENT,
            self.cfg.rollout_timeout,
        )
        .await?)
    }

    /// On a real install, removes what an earlier fake install left of
    /// fake-workersync, before the bundle starts hivekeep-controller (whose
    /// startup sweep then deletes the fake Workers). The rest of its manifest
    /// is deleted on every real install, so a failed run leaves nothing
    /// behind for long.
    pub(crate) async fn remove_fake_workersync(&self) -> Result<()> {
        if self.cfg.fake_data_plane {
            return Ok(());
        }
        self.stop_fake_workersync().await?;
        Ok(self
            .kube()?
            .delete_path(&self.fake_workersync_path())
            .await?)
    }

    /// Waits for fake-workersync under `--fake-data-plane`.
    pub(crate) async fn wait_fake_workersync(&self) -> Result<()> {
        if !self.cfg.fake_data_plane {
            return Ok(());
        }
        Ok(self
            .kube()?
            .rollout_status(
                WorkloadKind::Deployment,
                self.namespace(),
                FAKE_WORKERSYNC_DEPLOYMENT,
                self.cfg.rollout_timeout,
            )
            .await?)
    }

    /// Redeploys only the keeper DaemonSet.
    pub async fn deploy_keeper(&self) -> Result<()> {
        log::step("deploy_keeper");
        self.require_installed_data_plane("deploy keeper").await?;
        self.ensure_crds().await?;
        self.ensure_system_namespace().await?;
        self.apply_otel_config().await?;
        self.apply_otel_endpoint_override().await?;
        let manifest = self.render(&self.keeper_manifest_path())?;
        let manifest = self.resolve_manifest_bytes(&manifest).await?;
        let k = self.kube()?;
        k.apply_bytes(&manifest).await?;
        k.rollout_status(
            WorkloadKind::DaemonSet,
            self.namespace(),
            KEEPER,
            self.cfg.rollout_timeout,
        )
        .await?;
        self.wait_fake_workersync().await
    }

    /// The keeper manifest for the environment.
    pub(crate) fn keeper_manifest_path(&self) -> PathBuf {
        if self.cfg.kind {
            self.cfg.manifest(&["kind", "keeper"])
        } else {
            self.cfg.manifest(&["keeper.yaml"])
        }
    }

    /// Redeploys the gate dataplane: router and egress.
    pub async fn deploy_gate(&self) -> Result<()> {
        log::step("deploy_gate");
        let provider = self.cfg.credential_provider()?;
        self.ensure_crds().await?;
        self.ensure_system_namespace().await?;
        self.apply_otel_config().await?;
        self.apply_otel_endpoint_override().await?;
        let router = self.render_gate_router_manifest().await?;
        self.kube()?.apply_bytes(&router).await?;
        self.ensure_egress_mitm_ca_pool_secret().await?;
        if provider.kubernetes() {
            self.deploy_k8s_credential_provider().await?;
        } else {
            self.remove_k8s_credential_provider().await?;
        }
        self.apply_gate_egress(&provider).await?;
        let mut deployments = vec![GATE_ROUTER, GATE_EGRESS];
        if provider.kubernetes() {
            deployments.push(K8S_CREDENTIAL_PROVIDER_DEPLOYMENT);
        }
        let k = self.kube()?;
        for name in deployments {
            k.rollout_status(
                WorkloadKind::Deployment,
                self.namespace(),
                name,
                self.cfg.rollout_timeout,
            )
            .await?;
        }
        Ok(())
    }

    /// Applies the SandboxConfig admission policy, then the default gVisor
    /// SandboxConfig (validated by the policy applied before it).
    pub async fn deploy_sandbox_config(&self) -> Result<()> {
        log::step("deploy_sandboxconfig");
        self.ensure_crds().await?;
        let k = self.kube()?;
        k.apply_path(&self.cfg.manifest(&["sandboxconfig-validation.yaml"]))
            .await?;
        Ok(
            k.apply_path(&self.cfg.manifest(&["sandboxconfig-gvisor.yaml"]))
                .await?,
        )
    }

    /// Creates the generated CRDs that are missing. Existing CRDs and RBAC
    /// change only through [`Self::deploy_crds`].
    pub async fn ensure_crds(&self) -> Result<()> {
        log::step("ensure_crds");
        let crds = self.generated_crds()?;
        let k = self.kube()?;
        let mut present = std::collections::BTreeMap::new();
        for crd in &crds {
            present.insert(
                crd.name().to_string(),
                k.exists(&crd_gvk(), "", crd.name()).await?,
            );
        }
        let missing = missing_crds(&crds, |name| {
            Ok(present.get(name).copied().unwrap_or(false))
        })?;
        if missing.is_empty() {
            return Ok(());
        }
        k.apply(&missing).await?;
        self.wait_crds_established(&missing).await
    }

    /// The generated CRDs, without the RBAC beside them.
    pub(crate) fn generated_crds(&self) -> Result<Vec<Object>> {
        let crd = crd_gvk();
        Ok(load_path(&self.cfg.manifest(&["generated"]))?
            .into_iter()
            .filter(|o| o.gvk() == crd)
            .collect())
    }

    /// Applies the generated CRDs and RBAC and waits for the CRDs to be
    /// Established.
    pub async fn deploy_crds(&self) -> Result<()> {
        log::step("deploy_crds");
        self.resolve_and_apply(&self.cfg.manifest(&["generated"]))
            .await?;
        let crds = self.generated_crds()?;
        self.wait_crds_established(&crds).await
    }

    async fn wait_crds_established(&self, crds: &[Object]) -> Result<()> {
        let k = self.kube()?;
        for crd in crds {
            k.wait_condition(
                &crd_gvk(),
                "",
                crd.name(),
                "Established",
                CRD_ESTABLISHED_TIMEOUT,
            )
            .await?;
        }
        // Later steps create custom resources of the kinds just installed.
        k.invalidate_discovery();
        Ok(())
    }
}

#[cfg(test)]
// Tests build a default Config and adjust the one field under test.
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::config::{test_config, Config, CLUSTER_SIZE_SIZE0, CLUSTER_SIZE_SIZE10};
    use crate::kube::stub;
    use crate::steps::overlay_tests::{fake_env, kustomize_missing};

    fn podcert_objects(cluster_size: &str) -> Vec<Object> {
        let mut cfg = test_config();
        cfg.cluster_size = cluster_size.into();
        let e = Env::local(cfg);
        let path = if e.cfg.size10() {
            e.cfg.manifest(&["podcert-size10"])
        } else {
            e.cfg.manifest(&["pod-certificate-controller.yaml"])
        };
        decode_manifest_bytes(&e.render(&path).unwrap()).unwrap()
    }

    /// Every WORKERS_PER_SIGNER value on the controller container.
    fn workers_per_signer(objs: &[Object]) -> Vec<Value> {
        let dep = objs
            .iter()
            .find(|o| o.kind() == "Deployment" && o.name() == PODCERT_CONTROLLER)
            .expect("deployment");
        dep.pointer("/spec/template/spec/containers/0/env")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|v| v.get("name").and_then(Value::as_str) == Some("WORKERS_PER_SIGNER"))
            .map(|v| v["value"].clone())
            .collect()
    }

    #[test]
    fn set_podcert_workers_per_signer_over_the_real_manifests() {
        if kustomize_missing() {
            return;
        }
        for size in [CLUSTER_SIZE_SIZE0, CLUSTER_SIZE_SIZE10] {
            let mut objs = podcert_objects(size);
            assert_eq!(workers_per_signer(&objs).len(), 1, "{size}");
            set_podcert_workers_per_signer(&mut objs, 8).unwrap();
            assert_eq!(workers_per_signer(&objs), vec![json!("8")], "{size}");
        }
    }

    #[test]
    fn set_podcert_workers_per_signer_appends_when_absent() {
        let mut objs = vec![json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"namespace": NAMESPACE_POD_CERT, "name": PODCERT_CONTROLLER},
            "spec": {"template": {"spec": {"containers": [{"name": "controller"}]}}},
        })];
        set_podcert_workers_per_signer(&mut objs, 3).unwrap();
        assert_eq!(workers_per_signer(&objs), vec![json!("3")]);
    }

    #[test]
    fn set_podcert_workers_per_signer_rejects_missing_deployment() {
        let mut objs: Vec<Object> = podcert_objects(CLUSTER_SIZE_SIZE0)
            .into_iter()
            .filter(|o| o.kind() != "Deployment")
            .collect();
        assert!(set_podcert_workers_per_signer(&mut objs, 2).is_err());
    }

    #[tokio::test]
    async fn deploy_pod_certificate_controller_creates_missing_cas() {
        // An empty manifests root: the deploy stops at rendering, after the
        // pools.
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.manifests = dir.path().to_path_buf();
        let (e, _) = fake_env(cfg, vec![stub(&Gvk::namespace(), "", NAMESPACE_POD_CERT)]);
        e.create_pod_certificate_controller_cas().await.unwrap();
        let k = e.kube().unwrap();
        let secret = crate::steps::create::SECRET_POSTGRES_CA;
        k.delete_named(
            &Gvk::new("", "v1", "Secret"),
            NAMESPACE_POD_CERT,
            secret,
            None,
        )
        .await
        .unwrap();
        let err = e.deploy_pod_certificate_controller().await.unwrap_err();
        assert!(
            err.to_string().contains("while reading"),
            "want the render error for the missing manifest: {err}"
        );
        assert!(
            k.secret_exists(NAMESPACE_POD_CERT, secret).await.unwrap(),
            "{secret} not recreated"
        );
    }

    #[test]
    fn generated_crds_leaves_out_rbac() {
        let e = Env::local(test_config());
        let crds = e.generated_crds().unwrap();
        assert!(crds.iter().all(|c| c.kind() == "CustomResourceDefinition"));
        let files = std::fs::read_dir(e.cfg.manifest(&["generated"]))
            .unwrap()
            .filter(|f| {
                let n = f
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .into_owned();
                n.starts_with("hivekeep.io_") && n.ends_with(".yaml")
            })
            .count();
        assert_eq!(crds.len(), files);
    }

    #[test]
    fn missing_crds_leaves_present_ones_alone() {
        let crd = |name: &str| stub(&crd_gvk(), "", name);
        let crds = vec![
            crd("workerpools.hivekeep.io"),
            crd("sandboxconfigs.hivekeep.io"),
            crd("csidriverconfigs.hivekeep.io"),
        ];
        let present = ["workerpools.hivekeep.io", "sandboxconfigs.hivekeep.io"];
        let missing = missing_crds(&crds, |n| Ok(present.contains(&n))).unwrap();
        let names: Vec<&str> = missing.iter().map(|m| m.name()).collect();
        assert_eq!(names, vec!["csidriverconfigs.hivekeep.io"]);
    }

    #[test]
    fn component_manifests_match_system_bundle() {
        if kustomize_missing() {
            return;
        }
        for kind in [false, true] {
            let mut cfg = test_config();
            cfg.kind = kind;
            let e = Env::local(cfg);
            let render =
                |p: &std::path::Path| decode_manifest_bytes(&e.render(p).unwrap()).unwrap();
            let bundle = render(&e.system_overlay_path());
            for (k, name, path) in [
                ("Deployment", API_SERVER, e.api_server_manifest_path()),
                (
                    "Deployment",
                    super::super::idpserver::IDP_SERVER,
                    e.idp_server_manifest_path(),
                ),
                ("DaemonSet", KEEPER, e.keeper_manifest_path()),
            ] {
                let find = |objs: &[Object]| {
                    objs.iter()
                        .find(|o| o.kind() == k && o.name() == name)
                        .cloned()
                };
                let want = find(&bundle)
                    .unwrap_or_else(|| panic!("kind={kind}: {k}/{name} missing from the bundle"));
                let got = find(&render(&path)).unwrap_or_else(|| {
                    panic!("kind={kind}: {k}/{name} missing from {}", path.display())
                });
                assert_eq!(
                    want,
                    got,
                    "kind={kind}: {} renders a different {k}/{name}",
                    path.display()
                );
            }
        }
    }

    #[test]
    fn deploy_options_validate() {
        assert!(DeployOptions::default().validate().is_ok());
        assert!(DeployOptions {
            setup_csi: "bogus".into()
        }
        .validate()
        .is_err());
    }
}
