//! The WorkerPool reconciler: applies the worker Deployment, resolves the
//! pool's SandboxConfigs and records them in status.

use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_k8s::crd::sandboxconfig::WORKER_POOL_PROTECTION_FINALIZER;
use hivekeep_k8s::crd::workerpool::CONDITION_RESOLVED;
use hivekeep_k8s::crd::{SandboxConfig, WorkerPool, WorkerPoolSandboxClassStatus};
use hivekeep_k8s::install_defaults::WORKER_SERVICE_ACCOUNT;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::ServiceAccount;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::{Client, ResourceExt};
use serde_json::json;

use super::error::{api_err, Error, Result};
use super::events::{object_reference, EventRecorder, EventType};
use super::meta::{find_condition, has_finalizer, now, selector_string};
use super::queue::ObjectKey;
use super::reader::Reader;
use super::runner::{Action, Reconcile};
use super::workerpool_apply::{build_deployment, WorkerPodSettings};
use super::workerpool_sandboxconfig::{
    claim_sandbox_configs, confirm_sandbox_config_switch, every_class_has_config,
    find_sandbox_class_status, resolve_sandbox_classes, switches_sandbox_config,
    switches_sandbox_config_from, worker_pool_may_use_sandbox_config,
};

/// Field manager of everything the WorkerPool reconciler applies.
pub const WORKER_POOL_FIELD_OWNER: &str = "workerpool-controller";
/// Reason of the Normal Event a pool emits each time a class's
/// `configRef` changes.
pub const EVENT_REASON_SANDBOX_CONFIG_SWITCHED: &str = "SandboxConfigSwitched";
/// Action of every Event a pool emits while resolving its SandboxConfigs.
pub const EVENT_ACTION_RESOLVE_SANDBOX_CONFIG: &str = "ResolveSandboxConfig";

/// Reconciles WorkerPools.
pub struct WorkerPoolReconciler {
    client: Client,
    api_reader: Client,
    pools: Reader<WorkerPool>,
    deployments: Reader<Deployment>,
    configs: Reader<SandboxConfig>,
    recorder: Arc<dyn EventRecorder>,
    pod_settings: WorkerPodSettings,
}

impl WorkerPoolReconciler {
    /// A reconciler writing through `client`, reading pools, Deployments
    /// and configs from the given readers, and confirming a config switch
    /// with a read through `api_reader`, which must not be a cache.
    pub fn new(
        client: Client,
        api_reader: Client,
        pools: Reader<WorkerPool>,
        deployments: Reader<Deployment>,
        configs: Reader<SandboxConfig>,
        recorder: Arc<dyn EventRecorder>,
        pod_settings: WorkerPodSettings,
    ) -> Self {
        WorkerPoolReconciler {
            client,
            api_reader,
            pools,
            deployments,
            configs,
            recorder,
            pod_settings,
        }
    }

    /// A reconciler doing every read through `client`.
    pub fn live(
        client: Client,
        recorder: Arc<dyn EventRecorder>,
        pod_settings: WorkerPodSettings,
    ) -> Self {
        Self::new(
            client.clone(),
            client.clone(),
            Reader::live_namespaced(client.clone()),
            Reader::live_namespaced(client.clone()),
            Reader::live_cluster(client),
            recorder,
            pod_settings,
        )
    }

    /// Like [`WorkerPoolReconciler::live`], confirming switches through
    /// `api_reader`.
    pub fn with_api_reader(mut self, api_reader: Client) -> Self {
        self.api_reader = api_reader;
        self
    }

    /// Applies the Deployment, resolves the pool's SandboxConfigs, and
    /// records them in status.
    ///
    /// A class switching to a new config does so in two status writes, so
    /// status never names a config after its protection finalizer was
    /// released:
    ///
    /// 1. Resolve every class from the cache and add the protection
    ///    finalizer to each config a class resolved to.
    /// 2. Write status with the new config in `pendingConfigRef` and the
    ///    config in use still in `configRef`; both are protected from now.
    /// 3. Read the new config from the API server. If it is live,
    ///    `configRef` moves to it; if it is gone or being deleted, the class
    ///    keeps the config in use.
    /// 4. Write status again and emit Events for what changed.
    ///
    /// The read in step 3 must follow the write in step 2: the protection
    /// reconciler releases only a config that is already being deleted,
    /// after a live list found no pool using it. If it released this one,
    /// that list ran before the claim was written, while the config was
    /// already being deleted, so the read sees the deletion.
    pub(crate) async fn reconcile_worker_pool(&self, mut wp: WorkerPool) -> Result<()> {
        let ns = wp.namespace().unwrap_or_default();
        let name = wp.name_any();
        tracing::info!(namespace = %ns, name = %name, "Reconciling worker pool");

        let existing = self
            .deployments
            .get(&ns, &name)
            .await
            .map_err(api_err("failed to get deployment"))?;
        // Worker pods take nothing from the SandboxConfig, so an existing
        // Deployment is applied first: a SandboxConfig error never holds
        // back a scale.
        if existing.is_some() {
            self.apply_deployment(&wp).await?;
        }

        let mut configs = self
            .configs
            .list()
            .await
            .map_err(api_err("failed to list SandboxConfigs"))?;
        let now = now();
        let (mut classes, _) = resolve_sandbox_classes(&wp, &configs, &now);
        protect_sandbox_configs(&self.client, &classes, &mut configs).await?;
        // The status before this reconcile, for the Events below.
        let prev = wp
            .status
            .as_ref()
            .map(|s| s.sandbox_classes.clone())
            .unwrap_or_default();
        if let Some(claim) = claim_sandbox_configs(&wp, &classes) {
            self.sync_status(&mut wp, existing.as_ref(), claim).await?;
            self.confirm_sandbox_config_switches(&wp, &mut classes, &configs, &now)
                .await?;
        }
        self.sync_status(&mut wp, existing.as_ref(), classes.clone())
            .await?;
        self.record_sandbox_config_events(&wp, &prev, &classes);
        // A pool without a Deployment gets one only once every class has a
        // config.
        if existing.is_none() && every_class_has_config(&classes) {
            self.apply_deployment(&wp).await?;
        }
        Ok(())
    }

    /// Reads every config a class switches to from the API server and
    /// settles the class on it, or on the config in use. `wp`'s status
    /// must already claim the new configs.
    async fn confirm_sandbox_config_switches(
        &self,
        wp: &WorkerPool,
        classes: &mut [WorkerPoolSandboxClassStatus],
        configs: &[SandboxConfig],
        now: &k8s_openapi::apimachinery::pkg::apis::meta::v1::Time,
    ) -> Result<()> {
        let live_api: Api<SandboxConfig> = Api::all(self.api_reader.clone());
        for class in classes.iter_mut() {
            if !switches_sandbox_config(wp, class) {
                continue;
            }
            let name = class
                .config_ref
                .as_ref()
                .map(|r| r.name.clone())
                .unwrap_or_default();
            let live = live_api
                .get_opt(&name)
                .await
                .map_err(api_err(format!("failed to get SandboxConfig {name:?}")))?;
            *class = confirm_sandbox_config_switch(wp, class.clone(), live.as_ref(), configs, now);
        }
        Ok(())
    }

    /// Emits an Event for every class whose state differs from `prev`, the
    /// status before this reconcile:
    ///
    /// - a Normal Event naming the old and new config when `configRef`
    ///   changed, so the pool's Events are the history of its configs;
    /// - a Warning Event when Resolved is False, unless `prev` reports the
    ///   same failure already.
    ///
    /// It runs after `classes` is written, so a reconcile whose write was
    /// rejected reports nothing and the switch is reported once.
    pub(crate) fn record_sandbox_config_events(
        &self,
        wp: &WorkerPool,
        prev: &[WorkerPoolSandboxClassStatus],
        classes: &[WorkerPoolSandboxClassStatus],
    ) {
        for class in classes {
            let before = find_sandbox_class_status(prev, class.name);
            if switches_sandbox_config_from(prev, class) {
                let new = class
                    .config_ref
                    .as_ref()
                    .map(|r| r.name.as_str())
                    .unwrap_or_default();
                let note = match before.and_then(|p| p.config_ref.as_ref()) {
                    Some(old) => format!(
                        "sandbox class {:?} switched from SandboxConfig {:?} to {new:?}",
                        class.name.as_str(),
                        old.name
                    ),
                    None => format!(
                        "sandbox class {:?} uses SandboxConfig {new:?}",
                        class.name.as_str()
                    ),
                };
                self.recorder.event(
                    object_reference(wp),
                    None,
                    EventType::Normal,
                    EVENT_REASON_SANDBOX_CONFIG_SWITCHED,
                    EVENT_ACTION_RESOLVE_SANDBOX_CONFIG,
                    note,
                );
            }
            let Some(cond) = find_condition(&class.conditions, CONDITION_RESOLVED) else {
                continue;
            };
            if cond.status == "True" {
                continue;
            }
            if let Some(p) = before.and_then(|p| find_condition(&p.conditions, &cond.type_)) {
                if p.status == cond.status && p.reason == cond.reason && p.message == cond.message {
                    continue;
                }
            }
            self.recorder.event(
                object_reference(wp),
                None,
                EventType::Warning,
                &cond.reason,
                EVENT_ACTION_RESOLVE_SANDBOX_CONFIG,
                cond.message.clone(),
            );
        }
    }

    async fn apply_deployment(&self, wp: &WorkerPool) -> Result<()> {
        let ns = wp.namespace().unwrap_or_default();
        if wp
            .spec
            .template
            .as_ref()
            .and_then(|t| t.service_account_name.as_ref())
            .is_none()
        {
            self.apply_worker_service_account(&ns).await?;
        }
        let dep = build_deployment(wp, &self.pod_settings);
        let api: Api<Deployment> = Api::namespaced(self.client.clone(), &ns);
        api.patch(
            &wp.name_any(),
            &PatchParams::apply(WORKER_POOL_FIELD_OWNER).force(),
            &Patch::Apply(&dep),
        )
        .await
        .map_err(api_err("failed to apply Deployment"))?;
        Ok(())
    }

    /// Creates the default ServiceAccount worker pods run as. The pools of
    /// a namespace share it, so none owns it.
    async fn apply_worker_service_account(&self, namespace: &str) -> Result<()> {
        let sa = ServiceAccount {
            metadata: ObjectMeta {
                name: Some(WORKER_SERVICE_ACCOUNT.into()),
                namespace: Some(namespace.into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let api: Api<ServiceAccount> = Api::namespaced(self.client.clone(), namespace);
        api.patch(
            WORKER_SERVICE_ACCOUNT,
            &PatchParams::apply(WORKER_POOL_FIELD_OWNER).force(),
            &Patch::Apply(&sa),
        )
        .await
        .map_err(api_err("failed to apply worker ServiceAccount"))?;
        Ok(())
    }

    /// Writes `wp`'s status: replica counts and selector from `dep` (kept
    /// as they are without one) and `classes`. Writes nothing when nothing
    /// changed; on a write, `wp` becomes the stored object.
    pub(crate) async fn sync_status(
        &self,
        wp: &mut WorkerPool,
        dep: Option<&Deployment>,
        classes: Vec<WorkerPoolSandboxClassStatus>,
    ) -> Result<()> {
        let current = wp.status.clone().unwrap_or_default();
        let mut want = current.clone();
        if let Some(dep) = dep {
            let st = dep.status.as_ref();
            want.replicas = st.and_then(|s| s.replicas).unwrap_or(0);
            want.ready_replicas = st.and_then(|s| s.ready_replicas).unwrap_or(0);
            want.selector = selector_string(dep.spec.as_ref().map(|s| &s.selector));
        }
        want.sandbox_classes = classes;
        if current == want {
            return Ok(());
        }
        wp.status = Some(want);
        let api: Api<WorkerPool> =
            Api::namespaced(self.client.clone(), &wp.namespace().unwrap_or_default());
        *wp = api
            .replace_status(&wp.name_any(), &PostParams::default(), wp)
            .await
            .map_err(api_err("failed to update WorkerPool status"))?;
        Ok(())
    }

    /// The pools whose resolution a change to `sc` can change. A failed
    /// list maps to nothing.
    pub async fn worker_pools_for_sandbox_config(&self, sc: &SandboxConfig) -> Vec<ObjectKey> {
        worker_pools_for_sandbox_config(&self.pools, sc).await
    }
}

/// See [`WorkerPoolReconciler::worker_pools_for_sandbox_config`].
pub async fn worker_pools_for_sandbox_config(
    pools: &Reader<WorkerPool>,
    sc: &SandboxConfig,
) -> Vec<ObjectKey> {
    match pools
        .list_filtered(|wp| worker_pool_may_use_sandbox_config(wp, sc))
        .await
    {
        Ok(pools) => pools.iter().map(ObjectKey::of).collect(),
        Err(err) => {
            tracing::error!(%err, sandbox_config = %sc.name_any(), "Failed to list worker pools for SandboxConfig");
            Vec::new()
        }
    }
}

#[async_trait]
impl Reconcile for WorkerPoolReconciler {
    async fn reconcile(&self, key: &ObjectKey) -> Result<Action> {
        let Some(wp) = self
            .pools
            .get(&key.namespace, &key.name)
            .await
            .map_err(api_err(format!("failed to get worker pool {key:?}")))?
        else {
            return Ok(Action::Done);
        };
        if wp.metadata.deletion_timestamp.is_some() {
            tracing::info!(pool = %key, "WorkerPool is being deleted");
            return Ok(Action::Done);
        }
        if let Err(err) = self.reconcile_worker_pool(wp).await {
            tracing::error!(%err, pool = %key, "Failed to reconcile worker pool");
            return Err(err);
        }
        Ok(Action::Done)
    }
}

/// Adds the protection finalizer to every config `classes` resolved to,
/// before status names it. This is the only place the finalizer is added:
/// a config no pool ever used deletes at once. A config deleted after the
/// cached read is caught when the switch is confirmed.
pub(crate) async fn protect_sandbox_configs(
    client: &Client,
    classes: &[WorkerPoolSandboxClassStatus],
    configs: &mut [SandboxConfig],
) -> Result<()> {
    for class in classes {
        let Some(r) = &class.config_ref else { continue };
        let Some(sc) = configs
            .iter_mut()
            .find(|sc| sc.metadata.name.as_deref() == Some(r.name.as_str()))
        else {
            return Err(Error::Invalid(format!(
                "resolved SandboxConfig {:?} is not in the list it was resolved from",
                r.name
            )));
        };
        add_protection_finalizer(client, sc).await?;
    }
    Ok(())
}

/// The finalizers of `sc` as a merge patch guarded by its
/// resourceVersion: a concurrent change fails the patch with a conflict.
fn finalizer_patch(sc: &SandboxConfig, finalizers: Vec<String>) -> serde_json::Value {
    json!({"metadata": {"finalizers": finalizers, "resourceVersion": sc.metadata.resource_version}})
}

/// Adds the protection finalizer to `sc`, which becomes the stored
/// object. A config being deleted is left alone: the API server rejects a
/// new finalizer on it.
pub async fn add_protection_finalizer(client: &Client, sc: &mut SandboxConfig) -> Result<()> {
    if has_finalizer(&sc.metadata, WORKER_POOL_PROTECTION_FINALIZER) || sc.is_deleting() {
        return Ok(());
    }
    let mut finalizers = sc.metadata.finalizers.clone().unwrap_or_default();
    finalizers.push(WORKER_POOL_PROTECTION_FINALIZER.into());
    patch_finalizers(client, sc, finalizers).await
}

/// Removes the protection finalizer from `sc`, which becomes the stored
/// object (if it still exists).
pub async fn remove_protection_finalizer(client: &Client, sc: &mut SandboxConfig) -> Result<()> {
    if !has_finalizer(&sc.metadata, WORKER_POOL_PROTECTION_FINALIZER) {
        return Ok(());
    }
    let finalizers: Vec<String> = sc
        .metadata
        .finalizers
        .iter()
        .flatten()
        .filter(|f| *f != WORKER_POOL_PROTECTION_FINALIZER)
        .cloned()
        .collect();
    patch_finalizers(client, sc, finalizers).await
}

async fn patch_finalizers(
    client: &Client,
    sc: &mut SandboxConfig,
    finalizers: Vec<String>,
) -> Result<()> {
    let name = sc.name_any();
    let api: Api<SandboxConfig> = Api::all(client.clone());
    let patch = finalizer_patch(sc, finalizers);
    *sc = api
        .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .map_err(api_err(format!(
            "failed to update finalizers of SandboxConfig {name:?}"
        )))?;
    Ok(())
}

#[cfg(test)]
#[path = "workerpool_tests.rs"]
mod tests;
