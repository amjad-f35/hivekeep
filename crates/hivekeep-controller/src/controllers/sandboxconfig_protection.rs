//! Releases a SandboxConfig that is being deleted once no WorkerPool uses
//! it, by removing the protection finalizer.

use async_trait::async_trait;
use hivekeep_k8s::crd::sandboxconfig::WORKER_POOL_PROTECTION_FINALIZER;
use hivekeep_k8s::crd::{SandboxConfig, WorkerPool};
use kube::api::{Api, ListParams};
use kube::Client;

use super::error::{api_err, Result};
use super::meta::has_finalizer;
use super::queue::ObjectKey;
use super::reader::Reader;
use super::runner::{Action, Reconcile};
use super::workerpool::remove_protection_finalizer;

/// Reconciles SandboxConfigs for the protection finalizer.
pub struct SandboxConfigProtectionReconciler {
    client: Client,
    api_reader: Client,
    configs: Reader<SandboxConfig>,
    pools: Reader<WorkerPool>,
}

impl SandboxConfigProtectionReconciler {
    /// A reconciler writing through `client`, reading configs and pools
    /// from the given readers, and checking for users with a list through
    /// `api_reader`, which must not be a cache: the cache can lag behind a
    /// pool that just started using a config.
    pub fn new(
        client: Client,
        api_reader: Client,
        configs: Reader<SandboxConfig>,
        pools: Reader<WorkerPool>,
    ) -> Self {
        SandboxConfigProtectionReconciler {
            client,
            api_reader,
            configs,
            pools,
        }
    }

    /// A reconciler doing every read through `client`.
    pub fn live(client: Client) -> Self {
        Self::new(
            client.clone(),
            client.clone(),
            Reader::live_cluster(client.clone()),
            Reader::live_namespaced(client),
        )
    }

    /// Whether any WorkerPool's status names the config. The cache is
    /// checked first; only when it finds no user is the API server listed,
    /// which is rare because a config is deleted far less often than a
    /// pool reconciles.
    async fn used_by_worker_pool(&self, name: &str) -> Result<bool> {
        let cached = self
            .pools
            .list_filtered(|wp| wp.sandbox_configs_in_use().contains(&name))
            .await
            .map_err(api_err(format!(
                "failed to list WorkerPools using SandboxConfig {name:?}"
            )))?;
        if !cached.is_empty() {
            return Ok(true);
        }
        let live = Api::<WorkerPool>::all(self.api_reader.clone())
            .list(&ListParams::default())
            .await
            .map_err(api_err("failed to list WorkerPools"))?;
        Ok(live
            .items
            .iter()
            .any(|wp| wp.sandbox_configs_in_use().contains(&name)))
    }
}

#[async_trait]
impl Reconcile for SandboxConfigProtectionReconciler {
    /// Decides whether one SandboxConfig can be released:
    ///
    /// - one not being deleted needs nothing;
    /// - one being deleted and still used keeps the finalizer, so it stays
    ///   Terminating (it is reconciled again when a pool using it changes);
    /// - one being deleted and unused loses it, and the API server finishes
    ///   the delete.
    ///
    /// Every config is also reconciled at startup, which releases one whose
    /// last user went away while the controller was down.
    async fn reconcile(&self, key: &ObjectKey) -> Result<Action> {
        let Some(mut sc) = self
            .configs
            .get("", &key.name)
            .await
            .map_err(api_err(format!(
                "failed to get SandboxConfig {:?}",
                key.name
            )))?
        else {
            return Ok(Action::Done);
        };
        if !sc.is_deleting() || !has_finalizer(&sc.metadata, WORKER_POOL_PROTECTION_FINALIZER) {
            return Ok(Action::Done);
        }
        if self.used_by_worker_pool(&key.name).await? {
            return Ok(Action::Done);
        }
        remove_protection_finalizer(&self.client, &mut sc).await?;
        Ok(Action::Done)
    }
}

/// The configs a WorkerPool event concerns: every config its status names.
pub fn sandbox_configs_for_worker_pool(wp: &WorkerPool) -> Vec<ObjectKey> {
    wp.sandbox_configs_in_use()
        .into_iter()
        .map(ObjectKey::cluster)
        .collect()
}

#[cfg(test)]
#[path = "sandboxconfig_protection_tests.rs"]
mod tests;
