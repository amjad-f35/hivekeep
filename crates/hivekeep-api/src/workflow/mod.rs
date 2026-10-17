//! The multi-step Actor operations: resume, suspend, revert, delete, tag,
//! template delete, crash and snapshot-state reports.
//!
//! Every step follows the ensure pattern: it derives from the persisted
//! record alone whether its work is already done, checks the lifecycle edge
//! it is about to take, and persists what it changed before returning. A
//! retried operation therefore fast-forwards to where the previous attempt
//! stopped. Operations on one Actor are serialized by its lease.

pub mod crash;
pub mod delete;
pub mod report;
pub mod resume;
pub mod resume_detach;
pub mod revert;
pub mod suspend;
pub mod tag;
pub mod template_delete;

use std::sync::Arc;

use hivekeep_core::preview::{Gate, PreviewGates};
use hivekeep_core::snapshot::{SnapshotOwner, SnapshotUri};
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{Lease, Store, StoreError};

use crate::error::{status, Error, Result};
use crate::fields;
use crate::keeper_dialer::KeeperDialer;
use crate::leases;
use crate::metrics::Metrics;
use crate::object_store::ObjectStore;
use crate::scheduling::Scheduler;
use crate::snapshots;
use crate::volumes::VolumeProvider;
use crate::worker_cache::WorkerCache;

/// Server settings the workflows read.
#[derive(Debug, Clone, Default)]
pub struct Settings {
    /// host:port of the egress gateway sent on every Run and Restore; empty
    /// leaves Actors without TCP egress.
    pub egress_gateway_address: String,
    /// The cluster-wide snapshot location.
    pub snapshot_location: String,
    /// Preview gates enabled on the server.
    pub gates: PreviewGates,
}

/// The dependencies of the Actor workflows.
pub struct ActorWorkflow {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) cache: Arc<WorkerCache>,
    pub(crate) scheduler: Scheduler,
    pub(crate) dialer: Arc<dyn KeeperDialer>,
    pub(crate) volumes: Arc<dyn VolumeProvider>,
    pub(crate) object_store: Option<Arc<dyn ObjectStore>>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) settings: Settings,
}

impl ActorWorkflow {
    /// Workflows over the given dependencies. Without an object store,
    /// external snapshots are left in place rather than copied or collected.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<dyn Store>,
        cache: Arc<WorkerCache>,
        scheduler: Scheduler,
        dialer: Arc<dyn KeeperDialer>,
        volumes: Arc<dyn VolumeProvider>,
        object_store: Option<Arc<dyn ObjectStore>>,
        metrics: Arc<Metrics>,
        settings: Settings,
    ) -> Self {
        ActorWorkflow {
            store,
            cache,
            scheduler,
            dialer,
            volumes,
            object_store,
            metrics,
            settings,
        }
    }

    pub(crate) fn external_volumes_enabled(&self) -> bool {
        self.settings.gates.is_enabled(Gate::ExternalVolumes)
    }

    pub(crate) fn snapshot_location(&self, t: &pb::ActorTemplate) -> Result<String> {
        snapshots::resolve_location(&self.settings.snapshot_location, &self.settings.gates, t)
    }

    pub(crate) async fn lease_actor(&self, r: &hivekeep_core::resource::ActorRef) -> Result<Lease> {
        leases::acquire(self.store.as_ref(), &leases::actor_key(r), "actor").await
    }

    pub(crate) async fn cleanup_snapshot(&self, prefix: &str) -> Result<()> {
        match &self.object_store {
            None => Ok(()),
            Some(os) => os
                .cleanup_snapshot(prefix)
                .await
                .map_err(|e| status::unavailable(e.to_string())),
        }
    }

    /// Releases each COMPLETED snapshot `before` recorded that `after` no
    /// longer does. Best effort: a failure leaves the objects until the
    /// Actor is deleted.
    pub(crate) async fn release_dropped_snapshots(&self, before: &pb::Actor, after: &pb::Actor) {
        let kept: Vec<&str> = after
            .status
            .iter()
            .flat_map(|s| s.snapshots.iter())
            .map(snapshots::durable_uri)
            .collect();
        for snap in before.status.iter().flat_map(|s| s.snapshots.iter()) {
            let dropped = snapshots::durable_uri(snap);
            if !snapshots::is_durable(snap) || kept.contains(&dropped) {
                continue;
            }
            if let Err(e) = self.release_replaced_snapshot(before, dropped).await {
                tracing::warn!(actor = %fields::actor_ref(before), snapshot_uri = dropped, error = %e,
                    "failed to release an external snapshot the actor no longer records; its objects stay until the actor is deleted");
            }
        }
    }

    async fn release_replaced_snapshot(&self, actor: &pb::Actor, previous: &str) -> Result<()> {
        if self.object_store.is_none() || previous.is_empty() {
            return Ok(());
        }
        let uri = SnapshotUri::parse(previous).map_err(|e| {
            status::internal(format!(
                "while parsing the replaced external snapshot {previous:?}: {e}"
            ))
        })?;
        if !uri.owned_by(&actor_snapshot_owner(actor)) {
            return Ok(());
        }
        self.cleanup_snapshot(uri.prefix().as_str()).await
    }
}

/// The owner identity of an Actor's own snapshots.
pub fn actor_snapshot_owner(actor: &pb::Actor) -> SnapshotOwner {
    SnapshotOwner::actor(fields::actor_hive(actor), fields::actor_uid(actor))
}

/// The ActorTemplate the Actor names. A missing one is FAILED_PRECONDITION
/// "actor template not found"; [`is_template_not_found`] recognizes it.
pub async fn resolve_template(store: &dyn Store, actor: &pb::Actor) -> Result<pb::ActorTemplate> {
    let r = fields::template_ref_of(actor);
    match store.get_actor_template(&r).await {
        Ok(t) => Ok(t),
        Err(StoreError::NotFound) => Err(status::failed_precondition(format!(
            "{TEMPLATE_NOT_FOUND}; ObjectRef: {}/{}",
            r.hive, r.name
        ))),
        Err(e) => Err(Error::from(e).context("while getting ActorTemplate")),
    }
}

const TEMPLATE_NOT_FOUND: &str = "actor template not found";

/// Whether `e` came from [`resolve_template`] not finding the template.
pub fn is_template_not_found(e: &Error) -> bool {
    matches!(e, Error::Status(s) if s.code() == tonic::Code::FailedPrecondition && s.message().starts_with(TEMPLATE_NOT_FOUND))
}
