//! PromoteSnapshot and DetachActor.

use std::time::Instant;

use hivekeep_core::resource::ActorRef;
use hivekeep_core::snapshot::SnapshotUri;
use hivekeep_core::validation::keeper::{
    validate_detach_actor_request, validate_promote_snapshot_request,
};
use hivekeep_proto::keeper::v1 as pb;
use tonic::Status;

use super::{invalid, Keeper};
use crate::context::Ctx;
use crate::keeper_metrics::{Phase, SnapshotOp};
use crate::labels::{fidelity_label, kind, phase};
use crate::snapshot_tracker::SnapshotRef;
use crate::worker_jwt::TransferActor;

impl Keeper {
    /// Uploads a suspended actor's local snapshot to object storage,
    /// keeping the local copy (as for creating a tag). It drives no worker:
    /// the sandbox is gone and the snapshot is self-describing.
    pub async fn promote_snapshot(
        &self,
        ctx: &Ctx,
        req: pb::PromoteSnapshotRequest,
    ) -> Result<pb::PromoteSnapshotResponse, Status> {
        invalid(validate_promote_snapshot_request(&self.d.validation, &req))?;
        let started = Instant::now();
        let snapshot = req.snapshot.clone().unwrap_or_default();
        let mut op = SnapshotOp {
            template_hive: req.actor_template_hive.clone(),
            template_name: req.actor_template_name.clone(),
            // Uploading a local checkpoint creates a durable latest snapshot.
            kind: kind::LATEST.into(),
            fidelity: fidelity_label(snapshot.fidelity).into(),
            sandbox_class: String::new(),
        };
        let mut persist = std::time::Duration::ZERO;
        let res = async {
            let uri = SnapshotUri::parse(
                snapshot
                    .object
                    .as_ref()
                    .map(|o| o.snapshot_uri.as_str())
                    .unwrap_or(""),
            )
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let _lock = self.lock_actor(ctx, &req.actor_uid).await?;
            let p = Instant::now();
            let r = self
                .promote_local_snapshot(
                    ctx,
                    &req.hive,
                    &req.actor_name,
                    &req.actor_uid,
                    &snapshot,
                    &uri,
                )
                .await;
            persist = p.elapsed();
            let (state, class) = r?;
            op.sandbox_class = class;
            Ok(pb::PromoteSnapshotResponse { state: Some(state) })
        }
        .await;
        if let Some(i) = &self.d.instruments {
            i.record_checkpoint(
                &op,
                &[
                    Phase(phase::PERSIST, persist),
                    Phase(phase::TOTAL, started.elapsed()),
                ],
            );
        }
        res
    }

    /// Makes the local snapshot `snapshot` durable at `uri` unless it is
    /// already, and records it as the latest durable snapshot. Returns the
    /// actor's state and the sandbox class the manifest names, if it
    /// uploaded.
    pub(super) async fn promote_local_snapshot(
        &self,
        ctx: &Ctx,
        hive: &str,
        name: &str,
        actor_uid: &str,
        snapshot: &pb::Snapshot,
        uri: &SnapshotUri,
    ) -> Result<(pb::ActorSnapshotState, String), Status> {
        let local_dir = self
            .d
            .paths
            .local_snapshot_dir(actor_uid, &snapshot.snapshot_uuid);
        let mut snap = SnapshotRef::new(snapshot.clone(), ActorRef::new(hive, name));
        // A detach or upload of this snapshot in progress finishes first,
        // and no detach starts until the promote ends.
        let (already, end) = self
            .d
            .snapshots
            .begin_promote(ctx, actor_uid, &snap)
            .await?;
        let mut class = String::new();
        if !already {
            let actor = TransferActor {
                hive: hive.into(),
                name: name.into(),
                uid: actor_uid.into(),
            };
            match self
                .d
                .transfer
                .upload_local_checkpoint_dir(ctx, &actor, snapshot, &local_dir, uri)
                .await
            {
                Ok((c, uploaded)) => {
                    class = c;
                    // The upload time is known only when this call wrote
                    // the manifest.
                    if uploaded {
                        snap.upload_time = Some(chrono::Utc::now());
                    }
                }
                Err(e) => {
                    end.end().await;
                    return Err(e);
                }
            }
        }
        let state = self.d.snapshots.commit_promoted(actor_uid, snap).await;
        end.end().await;
        Ok((state, class))
    }

    /// Makes the actor's latest local snapshot durable, then deletes its
    /// local snapshots on this node.
    pub async fn detach_actor(
        &self,
        ctx: &Ctx,
        req: pb::DetachActorRequest,
    ) -> Result<pb::DetachActorResponse, Status> {
        invalid(validate_detach_actor_request(&self.d.validation, &req))?;
        let _lock = self.lock_actor(ctx, &req.actor_uid).await?;
        let state = self.d.snapshots.detach(ctx, &req.actor_uid).await?;
        Ok(pb::DetachActorResponse { state: Some(state) })
    }
}
