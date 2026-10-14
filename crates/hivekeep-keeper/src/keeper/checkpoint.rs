//! Checkpoint: capture the workload into a snapshot and return the worker
//! to blank.

use std::time::{Duration, Instant};

use hivekeep_core::resource::{ActorAttribution, ActorRef};
use hivekeep_core::snapshot::SnapshotUri;
use hivekeep_core::validation::keeper::validate_checkpoint_request;
use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::worker::v1 as wpb;
use tonic::Status;

use super::spec::{build_worker_workload_spec, to_worker_fidelity};
use super::{invalid, regular_file_under, Keeper};
use crate::actordirs;
use crate::assets::{
    read_sandbox_record, runsc_path_for, validate_data_snapshot_files, validate_snapshot_files,
    SandboxAssetsRecord, MANIFEST_NAME,
};
use crate::atomicfile::create_dir_all_mode;
use crate::context::Ctx;
use crate::keeper_metrics::{
    checkpoint_snapshot_kind, log_timing, Phase, SnapshotOp, CHECKPOINT_DURATION_KEY,
};
use crate::labels::{fidelity_label, phase};
use crate::snapshot_tracker::{prune_local_checkpoints, SnapshotRef};
use crate::sparsefile::allocated_bytes;
use crate::worker_jwt::TransferActor;

/// Per-phase timing of one checkpoint.
#[derive(Default)]
pub(super) struct CheckpointTiming {
    pub(super) assets: Duration,
    pub(super) worker: Duration,
    pub(super) persist: Duration,
    pub(super) cleanup: Duration,
}

impl Keeper {
    /// Captures an actor's workload into a snapshot under its upload mode,
    /// and returns the actor's snapshot state.
    pub async fn checkpoint(
        &self,
        ctx: &Ctx,
        req: pb::CheckpointRequest,
    ) -> Result<pb::CheckpointResponse, Status> {
        invalid(validate_checkpoint_request(&self.d.validation, &req))?;
        let started = Instant::now();
        let snapshot = req.snapshot.clone().unwrap_or_default();
        let mut op = SnapshotOp {
            template_hive: req.actor_template_hive.clone(),
            template_name: req.actor_template_name.clone(),
            kind: checkpoint_snapshot_kind(&req).into(),
            fidelity: fidelity_label(snapshot.fidelity).into(),
            sandbox_class: String::new(),
        };
        let mut t = CheckpointTiming::default();
        let mut committed = false;
        let res = self
            .checkpoint_locked(ctx, &req, &snapshot, &mut op, &mut t, &mut committed)
            .await;
        let phases = [
            Phase(phase::SANDBOX_ASSETS, t.assets),
            Phase(phase::WORKER_CHECKPOINT, t.worker),
            Phase(phase::PERSIST, t.persist),
            Phase(phase::CLEANUP, t.cleanup),
            Phase(phase::TOTAL, started.elapsed()),
        ];
        // A retry answered from the committed snapshot took no checkpoint.
        if !committed {
            if let Some(i) = &self.d.instruments {
                i.record_checkpoint(&op, &phases);
            }
        }
        let who = ActorAttribution {
            actor: ActorRef::new(req.hive.clone(), req.actor_name.clone()),
            uid: req.actor_uid.clone(),
            template_hive: req.actor_template_hive.clone(),
            template_name: req.actor_template_name.clone(),
        };
        log_timing(
            "Checkpoint timing breakdown",
            CHECKPOINT_DURATION_KEY,
            &who,
            &op,
            res.as_ref().err(),
            &phases,
        );
        res
    }

    async fn checkpoint_locked(
        &self,
        ctx: &Ctx,
        req: &pb::CheckpointRequest,
        snapshot: &pb::Snapshot,
        op: &mut SnapshotOp,
        t: &mut CheckpointTiming,
        committed_out: &mut bool,
    ) -> Result<pb::CheckpointResponse, Status> {
        let uid = req.actor_uid.as_str();
        let _lock = self.lock_actor(ctx, uid).await?;
        let mode =
            pb::UploadMode::try_from(req.upload_config.as_ref().map(|c| c.mode).unwrap_or(0))
                .unwrap_or_default();
        // A detach in progress finishes first and none starts until this
        // returns, so no detach deletes the snapshot being written.
        let (committed, end) = self
            .d
            .snapshots
            .begin_checkpoint(ctx, uid, snapshot, mode)
            .await?;
        *committed_out = committed;
        let res = if committed {
            self.retried_checkpoint(ctx, req, snapshot, op, t).await
        } else {
            // Once underway, the checkpoint no longer follows its caller: the
            // worker may already have stopped the workload, and a retry
            // waits on the lock for this attempt to commit. Keep the later
            // of the caller's deadline and the checkpoint timeout.
            let mut deadline = tokio::time::Instant::now() + self.checkpoint_timeout;
            if let Some(d) = ctx.deadline() {
                deadline = deadline.max(d);
            }
            let op_ctx = Ctx::background().with_deadline(deadline);
            self.take_checkpoint(&op_ctx, req, snapshot, mode, op, t)
                .await
        };
        end.end().await;
        res
    }

    /// Answers a retry of a Checkpoint that committed its snapshot (and so
    /// deleted the workload), finishing the cleanup it may have left undone
    /// unless the actor has started again since.
    async fn retried_checkpoint(
        &self,
        ctx: &Ctx,
        req: &pb::CheckpointRequest,
        snapshot: &pb::Snapshot,
        op: &mut SnapshotOp,
        t: &mut CheckpointTiming,
    ) -> Result<pb::CheckpointResponse, Status> {
        let uid = req.actor_uid.as_str();
        let mode = req.upload_config.as_ref().map(|c| c.mode).unwrap_or(0);
        // Under immediate upload, make the snapshot durable as a promote
        // would: the attempt that committed it may have deferred its upload.
        if mode == pb::UploadMode::Immediate as i32 {
            let uri = SnapshotUri::parse(
                snapshot
                    .object
                    .as_ref()
                    .map(|o| o.snapshot_uri.as_str())
                    .unwrap_or(""),
            )
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let started = Instant::now();
            let res = self
                .promote_local_snapshot(ctx, &req.hive, &req.actor_name, uid, snapshot, &uri)
                .await;
            t.persist = started.elapsed();
            let (_, class) = res?;
            op.sandbox_class = class;
        }
        let state = self.d.snapshots.committed_checkpoint_state(uid).await;
        if self.d.snapshots.not_started_since(uid, snapshot).await {
            let started = Instant::now();
            let res = self
                .clean_up_after_checkpoint(ctx, req, state.latest_local.is_some())
                .await;
            t.cleanup = started.elapsed();
            res?;
        }
        Ok(pb::CheckpointResponse { state: Some(state) })
    }

    async fn take_checkpoint(
        &self,
        ctx: &Ctx,
        req: &pb::CheckpointRequest,
        snapshot: &pb::Snapshot,
        mode: pb::UploadMode,
        op: &mut SnapshotOp,
        t: &mut CheckpointTiming,
    ) -> Result<pb::CheckpointResponse, Status> {
        let uid = req.actor_uid.as_str();
        let paths = &self.d.paths;
        // The request no longer names the runtime: recover it from the
        // record, re-stage it (a cache hit), and pin it in the manifest.
        let mut rec = read_sandbox_record(paths, uid)?;
        op.sandbox_class = rec.sandbox_class.clone();
        let started = Instant::now();
        let assets = self
            .d
            .assets
            .ensure(ctx, &rec, &paths.sandbox_assets_dir(uid))
            .await;
        t.assets = started.elapsed();
        let asset_paths = assets?;

        let client = self.d.workers.connect(&req.worker_pod_uid).await?;
        let wspec = build_worker_workload_spec(req.spec.as_ref(), None)
            .map_err(|e| Status::invalid_argument(format!("invalid workload spec: {e}")))?;
        let started = Instant::now();
        let resp = client
            .checkpoint_workload(
                ctx,
                wpb::CheckpointWorkloadRequest {
                    hive: req.hive.clone(),
                    actor_name: req.actor_name.clone(),
                    actor_uid: uid.to_string(),
                    actor_template_hive: req.actor_template_hive.clone(),
                    actor_template_name: req.actor_template_name.clone(),
                    runsc_path: runsc_path_for(&asset_paths),
                    spec: Some(wspec),
                    fidelity: to_worker_fidelity(snapshot.fidelity),
                    actor_dirs: Some(paths.actor_dirs(uid)),
                },
            )
            .await;
        t.worker = started.elapsed();
        let resp = resp.map_err(|e| {
            let msg = format!("while calling worker CheckpointWorkload: {}", e.message());
            // A checkpoint cut short by its own deadline reports that, so the
            // control plane can tell a timeout from a worker failure.
            match ctx.err() {
                Some(c) => Status::new(Status::from(c).code(), msg),
                None => Status::internal(msg),
            }
        })?;
        // The snapshot commits on this node once the worker has written it.
        let snap = SnapshotRef {
            snapshot: snapshot.clone(),
            commit_time: Some(chrono::Utc::now()),
            upload_time: None,
            actor: ActorRef::new(req.hive.clone(), req.actor_name.clone()),
        };
        self.d.system_info.deregister(uid);
        let (files, data_files) = checkpoint_snapshot_files(&resp, should_have_snapshots(req))?;
        rec.snapshot_files = files;
        rec.data_snapshot_files = data_files;
        rec.hive = req.hive.clone();
        rec.actor_name = req.actor_name.clone();
        rec.actor_uid = uid.to_string();
        rec.actor_template_hive = req.actor_template_hive.clone();
        rec.actor_template_name = req.actor_template_name.clone();
        rec.fidelity = fidelity_label(snapshot.fidelity).to_string();

        let started = Instant::now();
        let state = match mode {
            pb::UploadMode::Deferred => {
                let r = self.move_local_checkpoint(req, &rec);
                if let Err(e) = r {
                    t.persist = started.elapsed();
                    return Err(Status::internal(format!(
                        "while moving to local snapshot: {e}"
                    )));
                }
                self.d.snapshots.commit_local(uid, snap).await
            }
            pb::UploadMode::Immediate => {
                // The workload is gone, so a retry cannot redo this upload:
                // it waits for the store as long as the context allows, and
                // a failure reaches the control plane as Internal, which
                // crashes the actor.
                if let Err(e) = self
                    .upload_external_checkpoint(ctx, req, snapshot, &rec)
                    .await
                {
                    t.persist = started.elapsed();
                    return Err(Status::internal(format!(
                        "while uploading external snapshot: {}",
                        e.message()
                    )));
                }
                let mut durable = snap.clone();
                durable.upload_time = Some(chrono::Utc::now());
                let mut state = self.d.snapshots.commit_durable(uid, durable).await;
                // Durable now, so the local copy only saves a download:
                // failing to keep it must not fail the checkpoint.
                match self.move_local_checkpoint(req, &rec) {
                    Err(err) => {
                        tracing::warn!(actor = %snap.actor, %err, "Failed to keep a local copy of the uploaded snapshot")
                    }
                    Ok(()) => state = self.d.snapshots.commit_local(uid, snap).await,
                }
                state
            }
            pb::UploadMode::Unspecified => {
                return Err(Status::invalid_argument(format!(
                    "unexpected upload mode: {mode:?}"
                )));
            }
        };
        t.persist = started.elapsed();
        let started = Instant::now();
        let res = self
            .clean_up_after_checkpoint(ctx, req, state.latest_local.is_some())
            .await;
        t.cleanup = started.elapsed();
        res?;
        Ok(pb::CheckpointResponse { state: Some(state) })
    }

    /// Finishes a Checkpoint that committed its snapshot: prunes the
    /// actor's other local snapshots, unmounts its external volumes, and
    /// empties its directories (or, with nothing kept locally, removes its
    /// files except the state file). Each step is idempotent.
    pub(super) async fn clean_up_after_checkpoint(
        &self,
        ctx: &Ctx,
        req: &pb::CheckpointRequest,
        keep_local: bool,
    ) -> Result<(), Status> {
        let uid = req.actor_uid.as_str();
        let paths = &self.d.paths;
        // Only now that this checkpoint committed its own: one failing
        // earlier leaves the previous local snapshot restorable. Best-
        // effort; the next checkpoint or terminate prunes again.
        let keep = self.d.snapshots.local_snapshots_to_keep(uid).await;
        if let Err(err) = prune_local_checkpoints(paths, uid, &keep) {
            tracing::warn!(actor = %format!("{}/{}", req.hive, req.actor_name), %err, "failed to prune superseded local checkpoints");
        }
        let volumes = req
            .spec
            .as_ref()
            .map(|s| s.volumes.as_slice())
            .unwrap_or(&[]);
        self.d
            .volumes
            .unmount_external_volumes(ctx, uid, volumes)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "while unmounting external volumes: {}",
                    e.message()
                ))
            })?;
        if keep_local {
            actordirs::reset_actor_dirs(paths, uid)
                .map_err(|e| Status::internal(format!("while resetting actor dirs: {e}")))
        } else {
            actordirs::remove_actor_files_except_state(paths, uid)
                .map_err(|e| Status::internal(format!("while removing actor files: {e}")))
        }
    }

    /// Stages the reported files (linked, so the checkpoint dir keeps them
    /// for a retry) and the manifest, stamped with this boot, inside the
    /// checkpoint dir, then renames the stage into the local checkpoints
    /// dir, so a failure leaves nothing there.
    pub(super) fn move_local_checkpoint(
        &self,
        req: &pb::CheckpointRequest,
        rec: &SandboxAssetsRecord,
    ) -> Result<(), String> {
        let uid = req.actor_uid.as_str();
        let paths = &self.d.paths;
        let checkpoint_dir = paths.checkpoint_state_dir(uid);
        let local_dir = paths.local_snapshot_dir(
            uid,
            &req.snapshot
                .as_ref()
                .map(|s| s.snapshot_uuid.clone())
                .unwrap_or_default(),
        );
        let stage = checkpoint_dir.join(".staged");
        actordirs::remove_all(&stage)
            .map_err(|e| format!("while clearing local checkpoint staging directory: {e}"))?;
        create_dir_all_mode(&stage, 0o700)
            .map_err(|e| format!("while creating local checkpoint staging directory: {e}"))?;
        for f in &rec.snapshot_files {
            let meta = regular_file_under(&checkpoint_dir, f)
                .map_err(|e| format!("while inspecting checkpoint file {f}: {e}"))?;
            if let Some(i) = &self.d.instruments {
                i.record_snapshot_size(
                    f,
                    allocated_bytes(&meta),
                    &req.actor_template_hive,
                    &req.actor_template_name,
                );
            }
            let dst = stage.join(f);
            if let Some(parent) = dst.parent() {
                create_dir_all_mode(parent, 0o700).map_err(|e| {
                    format!("while creating parent directory for {}: {e}", dst.display())
                })?;
            }
            std::fs::hard_link(checkpoint_dir.join(f), &dst)
                .map_err(|e| format!("failed to link {f}: {e}"))?;
        }
        let mut local = rec.clone();
        local.boot_id = self.d.transfer.boot_id().to_string();
        std::fs::write(stage.join(MANIFEST_NAME), local.to_json())
            .map_err(|e| format!("while writing snapshot manifest: {e}"))?;
        crate::atomicfile::chmod(&stage.join(MANIFEST_NAME), 0o600).map_err(|e| e.to_string())?;
        if let Some(parent) = local_dir.parent() {
            create_dir_all_mode(parent, 0o700)
                .map_err(|e| format!("while creating local checkpoint directory: {e}"))?;
        }
        // An attempt that died after this rename, before the commit, left
        // the directory behind.
        actordirs::remove_all(&local_dir)
            .map_err(|e| format!("while removing uncommitted local snapshot: {e}"))?;
        std::fs::rename(&stage, &local_dir).map_err(|e| {
            format!(
                "failed to move {} to {}: {e}",
                stage.display(),
                local_dir.display()
            )
        })
    }

    /// Uploads the checkpoint just taken, from the checkpoint dir.
    async fn upload_external_checkpoint(
        &self,
        ctx: &Ctx,
        req: &pb::CheckpointRequest,
        snapshot: &pb::Snapshot,
        rec: &SandboxAssetsRecord,
    ) -> Result<(), Status> {
        let uri = SnapshotUri::parse(
            snapshot
                .object
                .as_ref()
                .map(|o| o.snapshot_uri.as_str())
                .unwrap_or(""),
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let actor = TransferActor {
            hive: req.hive.clone(),
            name: req.actor_name.clone(),
            uid: req.actor_uid.clone(),
        };
        self.d
            .transfer
            .upload_snapshot(
                ctx,
                &actor,
                &uri,
                &self.d.paths.checkpoint_state_dir(&req.actor_uid),
                rec,
            )
            .await
    }
}

/// Whether the checkpoint must produce files: anything above VOLUMES
/// fidelity does, and VOLUMES does when the workload has durable data.
fn should_have_snapshots(req: &pb::CheckpointRequest) -> bool {
    let fidelity = req.snapshot.as_ref().map(|s| s.fidelity).unwrap_or(0);
    if fidelity != pb::SnapshotFidelity::Volumes as i32 {
        return true;
    }
    req.spec
        .as_ref()
        .is_some_and(|s| s.volumes.iter().any(|v| v.durable_dir.is_some()))
}

/// The files the worker reported, validated.
fn checkpoint_snapshot_files(
    resp: &wpb::CheckpointWorkloadResponse,
    required: bool,
) -> Result<(Vec<String>, Vec<String>), Status> {
    let files = resp.snapshot_files.clone();
    if files.is_empty() && required {
        return Err(Status::internal(
            "worker reported no snapshot files for checkpoint",
        ));
    }
    validate_snapshot_files(&files)
        .map_err(|e| Status::internal(format!("worker reported invalid snapshot files: {e}")))?;
    let data = resp.data_snapshot_files.clone();
    validate_data_snapshot_files(&files, &data).map_err(|e| {
        Status::internal(format!("worker reported invalid data snapshot files: {e}"))
    })?;
    Ok((files, data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_files() {
        let mut req = pb::CheckpointRequest {
            snapshot: Some(pb::Snapshot {
                fidelity: pb::SnapshotFidelity::Volumes as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(!should_have_snapshots(&req));
        req.spec = Some(pb::WorkloadSpec {
            containers: vec![],
            volumes: vec![pb::Volume {
                name: "d".into(),
                durable_dir: Some(pb::DurableDirVolumeSource {}),
                ..Default::default()
            }],
        });
        assert!(should_have_snapshots(&req));
        req.spec = None;
        req.snapshot.as_mut().unwrap().fidelity = pb::SnapshotFidelity::Memory as i32;
        assert!(should_have_snapshots(&req));
    }

    #[test]
    fn validates_reported_files() {
        let mut resp = wpb::CheckpointWorkloadResponse::default();
        assert!(checkpoint_snapshot_files(&resp, true).is_err());
        assert!(checkpoint_snapshot_files(&resp, false).is_ok());
        resp.snapshot_files = vec!["mem".into(), MANIFEST_NAME.into()];
        assert!(checkpoint_snapshot_files(&resp, true).is_err());
        resp.snapshot_files = vec!["mem".into(), "fs/img".into()];
        resp.data_snapshot_files = vec!["fs/img".into()];
        assert!(checkpoint_snapshot_files(&resp, true).is_ok());
        resp.data_snapshot_files = vec!["other".into()];
        assert!(checkpoint_snapshot_files(&resp, true).is_err());
    }
}

#[cfg(test)]
#[path = "checkpoint_behavior_tests.rs"]
mod behavior_tests;
