//! Restore: boot a workload from a snapshot, from its local copy when one
//! is usable and from object storage otherwise.

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use hivekeep_core::resource::{ActorAttribution, ActorRef};
use hivekeep_core::validation::keeper::validate_restore_request;
use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::worker::v1 as wpb;
use parking_lot::Mutex;
use tonic::{Code, Status};

use super::run::reset_sandbox_assets_dir;
use super::spec::{
    build_worker_workload_spec, prepare_oci_bundles, to_worker_egress_gateway, to_worker_fidelity,
    to_worker_resources,
};
use super::{
    ends_restore_failure_streak, invalid, read_local_snapshot, Keeper, LocalSnapshotError,
};
use crate::actordirs;
use crate::assets::{runsc_path_for, write_sandbox_record, SandboxAssetsRecord};
use crate::context::Ctx;
use crate::keeper_metrics::{
    assets_after_collateral, log_timing, restore_snapshot_kind, Phase, SnapshotOp,
    RESTORE_DURATION_KEY,
};
use crate::labels::{fidelity_label, phase};
use crate::snapshot_cache::{cache_mode_for, snapshot_cache_mode, CacheMode};
use crate::system_info::system_info_volumes_for;
use crate::worker_jwt::TransferActor;

/// Per-phase timing of one restore.
#[derive(Default)]
struct RestoreTiming {
    mount: Duration,
    manifest: Duration,
    assets: Duration,
    download: Duration,
    bundles: Duration,
    worker: Duration,
}

impl Keeper {
    /// Boots an actor's workload from a snapshot.
    pub async fn restore(
        &self,
        ctx: &Ctx,
        req: pb::RestoreRequest,
    ) -> Result<pb::RestoreResponse, Status> {
        invalid(validate_restore_request(&self.d.validation, &req))?;
        // The binaries are the runtime the control plane assigned, resolved
        // before any work on the node; the manifest supplies only the files
        // and the identity.
        let runtime = self
            .d
            .runtimes
            .advertised_runtime(req.sandbox_runtime.as_ref())?;
        let started = Instant::now();
        let snapshot = req.snapshot.clone().unwrap_or_default();
        let mut op = SnapshotOp {
            template_hive: req.actor_template_hive.clone(),
            template_name: req.actor_template_name.clone(),
            kind: String::new(),
            fidelity: fidelity_label(snapshot.fidelity).into(),
            sandbox_class: req
                .sandbox_runtime
                .as_ref()
                .map(|r| r.sandbox_class.clone())
                .unwrap_or_default(),
        };
        let t = Mutex::new(RestoreTiming::default());
        let res = async {
            let _lock = self.lock_actor(ctx, &req.actor_uid).await?;
            let res = self
                .restore_locked(ctx, &req, &snapshot, &runtime, &mut op, &t)
                .await;
            // Ends the actor's streak of retryable failures while the lock
            // is still held.
            if ends_restore_failure_streak(&res) {
                self.reset_restore_failures(&req.actor_uid);
            }
            res
        }
        .await;
        let t = t.into_inner();
        let phases = [
            Phase(phase::VOLUME_MOUNT, t.mount),
            Phase(phase::MANIFEST_FETCH, t.manifest),
            Phase(phase::SANDBOX_ASSETS, t.assets),
            Phase(phase::DOWNLOAD, t.download),
            Phase(phase::OCI_UNPACK, t.bundles),
            Phase(phase::WORKER_RESTORE, t.worker),
            Phase(phase::TOTAL, started.elapsed()),
        ];
        if let Some(i) = &self.d.instruments {
            i.record_restore(&op, &phases);
        }
        let who = ActorAttribution {
            actor: ActorRef::new(req.hive.clone(), req.actor_name.clone()),
            uid: req.actor_uid.clone(),
            template_hive: req.actor_template_hive.clone(),
            template_name: req.actor_template_name.clone(),
        };
        log_timing(
            "Restore timing breakdown",
            RESTORE_DURATION_KEY,
            &who,
            &op,
            res.as_ref().err(),
            &phases,
        );
        res
    }

    async fn restore_locked(
        &self,
        ctx: &Ctx,
        req: &pb::RestoreRequest,
        snapshot: &pb::Snapshot,
        runtime: &SandboxAssetsRecord,
        op: &mut SnapshotOp,
        t: &Mutex<RestoreTiming>,
    ) -> Result<pb::RestoreResponse, Status> {
        let uid = req.actor_uid.as_str();
        // A detach in progress finishes first; if it ran, the local copy is
        // gone and the restore reads object storage.
        let end = self.d.snapshots.begin_start(ctx, uid).await?;
        let mut registration = None;
        let res = self
            .restore_started(ctx, req, snapshot, runtime, op, t, &mut registration)
            .await;
        if res.is_err() {
            // Until RestoreWorkload the record still describes the previous
            // activation, so none of this one's links may stay behind, nor
            // its staged files, which hold cache entries or disk.
            let _ = actordirs::remove_all(&self.d.paths.sandbox_assets_dir(uid));
            let _ = actordirs::remove_all(&self.d.paths.restore_state_dir(uid));
            if let Some(r) = &registration {
                self.d.system_info.deregister_owned(r);
            }
        }
        end.end().await;
        res
    }

    #[allow(clippy::too_many_arguments)]
    async fn restore_started(
        &self,
        ctx: &Ctx,
        req: &pb::RestoreRequest,
        snapshot: &pb::Snapshot,
        runtime: &SandboxAssetsRecord,
        op: &mut SnapshotOp,
        t: &Mutex<RestoreTiming>,
        registration: &mut Option<std::sync::Arc<crate::system_info::RegisteredActor>>,
    ) -> Result<pb::RestoreResponse, Status> {
        let uid = req.actor_uid.as_str();
        let paths = &self.d.paths;
        let actor_ref = ActorRef::new(req.hive.clone(), req.actor_name.clone());
        let actor = TransferActor {
            hive: req.hive.clone(),
            name: req.actor_name.clone(),
            uid: uid.to_string(),
        };
        // Canceled so the control plane retries rather than crashes the
        // actor: a failure here points at the node, not the actor.
        self.d.system_info.deregister(uid);
        actordirs::reset_actor_dirs_keeping_volumes(paths, uid)
            .map_err(|e| Status::cancelled(format!("while resetting actor dirs: {e}")))?;
        // Another runtime than the one staged before may come, so the
        // assets dir is emptied for it.
        reset_sandbox_assets_dir(&paths.sandbox_assets_dir(uid))?;

        let spec = req.spec.as_ref();
        let started = Instant::now();
        let mounted = self
            .d
            .volumes
            .mount_external_volumes(ctx, uid, spec.map(|s| s.volumes.as_slice()).unwrap_or(&[]))
            .await;
        t.lock().mount = started.elapsed();
        mounted?;

        // The manifest lists the files to stage; read the local copy first
        // and fall back to object storage when it is absent or unusable.
        let started = Instant::now();
        let local_dir = paths.local_snapshot_dir(uid, &snapshot.snapshot_uuid);
        let durable = snapshot.object.clone().unwrap_or_default();
        let (rec, direct_local) = match read_local_snapshot(&local_dir, self.d.transfer.boot_id()) {
            Ok(rec) => (rec, true),
            Err(local_err) => {
                let why = match &local_err {
                    LocalSnapshotError::Missing => "no local copy".to_string(),
                    LocalSnapshotError::Unusable(e) => {
                        // The next checkpoint or terminate prunes it.
                        tracing::warn!(local_dir = %local_dir.display(), err = %e, "Local snapshot is unusable; restoring from object storage");
                        format!("unusable local snapshot: {e}")
                    }
                };
                let manifest = self
                    .d
                    .downloader
                    .fetch_manifest(
                        ctx,
                        &actor,
                        &durable.snapshot_uri,
                        snapshot_cache_mode(Some(&durable), CacheMode::Link),
                    )
                    .await;
                let manifest = match manifest {
                    Ok(m) => m,
                    Err(e) if e.code() == Code::NotFound => {
                        t.lock().manifest = started.elapsed();
                        return Err(Status::not_found(format!(
                            "snapshot {:?} has no usable local copy and no uploaded copy: {why}",
                            snapshot.snapshot_uuid
                        )));
                    }
                    Err(e) => {
                        t.lock().manifest = started.elapsed();
                        return Err(crate::status::wrap("while fetching snapshot manifest", e));
                    }
                };
                let rec = SandboxAssetsRecord::parse(&manifest).map_err(|e| {
                    t.lock().manifest = started.elapsed();
                    crate::status::wrap("while unmarshalling sandbox record", e)
                })?;
                (rec, false)
            }
        };
        t.lock().manifest = started.elapsed();
        op.kind = restore_snapshot_kind(direct_local, Some(&rec)).into();
        let checkpoint_dir = if direct_local {
            local_dir.clone()
        } else {
            paths.restore_state_dir(uid)
        };

        // Download and prepare concurrently: only RestoreWorkload needs
        // both, so the shorter leg hides behind the longer. The first leg
        // to fail cancels the other.
        let group = ctx.child();
        let first_failed = AtomicU8::new(0);
        // The phase the prep leg failed in, so a cancel by the download's
        // failure drops only the measurement it cut short.
        let prep_failed_phase = Mutex::new("");
        let fail = |leg: u8, e: Status| {
            let _ = first_failed.compare_exchange(0, leg, Ordering::SeqCst, Ordering::SeqCst);
            group.cancel();
            e
        };
        let download = async {
            if direct_local {
                // Restore in place from the checked local snapshot.
                return Ok(());
            }
            let started = Instant::now();
            // Cache-eligible only when declared shared; how a hit is served
            // depends on the class running the restored workload.
            let mode = snapshot_cache_mode(Some(&durable), cache_mode_for(&runtime.sandbox_class));
            let r = self
                .d
                .downloader
                .download(
                    &group,
                    &actor,
                    &durable.snapshot_uri,
                    &checkpoint_dir,
                    &rec.snapshot_files,
                    mode,
                )
                .await;
            t.lock().download = started.elapsed();
            r.map_err(|e| fail(1, e))
        };
        // Recorded as soon as it exists, so a later failure of this leg
        // still removes the registration it made.
        let registered = Mutex::new(None);
        let prep = async {
            let started = Instant::now();
            let assets = self
                .d
                .assets
                .ensure(&group, runtime, &paths.sandbox_assets_dir(uid))
                .await;
            t.lock().assets = started.elapsed();
            let assets = assets.map_err(|e| {
                *prep_failed_phase.lock() = phase::SANDBOX_ASSETS;
                fail(2, e)
            })?;
            let reg = self
                .d
                .system_info
                .register(uid, &actor_ref, system_info_volumes_for(paths, uid, spec))
                .map_err(|e| {
                    *prep_failed_phase.lock() = phase::OCI_UNPACK;
                    fail(3, e)
                })?;
            *registered.lock() = Some(reg.clone());
            let started = Instant::now();
            let specs = prepare_oci_bundles(&group, self.d.images.as_ref(), paths, uid, spec).await;
            t.lock().bundles = started.elapsed();
            let specs = specs.map_err(|e| {
                *prep_failed_phase.lock() = phase::OCI_UNPACK;
                fail(3, e)
            })?;
            Ok::<_, Status>((assets, reg, specs))
        };
        let (dl, pr) = tokio::join!(download, prep);
        *registration = registered.into_inner();
        if dl.is_err() || pr.is_err() {
            // A leg cancelled by the other's failure measured nothing real.
            let first = first_failed.load(Ordering::SeqCst);
            let mut tm = t.lock();
            if dl.is_err() && first != 1 {
                tm.download = Duration::ZERO;
            }
            if pr.is_err() && first == 1 {
                // The assets phase finished before the cancel unless it was
                // the one cut short.
                tm.assets = assets_after_collateral(*prep_failed_phase.lock(), tm.assets);
                tm.bundles = Duration::ZERO;
            }
            drop(tm);
            return Err(match (dl, pr) {
                (Err(e), _) if first == 1 => e,
                (_, Err(e)) => e,
                (Err(e), _) => e,
                _ => unreachable!("one leg failed"),
            });
        }
        let (asset_paths, _, container_specs) = pr.expect("checked above");

        let client = self.d.workers.connect(&req.worker_pod_uid).await?;
        let wspec = build_worker_workload_spec(spec, Some(&container_specs))
            .map_err(|e| Status::invalid_argument(format!("invalid workload spec: {e}")))?;
        // Recorded before RestoreWorkload, as Run does, so a Checkpoint or
        // Terminate resolves the runtime once the sandbox starts.
        write_sandbox_record(paths, uid, runtime)?;
        let mut dirs = paths.actor_dirs(uid);
        dirs.restore_dir = checkpoint_dir.to_string_lossy().into_owned();
        let started = Instant::now();
        let resp = client
            .restore_workload(
                ctx,
                wpb::RestoreWorkloadRequest {
                    hive: req.hive.clone(),
                    actor_name: req.actor_name.clone(),
                    actor_uid: uid.to_string(),
                    actor_template_hive: req.actor_template_hive.clone(),
                    actor_template_name: req.actor_template_name.clone(),
                    runsc_path: runsc_path_for(&asset_paths),
                    spec: Some(wspec),
                    runtime_asset_paths: asset_paths.clone().into_iter().collect(),
                    fidelity: to_worker_fidelity(snapshot.fidelity),
                    egress_gateway: to_worker_egress_gateway(req.egress_gateway.as_ref()),
                    resources: to_worker_resources(req.resources.as_ref()),
                    actor_dirs: Some(dirs),
                    preserve_restore_dir: direct_local,
                },
            )
            .await;
        t.lock().worker = started.elapsed();
        match resp {
            Ok(r) => Ok(pb::RestoreResponse {
                worker_epoch: r.worker_epoch,
            }),
            Err(e) => Err(self.bound_retryable_restore_error(
                uid,
                &snapshot.snapshot_uuid,
                worker_restore_error(ctx, e),
            )),
        }
    }
}

/// The error a failed RestoreWorkload reports to the control plane. When
/// the keeper's own context ended, its reason. When the worker reports
/// Unavailable, the restore was not acted on or was torn down before
/// serving, so the control plane retries. Anything else crashes the actor.
pub fn worker_restore_error(ctx: &Ctx, err: Status) -> Status {
    if let Some(c) = ctx.err() {
        let s = Status::from(c);
        return Status::new(
            s.code(),
            format!(
                "while calling worker RestoreWorkload: {}: {c}",
                err.message()
            ),
        );
    }
    if err.code() == Code::Unavailable {
        return Status::unavailable(format!(
            "while calling worker RestoreWorkload: {}",
            err.message()
        ));
    }
    Status::internal(format!(
        "while calling worker RestoreWorkload: {}",
        err.message()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_errors() {
        let ctx = Ctx::background();
        assert_eq!(
            worker_restore_error(&ctx, Status::unavailable("x")).code(),
            Code::Unavailable
        );
        assert_eq!(
            worker_restore_error(&ctx, Status::cancelled("x")).code(),
            Code::Internal
        );
        ctx.cancel();
        assert_eq!(
            worker_restore_error(&ctx, Status::internal("x")).code(),
            Code::Cancelled
        );
    }
}
