//! Terminate: tear the workload down and reclaim the actor's node state.

use hivekeep_core::validation::keeper::validate_terminate_request;
use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::worker::v1 as wpb;
use tonic::{Code, Status};

use super::spec::build_worker_workload_spec;
use super::{invalid, Keeper};
use crate::actordirs;
use crate::assets::{read_sandbox_record, runsc_path_for};
use crate::context::Ctx;
use crate::snapshot_tracker::prune_local_checkpoints;

impl Keeper {
    /// Terminates any workload on the worker, unmounts external volumes,
    /// and removes the actor's directories and snapshots from the node.
    pub async fn terminate(
        &self,
        ctx: &Ctx,
        req: pb::TerminateRequest,
    ) -> Result<pb::TerminateResponse, Status> {
        invalid(validate_terminate_request(&self.d.validation, &req))?;
        let uid = req.actor_uid.as_str();
        let _lock = self.lock_actor(ctx, uid).await?;
        // No background work begins while it runs; Forget cancels what is
        // in progress.
        let end = self.d.snapshots.begin_terminate(uid).await;
        let res = self.terminate_held(ctx, &req).await;
        end.end().await;
        res
    }

    async fn terminate_held(
        &self,
        ctx: &Ctx,
        req: &pb::TerminateRequest,
    ) -> Result<pb::TerminateResponse, Status> {
        let uid = req.actor_uid.as_str();
        let who = format!("actor: {}/{}, actorUID: {uid}", req.hive, req.actor_name);
        self.terminate_workload_on_worker(ctx, req).await?;
        self.d.system_info.deregister(uid);
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
                    "failed to unmount external volumes during terminate ({who}): {}",
                    e.message()
                ))
            })?;
        // The actor is gone, so none of its local snapshots can be restored
        // again. Forget stops its background work before the prune.
        self.d.snapshots.forget(uid).await.map_err(|e| {
            Status::internal(format!(
                "failed to delete snapshot state during terminate ({who}): {}",
                e.message()
            ))
        })?;
        prune_local_checkpoints(&self.d.paths, uid, &[]).map_err(|e| {
            Status::internal(format!(
                "failed to prune local checkpoints during terminate ({who}): {e}"
            ))
        })?;
        actordirs::remove_actor_dirs(&self.d.paths, uid).map_err(|e| {
            Status::internal(format!(
                "failed to remove actor directories during terminate ({who}): {e}"
            ))
        })?;
        self.reset_restore_failures(uid);
        Ok(pb::TerminateResponse {})
    }

    /// Tears the workload down on its worker. With no worker named, a worker
    /// whose socket is gone, or no sandbox record, there is nothing to tear
    /// down and the teardown is skipped; a worker that rejects the call
    /// fails it, since reclaiming a live sandbox's directories underneath
    /// it would be worse.
    async fn terminate_workload_on_worker(
        &self,
        ctx: &Ctx,
        req: &pb::TerminateRequest,
    ) -> Result<(), Status> {
        let uid = req.actor_uid.as_str();
        let pod = req.worker_pod_uid.as_str();
        let who = format!("actor: {}/{}, actorUID: {uid}", req.hive, req.actor_name);
        if pod.is_empty() {
            tracing::info!(actor_uid = uid, "no worker named during terminate; reclaiming the actor's node state without a sandbox teardown");
            return Ok(());
        }
        // The socket is the worker's liveness: it removes it on a graceful
        // exit, and the syncer removes the pod's directory with the pod.
        match std::fs::metadata(self.d.workers.socket_path(pod)) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!(actor_uid = uid, worker_pod_uid = pod, "worker is gone during terminate; reclaiming the actor's node state without a sandbox teardown");
                return Ok(());
            }
            Err(e) => {
                return Err(Status::internal(format!(
                    "failed to stat the worker socket during terminate ({who}): {e}"
                )))
            }
        }
        let rec = match read_sandbox_record(&self.d.paths, uid) {
            Ok(r) => r,
            Err(e) if e.code() == Code::NotFound => {
                // Run and Restore write it before starting the workload.
                tracing::info!(actor_uid = uid, "no sandbox record during terminate; reclaiming the actor's node state without a sandbox teardown");
                return Ok(());
            }
            Err(e) => {
                return Err(Status::internal(format!(
                    "failed to read sandbox record during terminate ({who}): {}",
                    e.message()
                )))
            }
        };
        let asset_paths = self
            .d
            .assets
            .ensure(ctx, &rec, &self.d.paths.sandbox_assets_dir(uid))
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "failed to ensure sandbox assets during terminate ({who}): {}",
                    e.message()
                ))
            })?;
        let client = self.d.workers.connect(pod).await.map_err(|e| {
            Status::internal(format!(
                "failed to dial worker for terminate ({who}): {}",
                e.message()
            ))
        })?;
        let wspec = build_worker_workload_spec(req.spec.as_ref(), None)
            .map_err(|e| Status::invalid_argument(format!("invalid workload spec: {e}")))?;
        let res = client
            .terminate_workload(
                ctx,
                wpb::TerminateWorkloadRequest {
                    hive: req.hive.clone(),
                    actor_name: req.actor_name.clone(),
                    actor_uid: uid.to_string(),
                    actor_template_hive: req.actor_template_hive.clone(),
                    actor_template_name: req.actor_template_name.clone(),
                    runsc_path: runsc_path_for(&asset_paths),
                    spec: Some(wspec),
                    actor_dirs: Some(self.d.paths.actor_dirs(uid)),
                },
            )
            .await;
        match res {
            Ok(()) => Ok(()),
            Err(e) if e.code() == Code::NotFound => {
                tracing::info!(
                    actor_uid = uid,
                    "workload not found on worker during terminate"
                );
                Ok(())
            }
            Err(e) if e.code() == Code::Unavailable => {
                // The socket outlived the process serving it.
                tracing::info!(actor_uid = uid, err = %e, "worker unreachable during terminate; reclaiming the actor's node state without a sandbox teardown");
                Ok(())
            }
            Err(e) => Err(Status::internal(format!(
                "failed calling worker TerminateWorkload ({who}): {}",
                e.message()
            ))),
        }
    }
}
