//! Run: boot a fresh workload.

use std::collections::BTreeMap;
use std::path::Path;

use hivekeep_core::resource::ActorRef;
use hivekeep_core::validation::keeper::validate_run_request;
use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::worker::v1 as wpb;
use tonic::Status;

use super::spec::{
    build_worker_workload_spec, prepare_oci_bundles, to_worker_egress_gateway, to_worker_resources,
};
use super::{invalid, Keeper};
use crate::actordirs;
use crate::assets::{runsc_path_for, write_sandbox_record};
use crate::context::Ctx;
use crate::paths::is_local;
use crate::status::fs_err;
use crate::system_info::system_info_volumes_for;
use crate::worker_dialer::worker_call_error;

impl Keeper {
    /// Boots a fresh workload for an actor on a worker.
    pub async fn run(&self, ctx: &Ctx, req: pb::RunRequest) -> Result<pb::RunResponse, Status> {
        invalid(validate_run_request(&self.d.validation, &req))?;
        // Resolved before any work on the node, so an invalid request
        // changes nothing.
        let rec = self
            .d
            .runtimes
            .advertised_runtime(req.sandbox_runtime.as_ref())?;
        let _lock = self.lock_actor(ctx, &req.actor_uid).await?;
        let end = self.d.snapshots.begin_start(ctx, &req.actor_uid).await?;
        let res = self.run_started(ctx, &req, &rec).await;
        end.end().await;
        res
    }

    async fn run_started(
        &self,
        ctx: &Ctx,
        req: &pb::RunRequest,
        rec: &crate::assets::SandboxAssetsRecord,
    ) -> Result<pb::RunResponse, Status> {
        let uid = req.actor_uid.as_str();
        let paths = &self.d.paths;
        // Fetched into a staging dir before the reset, so a failed fetch
        // leaves a live activation as it was. The staged set is swapped in
        // after the reset together with the record describing it.
        let staging = paths.sandbox_assets_staging_dir(uid);
        actordirs::remove_all(&staging)
            .map_err(|e| fs_err("while removing a stale sandbox assets staging dir", &e))?;
        let mut asset_paths = match self.d.assets.ensure(ctx, rec, &staging).await {
            Ok(p) => p,
            Err(e) => {
                let _ = actordirs::remove_all(&staging);
                return Err(e);
            }
        };
        let mut committed = false;
        let mut registration = None;
        let res = async {
            self.d.system_info.deregister(uid);
            actordirs::reset_actor_dirs_keeping_volumes(paths, uid)
                .map_err(|e| Status::cancelled(format!("while resetting actor dirs: {e}")))?;
            let spec = req.spec.as_ref();
            self.d
                .volumes
                .mount_external_volumes(ctx, uid, spec.map(|s| s.volumes.as_slice()).unwrap_or(&[]))
                .await?;
            // Record the binaries this actor runs so a Checkpoint, whose
            // request no longer names them, stages and pins the same ones.
            commit_sandbox_assets(&paths.sandbox_assets_dir(uid), &staging, &mut asset_paths)?;
            committed = true;
            write_sandbox_record(paths, uid, rec)
                .map_err(|e| crate::status::wrap("while recording sandbox assets", e))?;
            let actor = ActorRef::new(req.hive.clone(), req.actor_name.clone());
            registration = Some(self.d.system_info.register(
                uid,
                &actor,
                system_info_volumes_for(paths, uid, spec),
            )?);
            let container_specs =
                prepare_oci_bundles(ctx, self.d.images.as_ref(), paths, uid, spec).await?;
            let client = self.d.workers.connect(&req.worker_pod_uid).await?;
            let wspec = build_worker_workload_spec(spec, Some(&container_specs))
                .map_err(|e| Status::invalid_argument(format!("invalid workload spec: {e}")))?;
            let resp = client
                .run_workload(
                    ctx,
                    wpb::RunWorkloadRequest {
                        hive: req.hive.clone(),
                        actor_name: req.actor_name.clone(),
                        actor_uid: uid.to_string(),
                        actor_template_hive: req.actor_template_hive.clone(),
                        actor_template_name: req.actor_template_name.clone(),
                        runsc_path: runsc_path_for(&asset_paths),
                        spec: Some(wspec),
                        runtime_asset_paths: asset_paths.clone().into_iter().collect(),
                        egress_gateway: to_worker_egress_gateway(req.egress_gateway.as_ref()),
                        resources: to_worker_resources(req.resources.as_ref()),
                        actor_dirs: Some(paths.actor_dirs(uid)),
                    },
                )
                .await
                .map_err(|e| worker_call_error("RunWorkload", e))?;
            Ok(pb::RunResponse {
                worker_epoch: resp.worker_epoch,
            })
        }
        .await;
        if res.is_err() {
            let _ = actordirs::remove_all(&staging);
            if committed {
                let _ = actordirs::remove_all(&paths.sandbox_assets_dir(uid));
            }
            if let Some(r) = &registration {
                self.d.system_info.deregister_owned(r);
            }
        }
        res
    }
}

/// Replaces the actor's assets dir with the set staged at `staging`, and
/// rewrites `paths`, which point under `staging`, to the final location.
/// Nothing moves until every path has been checked.
pub fn commit_sandbox_assets(
    final_dir: &Path,
    staging: &Path,
    paths: &mut BTreeMap<String, String>,
) -> Result<(), Status> {
    let mut moved = BTreeMap::new();
    for (name, p) in paths.iter() {
        let rel = Path::new(p)
            .strip_prefix(staging)
            .ok()
            .filter(|r| is_local(r))
            .ok_or_else(|| {
                Status::internal(format!(
                    "staged asset {name:?} at {p:?} is not under {}",
                    staging.display()
                ))
            })?;
        moved.insert(
            name.clone(),
            final_dir.join(rel).to_string_lossy().into_owned(),
        );
    }
    actordirs::remove_all(final_dir)
        .map_err(|e| fs_err("while removing previous sandbox assets dir", &e))?;
    std::fs::rename(staging, final_dir)
        .map_err(|e| fs_err("while placing sandbox assets dir", &e))?;
    *paths = moved;
    Ok(())
}

/// Empties the actor's assets dir for Restore to stage into. Its entries
/// are links into the asset cache, so this never touches cached bytes.
pub fn reset_sandbox_assets_dir(dir: &Path) -> Result<(), Status> {
    actordirs::reset_dir("sandbox assets dir", dir, actordirs::remove_all, 0o700)
        .map_err(Status::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_moves_staged_assets_and_rewrites_paths() {
        let d = tempfile::tempdir().unwrap();
        let staging = d.path().join("staging");
        let fin = d.path().join("final");
        std::fs::create_dir_all(staging.join("gvisor")).unwrap();
        std::fs::write(staging.join("gvisor/runsc"), b"x").unwrap();
        std::fs::create_dir_all(&fin).unwrap();
        std::fs::write(fin.join("old"), b"old").unwrap();
        let mut paths = BTreeMap::new();
        paths.insert(
            "gvisor".to_string(),
            staging.join("gvisor/runsc").to_string_lossy().into_owned(),
        );
        commit_sandbox_assets(&fin, &staging, &mut paths).unwrap();
        assert_eq!(paths["gvisor"], fin.join("gvisor/runsc").to_string_lossy());
        assert!(
            fin.join("gvisor/runsc").exists() && !fin.join("old").exists() && !staging.exists()
        );
    }

    #[test]
    fn commit_refuses_paths_outside_staging() {
        let d = tempfile::tempdir().unwrap();
        let staging = d.path().join("staging");
        std::fs::create_dir_all(&staging).unwrap();
        let mut paths = BTreeMap::new();
        paths.insert("x".to_string(), "/etc/passwd".to_string());
        assert!(commit_sandbox_assets(&d.path().join("final"), &staging, &mut paths).is_err());
        assert!(staging.exists(), "nothing moved");
    }
}
