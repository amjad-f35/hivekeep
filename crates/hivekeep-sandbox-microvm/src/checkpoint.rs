//! CheckpointWorkload and TerminateWorkload, and tearing a VM down.
//!
//! A checkpoint pauses the guest (which makes the host-backed tars coherent:
//! the share is write-through, so every completed write is on the host and
//! no more can arrive), captures what the fidelity asks for concurrently,
//! and tears the VM down. Each level includes the ones below:
//!
//! - MEMORY: the VMM snapshot (config, state, sparse memory) plus base-id,
//!   and the tars below.
//! - ROOTFS: each container's rootfs upper, as a tar; the lower is rebuilt
//!   from the image at restore and never ships.
//! - VOLUMES: the durable-dir volumes, one tar each.
//!
//! The keeper then uploads the checkpoint dir and resets the Actor's dirs.
//! Checkpoints are allowed while the worker drains, so Actors can suspend on
//! eviction.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hivekeep_core::resource::ActorAttribution;
use hivekeep_proto::worker::v1 as pb;
use hivekeep_sandbox_gvisor::actor_lock::Rpc;
use hivekeep_sandbox_gvisor::attribution::ActorRequest;
use hivekeep_sandbox_gvisor::checkpoint_record::{record_checkpoint, reuse_checkpoint};
use hivekeep_sandbox_gvisor::error::{status, Context, Error, Result};
use hivekeep_sandbox_gvisor::phaselog::{self, Phase, CHECKPOINT_DURATION_KEY};
use hivekeep_sandbox_gvisor::ports::lifecycle_log;
use hivekeep_sandbox_gvisor::{mounts, validation};
use tokio_util::sync::CancellationToken;

use crate::actor_path::rootfs_upper_dir;
use crate::ch::{merge, Client};
use crate::fidelity::{includes_rootfs, validate_fidelity};
use crate::kata::paths::clh_socket_path;
use crate::phases;
use crate::rootfs_upper::{container_names, tar_rootfs_upper};
use crate::service::{RunningActor, WorkerService};
use crate::snapshot_config::BASE_ID_FILE;
use crate::snapshot_files::MEMORY_RANGES_FILE;
use crate::volumes::{
    durable_volume_names, has_csi_volumes, has_durable_volumes, tar_durable_volumes,
};

/// Merges the on-demand delta in `checkpoint_dir` with the base in
/// `restore_source_dir`. A preserved base is copied, never modified.
pub fn merge_on_demand_delta(
    restore_source_dir: &Path,
    checkpoint_dir: &Path,
    preserve: bool,
    cancel: &CancellationToken,
) -> std::io::Result<()> {
    let base = restore_source_dir.join(MEMORY_RANGES_FILE);
    let delta = checkpoint_dir.join(MEMORY_RANGES_FILE);
    if preserve {
        merge::merge_sparse_overlay(&base, &delta, &delta, cancel)
    } else {
        merge::merge_delta_into_base(&base, &delta, cancel)
    }
}

/// The names of the regular files directly in `dir`, sorted.
pub fn list_files(dir: &Path) -> std::io::Result<Vec<String>> {
    let mut files: Vec<String> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    files.sort();
    Ok(files)
}

#[derive(Default)]
struct CheckpointTimes {
    prep: Duration,
    pause: Duration,
    snapshot: Duration,
    durable: Duration,
    upper: Duration,
    teardown: Duration,
}

impl WorkerService {
    /// CheckpointWorkload. Not tied to the caller: it may already have
    /// stopped the workload when the caller goes away.
    pub async fn checkpoint_workload(
        self: &Arc<Self>,
        req: pb::CheckpointWorkloadRequest,
        cancel: CancellationToken,
    ) -> std::result::Result<pb::CheckpointWorkloadResponse, tonic::Status> {
        validation::into_status(validation::checkpoint_workload_request(&req))?;
        validate_fidelity(req.fidelity)?;
        let uid = req.actor_uid.clone();
        let _lock = self.locks.lock(&uid).await;
        let dirs = req.actor_dirs.clone().unwrap_or_default();
        let checkpoint_dir = PathBuf::from(&dirs.checkpoint_dir);
        if self.lookup_actor(&uid).is_none() {
            if let Some(resp) = reuse_checkpoint(&checkpoint_dir, req.fidelity) {
                tracing::info!(actor_uid = %uid, "Reusing completed checkpoint from previous attempt");
                return Ok(resp);
            }
        }
        let _rpc = self.in_flight.add(&uid, Rpc::Checkpoint, None);
        let attribution = req.attribution();
        let t_start = Instant::now();
        let mut times = CheckpointTimes::default();
        let res = self
            .checkpoint_inner(
                &req,
                &attribution,
                &dirs,
                &checkpoint_dir,
                &mut times,
                &cancel,
            )
            .await;
        let ph = |name: &'static str, d: Duration| Phase { name, d };
        phaselog::log_record(
            "Checkpoint timing breakdown",
            &phaselog::snapshot_phase_record(
                &attribution,
                req.fidelity,
                CHECKPOINT_DURATION_KEY,
                res.as_ref().err().map(Error::code),
                &[
                    ph(phases::PREP, times.prep),
                    ph(phases::PAUSE, times.pause),
                    ph(phases::SNAPSHOT, times.snapshot),
                    ph(phases::DURABLE_DIR, times.durable),
                    ph(phases::ROOTFS_UPPER, times.upper),
                    ph(phases::TEARDOWN, times.teardown),
                    ph(phases::TOTAL, t_start.elapsed()),
                ],
            ),
        );
        let resp = res?;
        if let Err(e) = record_checkpoint(&checkpoint_dir, req.fidelity, &resp) {
            tracing::warn!(actor_uid = %uid, err = %e, "Failed to record checkpoint completion marker");
        }
        Ok(resp)
    }

    async fn checkpoint_inner(
        self: &Arc<Self>,
        req: &pb::CheckpointWorkloadRequest,
        attribution: &ActorAttribution,
        dirs: &pb::ActorDirs,
        checkpoint_dir: &Path,
        times: &mut CheckpointTimes,
        cancel: &CancellationToken,
    ) -> Result<pb::CheckpointWorkloadResponse> {
        let t_start = Instant::now();
        let uid = attribution.uid.as_str();
        self.cfg.tunnel.deactivate(attribution).await?;
        lifecycle_log("Actor checkpointing", attribution);
        // Checked before the guest is touched: pausing first would leave the
        // Actor suspended for a call that could never succeed.
        let containers = req.spec.clone().unwrap_or_default().containers;
        let durable = has_durable_volumes(&containers);
        let fidelity = req.fidelity;
        match pb::SnapshotFidelity::try_from(fidelity) {
            Ok(pb::SnapshotFidelity::Memory | pb::SnapshotFidelity::Rootfs) => {}
            Ok(pb::SnapshotFidelity::Volumes) => {
                // CSI volumes are not captured; a VOLUMES snapshot of only
                // CSI volumes is accepted and holds nothing of them.
                if !durable && !has_csi_volumes(&containers) {
                    return Err(status::failed_precondition(
                        "no durable-dir or CSI volumes found for a VOLUMES snapshot",
                    )
                    .into());
                }
            }
            _ => {
                return Err(tonic::Status::invalid_argument(format!(
                    "unsupported snapshot fidelity: {fidelity}"
                ))
                .into())
            }
        }
        // Captured now: the checkpoint unhosts the Actor, and the final
        // record waits for the teardown.
        let hosted = self.lookup_actor(uid);
        let ra = self.running_vm(uid);
        let socket = ra
            .as_ref()
            .filter(|r| !r.api_socket.as_os_str().is_empty())
            .map_or_else(|| clh_socket_path(uid), |r| r.api_socket.clone());
        let client = Client::new(socket);
        client
            .wait_ready(Duration::from_secs(10), cancel)
            .await
            .context("while waiting for CH api-socket")?;
        // A paused guest cannot answer.
        if let Some(h) = &hosted {
            self.read_final(h).await;
        }
        let t_pause = Instant::now();
        times.prep = t_pause - t_start;
        let paused = client.pause().await;
        times.pause = t_pause.elapsed();
        paused.context("while pausing guest")?;
        match std::fs::remove_dir_all(checkpoint_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(Error::io(
                    format!("while clearing checkpoint dir {}", checkpoint_dir.display()),
                    e,
                ))
            }
        }
        {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(checkpoint_dir)
                .with_context(|| {
                    format!("while creating checkpoint dir {}", checkpoint_dir.display())
                })?;
        }
        // The pieces are captured concurrently: they read independent data
        // from the quiesced guest into distinct files, so the paused window
        // costs the slowest rather than the sum.
        let snap = async {
            if fidelity != pb::SnapshotFidelity::Memory as i32 {
                return Ok(Duration::ZERO);
            }
            let t = Instant::now();
            self.snapshot_vm_state(&client, ra.as_deref(), uid, checkpoint_dir, cancel)
                .await
                .map_err(|e| (e, t.elapsed()))
        };
        let durable_tars = async {
            if !durable {
                return (Ok(vec![]), Duration::ZERO);
            }
            let t = Instant::now();
            let (d, c, v, cl) = (
                PathBuf::from(&dirs.durable_dir_volume_mounts_dir),
                checkpoint_dir.to_path_buf(),
                durable_volume_names(&containers),
                cancel.clone(),
            );
            let r = tokio::task::spawn_blocking(move || tar_durable_volumes(&d, &c, &v, &cl))
                .await
                .map_err(|e| Error::msg(e.to_string()))
                .and_then(|r| r.map_err(|e| Error::io("archiving durable-dir volumes", e)));
            (r, t.elapsed())
        };
        let upper_tars = async {
            if !includes_rootfs(fidelity) {
                return (Ok(()), Duration::ZERO);
            }
            let t = Instant::now();
            let (u, c, n, cl) = (
                rootfs_upper_dir(dirs),
                checkpoint_dir.to_path_buf(),
                container_names(&containers),
                cancel.clone(),
            );
            let r = tokio::task::spawn_blocking(move || tar_rootfs_upper(&u, &c, &n, &cl))
                .await
                .map_err(|e| Error::msg(e.to_string()))
                .and_then(|r| r.map_err(|e| Error::io("archiving rootfs uppers", e)));
            (r, t.elapsed())
        };
        let (snap, (durable_files, d_durable), (upper, d_upper)) =
            tokio::join!(snap, durable_tars, upper_tars);
        times.durable = d_durable;
        times.upper = d_upper;
        match snap {
            Ok(d) => times.snapshot = d,
            Err((e, d)) => {
                times.snapshot = d;
                return Err(e);
            }
        }
        let durable_files = durable_files?;
        upper?;
        // Exactly the files written, so the keeper ships precisely this
        // snapshot.
        let snapshot_files = list_files(checkpoint_dir).context("while listing snapshot files")?;
        // The Actor returns to available. Best effort: the snapshot is on
        // disk for the keeper to ship.
        let t_teardown = Instant::now();
        if let Err(e) = self.terminate(attribution, dirs).await {
            tracing::warn!(actor_uid = uid, err = %e, "failed to terminate workload after checkpoint");
        }
        times.teardown = t_teardown.elapsed();
        lifecycle_log("Actor checkpointed", attribution);
        tracing::info!(id = uid, snapshot_files = ?snapshot_files, fidelity, pause = ?times.pause, snapshot = ?times.snapshot, durable_dir = ?times.durable, rootfs_upper = ?times.upper, teardown = ?times.teardown, "Actor checkpointed");
        self.record_final_if_ended(hosted.as_ref());
        Ok(pb::CheckpointWorkloadResponse {
            snapshot_files,
            data_snapshot_files: durable_files,
        })
    }

    /// Writes the paused guest into `checkpoint_dir`: the base id, the VMM
    /// snapshot, and for an on-demand-restored guest the merge that makes
    /// the sparse snapshot complete. Returns how long the snapshot took.
    async fn snapshot_vm_state(
        &self,
        client: &Client,
        ra: Option<&RunningActor>,
        uid: &str,
        checkpoint_dir: &Path,
        cancel: &CancellationToken,
    ) -> Result<Duration> {
        // The frozen base id, not anything derived from the config, whose
        // socket paths are rewritten on every restore.
        let base_id = ra
            .map(|r| r.base_id.as_str())
            .filter(|b| !b.is_empty())
            .unwrap_or(uid);
        std::fs::write(checkpoint_dir.join(BASE_ID_FILE), base_id)
            .with_context(|| format!("while writing {BASE_ID_FILE}"))?;
        tracing::info!(id = uid, dir = %checkpoint_dir.display(), "Snapshotting guest");
        let t = Instant::now();
        client
            .snapshot(checkpoint_dir)
            .await
            .context("while snapshotting guest")?;
        let d = t.elapsed();
        match ra {
            // An eager restore pulled everything into guest memory, so the
            // snapshot is the whole guest, not a delta.
            Some(r) if r.snapshot_is_self_contained => {
                tracing::info!(
                    id = uid,
                    "Snapshot is self-contained (eager restore); skipping merge"
                );
            }
            Some(r) => {
                if let Some(src) = &r.restore_source_dir {
                    let t = Instant::now();
                    let (s, c, pr, cl) = (
                        src.clone(),
                        checkpoint_dir.to_path_buf(),
                        r.preserve_restore_source,
                        cancel.clone(),
                    );
                    tokio::task::spawn_blocking(move || merge_on_demand_delta(&s, &c, pr, &cl))
                        .await
                        .map_err(|e| Error::msg(e.to_string()))?
                        .context("while merging OnDemand delta into restore source")?;
                    tracing::info!(id = uid, merge = ?t.elapsed(), "Merged OnDemand delta into base (complete snapshot)");
                }
            }
            None => {}
        }
        Ok(d)
    }

    /// Stops the VM of `id` (`ra` may be gone, e.g. after a worker restart)
    /// and clears its host state.
    pub(crate) async fn teardown_actor(
        &self,
        id: &str,
        dirs: Option<&pb::ActorDirs>,
        ra: Option<Arc<RunningActor>>,
    ) -> Result<()> {
        // Stop offering the guest to stats first, so a poll mid-teardown
        // gets "no numbers right now" rather than a closed connection.
        self.set_guest_stats(id, None);
        if let Some(ra) = &ra {
            ra.stopping.store(true, Ordering::SeqCst);
            // Closing the agent ends the log forwarders reading over it.
            if let Some(ac) = ra.guest_agent.lock().take() {
                ac.close();
            }
            if let Some(p) = &ra.ch_proc {
                p.stop().await;
            }
            // virtiofsd after the VMM, its only client.
            if let Some(p) = &ra.vfsd_proc {
                p.stop().await;
            }
        }
        // Also drops the merged overlays, which must go before the upper dir
        // they write to.
        self.cleanup_sandbox_state(id).await;
        let Some(dirs) = dirs else { return Ok(()) };
        match std::fs::remove_dir_all(rootfs_upper_dir(dirs)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!(actor_uid = id, err = %e, "Failed to remove rootfs upper dir"),
        }
        // So the keeper's bundle wipe does not strand live mounts here.
        match mounts::unmount_all_under(Path::new(&dirs.oci_bundle_dir)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::io("while unmounting bundle rootfs overlays", e)),
        }
    }

    /// TerminateWorkload.
    pub async fn terminate_workload(
        self: &Arc<Self>,
        req: pb::TerminateWorkloadRequest,
    ) -> std::result::Result<pb::TerminateWorkloadResponse, tonic::Status> {
        validation::into_status(validation::terminate_workload_request(&req))?;
        let _lock = self.locks.lock(&req.actor_uid).await;
        let attribution = req.attribution();
        let hosted = self.lookup_actor(&attribution.uid);
        if let Some(h) = &hosted {
            self.read_final(h).await;
        }
        let r = self
            .terminate(&attribution, &req.actor_dirs.clone().unwrap_or_default())
            .await;
        self.record_final_if_ended(hosted.as_ref());
        r.map_err(|e| Error::Context {
            context: "failed to terminate workload".into(),
            source: Box::new(e),
        })?;
        lifecycle_log("Actor terminated", &attribution);
        Ok(pb::TerminateWorkloadResponse {})
    }

    /// Ends the activation a re-host is about to replace: a VM still running
    /// for the Actor (after a checkpoint that failed before teardown, or a
    /// retried Run or Restore) is read for its final record and stopped.
    pub(crate) async fn end_previous_activation(
        &self,
        uid: &str,
        dirs: Option<&pb::ActorDirs>,
    ) -> Result<()> {
        let old = self.lookup_actor(uid);
        if let Some(ra) = self.running_vm(uid) {
            if let Some(h) = &old {
                self.read_final(h).await;
            }
            self.teardown_actor(uid, dirs, Some(ra))
                .await
                .context("while stopping the actor's previous micro-VM")?;
        }
        if let Some(h) = &old {
            self.record_final(h);
        }
        Ok(())
    }

    /// Deactivates networking, tears the VM down and unhosts the Actor.
    pub(crate) async fn terminate(&self, a: &ActorAttribution, dirs: &pb::ActorDirs) -> Result<()> {
        let mut errs = Vec::new();
        if let Err(e) = self.cfg.tunnel.deactivate(a).await {
            errs.push(Error::Context {
                context: "while deactivating actor networking".into(),
                source: Box::new(e),
            });
        }
        if let Err(e) = self
            .teardown_actor(&a.uid, Some(dirs), self.running_vm(&a.uid))
            .await
        {
            errs.push(Error::Context {
                context: "while tearing down actor".into(),
                source: Box::new(e),
            });
        }
        // After teardown: a failed checkpoint may leave the VM running.
        if let Err(e) = self.unhost_actor(&a.uid).await {
            errs.push(Error::Context {
                context: "while cleaning up actor network".into(),
                source: Box::new(e),
            });
        }
        Error::join(errs).map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: usize = 4096;

    fn write_pages(path: &Path, off: u64, data: &[u8]) {
        use std::os::unix::fs::FileExt;
        let f = std::fs::File::create(path).unwrap();
        f.set_len(2 * PAGE as u64).unwrap();
        f.write_all_at(data, off).unwrap();
    }

    // The merges rely on Linux hole reporting; see ch::merge.
    #[cfg(target_os = "linux")]
    #[test]
    fn merging_respects_a_preserved_source() {
        use std::os::unix::fs::MetadataExt;
        let base0 = vec![0xAA; PAGE];
        let delta1 = vec![0xBB; PAGE];
        let want: Vec<u8> = base0.iter().chain(delta1.iter()).copied().collect();
        for preserve in [true, false] {
            let (restore, ckpt) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let (base, delta) = (
                restore.path().join(MEMORY_RANGES_FILE),
                ckpt.path().join(MEMORY_RANGES_FILE),
            );
            write_pages(&base, 0, &base0);
            write_pages(&delta, PAGE as u64, &delta1);
            merge_on_demand_delta(
                restore.path(),
                ckpt.path(),
                preserve,
                &CancellationToken::new(),
            )
            .unwrap();
            assert!(
                std::fs::read(&delta).unwrap() == want,
                "preserve={preserve}: merged image differs"
            );
            if preserve {
                let mut want_base = base0.clone();
                want_base.extend(vec![0; PAGE]);
                assert!(
                    std::fs::read(&base).unwrap() == want_base,
                    "the preserved base was mutated"
                );
            } else {
                assert_eq!(
                    std::fs::metadata(&base).unwrap().ino(),
                    std::fs::metadata(&delta).unwrap().ino()
                );
            }
        }
    }

    #[test]
    fn lists_regular_files() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("b"), "").unwrap();
        std::fs::write(d.path().join("a"), "").unwrap();
        std::fs::create_dir(d.path().join("dir")).unwrap();
        assert_eq!(list_files(d.path()).unwrap(), ["a", "b"]);
        let _ = write_pages;
    }
}
