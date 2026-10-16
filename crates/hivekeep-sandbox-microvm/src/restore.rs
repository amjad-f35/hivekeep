//! RestoreWorkload: bringing an Actor back from a snapshot, possibly on
//! another pod. What that means depends on the fidelity it was taken with:
//!
//! - MEMORY: relaunch the VMM from the snapshot and resume the guest.
//! - ROOTFS: as VOLUMES, with each container's rootfs upper re-materialized
//!   from its tar first, so the cold boot sees the Actor's rootfs writes.
//! - VOLUMES: no guest to resume; restore the durable-dir volumes and
//!   cold-boot from the image.
//!
//! The keeper has put the snapshot's files in the restore dir and re-created
//! the durable-dir volume directories, empty.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hivekeep_proto::worker::v1 as pb;
use hivekeep_sandbox_gvisor::actor_lock::Rpc;
use hivekeep_sandbox_gvisor::attribution::ActorRequest;
use hivekeep_sandbox_gvisor::error::{status, Context, Error, Result};
use hivekeep_sandbox_gvisor::phaselog::{self, Phase, RESTORE_DURATION_KEY};
use hivekeep_sandbox_gvisor::ports::lifecycle_log;
use hivekeep_sandbox_gvisor::wakeup_probe::ACTOR_VETH_IP;
use hivekeep_sandbox_gvisor::{mounts, validation};
use tokio_util::sync::CancellationToken;

use crate::actor_cgroup::ActorLeaf;
use crate::actor_path::rootfs_upper_dir;
use crate::boot::{dial_agent_retry, is_guest_stopped, workload_ids, BootParams, CLEANUP_TIMEOUT};
use crate::ch::{
    self, LaunchOptions, RestoredNet, VmmInfo, MEM_RESTORE_EAGER, MEM_RESTORE_ON_DEMAND,
};
use crate::fidelity::{includes_memory, validate_fidelity, validate_runtime_asset_paths};
use crate::kata::agent::RandomDevReseeder;
use crate::kata::paths::{vm_dir, vsock_socket_path};
use crate::phases;
use crate::rootfs_upper::{container_names, untar_rootfs_upper};
use crate::service::{RunningActor, WorkerService};
use crate::sizing::SandboxSize;
use crate::snapshot_config::{rewrite_snapshot_paths, BASE_ID_FILE};
use crate::snapshot_files::{drop_actor_snapshot_cache_async, maybe_drop_staged_memory_image};
use crate::vm_boot::{kernel_and_image, resolve_runtime};
use crate::volumes::{durable_volume_names, has_durable_volumes, untar_durable_volumes};
use crate::wakeup_probe;

/// Bounds the CRNG reseed; it must cover a whole resume wave.
pub const RESEED_TIMEOUT: Duration = Duration::from_secs(60);

/// How the VMM should load guest RAM. On demand is what we want: an idle
/// restored Actor then holds its working set, not its snapshot. Eager is
/// right on a VMM that prefaults unconditionally, where on demand starves the
/// guest so its wakeup probe never passes.
pub fn restore_mem_mode(info: &VmmInfo) -> &'static str {
    if !info.prefaults_unconditionally() {
        return MEM_RESTORE_ON_DEMAND;
    }
    if info.version.is_empty() && info.build_version.is_empty() {
        tracing::warn!(
            mode = MEM_RESTORE_EAGER,
            "cloud-hypervisor did not report a version; restoring eagerly"
        );
    }
    MEM_RESTORE_EAGER
}

/// 32 bytes of fresh entropy for the guest CRNG.
pub fn new_reseed_nonce() -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut nonce = vec![0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut nonce)?;
    Ok(nonce)
}

/// Mixes per-restore entropy into the restored guest's CRNG, so clones of one
/// snapshot diverge instead of producing identical randomness. One call,
/// bounded by [`RESEED_TIMEOUT`].
pub async fn reseed_guest_crng(ac: &dyn RandomDevReseeder) -> Result<()> {
    let nonce = new_reseed_nonce().context("generating reseed nonce")?;
    match tokio::time::timeout(RESEED_TIMEOUT, ac.reseed_random_dev(&nonce)).await {
        Ok(r) => r.map_err(Error::from),
        Err(_) => Err(Error::Context {
            context: "agent ReseedRandomDev".into(),
            source: Box::new(Error::DeadlineExceeded),
        }),
    }
}

/// Classifies a failure to resume the guest or reach its agent before it
/// serves traffic. The failed attempt leaves nothing behind, so it is
/// UNAVAILABLE and safe to retry; except when the caller went away (its own
/// cancellation), or the guest stopped (the same snapshot would likely stop
/// it again).
pub fn restore_guest_ready_error(cancel: &CancellationToken, step: &str, err: Error) -> Error {
    if cancel.is_cancelled() {
        return Error::Context {
            context: format!("while {step}: {err}"),
            source: Box::new(Error::Cancelled),
        };
    }
    if is_guest_stopped(&err) {
        return Error::Context {
            context: format!("while {step}"),
            source: Box::new(err),
        };
    }
    Error::Status(status::unavailable(format!("while {step}: {err}")))
}

impl WorkerService {
    /// RestoreWorkload.
    pub async fn restore_workload(
        self: &Arc<Self>,
        req: pb::RestoreWorkloadRequest,
        cancel: CancellationToken,
    ) -> std::result::Result<pb::RestoreWorkloadResponse, tonic::Status> {
        validation::into_status(validation::restore_workload_request(&req))?;
        let epoch = self.epoch.require()?;
        validate_fidelity(req.fidelity)?;
        validate_runtime_asset_paths(
            &self.cfg.actors_dir,
            &req.actor_uid,
            req.actor_dirs.as_ref(),
            &req.runtime_asset_paths,
        )?;
        let uid = req.actor_uid.clone();
        let _lock = self
            .locks
            .lock_cancellable(&uid, &cancel)
            .await
            .ok_or_else(|| {
                tonic::Status::cancelled("gave up waiting for the actor's lock: context canceled")
            })?;
        let cancel = cancel.child_token();
        let _rpc = self.begin_rpc(&uid, Rpc::Restore, Some(cancel.clone()))?;
        let attribution = req.attribution();
        self.cfg.tunnel.deactivate(&attribution).await?;
        let mut p = BootParams {
            attribution: attribution.clone(),
            actor_dirs: req.actor_dirs.clone().unwrap_or_default(),
            containers: req.spec.clone().unwrap_or_default().containers,
            asset_paths: req.runtime_asset_paths.clone(),
            egress_gateway: req.egress_gateway.clone(),
            size: SandboxSize::from_limits(req.resources.as_ref().and_then(|r| r.limits.as_ref())),
            rootfs_upper_snapshot_dir: None,
        };
        let restore_dir = PathBuf::from(&p.actor_dirs.restore_dir);
        let durable_dir = PathBuf::from(&p.actor_dirs.durable_dir_volume_mounts_dir);
        let t_start = Instant::now();
        lifecycle_log("Actor restoring", &attribution);
        self.end_previous_activation(&uid, Some(&p.actor_dirs))
            .await?;
        // Only a MEMORY restore resumes the guest and its CPU counters.
        self.host_actor(&attribution, includes_memory(req.fidelity))
            .await?;
        let res = async {
            // Before anything can observe the volumes: the share's virtiofsd
            // for MEMORY, the cold-started workload otherwise.
            if has_durable_volumes(&p.containers) {
                let (d, r, v) = (durable_dir.clone(), restore_dir.clone(), durable_volume_names(&p.containers));
                tokio::task::spawn_blocking(move || untar_durable_volumes(&d, &r, &v))
                    .await
                    .map_err(|e| Error::msg(e.to_string()))?
                    .context("while restoring durable-dir volumes")?;
            }
            match pb::SnapshotFidelity::try_from(req.fidelity) {
                Ok(pb::SnapshotFidelity::Memory) => {
                    self.restore_memory_fidelity(&p, req.fidelity, &restore_dir, req.preserve_restore_dir, t_start, &cancel).await
                }
                Ok(f @ (pb::SnapshotFidelity::Rootfs | pb::SnapshotFidelity::Volumes)) => {
                    if f == pb::SnapshotFidelity::Rootfs {
                        p.rootfs_upper_snapshot_dir = Some(restore_dir.clone());
                    }
                    self.cold_boot_actor_retrying(&p, &cancel).await?;
                    let total = t_start.elapsed();
                    tracing::info!(id = %uid, total = ?total, fidelity = ?f, "Actor restored (cold boot)");
                    phaselog::log_record(
                        "Restore timing breakdown",
                        &phaselog::snapshot_phase_record(&attribution, req.fidelity, RESTORE_DURATION_KEY, None, &[Phase { name: phases::TOTAL, d: total }]),
                    );
                    Ok(())
                }
                _ => Err(Error::Status(tonic::Status::invalid_argument(format!("unsupported snapshot fidelity: {}", req.fidelity)))),
            }
        }
        .await;
        if let Err(e) = res {
            let _ = tokio::time::timeout(CLEANUP_TIMEOUT, self.unhost_actor(&uid)).await;
            return Err(e.into());
        }
        lifecycle_log("Actor restored", &attribution);
        Ok(pb::RestoreWorkloadResponse {
            worker_epoch: epoch,
        })
    }

    /// Restores a whole-guest snapshot: repoint its per-Actor paths, rebuild
    /// the uppers and merged trees at the frozen paths, start virtiofsd,
    /// rebuild the taps (the snapshot's virtio-net is fd-backed), relaunch the
    /// VMM from the snapshot and resume.
    pub(crate) async fn restore_memory_fidelity(
        self: &Arc<Self>,
        p: &BootParams,
        fidelity: i32,
        restore_dir: &Path,
        preserve: bool,
        t_start: Instant,
        cancel: &CancellationToken,
    ) -> Result<()> {
        // Checked before anything is prepared: the snapshot's kernel and
        // image are repointed at these.
        let (kernel, image) = kernel_and_image(&p.asset_paths).map_err(Error::Msg)?;
        let rr = resolve_runtime(&p.asset_paths, &self.cfg.ch_binary);
        let egress = self
            .cfg
            .tunnel
            .prepare_egress(&p.attribution, p.egress_gateway.as_ref())
            .await?;
        let uid = p.attribution.uid.clone();
        self.cleanup_sandbox_state(&uid).await;
        let mut started: Vec<Arc<crate::child::Process>> = Vec::new();
        let mut agent: Option<crate::kata::agent::AgentClient> = None;
        let mut untar: Option<tokio::task::JoinHandle<std::io::Result<()>>> = None;
        let res = async {
            rewrite_snapshot_paths(restore_dir, &uid, &kernel, &image)
                .context("while rewriting snapshot paths")?;
            let src_id = std::fs::read_to_string(restore_dir.join(BASE_ID_FILE))
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| uid.clone());
            std::fs::create_dir_all(vm_dir(&uid)).context("while creating VM dir")?;
            let t_prep = Instant::now();
            // Re-materialize the uppers in the background, hidden behind the
            // bundle preparation; joined before the overlays need them.
            let (u, r, n) = (
                rootfs_upper_dir(&p.actor_dirs),
                restore_dir.to_path_buf(),
                container_names(&p.containers),
            );
            untar = Some(tokio::task::spawn_blocking(move || {
                untar_rootfs_upper(&u, &r, &n)
            }));
            let ctrs = self.build_actor_containers(&uid, &p.actor_dirs, &p.containers)?;
            let t_bundles = Instant::now();
            if let Some(h) = untar.take() {
                h.await
                    .map_err(|e| Error::msg(e.to_string()))?
                    .context("while restoring the rootfs uppers")?;
            }
            let t_upper = Instant::now();
            let leaf = ActorLeaf::open(&self.cfg.cgroup_root, &uid, p.size.milli_cpu)
                .context("while opening the actor's cgroup")?;
            let vfsd = self
                .stage_merged_rootfs(
                    &rr,
                    &uid,
                    &p.actor_dirs,
                    &ctrs,
                    &p.containers,
                    &leaf,
                    cancel,
                )
                .await?;
            started.push(vfsd.clone());
            let t_lowers = Instant::now();
            let devs = ch::client::snapshot_net_devices(restore_dir)
                .context("while reading snapshot net devices")?;
            let mut taps = Vec::new();
            let mut nets = Vec::new();
            for (i, nd) in devs.iter().enumerate() {
                let fds = self
                    .cfg
                    .taps
                    .create_tap(&uid, &format!("tap{i}_kata"), nd.queue_pairs)
                    .await
                    .with_context(|| format!("while building restore tap for {}", nd.id))?;
                use std::os::fd::AsRawFd;
                nets.push(RestoredNet {
                    id: nd.id.clone(),
                    fds: fds.iter().map(|f| f.as_raw_fd()).collect(),
                });
                taps.extend(fds);
            }
            let t_tap = Instant::now();
            let api_socket = vm_dir(&uid).join("clh-api-restore.sock");
            let (ch_proc, client) = ch::launch_vmm(
                LaunchOptions {
                    binary: &rr.ch_binary,
                    api_socket: api_socket.clone(),
                    leaf: Some(&leaf),
                },
                cancel,
            )
            .await
            .context("while launching VMM for restore")?;
            started.push(ch_proc.clone());
            let t_launch = Instant::now();
            // On demand, the VMM pages from the restore dir for the VM's whole
            // life and the next snapshot holds only the delta; eager reads it
            // all now, and nothing pages or merges against it afterwards.
            let info = client.info();
            let mem_mode = restore_mem_mode(&info);
            tracing::info!(mode = mem_mode, vmm_version = %info.version, "restoring guest memory");
            client
                .restore_with_net_fds(restore_dir, &nets, mem_mode)
                .await
                .context("while restoring VM with net FDs")?;
            drop(taps);
            let t_vm_restore = Instant::now();
            // Dropped before resuming, so guest RAM and the snapshot's page
            // cache do not overlap during resume and the wakeup probe.
            maybe_drop_staged_memory_image(restore_dir, mem_mode, preserve);
            drop_actor_snapshot_cache_async(restore_dir, mem_mode == MEM_RESTORE_EAGER);
            if let Err(e) = client.resume().await {
                return Err(restore_guest_ready_error(
                    cancel,
                    "resuming restored guest",
                    Error::io("resume", e),
                ));
            }
            let t_resume = Instant::now();
            let ac = dial_agent_retry(&vsock_socket_path(&uid), Duration::from_secs(15), cancel)
                .await
                .map_err(|e| {
                    restore_guest_ready_error(cancel, "dialing kata-agent after resume", e)
                })?;
            agent = Some(ac.clone());
            // The guest resumed with the entropy pool frozen in the snapshot,
            // shared with every other clone of it. Egress opens only once the
            // reseed succeeded, so nothing leaves the host drawn from shared
            // CRNG state.
            reseed_guest_crng(&ac)
                .await
                .map_err(|e| restore_guest_ready_error(cancel, "reseeding guest CRNG", e))?;
            self.cfg.tunnel.activate_egress(&p.attribution, egress)?;
            wakeup_probe::wait_all(
                &p.containers,
                ACTOR_VETH_IP,
                Some(self.sandbox_dialer(&uid)),
                cancel,
            )
            .await
            .context("while waiting for container wakeup probe")?;
            let wakeup = t_resume.elapsed();
            let total = t_start.elapsed();
            tracing::info!(
                id = %uid,
                prep = ?t_prep - t_start,
                bundles = ?t_bundles - t_prep,
                upper_join = ?t_upper - t_bundles,
                lowers = ?t_lowers - t_upper,
                tap = ?t_tap - t_lowers,
                vmm_launch = ?t_launch - t_tap,
                vm_restore = ?t_vm_restore - t_launch,
                resume = ?t_resume - t_vm_restore,
                wakeup_probe = ?wakeup,
                total = ?total,
                "Actor restore phases"
            );
            let ph = |name: &'static str, d: Duration| Phase { name, d };
            phaselog::log_record(
                "Restore timing breakdown",
                &phaselog::snapshot_phase_record(
                    &p.attribution,
                    fidelity,
                    RESTORE_DURATION_KEY,
                    None,
                    &[
                        ph(phases::PREP, t_prep - t_start),
                        ph(phases::BUNDLES, t_bundles - t_prep),
                        ph(phases::UPPER_JOIN, t_upper - t_bundles),
                        ph(phases::LOWERS, t_lowers - t_upper),
                        ph(phases::TAP, t_tap - t_lowers),
                        ph(phases::VMM_LAUNCH, t_launch - t_tap),
                        ph(phases::VM_RESTORE, t_vm_restore - t_launch),
                        ph(phases::RESUME, t_resume - t_vm_restore),
                        ph(phases::WAKEUP_PROBE, wakeup),
                        ph(phases::TOTAL, total),
                    ],
                ),
            );
            let ra = Arc::new(RunningActor {
                base_id: src_id,
                ch_proc: Some(ch_proc),
                vfsd_proc: Some(vfsd),
                api_socket,
                stopping: Default::default(),
                restore_source_dir: Some(restore_dir.to_path_buf()),
                preserve_restore_source: preserve,
                snapshot_is_self_contained: mem_mode == MEM_RESTORE_EAGER,
                guest_agent: parking_lot::Mutex::new(Some(ac.clone())),
                workload_ids: workload_ids(&ctrs),
            });
            self.cfg
                .tunnel
                .activate_ingress(&p.attribution, self.sandbox_dialer(&uid))?;
            Ok((ra, ctrs))
        }
        .await;
        // Drain the background untar before anything below touches the upper.
        if let Some(h) = untar.take() {
            let _ = h.await;
        }
        match res {
            Ok((ra, ctrs)) => {
                let ac = agent.expect("a restored actor has an agent");
                self.publish_vm(&p.attribution, &p.actor_dirs, ra, &ac, &ctrs);
                tracing::info!(id = %uid, total = ?t_start.elapsed(), "Actor restored (overlay rootfs)");
                Ok(())
            }
            Err(e) => {
                if let Some(ac) = agent {
                    ac.close();
                }
                for proc in started.iter().rev() {
                    proc.stop().await;
                }
                if let Ok(Err(err)) = tokio::time::timeout(
                    CLEANUP_TIMEOUT,
                    self.cfg.tunnel.deactivate(&p.attribution),
                )
                .await
                {
                    tracing::warn!(err = %err, "Failed to deactivate actor networking after Restore failure");
                }
                self.cleanup_failed_restore_rootfs(&uid, &p.actor_dirs)
                    .await;
                Err(e)
            }
        }
    }

    /// Removes what a failed memory restore assembled on the host: the
    /// sandbox state (detaching the merged overlays), the re-materialized
    /// upper dir, and the bundle rootfs overlays. The caller has stopped the
    /// VMM and virtiofsd and drained the untar.
    pub(crate) async fn cleanup_failed_restore_rootfs(&self, uid: &str, dirs: &pb::ActorDirs) {
        self.cleanup_sandbox_state(uid).await;
        match std::fs::remove_dir_all(rootfs_upper_dir(dirs)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                tracing::warn!(actor_uid = uid, err = %e, "Failed to remove rootfs upper dir after Restore failure")
            }
        }
        if let Err(e) = mounts::unmount_all_under(Path::new(&dirs.oci_bundle_dir)) {
            tracing::warn!(actor_uid = uid, err = %e, "Failed to unmount bundle rootfs overlays after Restore failure");
        }
    }
}

#[cfg(test)]
mod tests;
