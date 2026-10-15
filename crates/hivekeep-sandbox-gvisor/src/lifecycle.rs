//! The lifecycle RPCs: run, checkpoint, restore and terminate a workload.
//!
//! The keeper's side of the contract: before Run or Restore it has staged the
//! right runsc binary and the application bundles (and for Restore, the
//! snapshot); after Checkpoint it ships the listed files and resets the
//! Actor's directories.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use futures::future::BoxFuture;
use hivekeep_core::resource::ActorAttribution;
use hivekeep_proto::worker::v1 as pb;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::actor_lock::{ActorGuard, Rpc};
use crate::actor_path::{create_private_dir, reset_runsc_state_and_pid_file_dirs};
use crate::attribution::ActorRequest;
use crate::checkpoint_record::{record_checkpoint, reuse_checkpoint};
use crate::ckpt_cgroup::{checkpoint_isolated, drop_actor_checkpoint_cache_async, WaitRestore};
use crate::cleanup::{cleanup_containers, delete_containers, stop_containers};
use crate::error::{Context, Error, Result};
use crate::fs_checkpoint::{
    durable_fs_checkpoint_paths, durable_snapshot_files, fs_restore_args, fs_restore_containers,
    list_snapshot_files, rootfs_fs_checkpoint_paths,
};
use crate::hosted::{HostedActor, WorkloadSession};
use crate::memlimit::{pause_memory_watch, resume_memory_watch};
use crate::mounts::unmount_all_under;
use crate::oci::SANDBOX_ID;
use crate::phaselog::{self, lap, CheckpointTiming, RestoreTiming};
use crate::ports::lifecycle_log;
use crate::request_checks::{validate_fidelity, validate_runsc_path};
use crate::rootfs::setup_app_rootfs;
use crate::runsc::{durable_volume_names, ContainerRuntime, Output, RunscCommand};
use crate::sandbox_process::{SandboxProcess, SANDBOX_EXIT_TIMEOUT};
use crate::service::{timeout_token, WorkerService, CLEANUP_TIMEOUT};
use crate::sizing::SandboxSize;
use crate::validation;

fn out(a: &ActorAttribution, container: &str) -> Output {
    Output::Actor {
        attribution: a.clone(),
        container: container.into(),
    }
}

fn containers_of(spec: Option<&pb::WorkloadSpec>) -> Vec<pb::Container> {
    spec.map(|s| s.containers.clone()).unwrap_or_default()
}

/// The names of the application containers, which graceful shutdown kills.
/// The sandbox is never among them: container names are DNS labels, which
/// the sandbox id is not.
pub(crate) fn container_names(containers: &[pb::Container]) -> Vec<String> {
    containers.iter().map(|c| c.name.clone()).collect()
}

/// Everything a Run or Restore needs, from its request.
struct Startup {
    attribution: ActorAttribution,
    runsc_path: String,
    dirs: pb::ActorDirs,
    containers: Vec<pb::Container>,
    spec: Option<pb::WorkloadSpec>,
    resources: Option<pb::ResourceRequirements>,
    gateway: Option<pb::EgressGateway>,
}

impl WorkerService {
    fn runtime(&self, cmd: RunscCommand) -> Arc<dyn ContainerRuntime> {
        self.cfg.runtimes.runtime(cmd)
    }

    fn bare_runtime(
        &self,
        runsc_path: &str,
        uid: &str,
        dirs: &pb::ActorDirs,
    ) -> Arc<dyn ContainerRuntime> {
        self.runtime(RunscCommand::bare(
            runsc_path,
            uid,
            dirs,
            self.cfg.flags.clone(),
        ))
    }

    fn startup_runtime(&self, s: &Startup) -> Arc<dyn ContainerRuntime> {
        self.runtime(RunscCommand {
            path: s.runsc_path.clone(),
            actor_uid: s.attribution.uid.clone(),
            actor_dirs: s.dirs.clone(),
            size: SandboxSize::from_limits(s.resources.as_ref().and_then(|r| r.limits.as_ref())),
            durable_volumes: durable_volume_names(s.spec.as_ref()),
            containers: s.containers.clone(),
            flags: self.cfg.flags.clone(),
        })
    }

    async fn lock_actor(
        &self,
        uid: &str,
        cancel: &CancellationToken,
    ) -> Result<ActorGuard, tonic::Status> {
        self.locks
            .lock_cancellable(uid, cancel)
            .await
            .ok_or_else(|| {
                tonic::Status::cancelled("gave up waiting for the actor's lock: context canceled")
            })
    }

    /// Clears what a previous activation may have left before a new one.
    async fn prepare_startup(&self, s: &Startup) -> Result<()> {
        reset_runsc_state_and_pid_file_dirs(&s.dirs).context("while resetting runsc state")?;
        self.remove_stale_sandbox_leaf(&s.attribution.uid).await
    }

    /// Undoes a failed Run or Restore: networking, the containers created so
    /// far, bundle overlays, and the hosted record.
    async fn fail_startup(
        &self,
        err: Error,
        op: &str,
        s: &Startup,
        hosted: &HostedActor,
        rt: &dyn ContainerRuntime,
        created: &[String],
    ) -> Error {
        let err = if hosted.oom_killed.load(Ordering::SeqCst) {
            Error::Context {
                context: "killed for exceeding its memory limit".into(),
                source: Box::new(err),
            }
        } else {
            err
        };
        let cleanup = timeout_token(CLEANUP_TIMEOUT);
        if let Err(e) = self.cfg.tunnel.deactivate(&s.attribution).await {
            tracing::warn!(operation = op, err = %e, "Failed to deactivate actor networking after failure");
        }
        delete_containers(rt, created, op, &cleanup).await;
        if let Err(e) = unmount_all_under(Path::new(&s.dirs.oci_bundle_dir)) {
            tracing::warn!(operation = op, actor_uid = %s.attribution.uid, err = %e, "Failed to unmount bundle rootfs overlays after failure");
        }
        if let Err(e) = self.unhost_actor(&s.attribution.uid).await {
            tracing::warn!(operation = op, err = %e, "Failed to clean up actor network after failure");
        }
        err
    }

    /// The common end of a successful Run or Restore.
    async fn finish_startup(
        self: &Arc<Self>,
        s: &Startup,
        hosted: &Arc<HostedActor>,
        rt: Arc<dyn ContainerRuntime>,
    ) {
        let uid = &s.attribution.uid;
        self.set_session(
            uid,
            WorkloadSession {
                runtime: rt,
                containers: container_names(&s.containers),
            },
        );
        self.record_initial(hosted).await;
        self.watch_sandbox(
            hosted.clone(),
            s.runsc_path.clone(),
            s.dirs.clone(),
            s.containers.clone(),
        );
    }

    /// `Worker/RunWorkload`: boots the workload's sandbox and containers.
    pub async fn run_workload(
        self: &Arc<Self>,
        req: pb::RunWorkloadRequest,
        cancel: CancellationToken,
    ) -> Result<pb::RunWorkloadResponse, tonic::Status> {
        validation::into_status(validation::run_workload_request(&req))?;
        let epoch = self.epoch.require()?;
        let s = Startup {
            attribution: req.attribution(),
            runsc_path: req.runsc_path.clone(),
            dirs: req.actor_dirs.clone().unwrap_or_default(),
            containers: containers_of(req.spec.as_ref()),
            spec: req.spec.clone(),
            resources: req.resources,
            gateway: req.egress_gateway.clone(),
        };
        let uid = s.attribution.uid.clone();
        validate_runsc_path(&self.cfg.actors_dir, &uid, &s.dirs, &s.runsc_path)?;
        let _lock = self.lock_actor(&uid, &cancel).await?;
        let cancel = cancel.child_token();
        let _rpc = self.begin_rpc(&uid, Rpc::Run, Some(cancel.clone()))?;
        self.prepare_startup(&s).await?;
        self.cfg.tunnel.deactivate(&s.attribution).await?;
        lifecycle_log("Actor starting", &s.attribution);
        let egress = self
            .cfg
            .tunnel
            .prepare_egress(&s.attribution, s.gateway.as_ref())
            .await?;
        // Hosted before boot, so stats cover startup.
        let hosted = self.host_actor(s.attribution.clone()).await?;
        let rt = self.startup_runtime(&s);
        let mut created = Vec::new();
        let booted: Result<()> = async {
            self.cfg.tunnel.activate_egress(&s.attribution, egress)?;
            created.push(SANDBOX_ID.to_string());
            let sb = out(&s.attribution, SANDBOX_ID);
            rt.create(&cancel, &sb, SANDBOX_ID, &[])
                .await
                .context("while creating the sandbox")?;
            self.pin_sandbox(&hosted, &s.dirs);
            self.throttle_sandbox_memory(&hosted);
            rt.start(&cancel, &sb, SANDBOX_ID)
                .await
                .context("while starting the sandbox")?;
            for c in &s.containers {
                let o = out(&s.attribution, &c.name);
                setup_app_rootfs(&*self.cfg.rootfs, &s.dirs, &c.name)
                    .with_context(|| format!("while composing {:?} rootfs", c.name))?;
                created.push(c.name.clone());
                rt.create(&cancel, &o, &c.name, &[])
                    .await
                    .with_context(|| {
                        format!("while creating {:?} application container", c.name)
                    })?;
                rt.start(&cancel, &o, &c.name).await.with_context(|| {
                    format!("while starting {:?} application container", c.name)
                })?;
            }
            self.cfg
                .prober
                .wait_all(&s.containers, self.sandbox_dialer(&uid), &cancel)
                .await
                .context("while waiting for container wakeup probe")?;
            self.cfg
                .tunnel
                .activate_ingress(&s.attribution, self.sandbox_dialer(&uid))?;
            Ok(())
        }
        .await;
        if let Err(e) = booted {
            return Err(self
                .fail_startup(e, "Run", &s, &hosted, &*rt, &created)
                .await
                .into());
        }
        lifecycle_log("Actor started", &s.attribution);
        self.finish_startup(&s, &hosted, rt).await;
        Ok(pb::RunWorkloadResponse {
            worker_epoch: epoch,
        })
    }

    /// `Worker/RestoreWorkload`: restores a workload from a checkpoint (or,
    /// at VOLUMES and ROOTFS fidelity, cold-boots it over restored
    /// filesystems).
    pub async fn restore_workload(
        self: &Arc<Self>,
        req: pb::RestoreWorkloadRequest,
        cancel: CancellationToken,
    ) -> Result<pb::RestoreWorkloadResponse, tonic::Status> {
        validation::into_status(validation::restore_workload_request(&req))?;
        let epoch = self.epoch.require()?;
        // Taken before the lock, so prep covers waiting for it.
        let t_start = Instant::now();
        validate_fidelity(req.fidelity)?;
        let s = Startup {
            attribution: req.attribution(),
            runsc_path: req.runsc_path.clone(),
            dirs: req.actor_dirs.clone().unwrap_or_default(),
            containers: containers_of(req.spec.as_ref()),
            spec: req.spec.clone(),
            resources: req.resources,
            gateway: req.egress_gateway.clone(),
        };
        let uid = s.attribution.uid.clone();
        validate_runsc_path(&self.cfg.actors_dir, &uid, &s.dirs, &s.runsc_path)?;
        let _lock = self.lock_actor(&uid, &cancel).await?;
        let cancel = cancel.child_token();
        let mut timing = RestoreTiming::default();
        let result = self
            .restore_locked(&s, &req, &cancel, t_start, &mut timing)
            .await;
        timing.total = t_start.elapsed();
        let mut rec = phaselog::snapshot_phase_record(
            &s.attribution,
            req.fidelity,
            phaselog::RESTORE_DURATION_KEY,
            result.as_ref().err().map(tonic::Status::code),
            &timing.phases(),
        );
        rec.insert(
            phaselog::CONTAINER_COUNT_KEY.into(),
            s.containers.len().into(),
        );
        phaselog::log_record("Restore timing breakdown", &rec);
        result.map(|()| pb::RestoreWorkloadResponse {
            worker_epoch: epoch,
        })
    }

    async fn restore_locked(
        self: &Arc<Self>,
        s: &Startup,
        req: &pb::RestoreWorkloadRequest,
        cancel: &CancellationToken,
        t_start: Instant,
        timing: &mut RestoreTiming,
    ) -> Result<(), tonic::Status> {
        let uid = &s.attribution.uid;
        let fidelity = pb::SnapshotFidelity::try_from(req.fidelity).unwrap_or_default();
        let mut t_last = t_start;
        let _rpc = self.begin_rpc(uid, Rpc::Restore, Some(cancel.clone()))?;
        self.prepare_startup(s).await?;
        let deactivated = self.cfg.tunnel.deactivate(&s.attribution).await;
        timing.prep = lap(&mut t_last);
        deactivated?;
        lifecycle_log("Actor restoring", &s.attribution);
        let egress = self
            .cfg
            .tunnel
            .prepare_egress(&s.attribution, s.gateway.as_ref())
            .await;
        timing.egress_prepare = lap(&mut t_last);
        let egress = egress?;
        let hosted = self.host_actor(s.attribution.clone()).await;
        timing.net_setup = lap(&mut t_last);
        let hosted = hosted?;
        let rt = self.startup_runtime(s);
        let restore_dir = PathBuf::from(&s.dirs.restore_dir);
        let mut created = Vec::new();
        let mut restore_args = Vec::new();
        let booted: Result<()> = async {
            let activated = self.cfg.tunnel.activate_egress(&s.attribution, egress);
            timing.activate = lap(&mut t_last);
            activated?;
            restore_args = fs_restore_args(&restore_dir, fidelity)?;
            let image = restore_dir.display().to_string();
            let sb = out(&s.attribution, SANDBOX_ID);
            created.push(SANDBOX_ID.to_string());
            let r = rt.create(cancel, &sb, SANDBOX_ID, &restore_args).await;
            timing.sandbox_create = lap(&mut t_last);
            r.context("while creating the sandbox")?;
            self.pin_sandbox(&hosted, &s.dirs);
            self.throttle_sandbox_memory(&hosted);
            if fidelity == pb::SnapshotFidelity::Memory {
                let r = rt.restore(cancel, &sb, SANDBOX_ID, &image).await;
                timing.sandbox_restore = lap(&mut t_last);
                r.context("while restoring the sandbox")?;
            } else {
                // Cold boot over the restored filesystem bundles.
                let r = rt.start(cancel, &sb, SANDBOX_ID).await;
                timing.sandbox_restore = lap(&mut t_last);
                r.context("while starting the sandbox")?;
            }
            for c in &s.containers {
                let o = out(&s.attribution, &c.name);
                timing.app_create += lap(&mut t_last);
                let r = setup_app_rootfs(&*self.cfg.rootfs, &s.dirs, &c.name);
                timing.app_rootfs += lap(&mut t_last);
                r.with_context(|| format!("while composing {:?} rootfs", c.name))?;
                created.push(c.name.clone());
                let r = rt.create(cancel, &o, &c.name, &[]).await;
                timing.app_create += lap(&mut t_last);
                r.with_context(|| format!("while creating {:?} application container", c.name))?;
                if fidelity == pb::SnapshotFidelity::Memory {
                    let r = rt.restore(cancel, &o, &c.name, &image).await;
                    timing.app_restore += lap(&mut t_last);
                    r.with_context(|| {
                        format!("while restoring {:?} application container", c.name)
                    })?;
                } else {
                    let r = rt.start(cancel, &o, &c.name).await;
                    timing.app_restore += lap(&mut t_last);
                    r.with_context(|| {
                        format!("while starting {:?} application container", c.name)
                    })?;
                }
            }
            let r = self
                .cfg
                .prober
                .wait_all(&s.containers, self.sandbox_dialer(uid), cancel)
                .await;
            timing.wakeup_probe = lap(&mut t_last);
            r.context("while waiting for container wakeup probe")?;
            let r = self
                .cfg
                .tunnel
                .activate_ingress(&s.attribution, self.sandbox_dialer(uid));
            timing.activate += lap(&mut t_last);
            r
        }
        .await;
        if let Err(e) = booted {
            return Err(self
                .fail_startup(e, "Restore", s, &hosted, &*rt, &created)
                .await
                .into());
        }

        // Release the snapshot's pages once background loading finishes.
        let wait_rt = rt.clone();
        let fs_waits = if restore_args.is_empty() {
            Vec::new()
        } else {
            fs_restore_containers(fidelity, &s.containers)
        };
        let wait: WaitRestore = Box::new(
            move |tok: CancellationToken| -> BoxFuture<'static, Result<()>> {
                Box::pin(async move {
                    if fidelity == pb::SnapshotFidelity::Memory {
                        wait_rt.wait_restore(&tok, SANDBOX_ID).await?;
                    }
                    for c in fs_waits {
                        wait_rt.wait_fs_restore(&tok, &c).await?;
                    }
                    Ok(())
                })
            },
        );
        drop_actor_checkpoint_cache_async(
            hosted.token.clone(),
            uid,
            &restore_dir,
            req.preserve_restore_dir,
            Some(wait),
            self.cfg.evict.clone(),
        );
        lifecycle_log("Actor restored", &s.attribution);
        self.finish_startup(s, &hosted, rt).await;
        Ok(())
    }

    /// `Worker/CheckpointWorkload`: saves the workload and tears it down.
    /// Allowed while draining, so Actors can suspend on shutdown, and not
    /// cancelable once it holds the lock: it may already have stopped the
    /// workload.
    pub async fn checkpoint_workload(
        self: &Arc<Self>,
        req: pb::CheckpointWorkloadRequest,
        cancel: CancellationToken,
    ) -> Result<pb::CheckpointWorkloadResponse, tonic::Status> {
        validation::into_status(validation::checkpoint_workload_request(&req))?;
        let t_start = Instant::now();
        validate_fidelity(req.fidelity)?;
        let attribution = req.attribution();
        let uid = attribution.uid.clone();
        let dirs = req.actor_dirs.clone().unwrap_or_default();
        validate_runsc_path(&self.cfg.actors_dir, &uid, &dirs, &req.runsc_path)?;
        let _lock = self.lock_actor(&uid, &cancel).await?;
        if self.lookup_actor(&uid).is_none() {
            if let Some(resp) = reuse_checkpoint(Path::new(&dirs.checkpoint_dir), req.fidelity) {
                tracing::info!(actor_uid = %uid, "Reusing completed checkpoint from previous attempt");
                return Ok(resp);
            }
        }
        let _rpc = self.in_flight.add(&uid, Rpc::Checkpoint, None);
        let laps = Laps::new(t_start);
        let result = self
            .checkpoint_locked(&req, &attribution, &dirs, &laps)
            .await;
        let mut timing = *laps.timing.lock();
        timing.total = t_start.elapsed();
        let rec = phaselog::snapshot_phase_record(
            &attribution,
            req.fidelity,
            phaselog::CHECKPOINT_DURATION_KEY,
            result.as_ref().err().map(Error::code),
            &timing.phases(),
        );
        phaselog::log_record("Checkpoint timing breakdown", &rec);
        result.map_err(Into::into)
    }

    async fn checkpoint_locked(
        self: &Arc<Self>,
        req: &pb::CheckpointWorkloadRequest,
        attribution: &ActorAttribution,
        dirs: &pb::ActorDirs,
        laps: &Laps,
    ) -> Result<pb::CheckpointWorkloadResponse> {
        let uid = &attribution.uid;
        let cancel = CancellationToken::new();
        self.cfg.tunnel.deactivate(attribution).await?;
        lifecycle_log("Actor checkpointing", attribution);
        // Read before the snapshot: a MEMORY checkpoint stops the sandbox and
        // its cgroup. The final record waits for the teardown.
        let hosted = self.lookup_actor(uid);
        if let Some(h) = &hosted {
            self.read_final(h).await;
        }
        let rt = self.bare_runtime(&req.runsc_path, uid, dirs);
        let checkpoint_path = PathBuf::from(&dirs.checkpoint_dir);
        // Clear partial files of an earlier failed attempt.
        match std::fs::remove_dir_all(&checkpoint_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::io("while clearing checkpoint directory", e)),
        }
        let made = create_private_dir(&checkpoint_path);
        laps.lap(|t, d| t.prep = d);
        made.context("while creating checkpoint directory")?;

        let containers = containers_of(req.spec.as_ref());
        let durable = durable_fs_checkpoint_paths(&containers);
        let mut fs_paths = rootfs_fs_checkpoint_paths(&containers);
        fs_paths.extend(durable.iter().cloned());
        let image = checkpoint_path.display().to_string();
        let fidelity = pb::SnapshotFidelity::try_from(req.fidelity).unwrap_or_default();

        let checkpoint: BoxFuture<'_, Result<()>> = match fidelity {
            pb::SnapshotFidelity::Volumes => {
                if durable.is_empty() {
                    return Err(Error::msg(
                        "no durable-dir volumes found for VOLUMES snapshot",
                    ));
                }
                Box::pin(async {
                    let r = rt
                        .fs_checkpoint(&cancel, SANDBOX_ID, &image, &durable)
                        .await;
                    laps.lap(|t, d| t.fs_checkpoint = d);
                    r.context("while checkpointing durable-dir volumes")
                })
            }
            pb::SnapshotFidelity::Rootfs => Box::pin(async {
                let r = rt
                    .fs_checkpoint(&cancel, SANDBOX_ID, &image, &fs_paths)
                    .await;
                laps.lap(|t, d| t.fs_checkpoint = d);
                r.context("while checkpointing container filesystems")
            }),
            pb::SnapshotFidelity::Memory => Box::pin(async {
                // Rootfs overlays go to the rootfs/ bundle and durable-dir
                // volumes to fs/.
                let pinned = hosted.as_ref().and_then(|h| {
                    h.stopping.store(true, Ordering::SeqCst);
                    h.sandbox()
                });
                // Never pinned: the pid file still beats cleaning up before
                // the sandbox is reaped.
                let sandbox = pinned.or_else(|| match SandboxProcess::open(dirs) {
                    Ok(p) => Some(Arc::new(p)),
                    Err(e) => {
                        tracing::warn!(err = %e, "Cannot watch the sandbox exit after checkpoint");
                        None
                    }
                });
                if let Err(e) = rt.checkpoint(&cancel, SANDBOX_ID, &image, &fs_paths).await {
                    // The Actor may stay running, so its sandbox dying is a
                    // crash again.
                    if let Some(h) = &hosted {
                        h.stopping.store(false, Ordering::SeqCst);
                    }
                    laps.lap(|t, d| t.checkpoint = d);
                    return Err(e).context("while checkpointing the sandbox");
                }
                // The checkpoint stops the sandbox; runsc delete fails until
                // it is reaped.
                if let Some(sb) = sandbox {
                    if let Err(e) = sb.wait_gone(SANDBOX_EXIT_TIMEOUT).await {
                        tracing::warn!(err = %e, "Sandbox did not go away after checkpoint");
                    }
                }
                laps.lap(|t, d| t.checkpoint = d);
                Ok(())
            }),
            pb::SnapshotFidelity::Unspecified => {
                return Err(Error::msg(format!(
                    "unsupported snapshot fidelity: {}",
                    req.fidelity
                )))
            }
        };

        // Tears the workload down (unhosting the Actor) after the snapshot.
        let terminate = || async {
            let cleanup = timeout_token(CLEANUP_TIMEOUT);
            let r = self
                .terminate_workload(attribution, &req.runsc_path, dirs, &containers, &cleanup)
                .await;
            if let Err(e) = &r {
                tracing::warn!(actor = %attribution.actor, actor_uid = %uid, err = %e, "failed to terminate workload after checkpoint");
            }
            laps.lap(|t, d| t.teardown = d);
            self.record_final_if_ended(hosted.as_ref());
            r
        };

        // The sentry leaves its sandbox leaf for the checkpoint cgroup.
        let memory_limit = match &hosted {
            Some(h) => pause_memory_watch(h).await,
            None => 0,
        };
        if let Err(e) = checkpoint_isolated(
            &self.cfg.cgroup_root,
            uid,
            dirs,
            self.cfg.move_pid.clone(),
            checkpoint,
            terminate,
        )
        .await
        {
            if let Some(h) = &hosted {
                if memory_limit != 0 {
                    resume_memory_watch(h, memory_limit).await;
                }
            }
            return Err(e);
        }

        // Exactly the files runsc wrote, so the keeper ships precisely them.
        let snapshot_files =
            list_snapshot_files(&checkpoint_path).context("while listing checkpoint files")?;
        let data_snapshot_files = durable_snapshot_files(&snapshot_files);
        lifecycle_log("Actor checkpointed", attribution);
        let resp = pb::CheckpointWorkloadResponse {
            snapshot_files,
            data_snapshot_files,
        };
        if let Err(e) = record_checkpoint(&checkpoint_path, req.fidelity, &resp) {
            tracing::warn!(actor_uid = %uid, err = %e, "Failed to record checkpoint completion marker");
        }
        Ok(resp)
    }

    /// `Worker/TerminateWorkload`: stops the workload and cleans up its
    /// network and bundle state.
    pub async fn terminate_workload_rpc(
        self: &Arc<Self>,
        req: pb::TerminateWorkloadRequest,
        cancel: CancellationToken,
    ) -> Result<pb::TerminateWorkloadResponse, tonic::Status> {
        validation::into_status(validation::terminate_workload_request(&req))?;
        let dirs = req.actor_dirs.clone().unwrap_or_default();
        let attribution = req.attribution();
        validate_runsc_path(
            &self.cfg.actors_dir,
            &attribution.uid,
            &dirs,
            &req.runsc_path,
        )?;
        let _lock = self.lock_actor(&attribution.uid, &cancel).await?;
        let hosted = self.lookup_actor(&attribution.uid);
        if let Some(h) = &hosted {
            self.read_final(h).await;
        }
        // Detached from the caller: a deadline in the middle of `runsc
        // delete` would leave the container without its record.
        let cleanup = timeout_token(CLEANUP_TIMEOUT);
        let r = self
            .terminate_workload(
                &attribution,
                &req.runsc_path,
                &dirs,
                &containers_of(req.spec.as_ref()),
                &cleanup,
            )
            .await;
        self.record_final_if_ended(hosted.as_ref());
        r.context("failed to terminate workload")?;
        lifecycle_log("Actor terminated", &attribution);
        Ok(pb::TerminateWorkloadResponse {})
    }

    /// Stops and deletes the Actor's containers, resets its runsc state,
    /// detaches its bundle overlays and unhosts it. Every step runs; their
    /// failures are joined.
    pub(crate) async fn terminate_workload(
        &self,
        attribution: &ActorAttribution,
        runsc_path: &str,
        dirs: &pb::ActorDirs,
        containers: &[pb::Container],
        cancel: &CancellationToken,
    ) -> Result<()> {
        let uid = &attribution.uid;
        if let Some(h) = self.lookup_actor(uid) {
            h.stopping.store(true, Ordering::SeqCst);
        }
        let mut errs = Vec::new();
        if let Err(e) = self.cfg.tunnel.deactivate(attribution).await {
            errs.push(Error::Context {
                context: "while deactivating actor networking".into(),
                source: Box::new(e),
            });
        }
        let rt = self.bare_runtime(runsc_path, uid, dirs);
        // Stopped before deleted, so no live container outlives its bundle.
        stop_containers(&*rt, containers, cancel).await;
        if let Err(e) = cleanup_containers(&*rt, containers, cancel).await {
            errs.push(Error::Context {
                context: "while cleaning up runsc containers".into(),
                source: Box::new(e),
            });
        }
        // The Actor may resume elsewhere, so reset here rather than on the
        // next Run.
        if let Err(e) = reset_runsc_state_and_pid_file_dirs(dirs) {
            errs.push(Error::io(
                "while resetting runsc state and pid file dirs",
                e,
            ));
        }
        // Before the keeper wipes the bundles out from under live mounts.
        if let Err(e) = unmount_all_under(Path::new(&dirs.oci_bundle_dir)) {
            errs.push(Error::io("while unmounting bundle rootfs overlays", e));
        }
        if let Err(e) = self.unhost_actor(uid).await {
            errs.push(Error::Context {
                context: "while cleaning up actor network".into(),
                source: Box::new(e),
            });
        }
        Error::join(errs).map_or(Ok(()), Err)
    }
}

/// Checkpoint phase timing shared by the checkpoint and teardown steps.
struct Laps {
    last: Mutex<Instant>,
    timing: Mutex<CheckpointTiming>,
}

impl Laps {
    fn new(start: Instant) -> Self {
        Laps {
            last: Mutex::new(start),
            timing: Mutex::new(CheckpointTiming::default()),
        }
    }

    fn lap(&self, set: impl FnOnce(&mut CheckpointTiming, std::time::Duration)) {
        let d = lap(&mut self.last.lock());
        set(&mut self.timing.lock(), d);
    }
}

#[cfg(test)]
mod tests;
