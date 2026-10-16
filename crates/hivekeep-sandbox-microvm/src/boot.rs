//! RunWorkload: cold-booting an Actor as a micro-VM.
//!
//! The worker boots the VMM itself and gives each container a rootfs merged
//! on the host, served over the one virtio-fs share. It creates the VM
//! (kernel, guest image, share), adds the tap, boots, then does what a kata
//! shim would: create the agent sandbox, configure guest networking, and
//! create and start each container.

use std::collections::HashMap;
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hivekeep_core::resource::ActorAttribution;
use hivekeep_proto::worker::v1 as pb;
use hivekeep_sandbox_gvisor::actor_lock::Rpc;
use hivekeep_sandbox_gvisor::attribution::ActorRequest;
use hivekeep_sandbox_gvisor::error::{Context, Error, Result};
use hivekeep_sandbox_gvisor::oci::image_volume_mount_path;
use hivekeep_sandbox_gvisor::ports::lifecycle_log;
use hivekeep_sandbox_gvisor::wakeup_probe::ACTOR_VETH_IP;
use hivekeep_sandbox_gvisor::{mounts, validation};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::actor_cgroup::ActorLeaf;
use crate::actor_path::{oci_bundle_path, rootfs_upper_dir};
use crate::ch::{self, LaunchOptions};
use crate::child::Process;
use crate::envelope::{check_resource_envelope, ContainerLimits, GuestEnvelope};
use crate::fidelity::validate_runtime_asset_paths;
use crate::guest_net::{configure_guest_network, ACTOR_GUEST_MAC, BOOT_TAP};
use crate::guest_spec::{self, guest_container_id, HOSTNAME};
use crate::kata::agent::{debug_console_dump, AgentClient};
use crate::kata::overlay::{self, start_virtiofsd, VirtiofsdOptions};
use crate::kata::paths::{
    console_log_path, serial_log_path, shared_dir, virtiofsd_socket_path, vm_dir, vsock_socket_path,
};
use crate::rootfs_upper::{container_names, stage_rootfs_upper};
use crate::service::{GuestStatsTarget, RunningActor, WorkerService};
use crate::sizing::SandboxSize;
use crate::vm_boot::{
    self, agent_init, build_vm_config, kernel_params, resolve_guest_mem_mib, resolve_runtime,
    ResolvedRuntime, VmSpec,
};
use crate::{actor_log, volumes, wakeup_probe};

/// Cold boots tried when the VM stops before the agent answers: one retry
/// covers a transient guest death on a contended host; beyond that the fault
/// is not transient.
pub const COLD_BOOT_ATTEMPTS: u32 = 2;
/// How long one agent dial may take.
pub const AGENT_DIAL_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(300);
/// The pause between agent dials. What dominates is the guest not listening
/// yet, so the poll should cost a fraction of that.
pub const AGENT_DIAL_INTERVAL: Duration = Duration::from_millis(20);
/// The message of a guest that stopped before its agent answered. A cold
/// boot has no observable effect until the agent runs containers, so it can
/// start over on this.
pub const GUEST_STOPPED: &str = "micro-VM stopped before the kata-agent answered";
/// How long cleanup after a failure may take.
pub(crate) const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

/// Whether `e` is, or wraps, a guest that stopped before its agent answered.
pub fn is_guest_stopped(e: &Error) -> bool {
    e.to_string().contains(GUEST_STOPPED)
}

/// What a cold boot needs about an Actor, from a Run request or from a
/// Restore whose fidelity leaves the workload to cold-start.
#[derive(Clone)]
pub(crate) struct BootParams {
    pub attribution: ActorAttribution,
    pub actor_dirs: pb::ActorDirs,
    pub containers: Vec<pb::Container>,
    pub asset_paths: HashMap<String, String>,
    /// `None` unless egress goes through the tunnel.
    pub egress_gateway: Option<pb::EgressGateway>,
    /// The Actor's declared limits, which size the VM.
    pub size: SandboxSize,
    /// A snapshot whose rootfs-upper tars the boot re-materializes.
    pub rootfs_upper_snapshot_dir: Option<PathBuf>,
}

/// One container prepared for the shared micro-VM.
pub(crate) struct ActorContainer {
    pub name: String,
    /// The guest container id, also its directory in the share.
    pub id: String,
    pub bundle: PathBuf,
    /// The overlay's lower.
    pub bundle_rootfs: PathBuf,
    pub spec: guest_spec::Spec,
    pub image_mounts: Vec<pb::VolumeMount>,
}

/// The guest container ids of the containers, in order: what graceful
/// shutdown signals and what the stats sum.
pub(crate) fn workload_ids(ctrs: &[ActorContainer]) -> Vec<String> {
    ctrs.iter().map(|c| c.id.clone()).collect()
}

/// Waits for `path` to exist, up to `d`.
pub(crate) async fn wait_for_file(path: &Path, d: Duration) -> bool {
    let end = tokio::time::Instant::now() + d;
    loop {
        if path.exists() {
            return true;
        }
        if tokio::time::Instant::now() >= end {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Polls the agent's hybrid-vsock CONNECT until it answers or `timeout`
/// passes. A missing socket ends the poll at once: the caller waited for it
/// to appear, and the VMM removes it when the VM stops, so the guest died.
pub async fn dial_agent_retry(
    vsock: &Path,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<AgentClient> {
    let start = Instant::now();
    let mut last: Option<io::Error> = None;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        match AgentClient::dial(vsock, AGENT_DIAL_ATTEMPT_TIMEOUT).await {
            Ok(ac) => {
                tracing::info!(attempts = attempt, elapsed = ?start.elapsed(), "kata-agent answered");
                return Ok(ac);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(Error::Context {
                    context: format!(
                        "{GUEST_STOPPED} (cloud-hypervisor removed {})",
                        vsock.display()
                    ),
                    source: Box::new(Error::io("dialing the agent", e)),
                });
            }
            Err(e) => {
                if last.is_none() {
                    tracing::debug!(err = %e, attempt_took = ?start.elapsed(), "kata-agent not answering yet");
                }
                last = Some(e);
            }
        }
        if start.elapsed() >= timeout {
            return Err(Error::io(
                "dialing the agent",
                last.unwrap_or_else(|| io::Error::other("timed out")),
            ));
        }
        tokio::select! {
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            _ = tokio::time::sleep(AGENT_DIAL_INTERVAL) => {}
        }
    }
}

/// The last `n` bytes of `s`.
pub(crate) fn tail(s: &str, n: usize) -> &str {
    if s.len() <= n {
        return s;
    }
    let mut i = s.len() - n;
    while !s.is_char_boundary(i) {
        i += 1;
    }
    &s[i..]
}

/// Logs what the host recorded about a guest that never reached its agent:
/// the console tail and virtiofsd's log (the VMM stops the VM when a
/// vhost-user backend dies, which leaves the console silent).
pub(crate) fn log_guest_boot_diagnostics(uid: &str, console: &Path) {
    for (name, path) in [
        ("console", console.to_path_buf()),
        ("serial", serial_log_path(uid)),
        ("virtiofsd", virtiofsd_log_path(uid)),
    ] {
        if let Ok(b) = std::fs::read_to_string(&path) {
            if !b.is_empty() {
                tracing::error!(
                    log = name,
                    tail = tail(&b, 3000),
                    "agent dial failed; guest boot diagnostics"
                );
            }
        }
    }
}

/// Where virtiofsd logs.
pub(crate) fn virtiofsd_log_path(id: &str) -> PathBuf {
    vm_dir(id).join("virtiofsd.log")
}

impl WorkerService {
    /// RunWorkload.
    pub async fn run_workload(
        self: &Arc<Self>,
        req: pb::RunWorkloadRequest,
        cancel: CancellationToken,
    ) -> std::result::Result<pb::RunWorkloadResponse, tonic::Status> {
        validation::into_status(validation::run_workload_request(&req))?;
        let epoch = self.epoch.require()?;
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
        let _rpc = self.begin_rpc(&uid, Rpc::Run, Some(cancel.clone()))?;
        let attribution = req.attribution();
        self.cfg.tunnel.deactivate(&attribution).await?;
        let p = BootParams {
            attribution: attribution.clone(),
            actor_dirs: req.actor_dirs.clone().unwrap_or_default(),
            containers: req.spec.clone().unwrap_or_default().containers,
            asset_paths: req.runtime_asset_paths.clone(),
            egress_gateway: req.egress_gateway.clone(),
            size: SandboxSize::from_limits(req.resources.as_ref().and_then(|r| r.limits.as_ref())),
            rootfs_upper_snapshot_dir: None,
        };
        lifecycle_log("Actor starting", &attribution);
        self.end_previous_activation(&uid, Some(&p.actor_dirs))
            .await?;
        // Hosted before boot, so stats include the startup.
        self.host_actor(&attribution, false).await?;
        if let Err(e) = self.cold_boot_actor_retrying(&p, &cancel).await {
            let _ = tokio::time::timeout(CLEANUP_TIMEOUT, self.unhost_actor(&uid)).await;
            return Err(e.into());
        }
        lifecycle_log("Actor started", &attribution);
        tracing::info!(id = %uid, "Actor started (overlay rootfs)");
        Ok(pb::RunWorkloadResponse {
            worker_epoch: epoch,
        })
    }

    /// Cold-boots, retrying when the VM stopped before its agent answered:
    /// such a guest ran none of the Actor's containers, and the failed
    /// attempt tore everything down.
    pub(crate) async fn cold_boot_actor_retrying(
        self: &Arc<Self>,
        p: &BootParams,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let mut attempt = 1;
        loop {
            match self.cold_boot_actor(p, cancel).await {
                Err(e) if attempt < COLD_BOOT_ATTEMPTS && is_guest_stopped(&e) => {
                    tracing::warn!(id = %p.attribution.uid, attempt, err = %e, "Micro-VM stopped before the kata-agent answered; retrying cold boot");
                    // The failed attempt retired the network; rebuild it.
                    self.host_actor(&p.attribution, false).await?;
                    attempt += 1;
                }
                r => return r,
            }
        }
    }

    /// Boots the Actor's VM from scratch and starts its containers. The
    /// caller holds the lifecycle lock.
    async fn cold_boot_actor(
        self: &Arc<Self>,
        p: &BootParams,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let (kernel, image) = vm_boot::kernel_and_image(&p.asset_paths).map_err(Error::Msg)?;
        let rr = resolve_runtime(&p.asset_paths, &self.cfg.ch_binary);
        let egress = self
            .cfg
            .tunnel
            .prepare_egress(&p.attribution, p.egress_gateway.as_ref())
            .await?;
        let uid = p.attribution.uid.clone();
        self.cleanup_sandbox_state(&uid).await;
        let res = async {
            std::fs::create_dir_all(vm_dir(&uid)).context("while creating VM dir")?;
            // Stage the upper in the background, hidden behind the bundle
            // preparation, and join it before the overlay mounts.
            let (d, snap, names) = (
                p.actor_dirs.clone(),
                p.rootfs_upper_snapshot_dir.clone(),
                container_names(&p.containers),
            );
            let mut upper = Some(tokio::task::spawn_blocking(move || {
                stage_rootfs_upper(&d, snap.as_deref(), &names)
            }));
            let r = self
                .cold_boot_steps(p, &rr, &kernel, &image, egress, &mut upper, cancel)
                .await;
            if let Some(h) = upper.take() {
                let _ = h.await;
            }
            r
        }
        .await;
        if res.is_err() {
            let deactivate = self.cfg.tunnel.deactivate(&p.attribution);
            if let Ok(Err(e)) = tokio::time::timeout(CLEANUP_TIMEOUT, deactivate).await {
                tracing::warn!(err = %e, "Failed to deactivate actor networking after Run failure");
            }
            if let Err(e) = mounts::unmount_all_under(Path::new(&p.actor_dirs.oci_bundle_dir)) {
                tracing::warn!(err = %e, "Failed to unmount bundle rootfs overlays after Run failure");
            }
        }
        res
    }

    #[allow(clippy::too_many_arguments)]
    async fn cold_boot_steps(
        self: &Arc<Self>,
        p: &BootParams,
        rr: &ResolvedRuntime,
        kernel: &str,
        image: &str,
        egress: hivekeep_sandbox_gvisor::ports::Egress,
        upper: &mut Option<JoinHandle<io::Result<()>>>,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let uid = p.attribution.uid.as_str();
        // Egress before the guest boots, ingress after the wakeup probes.
        self.cfg.tunnel.activate_egress(&p.attribution, egress)?;
        let vcpus = p.size.vcpus();
        if vcpus == 0 {
            return Err(Error::msg(
                "actor has no cpu limit to size the micro-VM's vCPUs",
            ));
        }
        let mem_mib = resolve_guest_mem_mib(p.size.memory_bytes).map_err(Error::Msg)?;
        let ctrs = self.build_actor_containers(uid, &p.actor_dirs, &p.containers)?;
        // Limits the guest can never satisfy are refused before the agent
        // sees them. Restore does not repeat this: the template is
        // immutable, so its limits were checked when its golden was built.
        let limits: Vec<ContainerLimits<'_>> = ctrs
            .iter()
            .map(|c| {
                let r = c.spec.linux.as_ref().and_then(|l| l.resources.as_ref());
                ContainerLimits {
                    name: &c.name,
                    memory: r.and_then(|r| r.memory.as_ref()).and_then(|m| m.limit),
                    cpu: r.and_then(|r| r.cpu.as_ref()),
                }
            })
            .collect();
        check_resource_envelope(&limits, GuestEnvelope { mem_mib, vcpus })?;
        if let Some(h) = upper.take() {
            h.await
                .map_err(|e| Error::msg(e.to_string()))?
                .context("while staging the rootfs upper")?;
        }
        let leaf = ActorLeaf::open(&self.cfg.cgroup_root, uid, p.size.milli_cpu)
            .context("while opening the actor's cgroup")?;
        let vfsd = self
            .stage_merged_rootfs(rr, uid, &p.actor_dirs, &ctrs, &p.containers, &leaf, cancel)
            .await?;
        let mut started: Vec<Arc<Process>> = vec![vfsd.clone()];
        let mut agent: Option<AgentClient> = None;
        let res = async {
            let api_socket = vm_dir(uid).join("clh-api.sock");
            let (ch_proc, client) = ch::launch_vmm(
                LaunchOptions {
                    binary: &rr.ch_binary,
                    api_socket: api_socket.clone(),
                    leaf: Some(&leaf),
                },
                cancel,
            )
            .await
            .context("while launching VMM")?;
            started.push(ch_proc.clone());
            let console = console_log_path(uid);
            let cfg = build_vm_config(&VmSpec {
                id: uid,
                kernel,
                image,
                kernel_params: &kernel_params(self.cfg.guest_debug),
                console_log: &console.to_string_lossy(),
                mem_mib,
                vcpus,
                agent_init: agent_init(&client.info()),
                debug: self.cfg.guest_debug,
            });
            client.create_vm(&cfg).await.context("while creating VM")?;
            let taps = self
                .cfg
                .taps
                .create_tap(uid, BOOT_TAP, 1)
                .await
                .context("while building tap")?;
            let fds: Vec<i32> = taps.iter().map(|f| f.as_raw_fd()).collect();
            // The VMM dups the fds it adopts; ours close below.
            client
                .add_net_with_fds(ACTOR_GUEST_MAC, 2 * fds.len(), &fds)
                .await
                .context("while adding net device")?;
            drop(taps);
            client.boot_vm().await.context("while booting VM")?;
            tracing::info!(id = uid, api = %api_socket.display(), "Micro-VM booted");
            let t_booted = Instant::now();
            let vsock = vsock_socket_path(uid);
            if !wait_for_file(&vsock, Duration::from_secs(15)).await {
                return Err(Error::msg(format!(
                    "kata-agent vsock socket {} did not appear",
                    vsock.display()
                )));
            }
            let t_vsock = Instant::now();
            let ac = match dial_agent_retry(&vsock, Duration::from_secs(60), cancel).await {
                Ok(ac) => ac,
                Err(e) => {
                    log_guest_boot_diagnostics(uid, &console);
                    return Err(Error::Context {
                        context: "while dialing kata-agent".into(),
                        source: Box::new(e),
                    });
                }
            };
            agent = Some(ac.clone());
            let t_dialed = Instant::now();
            self.start_actor_containers(&ac, uid, &vsock, &ctrs).await?;
            let t_containers = Instant::now();
            wakeup_probe::wait_all(
                &p.containers,
                ACTOR_VETH_IP,
                Some(self.sandbox_dialer(uid)),
                cancel,
            )
            .await
            .context("while waiting for container wakeup probe")?;
            tracing::info!(
                id = uid,
                vsock_wait = ?t_vsock - t_booted,
                agent_dial = ?t_dialed - t_vsock,
                containers = ?t_containers - t_dialed,
                wakeup_probe = ?t_containers.elapsed(),
                since_boot = ?t_booted.elapsed(),
                "Actor boot phases"
            );
            let ra = Arc::new(RunningActor {
                base_id: uid.to_string(),
                ch_proc: Some(ch_proc),
                vfsd_proc: Some(vfsd.clone()),
                api_socket,
                stopping: Default::default(),
                restore_source_dir: None,
                preserve_restore_source: false,
                snapshot_is_self_contained: false,
                guest_agent: parking_lot::Mutex::new(Some(ac.clone())),
                workload_ids: workload_ids(&ctrs),
            });
            self.cfg
                .tunnel
                .activate_ingress(&p.attribution, self.sandbox_dialer(uid))?;
            Ok(ra)
        }
        .await;
        let ra = match res {
            Ok(ra) => ra,
            Err(e) => {
                if let Some(ac) = agent {
                    ac.close();
                }
                for proc in started.iter().rev() {
                    proc.stop().await;
                }
                return Err(e);
            }
        };
        let ac = agent.expect("a booted actor has an agent");
        self.publish_vm(&p.attribution, &p.actor_dirs, ra, &ac, &ctrs);
        Ok(())
    }

    /// Publishes a booted or restored VM: the running record and its crash
    /// watch, log forwarding, the stats target, and the initial reading.
    /// Runs past every error return, so nothing points at a connection a
    /// failed attempt closed.
    pub(crate) fn publish_vm(
        self: &Arc<Self>,
        a: &ActorAttribution,
        dirs: &pb::ActorDirs,
        ra: Arc<RunningActor>,
        ac: &AgentClient,
        ctrs: &[ActorContainer],
    ) {
        let uid = &a.uid;
        self.set_running_vm(uid, ra.clone());
        {
            let (s, a, d, ra) = (self.clone(), a.clone(), dirs.clone(), ra.clone());
            tokio::spawn(async move { s.watch_vm(a, d, ra).await });
        }
        for c in ctrs {
            actor_log::start(ac, a, &c.id, &c.name);
        }
        self.set_guest_stats(
            uid,
            Some(Arc::new(GuestStatsTarget {
                actor_uid: uid.clone(),
                agent: Arc::new(ac.clone()),
                workload_ids: ra.workload_ids.clone(),
            })),
        );
        // Looked up under the caller's lock: this activation even if the
        // read outlives the RPC.
        if let Some(h) = self.lookup_actor(uid) {
            let s = self.clone();
            tokio::spawn(async move { s.record_initial(&h).await });
        }
    }

    /// Prepares each container for the shared micro-VM: its OCI spec shaped
    /// for the guest, its bundle rootfs composed (the overlay's lower) and the
    /// guest's `/etc` files written into it. Run and Restore both come here.
    pub(crate) fn build_actor_containers(
        &self,
        uid: &str,
        dirs: &pb::ActorDirs,
        containers: &[pb::Container],
    ) -> Result<Vec<ActorContainer>> {
        containers
            .iter()
            .map(|c| {
                let bundle = oci_bundle_path(dirs, &c.name);
                let mut spec = guest_spec::build(uid, dirs, c);
                guest_spec::shape_micro_vm(&mut spec, dirs, &c.name).map_err(|e| {
                    Error::msg(format!("while shaping the OCI spec for {:?}: {e}", c.name))
                })?;
                self.cfg
                    .rootfs
                    .setup_bundle_rootfs(&bundle)
                    .with_context(|| format!("while composing rootfs for {:?}", c.name))?;
                let bundle_rootfs = bundle.join("rootfs");
                crate::etc_files::write_rootfs_etc_files(&bundle_rootfs, HOSTNAME)
                    .with_context(|| format!("while writing guest /etc files for {:?}", c.name))?;
                Ok(ActorContainer {
                    name: c.name.clone(),
                    id: guest_container_id(&c.name),
                    bundle,
                    bundle_rootfs,
                    spec,
                    image_mounts: c.image_volume_mounts.clone(),
                })
            })
            .collect()
    }

    /// Mounts each container's merged rootfs and the volume binds into the
    /// share, then starts the one virtiofsd serving it. Runs after the
    /// sandbox state sweep and the upper staging.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_merged_rootfs(
        &self,
        rr: &ResolvedRuntime,
        id: &str,
        dirs: &pb::ActorDirs,
        ctrs: &[ActorContainer],
        containers: &[pb::Container],
        leaf: &ActorLeaf,
        cancel: &CancellationToken,
    ) -> Result<Arc<Process>> {
        let upper_base = rootfs_upper_dir(dirs);
        for c in ctrs {
            overlay::stage_merged_rootfs(&c.bundle_rootfs, &upper_base, &c.name, id, &c.id)
                .with_context(|| format!("while staging merged rootfs for {:?}", c.name))?;
            for m in &c.image_mounts {
                overlay::stage_image_volume(
                    &image_volume_mount_path(&c.bundle, &m.name),
                    id,
                    &c.id,
                    &m.name,
                )
                .with_context(|| {
                    format!("while staging image volume {:?} for {:?}", m.name, c.name)
                })?;
            }
        }
        if volumes::has_durable_volumes(containers) {
            volumes::stage_durable_volumes(id, Path::new(&dirs.durable_dir_volume_mounts_dir))
                .context("while staging durable-dir volumes")?;
        }
        if volumes::has_csi_volumes(containers) {
            volumes::stage_csi_volumes(id, Path::new(&dirs.external_volume_mounts_dir))
                .context("while staging CSI volumes")?;
        }
        if volumes::has_system_info_volumes(containers) {
            volumes::stage_system_info_volumes(id, Path::new(&dirs.system_info_volume_roots_dir))
                .context("while staging system-info volumes")?;
        }
        start_virtiofsd(
            VirtiofsdOptions {
                binary: &rr.virtiofsd,
                socket: virtiofsd_socket_path(id),
                shared: shared_dir(id),
                log: Some(virtiofsd_log_path(id)),
                leaf: Some(leaf),
            },
            cancel,
        )
        .await
        .context("while starting virtiofsd")
    }

    /// The post-boot agent setup: the sandbox (with the share mounted), the
    /// guest network, then each container on its merged rootfs.
    pub(crate) async fn start_actor_containers(
        &self,
        ac: &AgentClient,
        id: &str,
        vsock: &Path,
        ctrs: &[ActorContainer],
    ) -> Result<()> {
        let t0 = Instant::now();
        let hostname = ctrs
            .first()
            .map_or(HOSTNAME.to_string(), |c| c.spec.hostname.clone());
        tokio::time::timeout(
            Duration::from_secs(20),
            ac.create_sandbox_for_actor(id, &hostname),
        )
        .await
        .map_err(|_| Error::DeadlineExceeded)
        .and_then(|r| r.map_err(Error::from))
        .context("while creating agent sandbox")?;
        let t_sandbox = Instant::now();
        let mtu = self.cfg.taps.tap_mtu(id, BOOT_TAP);
        let net = tokio::time::timeout(Duration::from_secs(20), configure_guest_network(ac, mtu))
            .await
            .map_err(|_| Error::DeadlineExceeded)
            .and_then(|r| r.map_err(Error::from));
        if let Err(e) = net {
            if self.cfg.guest_debug {
                let dump = debug_console_dump(vsock, "ip addr 2>&1; echo '== route =='; ip route 2>&1; echo '== neigh =='; ip neigh 2>&1").await;
                tracing::error!(dump, "guest network config failed; dump");
            }
            return Err(Error::Context {
                context: "while configuring guest network".into(),
                source: Box::new(e),
            });
        }
        let t_network = Instant::now();
        for c in ctrs {
            let r = tokio::time::timeout(
                Duration::from_secs(30),
                ac.start_rootfs_container(&c.id, &c.spec),
            )
            .await
            .map_err(|_| Error::DeadlineExceeded)
            .and_then(|r| r.map_err(Error::from));
            if let Err(e) = r {
                if self.cfg.guest_debug {
                    let dump = debug_console_dump(
                        vsock,
                        &format!(
                            "echo '== shared/containers =='; ls -la {gs}/ 2>&1 | head -40; echo '== rootfs =='; ls {gs}/{id}/rootfs/ 2>&1 | head; echo '== mounts =='; grep -E 'kata|virtiofs' /proc/mounts 2>&1",
                            gs = guest_spec::GUEST_SHARED_DIR,
                            id = c.id
                        ),
                    )
                    .await;
                    tracing::error!(container = %c.name, dump, "rootfs container failed; dump");
                }
                return Err(Error::Context {
                    context: format!("while starting rootfs container {:?}", c.name),
                    source: Box::new(e),
                });
            }
        }
        tracing::info!(id, sandbox = ?t_sandbox - t0, network = ?t_network - t_sandbox, containers = ?t_network.elapsed(), container_count = ctrs.len(), "Agent setup phases");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::ServiceConfig;

    #[tokio::test]
    async fn a_missing_socket_means_the_guest_stopped() {
        let d = tempfile::tempdir().unwrap();
        let start = Instant::now();
        let err = dial_agent_retry(
            &d.path().join("clh.sock"),
            Duration::from_secs(30),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(is_guest_stopped(&err), "{err}");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "polled for the whole timeout"
        );
    }

    fn dead_socket(dir: &Path) -> PathBuf {
        let p = dir.join("clh.sock");
        let l = std::os::unix::net::UnixListener::bind(&p).unwrap();
        drop(l);
        assert!(p.exists());
        p
    }

    #[tokio::test]
    async fn a_guest_not_listening_yet_is_polled() {
        let d = tempfile::tempdir().unwrap();
        let p = dead_socket(d.path());
        let timeout = Duration::from_millis(700);
        let start = Instant::now();
        let err = dial_agent_retry(&p, timeout, &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(!is_guest_stopped(&err), "{err}");
        assert!(
            start.elapsed() >= timeout,
            "gave up after {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn the_dial_honours_cancellation() {
        let d = tempfile::tempdir().unwrap();
        let p = dead_socket(d.path());
        let cancel = CancellationToken::new();
        cancel.cancel();
        let err = dial_agent_retry(&p, Duration::from_secs(60), &cancel)
            .await
            .unwrap_err();
        assert!(err.is_context_error(), "{err}");
    }

    #[test]
    fn workload_ids_name_the_guest_containers() {
        let d = tempfile::tempdir().unwrap();
        let dirs = pb::ActorDirs {
            oci_bundle_dir: d.path().to_string_lossy().into(),
            ..Default::default()
        };
        let mut containers = Vec::new();
        for name in ["counter", "w"] {
            std::fs::create_dir_all(oci_bundle_path(&dirs, name).join("rootfs")).unwrap();
            containers.push(pb::Container {
                name: name.into(),
                container_spec: Some(pb::ContainerSpec {
                    args: vec!["/app".into()],
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
        let s = WorkerService::new(ServiceConfig::default());
        let ctrs = s.build_actor_containers("uid", &dirs, &containers).unwrap();
        assert_eq!(workload_ids(&ctrs), ["c_counter", "c_w"]);
    }

    #[test]
    fn tails() {
        assert_eq!(tail("abcdef", 3), "def");
        assert_eq!(tail("ab", 3), "ab");
    }
}
