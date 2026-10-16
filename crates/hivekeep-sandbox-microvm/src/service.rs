//! The worker's state: the hosted Actors, their micro-VMs and guest
//! connections, admission against the capacity ceiling, and the seams
//! (tunnel, taps, rootfs composition, usage sink, crash reporter) the node
//! plugs in.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hivekeep_core::resource::ActorAttribution;
use hivekeep_sandbox_gvisor::actor_lock::{InFlight, InFlightGuard, Locks, Rpc};
use hivekeep_sandbox_gvisor::error::{status, Error, Result};
use hivekeep_sandbox_gvisor::node_path;
use hivekeep_sandbox_gvisor::ports::{
    CrashReporter, Dialer, Epoch, NoTunnel, SandboxNetwork, Tunnel, UsageSink,
};
use hivekeep_sandbox_gvisor::rootfs::{PlainRootfs, RootfsComposer};
use parking_lot::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use crate::activation::Activation;
use crate::actor_cgroup::{self, DEFAULT_CGROUP_ROOT};
use crate::child::Process;
use crate::guest_net::{HostTaps, TapFactory};
use crate::kata::agent::{AgentClient, ContainerStatsReader};
use crate::kata::cleanup::cleanup_sandbox_state;

/// How many guests sweeps and initial readings ask at once.
pub const STATS_FAN_OUT: usize = 32;

/// What the worker is configured with.
pub struct ServiceConfig {
    /// The worker pod's UID.
    pub pod_uid: String,
    /// The VMM binary when a request brings none.
    pub ch_binary: String,
    /// Boot guests with the agent's debug log and console.
    pub guest_debug: bool,
    /// How many Actors this worker hosts at once.
    pub max_actors: usize,
    /// Per-Actor networking.
    pub tunnel: Arc<dyn Tunnel>,
    /// The taps the VMM adopts.
    pub taps: Arc<dyn TapFactory>,
    /// Composes bundle rootfs trees.
    pub rootfs: Arc<dyn RootfsComposer>,
    /// Where usage records go; none writes nothing.
    pub usage: Option<Arc<dyn UsageSink>>,
    /// Reports Actors whose VM died; none reports nothing.
    pub crash_reporter: Option<Arc<dyn CrashReporter>>,
    /// The worker pod's cgroup scope.
    pub cgroup_root: PathBuf,
    /// The keeper's per-Actor directories.
    pub actors_dir: PathBuf,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        ServiceConfig {
            pod_uid: String::new(),
            ch_binary: "cloud-hypervisor".into(),
            guest_debug: false,
            max_actors: 1000,
            tunnel: Arc::new(NoTunnel),
            taps: Arc::new(HostTaps),
            rootfs: Arc::new(PlainRootfs),
            usage: None,
            crash_reporter: None,
            cgroup_root: DEFAULT_CGROUP_ROOT.into(),
            actors_dir: node_path::actors_dir(),
        }
    }
}

/// One Actor's live micro-VM. The worker owns the VMM and virtiofsd
/// processes directly, booted by Run or relaunched by Restore.
pub struct RunningActor {
    /// The frozen base sandbox id of the Actor's restore lineage: its own uid
    /// after a cold boot, the snapshot's after a restore. Checkpoints write it
    /// back so the chain survives suspend, resume, suspend.
    pub base_id: String,
    /// The VMM.
    pub ch_proc: Option<Arc<Process>>,
    /// The virtiofsd serving the share; stopped after the VMM.
    pub vfsd_proc: Option<Arc<Process>>,
    /// The VMM's API socket.
    pub api_socket: PathBuf,
    /// Set once teardown starts, so the crash watch can tell an expected exit.
    pub stopping: AtomicBool,
    /// The snapshot an on-demand restore pages guest RAM from; the next
    /// checkpoint overlays its sparse snapshot onto it.
    pub restore_source_dir: Option<PathBuf>,
    /// The restore source is a preserved snapshot: merge by copying.
    pub preserve_restore_source: bool,
    /// Restored eagerly: every page is resident, so the next snapshot is
    /// complete on its own.
    pub snapshot_is_self_contained: bool,
    /// The agent connection kept past boot for log forwarding and stats;
    /// closed by teardown.
    pub guest_agent: Mutex<Option<AgentClient>>,
    /// The guest container ids of the workloads, for graceful shutdown.
    pub workload_ids: Vec<String>,
}

impl RunningActor {
    /// A record with only processes, for tests and partial boots.
    pub fn with_processes(ch: Option<Arc<Process>>, vfsd: Option<Arc<Process>>) -> RunningActor {
        RunningActor {
            base_id: String::new(),
            ch_proc: ch,
            vfsd_proc: vfsd,
            api_socket: PathBuf::new(),
            stopping: AtomicBool::new(false),
            restore_source_dir: None,
            preserve_restore_source: false,
            snapshot_is_self_contained: false,
            guest_agent: Mutex::new(None),
            workload_ids: vec![],
        }
    }
}

/// Everything a stats read needs to sample a live guest, published once the
/// containers are up and cleared before teardown closes the connection.
pub struct GuestStatsTarget {
    /// The Actor these containers belong to, checked against the
    /// attribution before anything is reported.
    pub actor_uid: String,
    /// The agent connection, shared with log forwarding.
    pub agent: Arc<dyn ContainerStatsReader>,
    /// The guest containers to sum.
    pub workload_ids: Vec<String>,
}

#[derive(Default)]
pub(crate) struct HostedMut {
    pub network: Option<Arc<dyn SandboxNetwork>>,
    pub vm: Option<Arc<RunningActor>>,
    pub guest: Option<Arc<GuestStatsTarget>>,
}

/// One hosted Actor: one activation. A re-hosted Actor is a new record, so
/// pointer identity tells activations apart.
pub struct HostedActor {
    /// Who it is; fixed at admission.
    pub attribution: ActorAttribution,
    /// The activation's usage; fixed at admission.
    pub usage: Activation,
    pub(crate) inner: Mutex<HostedMut>,
}

impl HostedActor {
    /// A record for `attribution` with `usage`.
    pub fn new(attribution: ActorAttribution, usage: Activation) -> Arc<HostedActor> {
        Arc::new(HostedActor {
            attribution,
            usage,
            inner: Mutex::default(),
        })
    }
}

/// An admitted Actor's record and the network it had before, if any.
pub(crate) type Admitted = (Arc<HostedActor>, Option<Arc<dyn SandboxNetwork>>);

#[derive(Default)]
pub(crate) struct State {
    pub actors: HashMap<String, Arc<HostedActor>>,
    /// Actors whose network cleanup is still running; they hold capacity.
    pub draining: usize,
}

/// The micro-VM worker.
pub struct WorkerService {
    pub(crate) cfg: ServiceConfig,
    pub(crate) locks: Locks,
    pub(crate) in_flight: InFlight,
    /// Set on SIGTERM; new workload RPCs are then refused.
    pub(crate) shutting_down: AtomicBool,
    /// The epoch this process registered with.
    pub epoch: Arc<Epoch>,
    pub(crate) state: RwLock<State>,
    /// Bounds the guest reads of sweeps and initial readings.
    pub(crate) guest_slots: Arc<tokio::sync::Semaphore>,
}

impl WorkerService {
    /// A worker with `cfg`.
    pub fn new(cfg: ServiceConfig) -> Arc<WorkerService> {
        Arc::new(WorkerService {
            cfg,
            locks: Locks::new(),
            in_flight: InFlight::new(),
            shutting_down: AtomicBool::new(false),
            epoch: Arc::default(),
            state: RwLock::default(),
            guest_slots: Arc::new(tokio::sync::Semaphore::new(STATS_FAN_OUT)),
        })
    }

    /// The configuration.
    pub fn config(&self) -> &ServiceConfig {
        &self.cfg
    }

    /// Registers an RPC before checking for shutdown, so shutdown cannot
    /// miss it.
    pub(crate) fn begin_rpc(
        &self,
        uid: &str,
        rpc: Rpc,
        cancel: Option<CancellationToken>,
    ) -> std::result::Result<InFlightGuard<'_>, tonic::Status> {
        let g = self.in_flight.add(uid, rpc, cancel);
        self.reject_if_draining()?;
        Ok(g)
    }

    /// UNAVAILABLE once graceful shutdown began, so the control plane
    /// reschedules.
    pub(crate) fn reject_if_draining(&self) -> std::result::Result<(), tonic::Status> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(status::unavailable(
                "worker draining: not accepting new workloads",
            ));
        }
        Ok(())
    }

    /// Reserves capacity for an Actor. One already hosted keeps its slot (so
    /// a retry cannot lose it to another Actor) and its old network is
    /// returned for the caller to close. `resumes_guest` is set when the
    /// guest and its CPU counters come back from a snapshot.
    pub(crate) fn admit_actor(
        &self,
        attribution: &ActorAttribution,
        resumes_guest: bool,
    ) -> std::result::Result<Admitted, tonic::Status> {
        let mut st = self.state.write();
        let stale = match st.actors.get(&attribution.uid) {
            Some(old) => old.inner.lock().network.clone(),
            None if st.actors.len() + st.draining >= self.cfg.max_actors => {
                return Err(status::resource_exhausted(format!(
                    "worker is full: {} actors",
                    self.cfg.max_actors
                )));
            }
            None => None,
        };
        let hosted = HostedActor::new(
            attribution.clone(),
            Activation::new(SystemTime::now(), resumes_guest),
        );
        st.actors.insert(attribution.uid.clone(), hosted.clone());
        Ok((hosted, stale))
    }

    /// Admits the Actor and sets up its network, replacing a stale one.
    pub(crate) async fn host_actor(
        &self,
        attribution: &ActorAttribution,
        resumes_guest: bool,
    ) -> Result<Arc<HostedActor>> {
        let uid = &attribution.uid;
        let (hosted, stale) = self.admit_actor(attribution, resumes_guest)?;
        let forget = || {
            let mut st = self.state.write();
            if st.actors.get(uid).is_some_and(|h| Arc::ptr_eq(h, &hosted)) {
                st.actors.remove(uid);
            }
        };
        if let Some(stale) = stale {
            if let Err(e) = stale.close().await {
                forget();
                return Err(Error::msg(format!(
                    "while clearing stale state for actor {uid}: {e}"
                )));
            }
        }
        match self.cfg.tunnel.serve_sandbox(attribution).await {
            Ok(n) => {
                hosted.inner.lock().network = Some(n);
                Ok(hosted)
            }
            Err(e) => {
                forget();
                Err(Error::msg(format!(
                    "while setting up the sandbox network: {e}"
                )))
            }
        }
    }

    /// Kills what is left of the Actor's host processes and clears its
    /// sandbox directories.
    pub(crate) async fn cleanup_sandbox_state(&self, uid: &str) {
        if let Err(e) = actor_cgroup::kill_leaf(
            &self.cfg.cgroup_root,
            uid,
            Duration::from_secs(5),
            &CancellationToken::new(),
        )
        .await
        {
            tracing::warn!(err = %e, "Failed to kill the actor's leftover processes");
        }
        let id = uid.to_string();
        let _ = tokio::task::spawn_blocking(move || cleanup_sandbox_state(&id)).await;
    }

    /// Removes an Actor, its network and its cgroup. Repeated calls are fine;
    /// the caller stops the VM first. Capacity is held until the network is
    /// gone.
    pub(crate) async fn unhost_actor(&self, uid: &str) -> Result<()> {
        let hosted = {
            let mut st = self.state.write();
            let h = st.actors.remove(uid);
            if h.is_some() {
                st.draining += 1;
            }
            h
        };
        let Some(hosted) = hosted else { return Ok(()) };
        struct Release<'a>(&'a WorkerService);
        impl Drop for Release<'_> {
            fn drop(&mut self) {
                self.0.state.write().draining -= 1;
            }
        }
        let _release = Release(self);
        if let Err(e) = actor_cgroup::remove_leaf(&self.cfg.cgroup_root, uid) {
            tracing::warn!(err = %e, "Failed to remove the actor's cgroup");
        }
        let network = hosted.inner.lock().network.take();
        if let Some(n) = network {
            if let Err(e) = n.close().await {
                tracing::warn!(err = %e, "Failed to remove the sandbox network");
                return Err(e);
            }
        }
        Ok(())
    }

    /// The hosted record of `uid`, without its lifecycle lock.
    pub fn lookup_actor(&self, uid: &str) -> Option<Arc<HostedActor>> {
        self.state.read().actors.get(uid).cloned()
    }

    /// Whether `h` is still the record hosted for its Actor.
    pub(crate) fn is_current(&self, h: &Arc<HostedActor>) -> bool {
        self.lookup_actor(&h.attribution.uid)
            .is_some_and(|c| Arc::ptr_eq(&c, h))
    }

    /// Every hosted record.
    pub fn hosted_actors(&self) -> Vec<Arc<HostedActor>> {
        self.state.read().actors.values().cloned().collect()
    }

    /// Publishes the live VM of `uid`.
    pub(crate) fn set_running_vm(&self, uid: &str, vm: Arc<RunningActor>) {
        if let Some(h) = self.lookup_actor(uid) {
            h.inner.lock().vm = Some(vm);
        }
    }

    /// The live VM of `uid`.
    pub(crate) fn running_vm(&self, uid: &str) -> Option<Arc<RunningActor>> {
        self.lookup_actor(uid)?.inner.lock().vm.clone()
    }

    /// Sets or clears the stats target of `uid`.
    pub(crate) fn set_guest_stats(&self, uid: &str, guest: Option<Arc<GuestStatsTarget>>) {
        if let Some(h) = self.lookup_actor(uid) {
            h.inner.lock().guest = guest;
        }
    }

    /// `h`'s stats target.
    pub(crate) fn guest_of(&self, h: &HostedActor) -> Option<Arc<GuestStatsTarget>> {
        h.inner.lock().guest.clone()
    }

    /// Connects into `uid`'s sandbox through its network, whichever network
    /// the Actor has at the time of the dial.
    pub(crate) fn sandbox_dialer(self: &Arc<Self>, uid: &str) -> Dialer {
        let s = Arc::downgrade(self);
        let uid = uid.to_string();
        Arc::new(move |addr| {
            let (s, uid) = (s.clone(), uid.clone());
            Box::pin(async move {
                let network = s
                    .upgrade()
                    .and_then(|s| s.lookup_actor(&uid))
                    .and_then(|h| h.inner.lock().network.clone());
                match network {
                    Some(n) => n.dialer()(addr).await,
                    None => Err(std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        format!("actor {uid} is not hosted here"),
                    )),
                }
            })
        })
    }

    /// Hosts `attribution` directly, for tests of the stats paths: no network,
    /// the given stats target and activation.
    #[doc(hidden)]
    pub fn host_for_test(
        &self,
        attribution: ActorAttribution,
        guest: Option<Arc<GuestStatsTarget>>,
        usage: Activation,
    ) -> Arc<HostedActor> {
        let h = HostedActor::new(attribution, usage);
        h.inner.lock().guest = guest;
        self.state
            .write()
            .actors
            .insert(h.attribution.uid.clone(), h.clone());
        h
    }

    /// Unhosts `uid` without cleanup, for tests.
    #[doc(hidden)]
    pub fn unhost_for_test(&self, uid: &str) {
        self.state.write().actors.remove(uid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_sandbox_gvisor::attribution::uid_only;

    #[tokio::test]
    async fn readmission_keeps_a_hosted_actors_slot() {
        let s = WorkerService::new(ServiceConfig {
            max_actors: 1,
            ..Default::default()
        });
        let (first, _) = s.admit_actor(&uid_only("actor-a"), false).unwrap();
        let (again, _) = s
            .admit_actor(&uid_only("actor-a"), false)
            .expect("re-admitting a hosted actor on a full worker");
        assert!(
            !Arc::ptr_eq(&first, &again),
            "re-admission kept the old record"
        );
        let err = s.admit_actor(&uid_only("actor-b"), false).err().unwrap();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert_eq!(s.hosted_actors().len(), 1);
    }

    #[test]
    fn starts_empty() {
        let s = WorkerService::new(ServiceConfig::default());
        assert!(s.hosted_actors().is_empty());
        assert!(s
            .lookup_actor("aaaaaaaa-0000-4000-8000-000000000001")
            .is_none());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs root: creates network namespaces"]
    async fn hosting_the_same_actor_again_replaces_its_network() {
        use hivekeep_gate::netns;
        use hivekeep_sandbox_gvisor::netns_tunnel::NetnsTunnel;
        const UID: &str = "microvm-network-replace";
        let s = WorkerService::new(ServiceConfig {
            max_actors: 1,
            tunnel: Arc::new(NetnsTunnel::new(None, false)),
            ..Default::default()
        });
        let name = hivekeep_sandbox_gvisor::node_path::actor_netns_name(UID);
        let first = s.host_actor(&uid_only(UID), false).await.unwrap();
        let first_ns = netns::get_from_name(&name).unwrap();
        let second = s.host_actor(&uid_only(UID), false).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        let named = netns::get_from_name(&name).expect("the replacement is named");
        assert!(
            !named.same_namespace(&first_ns).unwrap(),
            "the name still refers to the first network"
        );
        s.unhost_actor(UID).await.unwrap();
        assert!(netns::get_from_name(&name).is_err());
    }
}
