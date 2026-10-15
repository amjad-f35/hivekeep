//! The worker service: its state, configuration and the guards every
//! lifecycle RPC passes through.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;

use crate::actor_lock::{InFlight, InFlightGuard, Locks, Rpc};
use crate::cgroup::DEFAULT_CGROUP_ROOT;
use crate::cgroup_stats::{self, Sample};
use crate::ckpt_cgroup::{default_evict, default_move_pid, Evict, MovePid};
use crate::error::status;
use crate::flags::FlagStore;
use crate::hosted::ActorTable;
use crate::node_path;
use crate::ports::{CrashReporter, Epoch, NoTunnel, Tunnel, UsageSink};
use crate::rootfs::{PlainRootfs, RootfsComposer};
use crate::runsc::{RunscFactory, RuntimeFactory};
use crate::wakeup_probe::{HttpProber, WakeupProber};

/// The whole budget for draining the worker on shutdown. It stays well under
/// the pod's termination grace period, so the escalation to SIGKILL happens
/// here rather than as a kubelet kill of the worker itself.
pub const WORKLOAD_GRACE_PERIOD: Duration = Duration::from_secs(30 * 60);

/// How long a container may take to exit after SIGKILL. Not drawn from the
/// grace period: by then the container has had its allowance.
pub const CONTAINER_KILL_TIMEOUT: Duration = Duration::from_secs(5);

/// How long teardown after a failure may take. Detached from the caller: a
/// deadline in the middle of `runsc delete` would leave the container without
/// its record.
pub const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a crash report keeps retrying.
pub const CRASH_REPORT_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Memory kept from Actors out of the worker pod's limit: the worker itself
/// and the page cache of checkpoints in flight.
pub const MEMORY_RESERVED_BYTES: u64 = 64 << 20;

/// Reads a sandbox cgroup; replaceable so tests can interleave lifecycle
/// changes with the lock-free stats reads.
pub type CgroupReader = Arc<dyn Fn(&std::path::Path) -> std::io::Result<Sample> + Send + Sync>;

/// Everything a [`WorkerService`] is built from.
pub struct ServiceConfig {
    /// How many Actors the worker hosts at once.
    pub max_actors: usize,
    /// The pod's cgroup scope.
    pub cgroup_root: PathBuf,
    /// The keeper's per-Actor directory root.
    pub actors_dir: PathBuf,
    /// Sandbox leaves get `memory.high` instead of `memory.max`.
    pub throttle_memory: bool,
    /// runsc flags.
    pub flags: Arc<FlagStore>,
    /// Builds runsc runtimes.
    pub runtimes: Arc<dyn RuntimeFactory>,
    /// Tunnel and sandbox networking.
    pub tunnel: Arc<dyn Tunnel>,
    /// Wakeup probing.
    pub prober: Arc<dyn WakeupProber>,
    /// Bundle rootfs composition.
    pub rootfs: Arc<dyn RootfsComposer>,
    /// Crash reporting; none drops reports.
    pub crash_reporter: Option<Arc<dyn CrashReporter>>,
    /// Usage records; none writes none.
    pub usage: Option<Arc<dyn UsageSink>>,
    /// Sandbox cgroup reads.
    pub read_cgroup: CgroupReader,
    /// Checkpoint cgroup pid moves.
    pub move_pid: MovePid,
    /// Restore page cache eviction.
    pub evict: Evict,
    /// Graceful shutdown budget.
    pub grace_period: Duration,
    /// Post-SIGKILL wait.
    pub container_kill_timeout: Duration,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        ServiceConfig {
            max_actors: 1000,
            cgroup_root: PathBuf::from(DEFAULT_CGROUP_ROOT),
            actors_dir: node_path::actors_dir(),
            throttle_memory: false,
            flags: FlagStore::empty(),
            runtimes: Arc::new(RunscFactory::default()),
            tunnel: Arc::new(NoTunnel),
            prober: Arc::new(HttpProber::default()),
            rootfs: Arc::new(PlainRootfs),
            crash_reporter: None,
            usage: None,
            read_cgroup: Arc::new(cgroup_stats::read),
            move_pid: default_move_pid(),
            evict: default_evict(),
            grace_period: WORKLOAD_GRACE_PERIOD,
            container_kill_timeout: CONTAINER_KILL_TIMEOUT,
        }
    }
}

/// Shepherds the gVisor sandboxes of the Actors this worker hosts.
pub struct WorkerService {
    /// Serializes lifecycle RPCs per Actor.
    pub(crate) locks: Locks,
    /// Lifecycle RPCs in flight, for shutdown.
    pub(crate) in_flight: InFlight,
    /// Hosted Actors; guards reads against lifecycle changes.
    pub(crate) table: RwLock<ActorTable>,
    /// Set once SIGTERM arrives; new workloads are refused from then on.
    pub(crate) shutting_down: AtomicBool,
    /// The epoch this process registered with.
    pub epoch: Arc<Epoch>,
    pub(crate) cfg: ServiceConfig,
}

impl WorkerService {
    /// A service with no Actors.
    pub fn new(cfg: ServiceConfig) -> Arc<Self> {
        Arc::new(WorkerService {
            locks: Locks::new(),
            in_flight: InFlight::new(),
            table: RwLock::new(ActorTable::default()),
            shutting_down: AtomicBool::new(false),
            epoch: Arc::default(),
            cfg,
        })
    }

    /// The configuration the service runs with.
    pub fn config(&self) -> &ServiceConfig {
        &self.cfg
    }

    /// Whether graceful shutdown began.
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    /// UNAVAILABLE once the worker is draining.
    pub(crate) fn reject_if_draining(&self) -> Result<(), tonic::Status> {
        if self.is_shutting_down() {
            Err(status::unavailable(
                "worker draining: not accepting new workloads",
            ))
        } else {
            Ok(())
        }
    }

    /// Registers a startup RPC, then refuses it if the worker is draining.
    /// Registering first means shutdown cannot miss it.
    pub(crate) fn begin_rpc(
        &self,
        uid: &str,
        rpc: Rpc,
        cancel: Option<CancellationToken>,
    ) -> Result<InFlightGuard<'_>, tonic::Status> {
        let g = self.in_flight.add(uid, rpc, cancel);
        self.reject_if_draining()?;
        Ok(g)
    }
}

/// A token cancelled after `d`.
pub(crate) fn timeout_token(d: Duration) -> CancellationToken {
    let t = CancellationToken::new();
    let c = t.clone();
    tokio::spawn(async move {
        tokio::time::sleep(d).await;
        c.cancel();
    });
    t
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn begin_rpc_registers_before_checking_for_a_drain() {
        let s = WorkerService::new(ServiceConfig::default());
        let g = s
            .begin_rpc("actor-a", Rpc::Run, Some(CancellationToken::new()))
            .unwrap();
        assert!(s.in_flight.names().contains_key("actor-a"));
        drop(g);
        s.shutting_down.store(true, Ordering::SeqCst);
        assert_eq!(
            s.begin_rpc("actor-b", Rpc::Run, None).err().unwrap().code(),
            tonic::Code::Unavailable
        );
        assert!(s.in_flight.names().is_empty());
    }
}
