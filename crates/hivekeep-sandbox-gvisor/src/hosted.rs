//! The worker's record of one hosted Actor activation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::SystemTime;

use hivekeep_core::resource::ActorAttribution;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::activation::Activation;
use crate::memory_watch::MemoryWatch;
use crate::ports::SandboxNetwork;
use crate::runsc::ContainerRuntime;
use crate::sandbox_process::SandboxProcess;

/// The runsc state of a running workload, for shutdown to signal and wait on.
#[derive(Clone)]
pub struct WorkloadSession {
    /// The Actor's runsc.
    pub runtime: Arc<dyn ContainerRuntime>,
    /// The application containers.
    pub containers: Vec<String>,
}

#[derive(Default)]
struct Inner {
    network: Option<Arc<dyn SandboxNetwork>>,
    session: Option<WorkloadSession>,
    sandbox: Option<Arc<SandboxProcess>>,
}

/// One Actor activation: attribution, network and runtime state. A new
/// record is made for every activation, so pointer identity tells them apart.
pub struct HostedActor {
    /// Who it is; known from the moment it is admitted.
    pub attribution: ActorAttribution,
    /// This activation's epoch, CPU baseline and latest sample.
    pub usage: Activation,
    /// Limits the dead-sandbox warning to one per activation.
    pub dead_reported: AtomicBool,
    /// Cancelled when the activation is replaced or unhosted.
    pub token: CancellationToken,
    /// Set before the worker itself stops the sandbox, so its exit is not
    /// taken for a crash.
    pub stopping: AtomicBool,
    /// The sandbox leaf's `memory.high`, or 0 while the watch leaves it be.
    pub memory_limit: AtomicU64,
    /// Set before the memory watch kills the sandbox.
    pub oom_killed: AtomicBool,
    /// Held while the watch checks or kills the sandbox.
    pub memory_watch: Arc<tokio::sync::Mutex<MemoryWatch>>,
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for HostedActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostedActor")
            .field("uid", &self.attribution.uid)
            .finish_non_exhaustive()
    }
}

impl HostedActor {
    /// A fresh activation. Its sandbox cgroup is new, so counters start at 0.
    pub fn new(attribution: ActorAttribution) -> Self {
        Self::with_usage(attribution, Activation::new(SystemTime::now(), false))
    }

    /// An activation with the given usage bookkeeping.
    pub fn with_usage(attribution: ActorAttribution, usage: Activation) -> Self {
        HostedActor {
            attribution,
            usage,
            dead_reported: AtomicBool::new(false),
            token: CancellationToken::new(),
            stopping: AtomicBool::new(false),
            memory_limit: AtomicU64::new(0),
            oom_killed: AtomicBool::new(false),
            memory_watch: Arc::new(tokio::sync::Mutex::new(MemoryWatch::default())),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// The sandbox network, once set up.
    pub fn network(&self) -> Option<Arc<dyn SandboxNetwork>> {
        self.inner.lock().network.clone()
    }

    /// Records the sandbox network.
    pub fn set_network(
        &self,
        n: Option<Arc<dyn SandboxNetwork>>,
    ) -> Option<Arc<dyn SandboxNetwork>> {
        std::mem::replace(&mut self.inner.lock().network, n)
    }

    /// The runsc session, once the containers exist.
    pub fn session(&self) -> Option<WorkloadSession> {
        self.inner.lock().session.clone()
    }

    /// Records the runsc session.
    pub fn set_session(&self, s: WorkloadSession) {
        self.inner.lock().session = Some(s);
    }

    /// The pinned sandbox process, if pinning worked.
    pub fn sandbox(&self) -> Option<Arc<SandboxProcess>> {
        self.inner.lock().sandbox.clone()
    }

    /// Pins (or with `None`, releases) the sandbox process.
    pub fn set_sandbox(&self, p: Option<Arc<SandboxProcess>>) {
        self.inner.lock().sandbox = p;
    }
}

/// The hosted Actors by uid, and how many are still draining their network.
#[derive(Default)]
pub struct ActorTable {
    /// Hosted Actors by uid.
    pub actors: HashMap<String, Arc<HostedActor>>,
    /// Unhosted Actors whose network teardown is still running; they keep
    /// their capacity until it finishes.
    pub draining: usize,
}
