//! Registering the worker process with the node-local keeper, and keeping
//! the sandbox runtimes it can run reported.
//!
//! Nothing else tells the control plane what this worker can supply, so a
//! worker that cannot register sits idle forever. Registration therefore
//! retries everything it can outlast, including the window before the Worker
//! record exists, and gives up only on a misconfiguration or when the control
//! plane refuses this process's epoch (another process registered since).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::ports::Epoch;
use async_trait::async_trait;
use futures::future::BoxFuture;
use hivekeep_proto::keeper::v1 as kpb;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::keeper_dial::KeeperDialConfig;
use crate::runtimes_file::{self, RuntimesClass, RuntimesFile};
use hivekeep_core::hardware::SCHEMA_VERSION_V1;

/// How long one call to the keeper may take.
pub const REPORT_TIMEOUT: Duration = Duration::from_secs(10);
/// The first retry delay.
pub const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
/// The longest retry delay.
pub const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// How often the runtimes are re-reported with no change seen.
pub const RESYNC_INTERVAL: Duration = Duration::from_secs(300);

/// Where the downward API projects the worker pod's limits.
pub const CAPACITY_MOUNT_PATH: &str = "/run/worker-capacity";
/// The CPU limit file, millicores.
pub const CPU_LIMIT_FILE: &str = "cpu_millicores";
/// The memory limit file, bytes.
pub const MEMORY_LIMIT_FILE: &str = "memory_bytes";

/// A failure no retry fixes.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    /// The configuration is incomplete or unusable.
    #[error("register worker: {0}")]
    Config(String),
    /// The keeper refused the request or this process's epoch.
    #[error("register worker: {0}")]
    Refused(tonic::Status),
    /// Cancelled.
    #[error("register worker: cancelled")]
    Cancelled,
}

fn read_limit(path: &Path) -> i64 {
    let raw = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(path = %path.display(), err = %e, "Ignoring unreadable capacity limit");
            return 0;
        }
    };
    match raw.trim().parse::<i64>() {
        Ok(v) if v >= 0 => v,
        _ => {
            tracing::warn!(path = %path.display(), value = raw.trim(), "Ignoring unusable capacity limit");
            0
        }
    }
}

/// The worker's capacity from the limits under `dir`, with its own Actor
/// limit. A missing or unusable limit is zero, which the control plane reads
/// as none: better to place nothing than to invent a number. The memory the
/// worker keeps for itself is subtracted, and must be less than the limit.
pub fn capacity_from_dir(
    dir: &Path,
    actors: i32,
    memory_reserved: i64,
) -> Result<kpb::WorkerResources, RegisterError> {
    let mut memory = read_limit(&dir.join(MEMORY_LIMIT_FILE));
    if memory > 0 {
        if memory <= memory_reserved {
            return Err(RegisterError::Config(format!(
                "pod memory limit {memory} bytes is no more than the {memory_reserved} the worker keeps for itself, raise the pool's limits.memory"
            )));
        }
        memory -= memory_reserved;
    }
    Ok(kpb::WorkerResources {
        cpu_millicores: read_limit(&dir.join(CPU_LIMIT_FILE)),
        memory_bytes: memory,
        actors,
    })
}

/// Splits the comma-separated pod IPs of the downward API.
pub fn parse_pod_ips(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

/// The compat of one runtime: its attributes, sorted by key, under v1.
pub fn build_compat(
    attrs: &std::collections::BTreeMap<String, String>,
) -> kpb::VersionedSandboxCompat {
    kpb::VersionedSandboxCompat {
        schema_version: SCHEMA_VERSION_V1.into(),
        attributes: attrs
            .iter()
            .map(|(k, v)| kpb::AttributeEntry {
                key: k.clone(),
                value: v.clone(),
            })
            .collect(),
    }
}

fn build_runtime(rt: &runtimes_file::Runtime) -> kpb::WorkerSandboxRuntime {
    kpb::WorkerSandboxRuntime {
        version: Some(build_compat(&rt.version)),
        actor_overhead: None,
    }
}

/// The runtimes request for one class of the file, for the process
/// registered with `epoch`.
pub fn build_runtimes_request(
    epoch: i64,
    class: &RuntimesClass,
) -> kpb::SetWorkerSandboxRuntimesRequest {
    kpb::SetWorkerSandboxRuntimesRequest {
        epoch,
        sandbox_classes: vec![kpb::WorkerSandboxClass {
            sandbox_class: class.sandbox_class.clone(),
            default_runtime: Some(build_runtime(&class.default_runtime)),
            restorable_runtimes: class
                .restorable_runtimes
                .iter()
                .map(build_runtime)
                .collect(),
        }],
    }
}

/// Reports `overhead` on every runtime: the restorable ones run under this
/// worker too, so they cost it the same.
pub fn set_actor_overhead(
    req: &mut kpb::SetWorkerSandboxRuntimesRequest,
    overhead: Option<&kpb::RuntimeOverhead>,
) {
    let Some(o) = overhead else { return };
    for c in &mut req.sandbox_classes {
        for rt in c
            .default_runtime
            .iter_mut()
            .chain(c.restorable_runtimes.iter_mut())
        {
            rt.actor_overhead = Some(*o);
        }
    }
}

/// How the worker reaches the keeper and what it registers.
#[derive(Debug, Clone)]
pub struct ReportConfig {
    /// The keeper connection.
    pub dial: KeeperDialConfig,
    /// How many Actors this worker hosts at once.
    pub actors: i32,
    /// The pod's IPs.
    pub pod_ips: Vec<String>,
    /// Memory the worker keeps for itself.
    pub memory_reserved_bytes: i64,
    /// What each Actor costs outside its own limits.
    pub actor_overhead: Option<kpb::RuntimeOverhead>,
    /// The class this worker implements.
    pub sandbox_class: String,
    /// The sandbox runtimes file the keeper keeps for this worker.
    pub runtimes_file: PathBuf,
    /// Where the pod's limits are projected.
    pub capacity_dir: PathBuf,
}

/// The keeper calls registration makes.
#[async_trait]
pub trait SupportClient: Send + Sync {
    /// A fresh epoch for this process.
    async fn acquire_worker_epoch(&self) -> Result<i64, tonic::Status>;
    /// Registers with `req.epoch`.
    async fn register_worker(&self, req: kpb::RegisterWorkerRequest) -> Result<(), tonic::Status>;
    /// Reports the runtimes.
    async fn set_worker_sandbox_runtimes(
        &self,
        req: kpb::SetWorkerSandboxRuntimesRequest,
    ) -> Result<(), tonic::Status>;
}

/// Dials the keeper for each call.
#[derive(Debug, Clone)]
pub struct DialingClient {
    /// The connection settings.
    pub dial: KeeperDialConfig,
}

impl DialingClient {
    async fn client(
        &self,
    ) -> Result<
        kpb::worker_support_client::WorkerSupportClient<tonic::transport::Channel>,
        tonic::Status,
    > {
        self.dial
            .connect()
            .await
            .map_err(|e| tonic::Status::unavailable(e.to_string()))
    }
}

fn with_timeout<T>(m: T) -> tonic::Request<T> {
    let mut r = tonic::Request::new(m);
    r.set_timeout(REPORT_TIMEOUT);
    r
}

#[async_trait]
impl SupportClient for DialingClient {
    async fn acquire_worker_epoch(&self) -> Result<i64, tonic::Status> {
        let mut c = self.client().await?;
        Ok(
            c.acquire_worker_epoch(with_timeout(kpb::AcquireWorkerEpochRequest::default()))
                .await?
                .into_inner()
                .epoch,
        )
    }

    async fn register_worker(&self, req: kpb::RegisterWorkerRequest) -> Result<(), tonic::Status> {
        self.client()
            .await?
            .register_worker(with_timeout(req))
            .await
            .map(drop)
    }

    async fn set_worker_sandbox_runtimes(
        &self,
        req: kpb::SetWorkerSandboxRuntimesRequest,
    ) -> Result<(), tonic::Status> {
        self.client()
            .await?
            .set_worker_sandbox_runtimes(with_timeout(req))
            .await
            .map(drop)
    }
}

/// A timer: the future resolves after the duration. A future that never
/// resolves disables that wakeup.
pub type After = Arc<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>;

/// The real timer.
pub fn real_after() -> After {
    Arc::new(|d| Box::pin(tokio::time::sleep(d)))
}

/// Acquires an epoch and registers with it, retrying with backoff until one
/// registration is accepted, and returns the epoch. A timed-out registration
/// may have landed, so it is resent with the same epoch. An invalid request
/// or a refused epoch is returned.
pub async fn register_once(
    client: &dyn SupportClient,
    mut req: kpb::RegisterWorkerRequest,
    initial: Duration,
    max: Duration,
    after: &After,
    cancel: &CancellationToken,
) -> Result<i64, RegisterError> {
    let mut backoff = initial;
    let mut wait = |err: &tonic::Status, msg: &str| {
        tracing::warn!(err = %err, "{msg}");
        let d = backoff;
        backoff = (backoff * 2).min(max);
        after(d)
    };
    while req.epoch == 0 {
        match client.acquire_worker_epoch().await {
            Ok(e) => req.epoch = e,
            Err(s) if s.code() == tonic::Code::InvalidArgument => {
                return Err(RegisterError::Refused(s))
            }
            Err(s) => {
                let t = wait(&s, "Could not acquire a worker epoch; will retry");
                tokio::select! {
                    _ = cancel.cancelled() => return Err(RegisterError::Cancelled),
                    _ = t => {}
                }
            }
        }
    }
    loop {
        match client.register_worker(req.clone()).await {
            Ok(()) => {
                tracing::info!(epoch = req.epoch, ips = ?req.ips, capacity = ?req.capacity, "Registered worker");
                return Ok(req.epoch);
            }
            Err(s)
                if matches!(
                    s.code(),
                    tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition
                ) =>
            {
                return Err(RegisterError::Refused(s));
            }
            Err(s) => {
                let t = wait(&s, "Could not register worker; will retry");
                tokio::select! {
                    _ = cancel.cancelled() => return Err(RegisterError::Cancelled),
                    _ = t => {}
                }
            }
        }
    }
}

/// Reads the runtimes file.
pub type ReadFn = Arc<dyn Fn() -> std::io::Result<RuntimesFile> + Send + Sync>;
/// Reports runtimes.
pub type ReportFn = Arc<
    dyn Fn(kpb::SetWorkerSandboxRuntimesRequest) -> BoxFuture<'static, Result<(), tonic::Status>>
        + Send
        + Sync,
>;

/// Reports the runtimes in the file whenever they differ from those last
/// reported.
pub struct Reconciler {
    /// The class reported.
    pub sandbox_class: String,
    /// For log messages.
    pub runtimes_file: PathBuf,
    /// Reported on every runtime.
    pub actor_overhead: Option<kpb::RuntimeOverhead>,
    /// The registered epoch.
    pub epoch: i64,
    /// Reads the file.
    pub read: ReadFn,
    /// Reports.
    pub report: ReportFn,
    /// The first retry delay.
    pub initial_backoff: Duration,
    /// The longest retry delay.
    pub max_backoff: Duration,
    /// The resync interval.
    pub resync: Duration,
    /// The timer.
    pub after: After,
    /// The last request the keeper accepted.
    pub reported: Option<kpb::SetWorkerSandboxRuntimesRequest>,
}

impl Reconciler {
    /// A reconciler with the default timings.
    pub fn new(
        sandbox_class: &str,
        runtimes_file: &Path,
        actor_overhead: Option<kpb::RuntimeOverhead>,
        epoch: i64,
        read: ReadFn,
        report: ReportFn,
    ) -> Self {
        Reconciler {
            sandbox_class: sandbox_class.into(),
            runtimes_file: runtimes_file.into(),
            actor_overhead,
            epoch,
            read,
            report,
            initial_backoff: INITIAL_BACKOFF,
            max_backoff: MAX_BACKOFF,
            resync: RESYNC_INTERVAL,
            after: real_after(),
            reported: None,
        }
    }

    /// Syncs at start, on every change signal, after a backoff following a
    /// transient failure, and every resync interval, until `cancel` fires or
    /// the epoch is refused. Changes are ignored while backing off, so rapid
    /// writes cannot bypass the backoff while the keeper is down.
    pub async fn run(
        &mut self,
        mut changes: Option<mpsc::Receiver<()>>,
        cancel: &CancellationToken,
    ) -> Result<(), RegisterError> {
        let mut backoff = self.initial_backoff;
        loop {
            let retry = self.sync().await?;
            let wait = if retry {
                let w = backoff;
                backoff = (backoff * 2).min(self.max_backoff);
                w
            } else {
                backoff = self.initial_backoff;
                self.resync
            };
            let timer = (self.after)(wait);
            let change = async {
                match (&mut changes, retry) {
                    (Some(rx), false) => rx.recv().await,
                    _ => futures::future::pending().await,
                }
            };
            tokio::select! {
                _ = cancel.cancelled() => return Err(RegisterError::Cancelled),
                c = change => {
                    if c.is_none() {
                        changes = None;
                    }
                }
                _ = timer => {}
            }
        }
    }

    /// Reads the file and reports it if needed. `Ok(true)` asks for a retry
    /// with backoff; a refused epoch is an error.
    pub async fn sync(&mut self) -> Result<bool, RegisterError> {
        let desired = match (self.read)() {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if self.reported.is_none() {
                    tracing::info!(path = %self.runtimes_file.display(), "Waiting for the sandbox runtimes file, which the keeper writes; see its logs if this persists");
                }
                return Ok(false);
            }
            Err(e) => {
                tracing::warn!(path = %self.runtimes_file.display(), err = %e, "Ignoring an unreadable sandbox runtimes file");
                return Ok(false);
            }
            Ok(f) => f,
        };
        let Some(class) = desired.class(&self.sandbox_class) else {
            tracing::warn!(path = %self.runtimes_file.display(), sandbox_class = %self.sandbox_class, "Ignoring a sandbox runtimes file without this worker's sandbox class");
            return Ok(false);
        };
        if class.default_runtime.version.is_empty() {
            tracing::warn!(path = %self.runtimes_file.display(), "Ignoring a sandbox runtimes file with no default runtime");
            return Ok(false);
        }
        let mut req = build_runtimes_request(self.epoch, class);
        set_actor_overhead(&mut req, self.actor_overhead.as_ref());
        if self.reported.as_ref() == Some(&req) {
            return Ok(false);
        }
        match (self.report)(req.clone()).await {
            Ok(()) => {
                tracing::info!(epoch = self.epoch, sandbox_classes = ?req.sandbox_classes, "Reported worker sandbox runtimes");
                self.reported = Some(req);
                Ok(false)
            }
            Err(s) if s.code() == tonic::Code::InvalidArgument => {
                tracing::error!(err = %s, "Sandbox runtimes rejected as invalid; waiting for the sandbox runtimes file to change");
                Ok(false)
            }
            Err(s) if s.code() == tonic::Code::FailedPrecondition => Err(RegisterError::Refused(s)),
            Err(s) => {
                tracing::warn!(err = %s, "Could not report sandbox runtimes; will retry");
                Ok(true)
            }
        }
    }
}

/// Registers, stores the epoch, then reports runtimes until `cancel` fires
/// or the epoch is refused.
pub async fn run(
    cfg: &ReportConfig,
    capacity: kpb::WorkerResources,
    client: Arc<dyn SupportClient>,
    epoch: &Epoch,
    changes: Option<mpsc::Receiver<()>>,
    cancel: &CancellationToken,
) -> Result<(), RegisterError> {
    let registered = register_once(
        client.as_ref(),
        kpb::RegisterWorkerRequest {
            epoch: 0,
            ips: cfg.pod_ips.clone(),
            capacity: Some(capacity),
        },
        INITIAL_BACKOFF,
        MAX_BACKOFF,
        &real_after(),
        cancel,
    )
    .await?;
    epoch.store(registered);
    let path = cfg.runtimes_file.clone();
    let c = client.clone();
    let mut r = Reconciler::new(
        &cfg.sandbox_class,
        &cfg.runtimes_file,
        cfg.actor_overhead,
        registered,
        Arc::new(move || runtimes_file::read(&path)),
        Arc::new(move |req| {
            let c = c.clone();
            Box::pin(async move { c.set_worker_sandbox_runtimes(req).await })
        }),
    );
    r.run(changes, cancel).await
}

/// Registers this worker process with the keeper and keeps its runtimes
/// reported. Returns early on a configuration error or a refused epoch.
pub async fn report(
    cfg: ReportConfig,
    epoch: Arc<Epoch>,
    cancel: CancellationToken,
) -> Result<(), RegisterError> {
    if cfg.sandbox_class.is_empty() {
        return Err(RegisterError::Config("SandboxClass is required".into()));
    }
    if cfg.runtimes_file.as_os_str().is_empty() {
        return Err(RegisterError::Config("RuntimesFile is required".into()));
    }
    if cfg.pod_ips.is_empty() {
        return Err(RegisterError::Config("PodIPs is required".into()));
    }
    cfg.dial
        .tls_config()
        .map_err(|e| RegisterError::Config(e.to_string()))?;
    let capacity = capacity_from_dir(&cfg.capacity_dir, cfg.actors, cfg.memory_reserved_bytes)?;
    let changes = crate::file_watch::watch_file_path(
        &cfg.runtimes_file,
        crate::file_watch::POLL_INTERVAL,
        cancel.clone(),
    );
    let client = Arc::new(DialingClient {
        dial: cfg.dial.clone(),
    });
    run(&cfg, capacity, client, &epoch, Some(changes), &cancel).await
}

#[cfg(test)]
mod tests;
