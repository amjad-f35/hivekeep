//! The Keeper service: the lifecycle RPCs the control plane sends a node.
//!
//! Run boots a fresh workload; Checkpoint captures it into a snapshot (kept
//! locally, uploaded now or later) and returns the worker to blank;
//! Restore boots a workload from a snapshot, local or in object storage;
//! PromoteSnapshot makes a local snapshot durable; DetachActor makes the
//! latest durable and releases the node's copy; Terminate tears everything
//! down. RPCs on one actor are serialized by a per-actor lock.

mod checkpoint;
mod grpc;
mod promote;
mod restore;
mod run;
pub mod spec;
mod terminate;

#[cfg(test)]
mod lifecycle_activation_tests;
#[cfg(test)]
mod lifecycle_checkpoint_tests;
#[cfg(test)]
mod lifecycle_detach_tests;
#[cfg(test)]
mod lifecycle_harness;
#[cfg(test)]
mod report_behavior_tests;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_core::validation::rules::Operation;
use parking_lot::Mutex;
use tonic::Status;

pub use grpc::KeeperService;

use crate::assets::{read_snapshot_manifest, SandboxAssetsRecord};
use crate::context::Ctx;
use crate::keeper_metrics::KeeperInstruments;
use crate::keyed_lock::{KeyedGuard, KeyedLocks};
use crate::oci::ImageCache;
use crate::paths::NodePaths;
use crate::prewarm::AssetFetcher;
use crate::runtime_index::RuntimeIndex;
use crate::snapshot_cache::SnapshotDownloader;
use crate::snapshot_tracker::Tracker;
use crate::snapshot_transfer::SnapshotTransfer;
use crate::system_info::SystemInfoRefresher;
use crate::volumes::VolumeManager;
use crate::worker_dialer::WorkerConnector;

/// Bounds a Checkpoint once it detaches from its caller, so a hung worker
/// or upload cannot hold the actor's lock forever.
pub const DEFAULT_CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Consecutive retryable worker failures to restore one snapshot after
/// which the keeper gives up and crashes the actor, rather than keep it
/// resuming forever.
const MAX_CONSECUTIVE_RETRYABLE_RESTORE_FAILURES: u32 = 3;

/// What the Keeper service works with.
pub struct KeeperDeps {
    /// The node's layout.
    pub paths: Arc<NodePaths>,
    /// The node's workers.
    pub workers: Arc<dyn WorkerConnector>,
    /// The node's image cache.
    pub images: Arc<dyn ImageCache>,
    /// Stages sandbox runtime binaries.
    pub assets: Arc<dyn AssetFetcher>,
    /// Moves snapshots to and from object storage.
    pub transfer: Arc<SnapshotTransfer>,
    /// Restores' snapshot reads, through the snapshot cache.
    pub downloader: Arc<SnapshotDownloader>,
    /// The runtimes the node's workers advertise.
    pub runtimes: Arc<RuntimeIndex>,
    /// Checkpoint and restore metrics.
    pub instruments: Option<KeeperInstruments>,
    /// External volumes.
    pub volumes: Arc<VolumeManager>,
    /// System-info volumes.
    pub system_info: Arc<SystemInfoRefresher>,
    /// Each actor's snapshots on this node.
    pub snapshots: Tracker,
    /// The validation context of incoming requests.
    pub validation: Operation,
}

/// The Keeper service.
pub struct Keeper {
    d: KeeperDeps,
    actor_locks: KeyedLocks<()>,
    restore_failures: Mutex<HashMap<String, (String, u32)>>,
    checkpoint_timeout: Duration,
}

impl Keeper {
    /// A service over `deps`.
    pub fn new(deps: KeeperDeps) -> Self {
        Keeper {
            d: deps,
            actor_locks: KeyedLocks::new(),
            restore_failures: Mutex::new(HashMap::new()),
            checkpoint_timeout: DEFAULT_CHECKPOINT_TIMEOUT,
        }
    }

    /// Shortens the checkpoint timeout, for tests.
    pub fn with_checkpoint_timeout(mut self, d: Duration) -> Self {
        self.checkpoint_timeout = d;
        self
    }

    /// The tracker.
    pub fn snapshots(&self) -> &Tracker {
        &self.d.snapshots
    }

    /// Takes the actor's RPC lock, or fails with `ctx`'s reason.
    async fn lock_actor(&self, ctx: &Ctx, actor_uid: &str) -> Result<KeyedGuard<()>, Status> {
        self.actor_locks
            .lock_ctx(ctx, actor_uid)
            .await
            .map_err(|e| {
                let s = Status::from(e);
                Status::new(
                    s.code(),
                    format!("gave up waiting for the actor's lock: {}", s.message()),
                )
            })
    }

    /// Counts a retryable (Unavailable) worker failure to restore
    /// `snapshot`; once the actor has failed so too often in a row, reports
    /// it as Internal so the control plane crashes it. Other errors pass.
    fn bound_retryable_restore_error(
        &self,
        actor_uid: &str,
        snapshot: &str,
        err: Status,
    ) -> Status {
        if err.code() != tonic::Code::Unavailable {
            return err;
        }
        let n = {
            let mut m = self.restore_failures.lock();
            let e = m
                .entry(actor_uid.to_string())
                .or_insert_with(|| (snapshot.to_string(), 0));
            if e.0 != snapshot {
                *e = (snapshot.to_string(), 0);
            }
            e.1 += 1;
            e.1
        };
        if n < MAX_CONSECUTIVE_RETRYABLE_RESTORE_FAILURES {
            return err;
        }
        self.reset_restore_failures(actor_uid);
        Status::internal(format!(
            "worker restore failed with a retryable error {n} times in a row, giving up: {}",
            err.message()
        ))
    }

    fn reset_restore_failures(&self, actor_uid: &str) {
        self.restore_failures.lock().remove(actor_uid);
    }
}

/// Whether a Restore ending with `res` ends the actor's streak of
/// retryable failures: success or an error that crashes the actor does;
/// Unavailable, Canceled and DeadlineExceeded keep it resuming, so they
/// leave the streak as it is.
fn ends_restore_failure_streak<T>(res: &Result<T, Status>) -> bool {
    match res {
        Ok(_) => true,
        Err(e) => !matches!(
            e.code(),
            tonic::Code::Unavailable | tonic::Code::Cancelled | tonic::Code::DeadlineExceeded
        ),
    }
}

/// Validation failures as InvalidArgument.
fn invalid(errs: hivekeep_core::field::ErrorList) -> Result<(), Status> {
    errs.into_result().map_err(|e| e.to_invalid_argument())
}

/// Why a node-local snapshot cannot be used.
#[derive(Debug)]
enum LocalSnapshotError {
    /// No manifest: the copy is absent.
    Missing,
    /// Present but unusable: unreadable, from an earlier boot, or missing
    /// a file an interrupted prune deleted.
    Unusable(String),
}

/// The manifest of the local snapshot in `dir`, once it records `boot_id`
/// and every file it lists is present as a regular file. File contents are
/// not checked.
fn read_local_snapshot(
    dir: &Path,
    boot_id: &str,
) -> Result<SandboxAssetsRecord, LocalSnapshotError> {
    let m = match read_snapshot_manifest(dir) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(LocalSnapshotError::Missing)
        }
        Err(e) => return Err(LocalSnapshotError::Unusable(e.to_string())),
    };
    let rec = SandboxAssetsRecord::parse(&m)
        .map_err(|e| LocalSnapshotError::Unusable(e.message().to_string()))?;
    if rec.boot_id != boot_id {
        return Err(LocalSnapshotError::Unusable(
            "written before the node last rebooted".into(),
        ));
    }
    check_local_snapshot_files(dir, &rec.snapshot_files).map_err(LocalSnapshotError::Unusable)?;
    Ok(rec)
}

/// Checks each file exists under `dir` as a regular file reached through
/// real directories only, so a symlink cannot point the worker outside the
/// snapshot.
fn check_local_snapshot_files(dir: &Path, files: &[String]) -> Result<(), String> {
    for f in files {
        regular_file_under(dir, f)?;
    }
    Ok(())
}

/// Whether `rel` names a regular file under `base` with no symlink on the
/// way. Returns its metadata.
fn regular_file_under(base: &Path, rel: &str) -> Result<std::fs::Metadata, String> {
    let mut p = base.to_path_buf();
    let parts: Vec<&str> = rel.split('/').collect();
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() || *part == "." || *part == ".." {
            return Err(format!("invalid snapshot file name {rel:?}"));
        }
        p.push(part);
        let m = std::fs::symlink_metadata(&p)
            .map_err(|e| format!("while checking local checkpoint file {rel}: {e}"))?;
        let last = i + 1 == parts.len();
        if last {
            if !m.is_file() {
                return Err(format!("local checkpoint file {rel} is not a regular file"));
            }
            return Ok(m);
        }
        if !m.is_dir() {
            return Err(format!(
                "local checkpoint file {rel} is not under a directory"
            ));
        }
    }
    Err(format!("invalid snapshot file name {rel:?}"))
}

#[cfg(test)]
mod service_tests;
