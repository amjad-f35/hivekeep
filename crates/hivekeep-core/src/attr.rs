//! Telemetry attributes shared by every Hivekeep component: the identity
//! keys stamped on spans and log records, and the bounded value sets metric
//! labels use. Keeping them in one place keeps a key, and each of its
//! values, meaning the same thing on every signal and in every binary.
//!
//! Keys are dotted `hk.*` names. `name` and `uid` follow the resource model:
//! `hk.actor.name` is the hive-scoped addressable name, `hk.actor.uid` the
//! server-assigned unique key; there is deliberately no `hk.actor.id`. The
//! hive and the template are top-level (`hk.hive`, `hk.template.*`) because
//! both appear in telemetry that is not about an Actor.
//!
//! This module carries no telemetry SDK types: attributes are plain
//! [`KeyValue`]s that each exporter converts.

use std::collections::BTreeMap;

use hivekeep_proto::api::v1 as api;
use hivekeep_proto::keeper::v1 as keeper;

use crate::resource::{ActorAttribution, ActorRef};

/// A typed attribute value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttrValue {
    /// A string value.
    Str(String),
    /// A 64-bit integer value; versions stay integers rather than strings.
    I64(i64),
}

impl AttrValue {
    /// The value as a string, integers in decimal.
    pub fn as_string(&self) -> String {
        match self {
            AttrValue::Str(s) => s.clone(),
            AttrValue::I64(i) => i.to_string(),
        }
    }
}

/// One attribute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValue {
    /// The key, one of this module's `*_KEY` constants.
    pub key: &'static str,
    /// The value.
    pub value: AttrValue,
}

impl KeyValue {
    /// A string attribute.
    pub fn str(key: &'static str, value: impl Into<String>) -> Self {
        KeyValue {
            key,
            value: AttrValue::Str(value.into()),
        }
    }

    /// An integer attribute.
    pub fn i64(key: &'static str, value: i64) -> Self {
        KeyValue {
            key,
            value: AttrValue::I64(value),
        }
    }
}

/// The hive an Actor (or other resource) lives in.
pub const HIVE_KEY: &str = "hk.hive";
/// An Actor's hive-scoped name.
pub const ACTOR_NAME_KEY: &str = "hk.actor.name";
/// An Actor's server-assigned uid.
pub const ACTOR_UID_KEY: &str = "hk.actor.uid";
/// A container the Actor's template declares. Distinct from the registry's
/// `container.name`, which collectors assign to the worker pod's own
/// containers.
pub const ACTOR_CONTAINER_NAME_KEY: &str = "hk.actor.container.name";
/// The Actor's template name.
pub const TEMPLATE_NAME_KEY: &str = "hk.template.name";
/// The Actor's template hive.
pub const TEMPLATE_HIVE_KEY: &str = "hk.template.hive";
/// The Actor record's version.
pub const ACTOR_VERSION_KEY: &str = "hk.actor.version";

/// Fallback for a template dimension the emitter cannot resolve. "unknown"
/// is a legal name, so a real object called that collides with the
/// fallback; that is accepted to keep the dimension bounded and non-empty.
pub const TEMPLATE_UNKNOWN: &str = "unknown";

/// `dim`, or [`TEMPLATE_UNKNOWN`] when it is empty.
pub fn normalize_template_dimension(dim: &str) -> &str {
    if dim.is_empty() {
        TEMPLATE_UNKNOWN
    } else {
        dim
    }
}

/// Hivekeep's attribute namespace. A producer merging untrusted fields into
/// a record drops everything under it, so nothing a workload sets reads as
/// platform-issued attribution downstream.
pub const RESERVED_NAMESPACE: &str = "hk.";

/// Trace-context fields of structured logs, named as the OpenTelemetry spec
/// fixes them for non-OTLP log formats: top-level, lowercase hex.
pub const LOG_TRACE_ID_FIELD: &str = "trace_id";
/// See [`LOG_TRACE_ID_FIELD`].
pub const LOG_SPAN_ID_FIELD: &str = "span_id";
/// See [`LOG_TRACE_ID_FIELD`].
pub const LOG_TRACE_FLAGS_FIELD: &str = "trace_flags";

/// A resource attribute: how the emitting component reached the collector.
pub const OTLP_RELAY_KEY: &str = "hk.otlp.relay";

/// An Actor's lifecycle state. Logs only: it is only ever recorded beside
/// Actor identity, which no metric may carry.
pub const ACTOR_STATE_KEY: &str = "hk.actor.state";

/// Values of [`ACTOR_STATE_KEY`], mirroring `ActorState` without its prefix.
/// `deleted` has no enum counterpart: the record is gone.
pub mod actor_state {
    #![allow(missing_docs)]
    pub const RESUMING: &str = "resuming";
    pub const RUNNING: &str = "running";
    pub const SUSPENDING: &str = "suspending";
    pub const SUSPENDED: &str = "suspended";
    pub const CRASHED: &str = "crashed";
    pub const DELETING: &str = "deleting";
    pub const REVERTING: &str = "reverting";
    pub const DELETED: &str = "deleted";
    pub const UNKNOWN: &str = "unknown";
}

/// The label value of a stored state, so producers report what the store
/// holds rather than a name typed by hand.
pub fn actor_state_value(state: i32) -> &'static str {
    use api::ActorState as S;
    match S::try_from(state) {
        Ok(S::Resuming) => actor_state::RESUMING,
        Ok(S::Running) => actor_state::RUNNING,
        Ok(S::Suspending) => actor_state::SUSPENDING,
        Ok(S::Suspended) => actor_state::SUSPENDED,
        Ok(S::Crashed) => actor_state::CRASHED,
        Ok(S::Deleting) => actor_state::DELETING,
        Ok(S::Reverting) => actor_state::REVERTING,
        _ => actor_state::UNKNOWN,
    }
}

// Metric-label keys: the only hk.* attributes allowed on datapoints, each
// with a small bounded value set. High-cardinality identity belongs on
// spans and logs.

/// The lifecycle operation an observation is about.
pub const ACTOR_OPERATION_NAME_KEY: &str = "hk.actor.operation.name";
/// A WorkerPool's namespace; pairs with [`WORKER_POOL_NAME_KEY`].
pub const WORKER_POOL_NAMESPACE_KEY: &str = "hk.workerpool.namespace";
/// A WorkerPool's name.
pub const WORKER_POOL_NAME_KEY: &str = "hk.workerpool.name";
/// A worker's occupancy.
pub const WORKER_STATE_KEY: &str = "hk.worker.state";
/// The sandbox class; set it through [`sandbox_class_attribute`].
pub const SANDBOX_CLASS_KEY: &str = "hk.sandbox.class";
/// Which snapshot an operation read or wrote.
pub const SNAPSHOT_KIND_KEY: &str = "hk.snapshot.kind";
/// What content a snapshot covers.
pub const SNAPSHOT_FIDELITY_KEY: &str = "hk.snapshot.fidelity";
/// Which step of a snapshot operation an observation timed.
pub const SNAPSHOT_PHASE_KEY: &str = "hk.snapshot.phase";
/// The node image cache's lookup outcome.
pub const IMAGE_CACHE_OUTCOME_KEY: &str = "hk.imagecache.outcome";
/// Which node file cache.
pub const FILE_CACHE_NAME_KEY: &str = "hk.filecache.name";
/// A file cache eviction pass's outcome.
pub const FILE_CACHE_GC_OUTCOME_KEY: &str = "hk.filecache.gc.outcome";
/// A scheduling attempt's outcome.
pub const SCHEDULER_OUTCOME_KEY: &str = "hk.scheduler.outcome";
/// Whether a routed request resumed its Actor.
pub const ROUTER_RESUME_KEY: &str = "hk.router.resume";
/// A routed request's outcome.
pub const ROUTER_OUTCOME_KEY: &str = "hk.router.outcome";
/// Where a usage sample was measured.
pub const STATS_SOURCE_KEY: &str = "hk.stats.source";

/// One activation of an Actor (a Run or Restore), as the unix-nano time it
/// began. Logs only.
pub const ACTOR_EPOCH_KEY: &str = "hk.actor.epoch";
/// Which usage sample a record is.
pub const STATS_KIND_KEY: &str = "hk.stats.kind";
/// Memory usage in bytes.
pub const STATS_MEMORY_USAGE_KEY: &str = "hk.stats.memory.usage";
/// Peak memory in bytes.
pub const STATS_MEMORY_PEAK_KEY: &str = "hk.stats.memory.peak";
/// Working set in bytes.
pub const STATS_MEMORY_WORKING_SET_KEY: &str = "hk.stats.memory.working_set";
/// CPU time in seconds, restarting at each activation.
pub const STATS_CPU_TIME_KEY: &str = "hk.stats.cpu.time";
/// OOM kills of the sandbox's cgroup. Logs only.
pub const SANDBOX_OOM_KILLS_KEY: &str = "hk.sandbox.oom_kills";

/// Values of [`STATS_KIND_KEY`].
pub mod stats_kind {
    #![allow(missing_docs)]
    pub const PERIODIC: &str = "periodic";
    pub const INITIAL: &str = "initial";
    pub const FINAL: &str = "final";
}

/// Values of [`STATS_SOURCE_KEY`]. The sources measure different things,
/// so rollups group by the key instead of summing across it.
pub mod stats_source {
    #![allow(missing_docs)]
    pub const UNSPECIFIED: &str = "unspecified";
    pub const CGROUP: &str = "cgroup";
    pub const GUEST_AGENT: &str = "guest-agent";
}

/// Values of [`ROUTER_RESUME_KEY`].
pub mod router_resume {
    /// The Actor was already running.
    pub const NONE: &str = "none";
    /// This request led a cold activation.
    pub const TRIGGERED: &str = "triggered";
    /// This request waited on another's cold activation.
    pub const JOINED: &str = "joined";
    /// The resume did not complete, so whether one ran is unknown.
    pub const UNKNOWN: &str = "unknown";
}

/// Values of [`IMAGE_CACHE_OUTCOME_KEY`]. Only `error` carries `error.type`.
pub mod image_cache_outcome {
    #![allow(missing_docs)]
    pub const HIT: &str = "hit";
    pub const MISS: &str = "miss";
    pub const ERROR: &str = "error";
    pub const CANCELLED: &str = "cancelled";
    pub const TIMEOUT: &str = "timeout";
}

/// Values of [`FILE_CACHE_NAME_KEY`].
pub mod file_cache {
    /// Sandbox runtime binaries, kernels and guest images.
    pub const SANDBOX_ASSET: &str = "sandbox_asset";
    /// Files of shared snapshots.
    pub const SNAPSHOT: &str = "snapshot";
}

/// Values of [`FILE_CACHE_GC_OUTCOME_KEY`]; quiet passes are not recorded.
pub mod file_cache_gc_outcome {
    /// The pass freed what the budget asked.
    pub const COMPLETE: &str = "complete";
    /// It freed less than asked.
    pub const SHORTFALL: &str = "shortfall";
    /// It could not size the cache and did nothing.
    pub const SKIPPED: &str = "skipped";
}

/// Values of [`ROUTER_OUTCOME_KEY`]. `no_capacity` and `denied` are
/// capacity and policy signals, not defects.
pub mod router_outcome {
    #![allow(missing_docs)]
    pub const OK: &str = "ok";
    pub const CANCELLED: &str = "cancelled";
    pub const TIMEOUT: &str = "timeout";
    pub const NO_CAPACITY: &str = "no_capacity";
    pub const FAILED_PRECONDITION: &str = "failed_precondition";
    pub const LOCK_CONFLICT: &str = "lock_conflict";
    pub const NOT_FOUND: &str = "not_found";
    pub const UNAVAILABLE: &str = "unavailable";
    pub const RATE_LIMITED: &str = "rate_limited";
    pub const DENIED: &str = "denied";
    pub const RESUME_ERROR: &str = "resume_error";
}

/// The registry's `error.type`, reused verbatim: failures are reported on
/// the same instrument through it, its absence meaning success.
pub const ERROR_TYPE_KEY: &str = "error.type";

/// Values of [`WORKER_STATE_KEY`]. Unschedulable wins over occupancy.
pub mod worker_state {
    #![allow(missing_docs)]
    pub const IDLE: &str = "idle";
    pub const PARTIAL: &str = "partial";
    pub const AT_CAPACITY: &str = "at_capacity";
    pub const UNSCHEDULABLE: &str = "unschedulable";
}

/// Values of [`ACTOR_OPERATION_NAME_KEY`]: the lifecycle operations the
/// control plane serves.
pub mod operation {
    #![allow(missing_docs)]
    pub const CREATE: &str = "create";
    pub const RESUME: &str = "resume";
    pub const SUSPEND: &str = "suspend";
    pub const DELETE: &str = "delete";
    pub const REVERT: &str = "revert";
    pub const UNKNOWN: &str = "unknown";
}

/// Every bounded lifecycle operation.
pub const ALL_OPERATIONS: [&str; 5] = [
    operation::CREATE,
    operation::RESUME,
    operation::SUSPEND,
    operation::DELETE,
    operation::REVERT,
];

/// `op` when it is a bounded lifecycle operation, else `unknown`.
pub fn normalize_operation_name(op: &str) -> &str {
    if ALL_OPERATIONS.contains(&op) {
        op
    } else {
        operation::UNKNOWN
    }
}

/// Values of [`SCHEDULER_OUTCOME_KEY`]. No capacity is an outcome, not an
/// error; only `error` carries `error.type`.
pub mod scheduler_outcome {
    #![allow(missing_docs)]
    pub const ASSIGNED: &str = "assigned";
    pub const NO_CAPACITY: &str = "no_capacity";
    pub const ERROR: &str = "error";
}

/// Values of [`SNAPSHOT_KIND_KEY`]: `local` restores a node-local snapshot,
/// `latest` pulls a durable one (a template's golden snapshot included),
/// `boot` starts from scratch.
pub mod snapshot_kind {
    #![allow(missing_docs)]
    pub const LATEST: &str = "latest";
    pub const LOCAL: &str = "local";
    pub const BOOT: &str = "boot";
}

/// Values of [`SNAPSHOT_FIDELITY_KEY`], mirroring `SnapshotFidelity`.
pub mod snapshot_fidelity {
    #![allow(missing_docs)]
    pub const VOLUMES: &str = "volumes";
    pub const ROOTFS: &str = "rootfs";
    pub const MEMORY: &str = "memory";
    pub const UNKNOWN: &str = "unknown";
}

/// How long past the upload delay an undurable local snapshot has waited.
pub const SNAPSHOT_UPLOAD_OVERDUE_BAND_KEY: &str = "hk.snapshot.upload_overdue_band";

/// Values of [`SNAPSHOT_UPLOAD_OVERDUE_BAND_KEY`].
pub mod snapshot_upload_overdue_band {
    #![allow(missing_docs)]
    pub const UNDER_1M: &str = "under_1m";
    pub const FROM_1M_TO_5M: &str = "1m_to_5m";
    pub const FROM_5M_TO_15M: &str = "5m_to_15m";
    pub const OVER_15M: &str = "over_15m";
}

/// The label value of a wire fidelity. Anything unmapped is `unknown`, so
/// no wire value widens the label set.
pub fn snapshot_fidelity_value(fidelity: i32) -> &'static str {
    use keeper::SnapshotFidelity as F;
    match F::try_from(fidelity) {
        Ok(F::Volumes) => snapshot_fidelity::VOLUMES,
        Ok(F::Rootfs) => snapshot_fidelity::ROOTFS,
        Ok(F::Memory) => snapshot_fidelity::MEMORY,
        _ => snapshot_fidelity::UNKNOWN,
    }
}

/// Values of [`SNAPSHOT_PHASE_KEY`]. Phases overlap, so they are independent
/// observations; summing them is meaningless.
pub mod snapshot_phase {
    #![allow(missing_docs)]
    pub const VOLUME_MOUNT: &str = "volume_mount";
    pub const MANIFEST_FETCH: &str = "manifest_fetch";
    pub const SANDBOX_ASSETS: &str = "sandbox_assets";
    pub const DOWNLOAD: &str = "download";
    pub const OCI_UNPACK: &str = "oci_unpack";
    pub const WORKER_RESTORE: &str = "worker_restore";
    pub const WORKER_CHECKPOINT: &str = "worker_checkpoint";
    /// Keeping (local) or uploading (external) the snapshot.
    pub const PERSIST: &str = "persist";
    pub const CLEANUP: &str = "cleanup";
    pub const TOTAL: &str = "total";
}

/// The [`normalize_sandbox_class`] fallback.
pub const SANDBOX_CLASS_UNKNOWN: &str = "unknown";

/// Bounds the sandbox class label: the class can come from a snapshot
/// manifest nothing validated, so anything unrecognized (empty included,
/// keeping a manifest problem visible) is `unknown`.
pub fn normalize_sandbox_class(class: &str) -> &str {
    if crate::sandbox_class::is_valid(class) {
        class
    } else {
        SANDBOX_CLASS_UNKNOWN
    }
}

/// The `hk.sandbox.class` attribute. No source of the class is validated,
/// so emitters set it through this rather than [`SANDBOX_CLASS_KEY`].
pub fn sandbox_class_attribute(class: &str) -> KeyValue {
    KeyValue::str(SANDBOX_CLASS_KEY, normalize_sandbox_class(class))
}

/// A WorkerPool's namespaced identity, or nothing when either half is
/// missing: half the pair identifies no pool, and an empty series would
/// merge same-named pools.
pub fn worker_pool_attributes(namespace: &str, name: &str) -> Vec<KeyValue> {
    if namespace.is_empty() || name.is_empty() {
        return Vec::new();
    }
    vec![
        KeyValue::str(WORKER_POOL_NAMESPACE_KEY, namespace),
        KeyValue::str(WORKER_POOL_NAME_KEY, name),
    ]
}

/// What is known before the Actor record resolves: the hive and name the
/// request addresses.
pub fn actor_ref_attributes(r: &ActorRef) -> Vec<KeyValue> {
    vec![
        KeyValue::str(HIVE_KEY, r.hive.as_str()),
        KeyValue::str(ACTOR_NAME_KEY, r.name.as_str()),
    ]
}

/// A loaded Actor's identity. `None` yields zero values.
pub fn actor_attributes(a: Option<&api::Actor>) -> Vec<KeyValue> {
    let md = a.and_then(|a| a.metadata.as_ref());
    let tmpl = a.and_then(|a| a.actor_template.as_ref());
    vec![
        KeyValue::str(HIVE_KEY, md.map_or("", |m| m.hive.as_str())),
        KeyValue::str(ACTOR_NAME_KEY, md.map_or("", |m| m.name.as_str())),
        KeyValue::str(ACTOR_UID_KEY, md.map_or("", |m| m.uid.as_str())),
        KeyValue::str(TEMPLATE_NAME_KEY, tmpl.map_or("", |t| t.name.as_str())),
        KeyValue::str(TEMPLATE_HIVE_KEY, tmpl.map_or("", |t| t.hive.as_str())),
        KeyValue::i64(ACTOR_VERSION_KEY, md.map_or(0, |m| m.version)),
    ]
}

/// The Actor identity on every Actor log record, as string labels. An empty
/// `container_name` omits that key, so filtering on it yields container
/// output only.
pub fn actor_log_labels(a: &ActorAttribution, container_name: &str) -> BTreeMap<String, String> {
    let mut m: BTreeMap<String, String> = actor_log_attrs(a)
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    if !container_name.is_empty() {
        m.insert(ACTOR_CONTAINER_NAME_KEY.into(), container_name.into());
    }
    m
}

/// The same identity as flat fields of a component's own log record. Must
/// agree with [`actor_log_labels`] key for key so the two streams join.
pub fn actor_log_attrs(a: &ActorAttribution) -> Vec<(&'static str, String)> {
    vec![
        (HIVE_KEY, a.actor.hive.clone()),
        (ACTOR_NAME_KEY, a.actor.name.clone()),
        (ACTOR_UID_KEY, a.uid.clone()),
        (TEMPLATE_HIVE_KEY, a.template_hive.clone()),
        (TEMPLATE_NAME_KEY, a.template_name.clone()),
    ]
}

/// [`actor_log_attrs`] before the Actor resolves: no uid yet, so it is
/// omitted rather than empty.
pub fn actor_ref_log_attrs(r: &ActorRef) -> Vec<(&'static str, String)> {
    vec![(HIVE_KEY, r.hive.clone()), (ACTOR_NAME_KEY, r.name.clone())]
}

/// An Actor's metric labels: template, sandbox class and operation, plus
/// the worker pool while it holds an assignment. High-cardinality identity
/// is left out.
pub fn actor_metric_attributes(
    a: Option<&api::Actor>,
    sandbox_class: &str,
    operation_name: &str,
) -> Vec<KeyValue> {
    let Some(a) = a else { return Vec::new() };
    let tmpl = a.actor_template.as_ref();
    let mut out = vec![
        KeyValue::str(TEMPLATE_HIVE_KEY, tmpl.map_or("", |t| t.hive.as_str())),
        KeyValue::str(TEMPLATE_NAME_KEY, tmpl.map_or("", |t| t.name.as_str())),
        sandbox_class_attribute(sandbox_class),
        KeyValue::str(
            ACTOR_OPERATION_NAME_KEY,
            normalize_operation_name(operation_name),
        ),
    ];
    let ass = a.status.as_ref().and_then(|s| s.worker_assignment.as_ref());
    out.extend(worker_pool_attributes(
        ass.map_or("", |w| w.worker_namespace.as_str()),
        ass.map_or("", |w| w.worker_pool.as_str()),
    ));
    out
}

#[cfg(test)]
mod tests;
