//! Sandbox runtime compatibility: the attributes a runtime is stamped with,
//! and whether a worker's runtime can restore a snapshot stamped with another.

use hivekeep_proto::api::v1 as api;
use hivekeep_proto::keeper::v1 as keeper;

use crate::cpu_features::{host_arch, FeatureMask};

/// The initial compatibility schema version.
pub const SCHEMA_VERSION_V1: &str = "v1";
/// The CPU architecture (`amd64`, `arm64`).
pub const ATTR_ARCHITECTURE: &str = "architecture";
/// The sandbox runtime bundle: a sha256 over a sandbox config version's
/// asset digests, computed by the keeper.
pub const ATTR_SANDBOX_BUNDLE_HASH: &str = "sandbox_bundle_hash";
/// The CPU features a gVisor runtime exposes, as a
/// [`crate::cpu_features`] bitmask. A snapshot's set must be a subset of the
/// worker's.
pub const ATTR_CPU_FEATURES: &str = "cpu_features";
/// The host CPU vendor (`GenuineIntel`, `AuthenticAMD`, arm64 implementer
/// `0x41`), reported by micro-VM runtimes.
pub const ATTR_CPU_VENDOR: &str = "cpu_vendor";
/// The host CPU family and model (`6:143`, `0x0:0xd40:1`), reported by
/// micro-VM runtimes.
pub const ATTR_CPU_MODEL: &str = "cpu_model";

/// A minimal v1 compat with only the host architecture, for synthetic workers
/// in tests and benchmarks. Real workers report what the keeper writes to
/// their sandbox runtimes file.
pub fn probe_host() -> keeper::VersionedSandboxCompat {
    keeper::VersionedSandboxCompat {
        schema_version: SCHEMA_VERSION_V1.into(),
        attributes: vec![keeper::AttributeEntry {
            key: ATTR_ARCHITECTURE.into(),
            value: host_arch().into(),
        }],
    }
}

/// What a worker must run to restore a snapshot: the runtime the snapshot was
/// stamped with, prepared once for matching against many workers. The default
/// requirement requires nothing.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Requirement {
    snap: Option<api::ActorSandboxRuntime>,
    compat: Option<api::VersionedSandboxCompat>,
    cpu_features: FeatureMask,
    /// The snapshot's cpu_features is not canonical; no worker satisfies it.
    bad_cpu_features: bool,
}

fn compat_requirement(compat: Option<&api::VersionedSandboxCompat>) -> Requirement {
    let mut r = Requirement {
        compat: compat.cloned(),
        ..Default::default()
    };
    if let Some(a) = compat
        .into_iter()
        .flat_map(|c| &c.attributes)
        .find(|a| a.key == ATTR_CPU_FEATURES)
    {
        match FeatureMask::parse(&a.value) {
            Some(m) => r.cpu_features = m,
            None => r.bad_cpu_features = true,
        }
    }
    r
}

fn runtime_of(
    class: &str,
    compat: Option<&api::VersionedSandboxCompat>,
) -> api::ActorSandboxRuntime {
    api::ActorSandboxRuntime {
        sandbox_class: class.into(),
        version: compat.cloned(),
    }
}

fn class_of<'a>(
    status: Option<&'a api::WorkerStatus>,
    class: &str,
) -> Option<&'a api::WorkerSandboxClass> {
    status?
        .sandbox_classes
        .iter()
        .find(|c| c.sandbox_class == class)
}

impl Requirement {
    /// Prepares `snap` for matching. `None` requires nothing.
    pub fn new(snap: Option<&api::ActorSandboxRuntime>) -> Self {
        let mut r = compat_requirement(snap.and_then(|s| s.version.as_ref()));
        r.snap = snap.cloned();
        r
    }

    /// The runtime this was prepared from, or `None` when it requires nothing.
    pub fn runtime(&self) -> Option<&api::ActorSandboxRuntime> {
        self.snap.as_ref()
    }

    /// Whether `rt`, the runtime an assignment records, can restore the
    /// snapshot: equal sandbox classes and a matching compat.
    pub fn matches(&self, rt: Option<&api::ActorSandboxRuntime>) -> bool {
        let Some(snap) = &self.snap else { return true };
        match rt {
            Some(rt) if rt.sandbox_class == snap.sandbox_class => {
                self.matches_compat(rt.version.as_ref())
            }
            _ => false,
        }
    }

    /// Whether `worker` can restore the snapshot: the same non-empty schema
    /// version and every snapshot attribute satisfied. `cpu_features` is
    /// satisfied by a superset; everything else must be equal. Keys only the
    /// worker has are ignored, so a worker can add a key under the same
    /// schema version without invalidating older snapshots.
    fn matches_compat(&self, worker: Option<&api::VersionedSandboxCompat>) -> bool {
        let (Some(worker), Some(compat)) = (worker, &self.compat) else {
            return false;
        };
        if worker.schema_version.is_empty() || worker.schema_version != compat.schema_version {
            return false;
        }
        if self.bad_cpu_features {
            return false;
        }
        compat.attributes.iter().all(|sa| {
            worker
                .attributes
                .iter()
                .find(|wa| wa.key == sa.key)
                .is_some_and(|wa| self.attribute_satisfied(&sa.key, &wa.value, &sa.value))
        })
    }

    fn attribute_satisfied(&self, key: &str, worker: &str, snap: &str) -> bool {
        if key == ATTR_CPU_FEATURES {
            return FeatureMask::parse(worker).is_some_and(|w| self.cpu_features.subset_of(&w));
        }
        worker == snap
    }

    /// The worker runtime that can restore the snapshot: in the worker's entry
    /// for the snapshot's class, the default first, then the restorable ones
    /// in order.
    fn matching_runtime<'a>(
        &self,
        status: Option<&'a api::WorkerStatus>,
    ) -> Option<&'a api::WorkerSandboxRuntime> {
        let snap = self.snap.as_ref()?;
        let c = class_of(status, &snap.sandbox_class)?;
        if let Some(def) = &c.default_runtime {
            if self.matches_compat(def.version.as_ref()) {
                return Some(def);
            }
        }
        c.restorable_runtimes
            .iter()
            .find(|rt| self.matches_compat(rt.version.as_ref()))
    }

    /// The stamp for the runtime that can restore the snapshot, or `None`.
    /// A requirement of nothing returns the cold-boot runtime.
    pub fn matching_runtime_stamp(
        &self,
        status: Option<&api::WorkerStatus>,
    ) -> Option<api::ActorSandboxRuntime> {
        let Some(snap) = &self.snap else {
            return cold_boot_runtime(status);
        };
        self.matching_runtime(status)
            .map(|rt| runtime_of(&snap.sandbox_class, rt.version.as_ref()))
    }

    /// Whether a worker with `status` can restore the snapshot.
    pub fn worker_matches(&self, status: Option<&api::WorkerStatus>) -> bool {
        self.snap.is_none() || self.matching_runtime(status).is_some()
    }

    /// The worker's runtime that [`Requirement::runtime_for`] stamps, as the
    /// worker reported it, or `None` before the worker registered.
    pub fn worker_runtime_for<'a>(
        &self,
        status: Option<&'a api::WorkerStatus>,
    ) -> Option<&'a api::WorkerSandboxRuntime> {
        if self.snap.is_some() {
            if let Some(rt) = self.matching_runtime(status) {
                return Some(rt);
            }
        }
        status?.sandbox_classes.first()?.default_runtime.as_ref()
    }

    /// The runtime a worker runs the Actor under: the matching runtime when it
    /// can restore the snapshot, else the cold-boot runtime.
    pub fn runtime_for(
        &self,
        status: Option<&api::WorkerStatus>,
    ) -> Option<api::ActorSandboxRuntime> {
        self.matching_runtime_stamp(status)
            .or_else(|| cold_boot_runtime(status))
    }
}

/// The runtime a fresh Actor starts under: the default runtime of the
/// worker's class, or `None` before the worker registered.
pub fn cold_boot_runtime(status: Option<&api::WorkerStatus>) -> Option<api::ActorSandboxRuntime> {
    let c = status?.sandbox_classes.first()?;
    Some(runtime_of(
        &c.sandbox_class,
        c.default_runtime.as_ref().and_then(|d| d.version.as_ref()),
    ))
}

/// The sandbox class a worker registered, or `""` before it has.
pub fn sandbox_class(status: Option<&api::WorkerStatus>) -> &str {
    status
        .and_then(|s| s.sandbox_classes.first())
        .map_or("", |c| c.sandbox_class.as_str())
}

/// One-off [`Requirement::matches`].
pub fn matches(
    rt: Option<&api::ActorSandboxRuntime>,
    snap: Option<&api::ActorSandboxRuntime>,
) -> bool {
    Requirement::new(snap).matches(rt)
}

/// Whether `worker` can restore a snapshot stamped with `snap`. Unlike
/// [`matches`], a missing `snap` is a stamp without identity and matches
/// nothing.
pub fn matches_compat(
    worker: Option<&api::VersionedSandboxCompat>,
    snap: Option<&api::VersionedSandboxCompat>,
) -> bool {
    compat_requirement(snap).matches_compat(worker)
}

/// One-off [`Requirement::matching_runtime_stamp`].
pub fn matching_runtime(
    status: Option<&api::WorkerStatus>,
    snap: Option<&api::ActorSandboxRuntime>,
) -> Option<api::ActorSandboxRuntime> {
    Requirement::new(snap).matching_runtime_stamp(status)
}

/// One-off [`Requirement::worker_matches`].
pub fn worker_matches(
    status: Option<&api::WorkerStatus>,
    snap: Option<&api::ActorSandboxRuntime>,
) -> bool {
    Requirement::new(snap).worker_matches(status)
}

/// One-off [`Requirement::runtime_for`].
pub fn runtime_for(
    status: Option<&api::WorkerStatus>,
    snap: Option<&api::ActorSandboxRuntime>,
) -> Option<api::ActorSandboxRuntime> {
    Requirement::new(snap).runtime_for(status)
}

#[cfg(test)]
mod tests;
