//! Which SandboxConfig each sandbox class of a WorkerPool uses.
//!
//! Pure functions over a pool and a list of configs; the WorkerPool
//! reconciler reads, protects and writes around them.

use hivekeep_k8s::crd::sandboxconfig::{
    CLASS_DEFAULT_ANNOTATION, WORKER_POOL_PROTECTION_FINALIZER,
};
use hivekeep_k8s::crd::workerpool::{
    CONDITION_RESOLVED, REASON_DEFAULT_CONFIG_NOT_FOUND, REASON_RESOLVED,
    REASON_SANDBOX_CLASS_MISMATCH, REASON_SANDBOX_CONFIG_DELETING, REASON_SANDBOX_CONFIG_NOT_FOUND,
};
use hivekeep_k8s::crd::{
    SandboxClass, SandboxConfig, SandboxConfigReference, WorkerPool, WorkerPoolSandboxClass,
    WorkerPoolSandboxClassStatus,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use kube::ResourceExt;

use super::meta::{has_finalizer, set_condition};

/// The Resolved condition of a class, before it is stamped with a
/// generation and transition time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    /// Whether the class's spec resolved.
    pub resolved: bool,
    /// The condition reason.
    pub reason: String,
    /// The condition message.
    pub message: String,
}

impl Resolution {
    fn ok(message: String) -> Self {
        Resolution {
            resolved: true,
            reason: REASON_RESOLVED.into(),
            message,
        }
    }

    fn failed(reason: &str, message: String) -> Self {
        Resolution {
            resolved: false,
            reason: reason.into(),
            message,
        }
    }

    fn to_condition(&self, generation: Option<i64>) -> Condition {
        Condition {
            type_: CONDITION_RESOLVED.into(),
            status: if self.resolved { "True" } else { "False" }.into(),
            reason: self.reason.clone(),
            message: self.message.clone(),
            observed_generation: generation,
            last_transition_time: Time(k8s_openapi::jiff::Timestamp::UNIX_EPOCH),
        }
    }
}

fn name_of(sc: &SandboxConfig) -> String {
    sc.name_any()
}

fn find<'a>(configs: &'a [SandboxConfig], name: &str) -> Option<&'a SandboxConfig> {
    configs
        .iter()
        .find(|sc| sc.metadata.name.as_deref() == Some(name))
}

/// Picks the SandboxConfig `entry`'s spec asks for, without falling back to
/// the config in use. On failure returns the condition reason and message.
///
/// - With `configRef`, that config is used. It must exist, belong to the
///   class, and not be being deleted.
/// - Without, the newest config of the class annotated as class default
///   and not being deleted is used; on a creation-time tie, the name that
///   sorts first.
pub fn resolve_latest_sandbox_config<'a>(
    entry: &WorkerPoolSandboxClass,
    configs: &'a [SandboxConfig],
) -> Result<&'a SandboxConfig, (&'static str, String)> {
    if let Some(r) = &entry.config_ref {
        let name = &r.name;
        let Some(sc) = find(configs, name) else {
            return Err((
                REASON_SANDBOX_CONFIG_NOT_FOUND,
                format!("SandboxConfig {name:?} not found"),
            ));
        };
        if sc.spec.sandbox_class != entry.name {
            return Err((
                REASON_SANDBOX_CLASS_MISMATCH,
                format!(
                    "SandboxConfig {name:?} is for sandbox class {:?}, not {:?}",
                    sc.spec.sandbox_class.as_str(),
                    entry.name.as_str()
                ),
            ));
        }
        if sc.is_deleting() {
            return Err((
                REASON_SANDBOX_CONFIG_DELETING,
                format!("SandboxConfig {name:?} is being deleted"),
            ));
        }
        return Ok(sc);
    }

    let is_default =
        |sc: &SandboxConfig| sc.spec.sandbox_class == entry.name && sc.is_class_default();
    let mut best: Option<&SandboxConfig> = None;
    for sc in configs {
        if is_default(sc) && !sc.is_deleting() && best.is_none_or(|b| newer_sandbox_config(sc, b)) {
            best = Some(sc);
        }
    }
    if let Some(b) = best {
        return Ok(b);
    }
    if configs.iter().any(|sc| is_default(sc) && sc.is_deleting()) {
        return Err((
            REASON_SANDBOX_CONFIG_DELETING,
            format!(
                "the default SandboxConfig for sandbox class {:?} is being deleted",
                entry.name.as_str()
            ),
        ));
    }
    Err((
        REASON_DEFAULT_CONFIG_NOT_FOUND,
        format!(
            "no SandboxConfig of sandbox class {:?} is annotated {CLASS_DEFAULT_ANNOTATION}=true",
            entry.name.as_str()
        ),
    ))
}

/// The config `entry` uses and its Resolved condition. `in_use` is the
/// config the pool's status already uses for the class. When the spec does
/// not resolve the pool keeps `in_use`; see [`keep_sandbox_config`].
pub fn resolve_sandbox_config<'a>(
    entry: &WorkerPoolSandboxClass,
    in_use: Option<&str>,
    configs: &'a [SandboxConfig],
) -> (Option<&'a SandboxConfig>, Resolution) {
    match resolve_latest_sandbox_config(entry, configs) {
        Ok(sc) => (
            Some(sc),
            Resolution::ok(format!("resolved to SandboxConfig {:?}", name_of(sc))),
        ),
        Err((reason, message)) => keep_sandbox_config(entry.name, in_use, configs, reason, message),
    }
}

/// The config a class keeps when its spec does not resolve, and a False
/// Resolved condition.
///
/// The class keeps `in_use`, so a running pool never loses its config, as
/// long as it still names a config of the class. A config in use that is
/// being deleted carries the protection finalizer while any pool's status
/// names it; one without is about to disappear and is not kept.
pub fn keep_sandbox_config<'a>(
    class: SandboxClass,
    in_use: Option<&str>,
    configs: &'a [SandboxConfig],
    reason: &str,
    mut message: String,
) -> (Option<&'a SandboxConfig>, Resolution) {
    let kept = in_use.and_then(|n| find(configs, n)).filter(|sc| {
        sc.spec.sandbox_class == class
            && (!sc.is_deleting() || has_finalizer(&sc.metadata, WORKER_POOL_PROTECTION_FINALIZER))
    });
    if let Some(k) = kept {
        message = format!("{message}; keeping {:?}", name_of(k));
    }
    (kept, Resolution::failed(reason, message))
}

/// Whether `a` takes precedence over `b` as a class default.
pub fn newer_sandbox_config(a: &SandboxConfig, b: &SandboxConfig) -> bool {
    let (ta, tb) = (
        &a.metadata.creation_timestamp,
        &b.metadata.creation_timestamp,
    );
    if ta != tb {
        return tb.as_ref().map(|t| t.0) < ta.as_ref().map(|t| t.0);
    }
    name_of(a) < name_of(b)
}

/// The status entries of `wp`'s pool.
pub fn status_classes(wp: &WorkerPool) -> &[WorkerPoolSandboxClassStatus] {
    wp.status
        .as_ref()
        .map(|s| s.sandbox_classes.as_slice())
        .unwrap_or(&[])
}

/// The entry of `class` in `statuses`.
pub fn find_sandbox_class_status(
    statuses: &[WorkerPoolSandboxClassStatus],
    class: SandboxClass,
) -> Option<&WorkerPoolSandboxClassStatus> {
    statuses.iter().find(|s| s.name == class)
}

/// The entry `wp`'s status records for `class`.
pub fn sandbox_class_status(
    wp: &WorkerPool,
    class: SandboxClass,
) -> Option<&WorkerPoolSandboxClassStatus> {
    find_sandbox_class_status(status_classes(wp), class)
}

fn in_use_of(wp: &WorkerPool, class: SandboxClass) -> Option<String> {
    sandbox_class_status(wp, class)
        .and_then(|s| s.config_ref.as_ref())
        .map(|r| r.name.clone())
}

/// Resolves every entry of `wp`'s spec. Returns one status entry per class
/// and whether every class resolved.
pub fn resolve_sandbox_classes(
    wp: &WorkerPool,
    configs: &[SandboxConfig],
    now: &Time,
) -> (Vec<WorkerPoolSandboxClassStatus>, bool) {
    let mut resolved = true;
    let statuses = wp
        .spec
        .sandbox_classes
        .iter()
        .map(|entry| {
            let in_use = in_use_of(wp, entry.name);
            let (sc, cond) = resolve_sandbox_config(entry, in_use.as_deref(), configs);
            resolved &= cond.resolved;
            sandbox_class_status_for(wp, entry.name, sc, &cond, now)
        })
        .collect();
    (statuses, resolved)
}

/// The status entry of `class`, using `sc` (if any) with `cond`. It starts
/// from the class's previous conditions, so the transition time only moves
/// when the condition's status does.
pub fn sandbox_class_status_for(
    wp: &WorkerPool,
    class: SandboxClass,
    sc: Option<&SandboxConfig>,
    cond: &Resolution,
    now: &Time,
) -> WorkerPoolSandboxClassStatus {
    let mut status = WorkerPoolSandboxClassStatus {
        name: class,
        ..Default::default()
    };
    if let Some(prev) = sandbox_class_status(wp, class) {
        status.conditions = prev.conditions.clone();
    }
    set_condition(
        &mut status.conditions,
        cond.to_condition(wp.metadata.generation),
        now.clone(),
    );
    status.config_ref = sc.map(|sc| SandboxConfigReference::new(name_of(sc)));
    status
}

/// Whether `class`, as resolved, uses a config other than the one `wp`'s
/// status uses for it.
pub fn switches_sandbox_config(wp: &WorkerPool, class: &WorkerPoolSandboxClassStatus) -> bool {
    switches_sandbox_config_from(status_classes(wp), class)
}

/// Whether `class`, as resolved, uses a config other than its entry in
/// `prev`.
pub fn switches_sandbox_config_from(
    prev: &[WorkerPoolSandboxClassStatus],
    class: &WorkerPoolSandboxClassStatus,
) -> bool {
    let Some(new) = &class.config_ref else {
        return false;
    };
    match find_sandbox_class_status(prev, class.name).and_then(|p| p.config_ref.as_ref()) {
        Some(old) => old.name != new.name,
        None => true,
    }
}

/// The status that claims every config a class in `classes` switches to,
/// or `None` if none switches. Each class keeps its status from `wp`, and a
/// switching class names its new config in `pendingConfigRef`, which
/// protects it like `configRef` while the switch is confirmed.
pub fn claim_sandbox_configs(
    wp: &WorkerPool,
    classes: &[WorkerPoolSandboxClassStatus],
) -> Option<Vec<WorkerPoolSandboxClassStatus>> {
    let mut claimed = false;
    let claim = classes
        .iter()
        .map(|class| {
            let mut status = match sandbox_class_status(wp, class.name) {
                Some(prev) => WorkerPoolSandboxClassStatus {
                    pending_config_ref: None,
                    ..prev.clone()
                },
                None => WorkerPoolSandboxClassStatus {
                    name: class.name,
                    ..Default::default()
                },
            };
            if switches_sandbox_config(wp, class) {
                status.pending_config_ref = class.config_ref.clone();
                claimed = true;
            }
            status
        })
        .collect();
    claimed.then_some(claim)
}

/// The status entry of a class switching to a new config. `live` is that
/// config as read from the API server after `wp`'s status claimed it. The
/// switch stands if it belongs to the class and is not being deleted;
/// otherwise the class keeps the config `wp`'s status uses, checked
/// against the cached `configs`, with a False condition.
pub fn confirm_sandbox_config_switch(
    wp: &WorkerPool,
    class: WorkerPoolSandboxClassStatus,
    live: Option<&SandboxConfig>,
    configs: &[SandboxConfig],
    now: &Time,
) -> WorkerPoolSandboxClassStatus {
    let name = class
        .config_ref
        .as_ref()
        .map(|r| r.name.clone())
        .unwrap_or_default();
    let (reason, message) = match live {
        None => (
            REASON_SANDBOX_CONFIG_NOT_FOUND,
            format!("SandboxConfig {name:?} not found"),
        ),
        Some(sc) if sc.spec.sandbox_class != class.name => (
            REASON_SANDBOX_CLASS_MISMATCH,
            format!(
                "SandboxConfig {name:?} is for sandbox class {:?}, not {:?}",
                sc.spec.sandbox_class.as_str(),
                class.name.as_str()
            ),
        ),
        Some(sc) if sc.is_deleting() => (
            REASON_SANDBOX_CONFIG_DELETING,
            format!("SandboxConfig {name:?} is being deleted"),
        ),
        Some(_) => return class,
    };
    let in_use = in_use_of(wp, class.name);
    let (sc, cond) = keep_sandbox_config(class.name, in_use.as_deref(), configs, reason, message);
    sandbox_class_status_for(wp, class.name, sc, &cond, now)
}

/// Whether every class has a config, resolved now or kept.
pub fn every_class_has_config(classes: &[WorkerPoolSandboxClassStatus]) -> bool {
    classes.iter().all(|c| c.config_ref.is_some())
}

/// Whether a change to `sc` can change `wp`'s resolution: the pool runs
/// its class (it may become or stop being the default), or the pool's spec
/// or status names it.
pub fn worker_pool_may_use_sandbox_config(wp: &WorkerPool, sc: &SandboxConfig) -> bool {
    let name = name_of(sc);
    wp.spec.sandbox_classes.iter().any(|e| {
        e.name == sc.spec.sandbox_class || e.config_ref.as_ref().is_some_and(|r| r.name == name)
    }) || wp.sandbox_configs_in_use().contains(&name.as_str())
}

#[cfg(test)]
#[path = "workerpool_sandboxconfig_tests.rs"]
mod tests;
