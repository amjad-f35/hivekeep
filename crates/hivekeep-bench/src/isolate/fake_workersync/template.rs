//! What a WorkerPool's spec fixes about each Worker it makes.

use std::collections::BTreeMap;

use hivekeep_k8s::crd::WorkerPool;
use hivekeep_proto::api::v1 as api;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::ResourceExt;

use crate::isolate::fakeworker::{DEFAULT_MAX_ACTORS, MAX_ACTORS_ANNOTATION};
use crate::isolate::quantity;

/// The sandbox class, CPU and memory limits (zero where unset) and Actor
/// count of a pool's Workers. Changing any of them replaces a real pool's
/// worker pods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Template {
    /// Sandbox class name.
    pub sandbox_class: String,
    /// CPU limit in millicores, 0 when unset.
    pub cpu_milli: i64,
    /// Memory limit in bytes, 0 when unset.
    pub memory_bytes: i64,
    /// Actors per Worker.
    pub actors: i32,
}

/// The sandbox class name of a pool: its first class, gVisor without one.
pub fn sandbox_class(wp: &WorkerPool) -> String {
    wp.spec
        .default_sandbox_class()
        .map(|c| c.as_str())
        .unwrap_or(hivekeep_core::sandbox_class::GVISOR)
        .to_string()
}

/// The pool's Actor capacity per Worker: the worker's default, unless the
/// pool's annotation overrides it. Bounded to 32 bits, the width of a
/// capacity report's Actor count.
pub fn max_actors(wp: &WorkerPool) -> Result<i32, String> {
    let Some(v) = wp.annotations().get(MAX_ACTORS_ANNOTATION) else {
        return Ok(DEFAULT_MAX_ACTORS);
    };
    match v.parse::<i32>() {
        Ok(n) if n >= 1 => Ok(n),
        _ => Err(format!(
            "WorkerPool {}/{}: {MAX_ACTORS_ANNOTATION} must be an integer from 1 to {}, got {v:?}",
            wp.namespace().unwrap_or_default(),
            wp.name_any(),
            i32::MAX
        )),
    }
}

/// The pool's template; fails only on a bad max-actors annotation.
pub fn template_of(wp: &WorkerPool) -> Result<Template, String> {
    let actors = max_actors(wp)?;
    let limits = wp
        .spec
        .template
        .as_ref()
        .and_then(|t| t.resources.as_ref())
        .and_then(|r| r.limits.as_ref());
    Ok(Template {
        sandbox_class: sandbox_class(wp),
        cpu_milli: quantity::cpu_milli(limits),
        memory_bytes: quantity::memory_bytes(limits),
        actors,
    })
}

impl Template {
    /// What a worker made from this template reports on a node with
    /// `allocatable`: CPU and memory from its limits, or, with no limit set,
    /// the node's allocatable, which the downward API projects into the
    /// worker for an unset limit.
    pub fn capacity(
        &self,
        allocatable: Option<&BTreeMap<String, Quantity>>,
    ) -> api::WorkerResources {
        let cpu = if self.cpu_milli == 0 {
            quantity::cpu_milli(allocatable)
        } else {
            self.cpu_milli
        };
        let memory = if self.memory_bytes == 0 {
            quantity::memory_bytes(allocatable)
        } else {
            self.memory_bytes
        };
        api::WorkerResources {
            actors: self.actors,
            cpu_millicores: cpu,
            memory_bytes: memory,
        }
    }
}
