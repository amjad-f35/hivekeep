//! Placement of Actors onto Workers.
//!
//! [`Scheduler::schedule`] filters the fleet for workers that satisfy an
//! Actor's [`Constraints`] and have room, samples two of them at random and
//! returns the less loaded ("power of two choices"), where load is the higher
//! of actor-slot and compute utilization. The same eligibility and room
//! checks are asked again by the store under the worker's row lock when the
//! Actor is bound, since the fleet view here comes from a watch-fed cache.

use std::collections::HashMap;
use std::sync::Arc;

use hivekeep_core::compute;
use hivekeep_proto::api::v1 as pb;
use parking_lot::Mutex;

use crate::runtime_compat::{self, Requirement};

/// What a worker must satisfy to host an Actor.
#[derive(Debug, Clone, Default)]
pub struct Constraints {
    /// When set, the sandbox class of the snapshot the Actor restores from;
    /// the worker's must equal it. Never relaxed.
    pub sandbox_class: String,
    /// The runtime a worker must be able to restore; may be relaxed.
    pub required_runtime: Requirement,
    /// The runtime of the snapshot being restored; never relaxed. Picks the
    /// worker runtime, and so the overhead, the Actor is booked with.
    pub snapshot_runtime: Requirement,
    /// The template's selector; must match when set.
    pub template_selector: Option<HashMap<String, String>>,
    /// The Actor's own selector; must match (an empty one matches all).
    pub actor_selector: HashMap<String, String>,
    /// When non-empty, the nodes the worker must run on.
    pub required_nodes: Vec<String>,
    /// The Actor's declared limits.
    pub limits: Option<pb::Resources>,
}

/// No worker that meets the constraints has room.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("no worker that meets the constraints has room")]
pub struct NoCapacity;

/// Why scheduling failed.
#[derive(Debug, thiserror::Error)]
pub enum ScheduleError {
    /// Nothing fits.
    #[error(transparent)]
    NoCapacity(#[from] NoCapacity),
    /// The fleet could not be read.
    #[error("while listing workers: {0}")]
    Source(String),
}

/// The whole fleet.
pub trait WorkerSource: Send + Sync {
    /// Every known worker.
    fn workers(&self) -> Result<Vec<Arc<pb::Worker>>, String>;
}

type Intn = dyn Fn(usize) -> usize + Send + Sync;

/// Answers placement questions against a [`WorkerSource`].
pub struct Scheduler {
    source: Arc<dyn WorkerSource>,
    intn: Box<Intn>,
}

impl Scheduler {
    /// A scheduler over `source` with a time-seeded random source.
    pub fn new(source: Arc<dyn WorkerSource>) -> Self {
        let state = Mutex::new(seed());
        Scheduler {
            source,
            intn: Box::new(move |n| {
                let mut s = state.lock();
                // xorshift64*
                *s ^= *s >> 12;
                *s ^= *s << 25;
                *s ^= *s >> 27;
                (s.wrapping_mul(0x2545_F491_4F6C_DD1D) % n as u64) as usize
            }),
        }
    }

    /// A scheduler sampling candidates with `intn(n)`, a value in `[0, n)`.
    /// With two or more candidates it is called with `n` and then `n - 1`.
    pub fn with_intn(
        source: Arc<dyn WorkerSource>,
        intn: impl Fn(usize) -> usize + Send + Sync + 'static,
    ) -> Self {
        Scheduler {
            source,
            intn: Box::new(intn),
        }
    }

    /// A worker that meets `c` and has room.
    pub fn schedule(&self, c: &Constraints) -> Result<Arc<pb::Worker>, ScheduleError> {
        let workers = self.source.workers().map_err(ScheduleError::Source)?;
        let mut candidates: Vec<Candidate> = workers
            .into_iter()
            .filter(|w| self.applies(w, c))
            .filter_map(|w| {
                let booking = booking(&w, c)?;
                check_room(&w, booking.as_ref())?;
                Some(Candidate::new(w))
            })
            .collect();
        match candidates.len() {
            0 => Err(NoCapacity.into()),
            1 => Ok(candidates.remove(0).worker),
            n => {
                let i = (self.intn)(n);
                let mut j = (self.intn)(n - 1);
                if j >= i {
                    j += 1;
                }
                if less_loaded(&candidates[j], &candidates[i]) {
                    Ok(candidates.swap_remove(j).worker)
                } else {
                    Ok(candidates.swap_remove(i).worker)
                }
            }
        }
    }

    /// Whether `worker` satisfies the non-capacity constraints.
    pub fn applies(&self, worker: &pb::Worker, c: &Constraints) -> bool {
        applies(worker, c)
    }

    /// Whether `worker` has room for one more Actor of this size.
    pub fn has_room(&self, worker: &pb::Worker, c: &Constraints) -> bool {
        has_room(worker, c)
    }
}

fn seed() -> u64 {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E37_79B9_7F4A_7C15);
    t | 1
}

/// Whether the worker's current process has registered and reported its
/// sandbox runtimes; until then it cannot host anything.
pub fn registered(worker: &pb::Worker) -> bool {
    let st = worker.status.as_ref();
    st.is_some_and(|s| s.registered_epoch != 0) && !runtime_compat::sandbox_class(st).is_empty()
}

/// Whether `worker` satisfies the non-capacity constraints.
pub fn applies(worker: &pb::Worker, c: &Constraints) -> bool {
    if !registered(worker) {
        return false;
    }
    let st = worker.status.as_ref();
    if !c.sandbox_class.is_empty() && runtime_compat::sandbox_class(st) != c.sandbox_class {
        return false;
    }
    if st.map(|s| s.state) != Some(pb::WorkerState::Active as i32) {
        return false;
    }
    if !c.required_nodes.is_empty() && !c.required_nodes.contains(&worker.node_name) {
        return false;
    }
    if !c.required_runtime.worker_matches(st) {
        return false;
    }
    if let Some(sel) = &c.template_selector {
        if !selector_matches(sel, &worker.labels) {
            return false;
        }
    }
    selector_matches(&c.actor_selector, &worker.labels)
}

/// Whether every `key=value` of `selector` is in `labels`.
pub fn selector_matches(
    selector: &HashMap<String, String>,
    labels: &HashMap<String, String>,
) -> bool {
    selector.iter().all(|(k, v)| labels.get(k) == Some(v))
}

/// Whether `worker` has room for one more Actor of this size.
pub fn has_room(worker: &pb::Worker, c: &Constraints) -> bool {
    match booking(worker, c) {
        Some(b) => check_room(worker, b.as_ref()).is_some(),
        None => false,
    }
}

/// What the Actor takes from the worker: its limits plus the memory
/// overhead of the runtime it would run under there. `None` when that
/// overflows, in which case the Actor fits nowhere.
pub fn booking(worker: &pb::Worker, c: &Constraints) -> Option<Option<pb::Resources>> {
    let overhead = c
        .snapshot_runtime
        .worker_runtime_for(worker.status.as_ref())
        .and_then(|rt| rt.actor_overhead);
    let Some(overhead) = overhead else {
        return Some(c.limits);
    };
    let limit = c.limits.map(|l| l.memory_bytes).unwrap_or(0);
    let scaled = per_gib(limit, overhead.memory_bytes_per_gib)?;
    let memory = checked_sum(&[limit, overhead.memory_bytes, scaled])?;
    Some(Some(pb::Resources {
        cpu_millicores: c.limits.map(|l| l.cpu_millicores).unwrap_or(0),
        memory_bytes: memory,
    }))
}

fn per_gib(n: i64, rate: i64) -> Option<i64> {
    if n < 0 || rate < 0 {
        return None;
    }
    let v = (n as u128 * rate as u128) >> 30;
    i64::try_from(v).ok()
}

fn checked_sum(vs: &[i64]) -> Option<i64> {
    vs.iter().try_fold(
        0i64,
        |acc, &v| {
            if v < 0 {
                None
            } else {
                acc.checked_add(v)
            }
        },
    )
}

struct Candidate {
    worker: Arc<pb::Worker>,
}

impl Candidate {
    fn new(worker: Arc<pb::Worker>) -> Self {
        Candidate { worker }
    }

    fn slots(&self) -> (i64, i64) {
        let st = self.worker.status.as_ref();
        let alloc = st.and_then(|s| s.allocated).map(|a| a.actors).unwrap_or(0) as i64;
        let cap = st.and_then(|s| s.capacity).map(|a| a.actors).unwrap_or(0) as i64;
        (alloc, cap)
    }

    fn resource_utilization(&self) -> f64 {
        let st = self.worker.status.as_ref();
        utilization(st.and_then(|s| s.capacity), st.and_then(|s| s.allocated))
    }
}

fn less_loaded(a: &Candidate, b: &Candidate) -> bool {
    let (a_alloc, a_cap) = a.slots();
    let (b_alloc, b_cap) = b.slots();
    let a_load = (a_alloc as f64 / a_cap.max(1) as f64).max(a.resource_utilization());
    let b_load = (b_alloc as f64 / b_cap.max(1) as f64).max(b.resource_utilization());
    if a_load != b_load {
        return a_load < b_load;
    }
    let (lhs, rhs) = (a_alloc * b_cap, b_alloc * a_cap);
    if lhs != rhs {
        return lhs < rhs;
    }
    a_cap - a_alloc > b_cap - b_alloc
}

fn utilization(capacity: Option<pb::WorkerResources>, used: Option<pb::WorkerResources>) -> f64 {
    let c = capacity.unwrap_or_default();
    let u = used.unwrap_or_default();
    let mut max_ratio: f64 = 0.0;
    let mut has_positive = false;
    for (cap, use_) in [
        (c.cpu_millicores, u.cpu_millicores),
        (c.memory_bytes, u.memory_bytes),
    ] {
        if cap > 0 {
            has_positive = true;
            max_ratio = max_ratio.max(use_ as f64 / cap as f64);
        }
    }
    if has_positive {
        max_ratio
    } else {
        1.0
    }
}

fn check_room(worker: &pb::Worker, want: Option<&pb::Resources>) -> Option<()> {
    let st = worker.status.as_ref();
    let capacity = st.and_then(|s| s.capacity.as_ref());
    let used = st.and_then(|s| s.allocated.as_ref());
    // Every assignment takes a slot, however small.
    if used.map(|u| u.actors).unwrap_or(0) >= capacity.map(|c| c.actors).unwrap_or(0) {
        return None;
    }
    let want = want.copied().unwrap_or_default();
    if want.cpu_millicores == 0 && want.memory_bytes == 0 {
        return Some(());
    }
    let free = compute::free(capacity, used);
    compute::covers(Some(&free), Some(&want)).then_some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const GI: i64 = 1 << 30;

    struct Fleet(Vec<Arc<pb::Worker>>);
    impl WorkerSource for Fleet {
        fn workers(&self) -> Result<Vec<Arc<pb::Worker>>, String> {
            Ok(self.0.clone())
        }
    }

    fn worker(
        name: &str,
        node: &str,
        actors: (i32, i32),
        cpu: (i64, i64),
        labels: &[(&str, &str)],
    ) -> pb::Worker {
        pb::Worker {
            metadata: Some(pb::ResourceMetadata {
                name: name.into(),
                ..Default::default()
            }),
            node_name: node.into(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            status: Some(pb::WorkerStatus {
                state: pb::WorkerState::Active as i32,
                registered_epoch: 1,
                capacity: Some(pb::WorkerResources {
                    cpu_millicores: cpu.0,
                    memory_bytes: 8 * GI,
                    actors: actors.1,
                }),
                allocated: Some(pb::WorkerResources {
                    cpu_millicores: cpu.1,
                    memory_bytes: 0,
                    actors: actors.0,
                }),
                sandbox_classes: vec![pb::WorkerSandboxClass {
                    sandbox_class: "gvisor".into(),
                    default_runtime: Some(pb::WorkerSandboxRuntime {
                        version: Some(pb::VersionedSandboxCompat {
                            schema_version: "v1".into(),
                            attributes: vec![],
                        }),
                        actor_overhead: None,
                    }),
                    restorable_runtimes: vec![],
                }],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn sched(ws: Vec<pb::Worker>) -> Scheduler {
        Scheduler::with_intn(
            Arc::new(Fleet(ws.into_iter().map(Arc::new).collect())),
            |_| 0,
        )
    }

    fn name(w: &pb::Worker) -> &str {
        &w.metadata.as_ref().unwrap().name
    }

    #[test]
    fn no_capacity_when_empty_or_full() {
        assert!(matches!(
            sched(vec![]).schedule(&Constraints::default()),
            Err(ScheduleError::NoCapacity(_))
        ));
        let full = worker("w", "n", (2, 2), (4000, 0), &[]);
        assert!(matches!(
            sched(vec![full]).schedule(&Constraints::default()),
            Err(ScheduleError::NoCapacity(_))
        ));
    }

    #[test]
    fn picks_less_loaded_of_two() {
        let busy = worker("busy", "n", (3, 4), (4000, 0), &[]);
        let idle = worker("idle", "n", (0, 4), (4000, 0), &[]);
        // intn always 0: i = 0, j = 1.
        let got = sched(vec![busy.clone(), idle.clone()])
            .schedule(&Constraints::default())
            .unwrap();
        assert_eq!(name(&got), "idle");
        let got = sched(vec![idle, busy])
            .schedule(&Constraints::default())
            .unwrap();
        assert_eq!(name(&got), "idle");
    }

    #[test]
    fn filters_by_constraints() {
        let a = worker("a", "node-a", (0, 4), (4000, 0), &[("pool", "x")]);
        let b = worker("b", "node-b", (0, 4), (4000, 0), &[("pool", "y")]);
        let s = sched(vec![a.clone(), b.clone()]);
        let mut c = Constraints {
            required_nodes: vec!["node-b".into()],
            ..Default::default()
        };
        assert_eq!(name(&s.schedule(&c).unwrap()), "b");
        c.required_nodes.clear();
        c.actor_selector.insert("pool".into(), "x".into());
        assert_eq!(name(&s.schedule(&c).unwrap()), "a");
        c.template_selector = Some([("pool".to_string(), "y".to_string())].into());
        assert!(s.schedule(&c).is_err());
        c.actor_selector.clear();
        c.sandbox_class = "microvm".into();
        assert!(s.schedule(&c).is_err());

        let mut unregistered = a.clone();
        unregistered.status.as_mut().unwrap().registered_epoch = 0;
        assert!(!registered(&unregistered));
        let mut draining = a;
        draining.status.as_mut().unwrap().state = pb::WorkerState::Draining as i32;
        assert!(!applies(&draining, &Constraints::default()));
    }

    #[test]
    fn room_accounts_for_limits_and_overhead() {
        let mut w = worker("w", "n", (0, 4), (2000, 1500), &[]);
        let c = Constraints {
            limits: compute::cpu_memory(1000, GI),
            ..Default::default()
        };
        assert!(!has_room(&w, &c));
        w.status
            .as_mut()
            .unwrap()
            .allocated
            .as_mut()
            .unwrap()
            .cpu_millicores = 500;
        assert!(has_room(&w, &c));

        w.status.as_mut().unwrap().sandbox_classes[0]
            .default_runtime
            .as_mut()
            .unwrap()
            .actor_overhead = Some(pb::RuntimeOverhead {
            memory_bytes: 64 << 20,
            memory_bytes_per_gib: 32 << 20,
        });
        let b = booking(&w, &c).unwrap().unwrap();
        assert_eq!(b.memory_bytes, GI + (64 << 20) + (32 << 20));
        assert_eq!(b.cpu_millicores, 1000);
        let huge = Constraints {
            limits: compute::cpu_memory(0, i64::MAX),
            ..Default::default()
        };
        assert!(booking(&w, &huge).is_none());
    }
}
