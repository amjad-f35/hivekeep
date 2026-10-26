//! User classes: what each virtual user runs, and the registry the worker
//! picks its one class from.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::control::ControlApi;
use super::dynconfig::{Codec, Holder};
use super::trace::Tracer;

/// Identifies one virtual user for the life of a run. IDs are never reused,
/// so a class can key per-user state by them.
pub type VuId = u64;

/// The work of one user class.
#[async_trait]
pub trait UserTask: Send + Sync + 'static {
    /// Runs one iteration for virtual user `vu`. The runner calls it in a
    /// loop for as long as the user exists; the first call for a `vu` is
    /// the user's start.
    async fn iterate(&self, vu: VuId);

    /// Cleans up everything the class created, once the run is over. The
    /// caller bounds it with a timeout.
    async fn shutdown(&self);
}

/// The dependencies a user class needs, built once at start-up.
#[derive(Clone)]
pub struct Config {
    /// The shared Control API client.
    pub api: Arc<dyn ControlApi>,
    /// The shared HTTP client for traffic through the gate router.
    pub http: reqwest::Client,
    /// The gate router's base URL, without a trailing slash.
    pub router_url: String,
    /// The Hive every Actor this worker creates lives in.
    pub hive: String,
    /// The runtime-mutable config, built from the class's codec. A class
    /// reads its knobs from it on every iteration.
    pub dyn_config: Arc<Holder>,
    /// Anchors sampled spans.
    pub tracer: Tracer,
    /// Actors each user creates and cycles through round-robin (iteration
    /// `i` targets actor `i % actors_per_user`); below 1 reads as 1 for the
    /// classes that honour it.
    pub actors_per_user: usize,
    /// Actors to create in the batch (spawn benchmark).
    pub total_actors: usize,
    /// Actors created concurrently (spawn benchmark).
    pub spawn_concurrency: usize,
    /// Per-Actor budget for create + resume + first request (spawn
    /// benchmark).
    pub actor_deadline: Duration,
}

/// The constructor of a class's config codec.
pub type CodecFn = fn() -> Box<dyn Codec>;

/// The constructor of a class's task.
pub type InitFn = fn(Arc<Config>) -> Arc<dyn UserTask>;

/// Declares one user class.
#[derive(Clone)]
pub struct Entry {
    /// The `--user-class` value that selects it.
    pub name: &'static str,
    /// The locust file the master loads for it.
    pub locust_file: &'static str,
    /// The locust user class name. The master's spawn messages count users
    /// by this name, so it must match or no users start.
    pub user_class: &'static str,
    /// The class's config codec; `None` for a class without knobs.
    pub config: Option<CodecFn>,
    /// Builds the class's task.
    pub init: InitFn,
}

impl Entry {
    /// A holder over this class's codec, at its defaults.
    pub fn holder(&self) -> Holder {
        match self.config {
            Some(codec) => Holder::new(codec()),
            None => Holder::without_knobs(),
        }
    }
}

/// The user classes a worker can run, by name.
#[derive(Default, Clone)]
pub struct Registry {
    entries: BTreeMap<&'static str, Entry>,
}

impl Registry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `e`. Two classes under one name is a programming error and
    /// panics.
    pub fn add(&mut self, e: Entry) {
        if self.entries.contains_key(e.name) {
            panic!("userclass: duplicate registration for {:?}", e.name);
        }
        self.entries.insert(e.name, e);
    }

    /// The class registered under `name`.
    pub fn lookup(&self, name: &str) -> Option<&Entry> {
        self.entries.get(name)
    }

    /// Every registered name, sorted.
    pub fn names(&self) -> Vec<&'static str> {
        self.entries.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Nop;

    #[async_trait]
    impl UserTask for Nop {
        async fn iterate(&self, _vu: VuId) {}
        async fn shutdown(&self) {}
    }

    fn entry(name: &'static str, user_class: &'static str) -> Entry {
        Entry {
            name,
            locust_file: "test.py",
            user_class,
            config: None,
            init: |_| Arc::new(Nop),
        }
    }

    #[test]
    fn add_rejects_duplicate() {
        let mut r = Registry::new();
        r.add(entry("test-dup", "TestDupUser"));
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            r.clone().add(entry("test-dup", "TestDupUser2"))
        }));
        assert!(res.is_err(), "a duplicate name must panic");
        assert_eq!(r.lookup("test-dup").unwrap().user_class, "TestDupUser");
    }

    #[test]
    fn lookup_unknown() {
        let mut r = Registry::new();
        r.add(entry("b", "B"));
        r.add(entry("a", "A"));
        assert!(r.lookup("nonexistent-class-name").is_none());
        assert_eq!(r.names(), vec!["a", "b"]);
        assert!(r.lookup("a").unwrap().holder().common().trace_probability == 0.0);
    }
}
