//! In-process [`Store`](crate::Store) backend.
//!
//! All state sits behind one mutex, held for the whole of every operation
//! (including the caller's `mutate`/`admit` closures), so operations are
//! serializable. Records are kept as messages and defaulted on read, exactly
//! like the PostgreSQL backend's decoded rows, and parent/child rules are
//! enforced as the database's foreign keys would: children need their parent,
//! a Hive cannot be deleted while it holds Actors, ActorTemplates or Tags,
//! and deleting an Actor, Hive or Worker removes its dependent sub-resources.

mod resources;
mod table;
mod workers;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_proto::api::v1 as pb;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::api::{HealthCheck, LeaseStore};
use crate::{Lease, Result, StoreError, WorkerEvent, WorkerEventType};

type Key = (String, String);

#[derive(Default)]
struct State {
    hives: BTreeMap<String, pb::Hive>,
    actors: BTreeMap<Key, pb::Actor>,
    egress_policies: BTreeMap<Key, pb::EgressPolicy>,
    templates: BTreeMap<Key, pb::ActorTemplate>,
    tags: BTreeMap<Key, pb::Tag>,
    global_access_policy: Option<pb::AccessPolicy>,
    hive_access_policies: BTreeMap<String, pb::AccessPolicy>,
    workers: BTreeMap<String, pb::Worker>,
    /// Actor uid -> (worker name, assignment).
    assignments: BTreeMap<String, (String, pb::ActorAssignment)>,
    watchers: Vec<mpsc::Sender<WorkerEvent>>,
}

impl State {
    /// Delivers an event to every watch. A watch whose buffer is full is
    /// ended rather than silently skipped, so its owner re-lists.
    fn publish(&mut self, kind: WorkerEventType, worker: &pb::Worker) {
        self.watchers.retain(|tx| {
            tx.try_send(WorkerEvent {
                kind,
                worker: worker.clone(),
            })
            .is_ok()
        });
    }
}

/// An in-memory store with the full contract of [`Store`](crate::Store).
/// Cloning is cheap and clones share state.
#[derive(Clone, Default)]
pub struct MemoryStore {
    state: Arc<Mutex<State>>,
    leases: Arc<Mutex<HashMap<String, u64>>>,
    next_lease: Arc<Mutex<u64>>,
}

impl MemoryStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore").finish_non_exhaustive()
    }
}

fn key(hive: &str, name: &str) -> Key {
    (hive.to_string(), name.to_string())
}

#[async_trait]
impl LeaseStore for MemoryStore {
    async fn acquire_lease(&self, key: &str) -> Result<Lease> {
        let token = {
            let mut n = self.next_lease.lock();
            *n += 1;
            *n
        };
        {
            let mut leases = self.leases.lock();
            if leases.contains_key(key) {
                return Err(StoreError::LeaseConflict);
            }
            leases.insert(key.to_string(), token);
        }
        let leases = self.leases.clone();
        let owned_key = key.to_string();
        let release = async move {
            let mut leases = leases.lock();
            if leases.get(&owned_key) == Some(&token) {
                leases.remove(&owned_key);
            }
        };
        Ok(Lease::new(key, CancellationToken::new(), release))
    }
}

#[async_trait]
impl HealthCheck for MemoryStore {
    async fn ping(&self) -> Result<()> {
        Ok(())
    }
}
