//! An in-memory Worker registry for tests.
//!
//! It reproduces what the syncer is written against and nothing more:
//! server-assigned uid and version, the uid+version precondition of every
//! update, the NOT_FOUND / ALREADY_EXISTS / ABORTED codes, Workers born
//! UNAVAILABLE, state calls that are idempotent down to leaving the version
//! alone, and paged listing. Request validation is the server's contract
//! and is not mirrored; a test that needs a rejection injects one.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_proto::api::v1 as pb;
use parking_lot::Mutex;
use tonic::Status;

use super::registry::WorkerRegistry;

type CreateHook = Box<dyn Fn(&pb::Worker) -> Result<(), Status> + Send + Sync>;
type ListHook = Box<dyn FnMut(&str) -> Result<(), Status> + Send + Sync>;
type UpdateHook = Box<dyn FnOnce(&str) + Send + Sync>;

#[derive(Default)]
struct State {
    workers: BTreeMap<String, pb::Worker>,
    uid_seq: u32,
    create_hook: Option<CreateHook>,
    list_hook: Option<ListHook>,
    update_hook: Option<UpdateHook>,
    list_page_size: usize,
}

#[derive(Default, Clone)]
pub struct FakeRegistry {
    st: Arc<Mutex<State>>,
}

fn state(w: &pb::Worker) -> i32 {
    w.status.as_ref().map(|s| s.state).unwrap_or_default()
}

fn not_found(name: &str) -> Status {
    Status::not_found(format!("Worker {name} not found"))
}

impl FakeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, name: &str) -> Option<pb::Worker> {
        self.st.lock().workers.get(name).cloned()
    }

    pub fn names(&self) -> Vec<String> {
        self.st.lock().workers.keys().cloned().collect()
    }

    /// Registers `w` as if created through the API (ACTIVE unless it
    /// carries a status).
    pub fn put(&self, w: pb::Worker) -> pb::Worker {
        Self::put_locked(&mut self.st.lock(), w)
    }

    fn put_locked(st: &mut State, mut w: pb::Worker) -> pb::Worker {
        st.uid_seq += 1;
        let name = w
            .metadata
            .as_ref()
            .map(|m| m.name.clone())
            .unwrap_or_default();
        w.metadata = Some(pb::ResourceMetadata {
            name: name.clone(),
            uid: format!("worker-uid-{}", st.uid_seq),
            version: 1,
            ..Default::default()
        });
        if w.status.is_none() {
            w.status = Some(pb::WorkerStatus {
                state: pb::WorkerState::Active as i32,
                ..Default::default()
            });
        }
        st.workers.insert(name, w.clone());
        w
    }

    pub fn set_create_hook(
        &self,
        h: impl Fn(&pb::Worker) -> Result<(), Status> + Send + Sync + 'static,
    ) {
        self.st.lock().create_hook = Some(Box::new(h));
    }

    pub fn set_list_hook(&self, h: impl FnMut(&str) -> Result<(), Status> + Send + Sync + 'static) {
        self.st.lock().list_hook = Some(Box::new(h));
    }

    /// Arms a hook run once before the next update applies, so a test can
    /// land a concurrent write.
    pub fn set_update_hook_once(&self, h: impl FnOnce(&str) + Send + Sync + 'static) {
        self.st.lock().update_hook = Some(Box::new(h));
    }

    pub fn set_list_page_size(&self, n: usize) {
        self.st.lock().list_page_size = n;
    }

    fn mark(&self, name: &str, to: pb::WorkerState) -> Result<Option<pb::Worker>, Status> {
        let mut st = self.st.lock();
        let stored = st.workers.get_mut(name).ok_or_else(|| not_found(name))?;
        if state(stored) == to as i32 {
            return Ok(Some(stored.clone()));
        }
        if state(stored) == pb::WorkerState::Draining as i32 {
            return Err(Status::failed_precondition(format!(
                "Worker {name} is draining"
            )));
        }
        stored.status.get_or_insert_with(Default::default).state = to as i32;
        stored.metadata.get_or_insert_with(Default::default).version += 1;
        Ok(Some(stored.clone()))
    }
}

#[async_trait]
impl WorkerRegistry for FakeRegistry {
    async fn get_worker(&self, name: &str) -> Result<pb::Worker, Status> {
        self.get(name).ok_or_else(|| not_found(name))
    }

    async fn create_worker(&self, worker: pb::Worker) -> Result<pb::Worker, Status> {
        let mut st = self.st.lock();
        if let Some(h) = &st.create_hook {
            h(&worker)?;
        }
        let name = worker
            .metadata
            .as_ref()
            .map(|m| m.name.clone())
            .unwrap_or_default();
        if st.workers.contains_key(&name) {
            return Err(Status::already_exists(format!(
                "Worker {name} already exists"
            )));
        }
        let mut created = worker;
        created.status = Some(pb::WorkerStatus {
            state: pb::WorkerState::Unavailable as i32,
            ..Default::default()
        });
        Ok(Self::put_locked(&mut st, created))
    }

    async fn update_worker(&self, worker: pb::Worker) -> Result<pb::Worker, Status> {
        let md = worker.metadata.clone().unwrap_or_default();
        // Outside the lock: the hook lands a concurrent write by calling in.
        let hook = self.st.lock().update_hook.take();
        if let Some(h) = hook {
            h(&md.name);
        }
        let mut st = self.st.lock();
        if md.uid.is_empty() || md.version == 0 {
            return Err(Status::invalid_argument(
                "update must carry metadata.uid and metadata.version",
            ));
        }
        let stored = st
            .workers
            .get(&md.name)
            .cloned()
            .ok_or_else(|| not_found(&md.name))?;
        let smd = stored.metadata.clone().unwrap_or_default();
        if md.uid != smd.uid || md.version != smd.version {
            return Err(Status::aborted("concurrent update conflict, please retry"));
        }
        let mut updated = worker;
        updated.metadata = stored.metadata.clone();
        updated.status = stored.status.clone();
        let mut probe = updated.clone();
        probe.labels = stored.labels.clone();
        if probe != stored {
            return Err(Status::invalid_argument(
                "update changed a field that is immutable after create",
            ));
        }
        updated
            .metadata
            .get_or_insert_with(Default::default)
            .version += 1;
        st.workers.insert(md.name, updated.clone());
        Ok(updated)
    }

    async fn delete_worker(&self, name: &str) -> Result<pb::Worker, Status> {
        self.st
            .lock()
            .workers
            .remove(name)
            .ok_or_else(|| not_found(name))
    }

    async fn drain_worker(&self, name: &str) -> Result<Option<pb::Worker>, Status> {
        let mut st = self.st.lock();
        let stored = st.workers.get_mut(name).ok_or_else(|| not_found(name))?;
        if state(stored) == pb::WorkerState::Draining as i32 {
            return Ok(Some(stored.clone()));
        }
        stored.status.get_or_insert_with(Default::default).state = pb::WorkerState::Draining as i32;
        stored.metadata.get_or_insert_with(Default::default).version += 1;
        Ok(Some(stored.clone()))
    }

    async fn mark_worker_available(&self, name: &str) -> Result<Option<pb::Worker>, Status> {
        self.mark(name, pb::WorkerState::Active)
    }

    async fn mark_worker_unavailable(&self, name: &str) -> Result<Option<pb::Worker>, Status> {
        self.mark(name, pb::WorkerState::Unavailable)
    }

    async fn list_workers(
        &self,
        page_size: i32,
        page_token: &str,
    ) -> Result<pb::ListWorkersResponse, Status> {
        let mut st = self.st.lock();
        if let Some(h) = st.list_hook.as_mut() {
            h(page_token)?;
        }
        let names: Vec<String> = st.workers.keys().cloned().collect();
        let start: usize = if page_token.is_empty() {
            0
        } else {
            page_token.parse().map_err(|_| {
                Status::invalid_argument(format!("malformed page token {page_token:?}"))
            })?
        };
        let mut size = usize::try_from(page_size).unwrap_or(0);
        if st.list_page_size > 0 {
            size = st.list_page_size;
        }
        if size == 0 {
            size = names.len();
        }
        let end = (start + size).min(names.len());
        Ok(pb::ListWorkersResponse {
            workers: names[start..end]
                .iter()
                .map(|n| st.workers[n].clone())
                .collect(),
            next_page_token: if end < names.len() {
                end.to_string()
            } else {
                String::new()
            },
        })
    }
}
