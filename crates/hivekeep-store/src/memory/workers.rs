//! Workers, their assignments and watches.

use async_trait::async_trait;
use hivekeep_core::compute::add_to_allocated;
use hivekeep_proto::api::v1 as pb;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::table::{self, read};
use super::MemoryStore;
use crate::api::*;
use crate::page_token::ListKind;
use crate::resource::{prepare_create, stamp_update, Resource};
use crate::watch::WATCH_BUFFER;
use crate::{
    DeletePreconditions, ListOptions, ListResponse, Precondition, Result, StoreError,
    WorkerEventType, WorkerWatch,
};

/// Advances a Worker's revision after an allocation change.
fn save_worker(worker: &mut pb::Worker) {
    let old = worker.metadata().cloned().unwrap_or_default();
    stamp_update(worker, &old, false);
}

fn allocated(worker: &pb::Worker) -> Option<&pb::WorkerResources> {
    worker.status.as_ref().and_then(|s| s.allocated.as_ref())
}

fn set_allocated(worker: &mut pb::Worker, total: Option<pb::WorkerResources>) {
    worker.status.get_or_insert_with(Default::default).allocated = total;
}

#[async_trait]
impl WorkerStore for MemoryStore {
    async fn create_worker(&self, worker: &pb::Worker) -> Result<pb::Worker> {
        let mut st = self.state.lock();
        let created = table::insert(&mut st.workers, worker.name().to_string(), worker)?;
        st.publish(WorkerEventType::Created, &created);
        Ok(created)
    }

    async fn get_worker(&self, name: &str) -> Result<pb::Worker> {
        table::get(&self.state.lock().workers, &name.to_string())
    }

    async fn list_workers(&self, opts: &ListOptions) -> Result<ListResponse<pb::Worker>> {
        let st = self.state.lock();
        let rows = st.workers.iter().map(|(n, v)| (vec![n.clone()], read(v)));
        table::paginate(rows, opts, ListKind::Worker, "", 1)
    }

    async fn update_worker(
        &self,
        name: &str,
        precondition: Precondition,
        mutate: Mutate<'_, pb::Worker>,
    ) -> Result<pb::Worker> {
        precondition.validate()?;
        let mut st = self.state.lock();
        let updated = table::update(
            &mut st.workers,
            &name.to_string(),
            &precondition.into(),
            Some(mutate),
            false,
        )?;
        st.publish(WorkerEventType::Updated, &updated);
        Ok(updated)
    }

    async fn mark_worker_for_deletion(
        &self,
        name: &str,
        precondition: DeletePreconditions,
        mutate: Option<Mutate<'_, pb::Worker>>,
    ) -> Result<pb::Worker> {
        let mut st = self.state.lock();
        let marked = table::update(
            &mut st.workers,
            &name.to_string(),
            &precondition,
            mutate,
            true,
        )?;
        st.publish(WorkerEventType::Updated, &marked);
        Ok(marked)
    }

    async fn delete_worker(
        &self,
        name: &str,
        precondition: DeletePreconditions,
    ) -> Result<pb::Worker> {
        let mut st = self.state.lock();
        let removed = table::remove(&mut st.workers, &name.to_string(), &precondition)?;
        st.assignments.retain(|_, (w, _)| w != name);
        st.publish(WorkerEventType::Deleted, &removed);
        Ok(removed)
    }

    async fn watch_workers(&self) -> Result<WorkerWatch> {
        let (tx, rx) = mpsc::channel(WATCH_BUFFER);
        self.state.lock().watchers.push(tx);
        Ok(WorkerWatch::new(rx, CancellationToken::new()))
    }
}

#[async_trait]
impl WorkerAssignmentStore for MemoryStore {
    async fn bind_actor_to_worker(
        &self,
        worker: &str,
        assignment: &pb::ActorAssignment,
        admit: Option<Admit<'_>>,
    ) -> Result<pb::ActorAssignment> {
        if assignment.actor_uid.is_empty() {
            return Err(StoreError::InvalidArgument(format!(
                "binding an assignment with no actor_uid to worker {worker}"
            )));
        }
        let uid = assignment.actor_uid.clone();
        let mut st = self.state.lock();
        let mut w = table::get(&st.workers, &worker.to_string())?;
        let previous = match st.assignments.get(&uid) {
            Some((holder, _)) if holder != worker => return Err(StoreError::FailedPrecondition),
            Some((_, prev)) => Some(prev.clone()),
            None => None,
        };

        let mut stored = assignment.clone();
        stored.metadata = Some(pb::ResourceMetadata {
            name: uid.clone(),
            ..Default::default()
        });
        match &previous {
            None => stored = prepare_create(&stored)?,
            Some(prev) => {
                let total = add_to_allocated(allocated(&w), prev, -1);
                set_allocated(&mut w, total);
                stamp_update(
                    &mut stored,
                    prev.metadata
                        .as_ref()
                        .expect("stored assignments carry metadata"),
                    false,
                );
            }
        }
        if let Some(admit) = admit {
            admit(&w)?;
        }
        let total = add_to_allocated(allocated(&w), &stored, 1);
        set_allocated(&mut w, total);
        save_worker(&mut w);

        st.assignments
            .insert(uid, (worker.to_string(), stored.clone()));
        st.workers.insert(worker.to_string(), w.clone());
        st.publish(WorkerEventType::Updated, &w);
        Ok(stored)
    }

    async fn release_actor_from_worker(
        &self,
        worker: &str,
        actor_uid: &str,
    ) -> Result<Option<pb::Worker>> {
        let mut st = self.state.lock();
        let mut w = table::get(&st.workers, &worker.to_string())?;
        let released = match st.assignments.get(actor_uid) {
            Some((holder, a)) if holder == worker => a.clone(),
            _ => return Ok(None),
        };
        st.assignments.remove(actor_uid);
        let total = add_to_allocated(allocated(&w), &released, -1);
        set_allocated(&mut w, total);
        save_worker(&mut w);
        st.workers.insert(worker.to_string(), w.clone());
        st.publish(WorkerEventType::Updated, &w);
        Ok(Some(w))
    }

    async fn get_worker_assignment(
        &self,
        worker: &str,
        actor_uid: &str,
    ) -> Result<pb::ActorAssignment> {
        match self.state.lock().assignments.get(actor_uid) {
            Some((holder, a)) if holder == worker => Ok(a.clone()),
            _ => Err(StoreError::NotFound),
        }
    }

    async fn list_worker_assignments(
        &self,
        worker: &str,
        opts: &ListOptions,
    ) -> Result<ListResponse<pb::ActorAssignment>> {
        let st = self.state.lock();
        let rows = st
            .assignments
            .iter()
            .filter(|(_, (w, _))| w == worker)
            .map(|(uid, (_, a))| (vec![uid.clone()], a.clone()));
        table::paginate(rows, opts, ListKind::WorkerAssignment, worker, 1)
    }

    async fn find_worker_hosting_actor(&self, actor_uid: &str) -> Result<String> {
        self.state
            .lock()
            .assignments
            .get(actor_uid)
            .map(|(w, _)| w.clone())
            .ok_or(StoreError::NotFound)
    }
}
