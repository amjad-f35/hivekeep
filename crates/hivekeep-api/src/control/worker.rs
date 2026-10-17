//! Worker RPCs.

use hivekeep_core::defaults;
use hivekeep_core::field::Path;
use hivekeep_core::validation::api as v;
use hivekeep_core::validation::Operation;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{Precondition, StoreError};
use parking_lot::Mutex;

use super::{
    delete_preconditions, internal, invalid, list_options, scrub_for_create, scrub_for_update,
    ControlService,
};
use crate::authz::{checks, model::rel};
use crate::context::CallContext;
use crate::error::{reject, status, Error, Result};
use crate::fields;

/// Ends a mutation that found its work already done; the Worker and its
/// version stay untouched.
#[derive(Debug, thiserror::Error)]
#[error("worker is already in the requested state")]
struct Unchanged;

fn name_of(r: Option<&pb::ObjectRef>) -> String {
    r.map(|r| r.name.clone()).unwrap_or_default()
}

fn validated_update(
    op: &Operation,
    new: &pb::Worker,
    old: &pb::Worker,
) -> hivekeep_store::Result<()> {
    let p = Path::from("worker");
    v::validate_worker_update(op, &p, new, old, false)
        .into_result()
        .map_err(|e| reject(Error::Status(e.to_invalid_argument())))?;
    v::validate_worker_update(op, &p, new, old, true)
        .into_result()
        .map_err(|e| reject(Error::Status(e.to_internal())))
}

impl ControlService {
    pub(crate) async fn list_worker_actor_assignments(
        &self,
        cc: &CallContext,
        req: pb::ListWorkerActorAssignmentsRequest,
    ) -> Result<pb::ListWorkerActorAssignmentsResponse> {
        self.authorize(
            cc,
            checks::worker_ref(rel::CAN_LIST_ACTOR_ASSIGNMENTS, req.worker.as_ref()),
        )
        .await?;
        invalid(v::validate_list_worker_actor_assignments_request(
            &self.op(cc),
            &req,
        ))?;
        let name = name_of(req.worker.as_ref());
        // A missing Worker is NOT_FOUND, not an empty page.
        self.get_worker_record(&name).await?;
        let page = self
            .store()
            .list_worker_assignments(&name, &list_options(req.options.as_ref()))
            .await?;
        Ok(pb::ListWorkerActorAssignmentsResponse {
            actor_assignments: page.items,
            next_page_token: page.next_page_token,
        })
    }

    pub(crate) async fn list_workers(
        &self,
        cc: &CallContext,
        req: pb::ListWorkersRequest,
    ) -> Result<pb::ListWorkersResponse> {
        self.authorize(cc, checks::on_global(rel::CAN_LIST_WORKERS))
            .await?;
        invalid(v::validate_list_workers_request(&self.op(cc), &req))?;
        let page = self
            .store()
            .list_workers(&list_options(req.options.as_ref()))
            .await?;
        Ok(pb::ListWorkersResponse {
            workers: page.items,
            next_page_token: page.next_page_token,
        })
    }

    async fn get_worker_record(&self, name: &str) -> Result<pb::Worker> {
        self.store().get_worker(name).await.map_err(|e| match e {
            StoreError::NotFound => status::not_found(format!("Worker {name} not found")),
            e => Error::from(e).context("while getting worker"),
        })
    }

    pub(crate) async fn get_worker(
        &self,
        cc: &CallContext,
        req: pb::GetWorkerRequest,
    ) -> Result<pb::Worker> {
        self.authorize(cc, checks::worker_ref(rel::CAN_GET, req.worker.as_ref()))
            .await?;
        invalid(v::validate_get_worker_request(&self.op(cc), &req))?;
        self.get_worker_record(&name_of(req.worker.as_ref())).await
    }

    pub(crate) async fn create_worker(
        &self,
        cc: &CallContext,
        mut req: pb::CreateWorkerRequest,
    ) -> Result<pb::Worker> {
        self.authorize(cc, checks::on_global(rel::CAN_CREATE_WORKER))
            .await?;
        if let Some(w) = req.worker.as_mut() {
            scrub_for_create(w.metadata.as_mut());
            w.status = None;
            defaults::apply(w);
        }
        let op = self.op(cc);
        invalid(v::validate_create_worker_request(&op, &req))?;
        let input = req.worker.unwrap_or_default();
        // A Worker takes no Actors until MarkWorkerAvailable.
        let mut out = input.clone();
        out.status = Some(pb::WorkerStatus {
            state: pb::WorkerState::Unavailable as i32,
            ..Default::default()
        });
        internal(v::validate_worker_update(
            &op,
            &Path::from("worker"),
            &out,
            &input,
            true,
        ))?;
        self.store().create_worker(&out).await.map_err(|e| match e {
            StoreError::AlreadyExists => status::already_exists(format!(
                "Worker {} already exists",
                fields::worker_name(&input)
            )),
            e => Error::from(e).context("while creating worker"),
        })
    }

    pub(crate) async fn update_worker(
        &self,
        cc: &CallContext,
        mut req: pb::UpdateWorkerRequest,
    ) -> Result<pb::Worker> {
        let name = req
            .worker
            .as_ref()
            .map(|w| fields::worker_name(w).to_string())
            .unwrap_or_default();
        self.authorize(
            cc,
            checks::on_worker(rel::CAN_UPDATE, &name, "worker.metadata.name"),
        )
        .await?;
        if let Some(w) = req.worker.as_mut() {
            scrub_for_update(w.metadata.as_mut());
            w.status = None;
        }
        let op = self.op(cc);
        invalid(v::validate_update_worker_request(&op, &req))?;
        let input = req.worker.unwrap_or_default();
        self.mutate_worker(&op, &name, Precondition::of(&input), &mut |to| {
            let (st, md) = (to.status.take(), to.metadata.take());
            *to = input.clone();
            to.status = st;
            to.metadata = md;
            defaults::apply(to);
            Ok(())
        })
        .await
    }

    pub(crate) async fn delete_worker(
        &self,
        cc: &CallContext,
        req: pb::DeleteWorkerRequest,
    ) -> Result<pb::Worker> {
        self.authorize(cc, checks::worker_ref(rel::CAN_DELETE, req.worker.as_ref()))
            .await?;
        invalid(v::validate_delete_worker_request(&self.op(cc), &req))?;
        self.inner
            .workers
            .delete_worker(
                &name_of(req.worker.as_ref()),
                delete_preconditions(req.options.as_ref()),
            )
            .await
    }

    pub(crate) async fn drain_worker(
        &self,
        cc: &CallContext,
        req: pb::DrainWorkerRequest,
    ) -> Result<pb::DrainWorkerResponse> {
        self.authorize(cc, checks::worker_ref(rel::CAN_DRAIN, req.worker.as_ref()))
            .await?;
        let op = self.op(cc);
        invalid(v::validate_drain_worker_request(&op, &req))?;
        let w = self
            .mutate_named_worker(
                &op,
                &name_of(req.worker.as_ref()),
                pb::WorkerState::Draining,
            )
            .await?;
        Ok(pb::DrainWorkerResponse { worker: Some(w) })
    }

    pub(crate) async fn mark_worker_available(
        &self,
        cc: &CallContext,
        req: pb::MarkWorkerAvailableRequest,
    ) -> Result<pb::MarkWorkerAvailableResponse> {
        self.authorize(cc, checks::worker_ref(rel::CAN_UPDATE, req.worker.as_ref()))
            .await?;
        let op = self.op(cc);
        invalid(v::validate_mark_worker_available_request(&op, &req))?;
        let w = self
            .mutate_named_worker(&op, &name_of(req.worker.as_ref()), pb::WorkerState::Active)
            .await?;
        Ok(pb::MarkWorkerAvailableResponse { worker: Some(w) })
    }

    pub(crate) async fn mark_worker_unavailable(
        &self,
        cc: &CallContext,
        req: pb::MarkWorkerUnavailableRequest,
    ) -> Result<pb::MarkWorkerUnavailableResponse> {
        self.authorize(cc, checks::worker_ref(rel::CAN_UPDATE, req.worker.as_ref()))
            .await?;
        let op = self.op(cc);
        invalid(v::validate_mark_worker_unavailable_request(&op, &req))?;
        let w = self
            .mutate_named_worker(
                &op,
                &name_of(req.worker.as_ref()),
                pb::WorkerState::Unavailable,
            )
            .await?;
        Ok(pb::MarkWorkerUnavailableResponse { worker: Some(w) })
    }

    /// Moves a Worker to `target` with guards read here. DRAINING is final;
    /// a Worker already in `target` is returned without a write.
    async fn mutate_named_worker(
        &self,
        op: &Operation,
        name: &str,
        target: pb::WorkerState,
    ) -> Result<pb::Worker> {
        let observed = self.get_worker_record(name).await?;
        self.mutate_worker(op, name, Precondition::of(&observed), &mut |to| {
            let current = fields::worker_state(to);
            if current == target {
                return Err(hivekeep_store::StoreError::rejected(Unchanged));
            }
            if current == pb::WorkerState::Draining {
                return Err(reject(status::failed_precondition(format!(
                    "Worker {name} is draining"
                ))));
            }
            to.status.get_or_insert_with(Default::default).state = target as i32;
            Ok(())
        })
        .await
    }

    async fn mutate_worker(
        &self,
        op: &Operation,
        name: &str,
        pre: Precondition,
        mutate: &mut (dyn FnMut(&mut pb::Worker) -> hivekeep_store::Result<()> + Send),
    ) -> Result<pb::Worker> {
        let unchanged: Mutex<Option<pb::Worker>> = Mutex::new(None);
        let res = self
            .store()
            .update_worker(name, pre, &mut |to| {
                let old = to.clone();
                if let Err(e) = mutate(to) {
                    if e.rejection::<Unchanged>().is_some() {
                        *unchanged.lock() = Some(old);
                    }
                    return Err(e);
                }
                validated_update(op, to, &old)
            })
            .await;
        match res {
            Ok(w) => Ok(w),
            Err(e) if e.rejection::<Unchanged>().is_some() => {
                Ok(unchanged.lock().take().unwrap_or_default())
            }
            Err(e) => Err(match Error::from(e) {
                Error::Store(StoreError::NotFound) => {
                    status::not_found(format!("Worker {name} not found"))
                }
                Error::Store(StoreError::UidConflict) => status::aborted(format!(
                    "Worker {name} is not the one the request describes"
                )),
                Error::Store(e @ StoreError::PreconditionRequired(_)) => {
                    status::invalid_argument(format!("while updating worker {name}: {e}"))
                }
                e => e,
            }),
        }
    }
}
