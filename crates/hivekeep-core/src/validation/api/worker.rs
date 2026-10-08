//! Worker validation: the resource, its assignments and the WorkerService
//! requests keepers send.

use hivekeep_proto::api::v1 as pb;

use super::common::*;
use crate::field::{ErrorList, FieldError, Path};
use crate::validation::checks;
use crate::validation::rules::{
    self, int, list, map, message, string, Format, HiveRule, Operation,
};

fn worker_status(
    op: &Operation,
    p: &Path,
    n: &pb::WorkerStatus,
    o: Option<&pb::WorkerStatus>,
) -> ErrorList {
    rules::all([
        int(op, &p.child("state"), n.state, o.map(|o| o.state))
            .required()
            .min(1)
            .max(3)
            .finish(),
        message(
            op,
            &p.child("capacity"),
            n.capacity.as_ref(),
            o.map(|o| o.capacity.as_ref()),
        )
        .optional()
        .then(|p, n, o| worker_resources(op, p, n, o))
        .finish(),
        message(
            op,
            &p.child("allocated"),
            n.allocated.as_ref(),
            o.map(|o| o.allocated.as_ref()),
        )
        .optional()
        .then(|p, n, o| worker_resources(op, p, n, o))
        .finish(),
        int(
            op,
            &p.child("observed_epoch"),
            n.observed_epoch,
            o.map(|o| o.observed_epoch),
        )
        .optional()
        .min(0)
        .finish(),
        worker_sandbox_classes(
            op,
            &p.child("sandbox_classes"),
            &n.sandbox_classes,
            o.map(|o| o.sandbox_classes.as_slice()),
            false,
        ),
        list(op, &p.child("ips"), &n.ips, o.map(|o| o.ips.as_slice()))
            .max_items(2)
            .optional()
            .custom(checks::dual_stack_ips)
            .finish(),
        int(
            op,
            &p.child("issued_epoch"),
            n.issued_epoch,
            o.map(|o| o.issued_epoch),
        )
        .optional()
        .min(0)
        .finish(),
        int(
            op,
            &p.child("registered_epoch"),
            n.registered_epoch,
            o.map(|o| o.registered_epoch),
        )
        .optional()
        .min(0)
        .finish(),
    ])
}

/// A Worker resource. Global-scoped; its pod identity is immutable.
pub fn worker(op: &Operation, p: &Path, n: &pb::Worker, o: Option<&pb::Worker>) -> ErrorList {
    let id = |name: &str, new: &'_ str, old: Option<&'_ str>, f: Format| {
        string(op, &p.child(name), new, old)
            .immutable()
            .required()
            .format(f)
            .finish()
    };
    rules::all([
        metadata_field(
            op,
            &p.child("metadata"),
            n.metadata.as_ref(),
            o.map(|o| o.metadata.as_ref()),
            HiveRule::Forbidden,
        ),
        id(
            "worker_namespace",
            &n.worker_namespace,
            o.map(|o| o.worker_namespace.as_str()),
            Format::DnsLabel,
        ),
        id(
            "worker_pool",
            &n.worker_pool,
            o.map(|o| o.worker_pool.as_str()),
            Format::DnsSubdomain,
        ),
        id(
            "worker_pod",
            &n.worker_pod,
            o.map(|o| o.worker_pod.as_str()),
            Format::DnsSubdomain,
        ),
        id(
            "worker_pod_uid",
            &n.worker_pod_uid,
            o.map(|o| o.worker_pod_uid.as_str()),
            Format::Uuid,
        ),
        id(
            "node_name",
            &n.node_name,
            o.map(|o| o.node_name.as_str()),
            Format::DnsSubdomain,
        ),
        string(
            op,
            &p.child("worker_spiffe_id"),
            &n.worker_spiffe_id,
            o.map(|o| o.worker_spiffe_id.as_str()),
        )
        .immutable()
        .optional()
        .max_len(2048)
        .custom(checks::spiffe_id)
        .finish(),
        map(op, &p.child("labels"), &n.labels, o.map(|o| &o.labels))
            .optional()
            .max_entries(64)
            .each_key(Format::LabelKey)
            .each_value(Format::LabelValue)
            .finish(),
        message(
            op,
            &p.child("status"),
            n.status.as_ref(),
            o.map(|o| o.status.as_ref()),
        )
        .optional()
        .then(|p, n, o| worker_status(op, p, n, o))
        .finish(),
    ])
}

/// An ActorAssignment sub-resource of a Worker.
pub fn actor_assignment(
    op: &Operation,
    p: &Path,
    n: &pb::ActorAssignment,
    o: Option<&pb::ActorAssignment>,
) -> ErrorList {
    rules::all([
        metadata_field(
            op,
            &p.child("metadata"),
            n.metadata.as_ref(),
            o.map(|o| o.metadata.as_ref()),
            HiveRule::Forbidden,
        ),
        object_ref_field(
            op,
            &p.child("actor"),
            n.actor.as_ref(),
            o.map(|o| o.actor.as_ref()),
            HiveRule::Required,
        ),
        string(
            op,
            &p.child("actor_uid"),
            &n.actor_uid,
            o.map(|o| o.actor_uid.as_str()),
        )
        .required()
        .format(Format::Uuid)
        .finish(),
        message(
            op,
            &p.child("resources"),
            n.resources.as_ref(),
            o.map(|o| o.resources.as_ref()),
        )
        .optional()
        .then(|p, n, o| resources(op, p, n, o))
        .finish(),
    ])
}

/// Validates a Worker against the stored one. With `require_status`, a
/// missing status is an error.
pub fn validate_worker_update(
    op: &Operation,
    p: &Path,
    new: &pb::Worker,
    old: &pb::Worker,
    require_status: bool,
) -> ErrorList {
    let mut errs = worker(&op.as_update(), p, new, Some(old));
    if require_status && new.status.is_none() {
        errs.push(FieldError::required(&p.child("status"), ""));
    }
    errs
}

fn worker_ref(op: &Operation, r: Option<&pb::ObjectRef>) -> ErrorList {
    object_ref_field(op, &Path::from("worker"), r, None, HiveRule::Forbidden)
}

fn actor_ref(op: &Operation, r: Option<&pb::ObjectRef>) -> ErrorList {
    object_ref_field(op, &Path::from("actor"), r, None, HiveRule::Required)
}

fn actor_uid(op: &Operation, uid: &str) -> ErrorList {
    string(op, &Path::from("actor_uid"), uid, None)
        .required()
        .format(Format::Uuid)
        .finish()
}

fn epoch(op: &Operation, e: i64) -> ErrorList {
    int(op, &Path::from("epoch"), e, None::<i64>)
        .required()
        .min(1)
        .finish()
}

/// `Control.ListWorkerActorAssignments`.
pub fn validate_list_worker_actor_assignments_request(
    op: &Operation,
    req: &pb::ListWorkerActorAssignmentsRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        worker_ref(&op, req.worker.as_ref()),
        list_options(&op, &Path::from("options"), req.options.as_ref()),
    ])
}

/// `Control.ListWorkers`.
pub fn validate_list_workers_request(op: &Operation, req: &pb::ListWorkersRequest) -> ErrorList {
    list_options(
        &op.as_create(),
        &Path::from("options"),
        req.options.as_ref(),
    )
}

/// `Control.GetWorker`.
pub fn validate_get_worker_request(op: &Operation, req: &pb::GetWorkerRequest) -> ErrorList {
    worker_ref(&op.as_create(), req.worker.as_ref())
}

/// `Control.CreateWorker`.
pub fn validate_create_worker_request(op: &Operation, req: &pb::CreateWorkerRequest) -> ErrorList {
    let op = op.as_create();
    message(&op, &Path::from("worker"), req.worker.as_ref(), None)
        .required()
        .then(|p, n, _| worker(&op, p, n, None))
        .finish()
}

/// `Control.UpdateWorker`. Only the request shape and metadata are checked;
/// the merged result is checked with [`validate_worker_update`].
pub fn validate_update_worker_request(op: &Operation, req: &pb::UpdateWorkerRequest) -> ErrorList {
    let op = op.as_create();
    message(&op, &Path::from("worker"), req.worker.as_ref(), None)
        .required()
        .then(|p, n, _| {
            metadata_field(
                &op,
                &p.child("metadata"),
                n.metadata.as_ref(),
                None,
                HiveRule::Forbidden,
            )
        })
        .finish()
}

/// `Control.DeleteWorker`. Each precondition is optional.
pub fn validate_delete_worker_request(op: &Operation, req: &pb::DeleteWorkerRequest) -> ErrorList {
    let op = op.as_create();
    rules::all([
        worker_ref(&op, req.worker.as_ref()),
        delete_options(&op, &Path::from("options"), req.options.as_ref()),
    ])
}

/// `Control.DrainWorker`.
pub fn validate_drain_worker_request(op: &Operation, req: &pb::DrainWorkerRequest) -> ErrorList {
    worker_ref(&op.as_create(), req.worker.as_ref())
}

/// `Control.MarkWorkerAvailable`.
pub fn validate_mark_worker_available_request(
    op: &Operation,
    req: &pb::MarkWorkerAvailableRequest,
) -> ErrorList {
    worker_ref(&op.as_create(), req.worker.as_ref())
}

/// `Control.MarkWorkerUnavailable`.
pub fn validate_mark_worker_unavailable_request(
    op: &Operation,
    req: &pb::MarkWorkerUnavailableRequest,
) -> ErrorList {
    worker_ref(&op.as_create(), req.worker.as_ref())
}

/// `WorkerService.AcquireWorkerEpoch`.
pub fn validate_acquire_worker_epoch_request(
    op: &Operation,
    req: &pb::AcquireWorkerEpochRequest,
) -> ErrorList {
    worker_ref(&op.as_create(), req.worker.as_ref())
}

/// `WorkerService.RegisterWorker`.
pub fn validate_register_worker_request(
    op: &Operation,
    req: &pb::RegisterWorkerRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        worker_ref(&op, req.worker.as_ref()),
        epoch(&op, req.epoch),
        list(&op, &Path::from("ips"), &req.ips, None)
            .max_items(2)
            .required()
            .custom(checks::dual_stack_ips)
            .finish(),
        message(&op, &Path::from("capacity"), req.capacity.as_ref(), None)
            .required()
            .then(|p, n, _| worker_resources(&op, p, n, None))
            .finish(),
    ])
}

/// `WorkerService.SetWorkerSandboxRuntimes`.
pub fn validate_set_worker_sandbox_runtimes_request(
    op: &Operation,
    req: &pb::SetWorkerSandboxRuntimesRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        worker_ref(&op, req.worker.as_ref()),
        epoch(&op, req.epoch),
        worker_sandbox_classes(
            &op,
            &Path::from("sandbox_classes"),
            &req.sandbox_classes,
            None,
            true,
        ),
    ])
}

/// `WorkerService.RequestActorSuspend`.
pub fn validate_request_actor_suspend_request(
    op: &Operation,
    req: &pb::RequestActorSuspendRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        worker_ref(&op, req.worker.as_ref()),
        actor_ref(&op, req.actor.as_ref()),
        actor_uid(&op, &req.actor_uid),
    ])
}

/// `WorkerService.ReportActorCrashed`.
pub fn validate_report_actor_crashed_request(
    op: &Operation,
    req: &pb::ReportActorCrashedRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        worker_ref(&op, req.worker.as_ref()),
        actor_ref(&op, req.actor.as_ref()),
        actor_uid(&op, &req.actor_uid),
        string(&op, &Path::from("message"), &req.message, None)
            .required()
            .max_len(4096)
            .finish(),
    ])
}

/// `WorkerService.ReportActorSnapshotState`. A report with neither
/// `latest_local` nor `uploading` is a detach and must name the snapshot it
/// made durable.
pub fn validate_report_actor_snapshot_state_request(
    op: &Operation,
    req: &pb::ReportActorSnapshotStateRequest,
) -> ErrorList {
    let op = op.as_create();
    let mut errs = rules::all([
        actor_ref(&op, req.actor.as_ref()),
        actor_uid(&op, &req.actor_uid),
        message(&op, &Path::from("state"), req.state.as_ref(), None)
            .required()
            .then(|p, n, _| actor_snapshot_state(&op, p, n))
            .finish(),
    ]);
    if let Some(st) = &req.state {
        if st.latest_local.is_none() && st.uploading.is_none() && st.latest_durable.is_none() {
            errs.push(FieldError::required(
                &Path::new(["state", "latest_durable"]),
                "a report with neither latest_local nor uploading must set latest_durable",
            ));
        }
    }
    errs
}
