//! Actor validation: the resource, its status, and the Actor RPC requests.

use hivekeep_proto::api::v1 as pb;

use super::common::*;
use crate::field::{ErrorList, FieldError, Path};
use crate::validation::checks;
use crate::validation::rules::{
    self, bytes, int, list, map, message, string, Format, HiveRule, Operation,
};

/// The worker hosting an Actor. Its identity fields may be set and cleared
/// but never re-pointed in place.
pub fn worker_assignment(
    op: &Operation,
    p: &Path,
    n: &pb::WorkerAssignment,
    o: Option<&pb::WorkerAssignment>,
) -> ErrorList {
    let s = |name: &str, new: &'_ str, old: Option<&'_ str>, f: Format| {
        string(op, &p.child(name), new, old)
            .no_modify()
            .required()
            .format(f)
            .finish()
    };
    rules::all([
        message(
            op,
            &p.child("worker"),
            n.worker.as_ref(),
            o.map(|o| o.worker.as_ref()),
        )
        .no_modify()
        .required()
        .then(|p, n, _| object_ref(op, p, n, HiveRule::Forbidden))
        .finish(),
        s(
            "worker_namespace",
            &n.worker_namespace,
            o.map(|o| o.worker_namespace.as_str()),
            Format::DnsLabel,
        ),
        s(
            "worker_pool",
            &n.worker_pool,
            o.map(|o| o.worker_pool.as_str()),
            Format::DnsSubdomain,
        ),
        s(
            "worker_pod",
            &n.worker_pod,
            o.map(|o| o.worker_pod.as_str()),
            Format::DnsSubdomain,
        ),
        s(
            "worker_pod_uid",
            &n.worker_pod_uid,
            o.map(|o| o.worker_pod_uid.as_str()),
            Format::Uuid,
        ),
        list(
            op,
            &p.child("worker_pod_ips"),
            &n.worker_pod_ips,
            o.map(|o| o.worker_pod_ips.as_slice()),
        )
        .max_items(2)
        .required()
        .custom(checks::dual_stack_ips)
        .finish(),
        s(
            "node_name",
            &n.node_name,
            o.map(|o| o.node_name.as_str()),
            Format::DnsSubdomain,
        ),
        int(
            op,
            &p.child("worker_epoch"),
            n.worker_epoch,
            o.map(|o| o.worker_epoch),
        )
        .optional()
        .min(0)
        .finish(),
        message(
            op,
            &p.child("sandbox_runtime"),
            n.sandbox_runtime.as_ref(),
            o.map(|o| o.sandbox_runtime.as_ref()),
        )
        .no_modify()
        .optional()
        .then(|p, n, o| actor_sandbox_runtime(op, p, n, o))
        .finish(),
        string(
            op,
            &p.child("worker_spiffe_id"),
            &n.worker_spiffe_id,
            o.map(|o| o.worker_spiffe_id.as_str()),
        )
        .no_modify()
        .optional()
        .max_len(2048)
        .custom(checks::spiffe_id)
        .finish(),
    ])
}

/// One external volume recorded on an Actor. Identity fields are set once.
pub fn external_volume_status(
    op: &Operation,
    p: &Path,
    n: &pb::ExternalVolumeStatus,
    o: Option<&pb::ExternalVolumeStatus>,
) -> ErrorList {
    rules::all([
        string(op, &p.child("name"), &n.name, o.map(|o| o.name.as_str()))
            .no_modify()
            .no_unset()
            .required()
            .format(Format::DnsLabel)
            .finish(),
        string(
            op,
            &p.child("storage_volume_id"),
            &n.storage_volume_id,
            o.map(|o| o.storage_volume_id.as_str()),
        )
        .no_modify()
        .no_unset()
        .optional()
        .max_len(256)
        .custom(checks::storage_volume_id)
        .finish(),
        string(
            op,
            &p.child("volume_type"),
            &n.volume_type,
            o.map(|o| o.volume_type.as_str()),
        )
        .no_modify()
        .no_unset()
        .required()
        .max_len(253)
        .custom(checks::volume_type)
        .finish(),
        map(
            op,
            &p.child("volume_context"),
            &n.volume_context,
            o.map(|o| &o.volume_context),
        )
        .optional()
        .custom(checks::csi_map)
        .finish(),
    ])
}

fn external_volumes(
    op: &Operation,
    p: &Path,
    n: &pb::ExternalVolumes,
    o: Option<&pb::ExternalVolumes>,
) -> ErrorList {
    list(
        op,
        &p.child("volumes"),
        &n.volumes,
        o.map(|o| o.volumes.as_slice()),
    )
    .max_items(32)
    .optional()
    .each_keyed(
        |v| v.name.clone(),
        |p, n, o| external_volume_status(op, p, n, o),
    )
    .unique_by(|v| v.name.clone())
    .finish()
}

/// Server-owned Actor state.
pub fn actor_status(
    op: &Operation,
    p: &Path,
    n: &pb::ActorStatus,
    o: Option<&pb::ActorStatus>,
) -> ErrorList {
    rules::all([
        int(op, &p.child("state"), n.state, o.map(|o| o.state))
            .required()
            .min(1)
            .max(7)
            .finish(),
        message(
            op,
            &p.child("worker_assignment"),
            n.worker_assignment.as_ref(),
            o.map(|o| o.worker_assignment.as_ref()),
        )
        .optional()
        .then(|p, n, o| worker_assignment(op, p, n, o))
        .finish(),
        message(
            op,
            &p.child("external_volumes"),
            n.external_volumes.as_ref(),
            o.map(|o| o.external_volumes.as_ref()),
        )
        .optional()
        .then(|p, n, o| external_volumes(op, p, n, o))
        .finish(),
        message(
            op,
            &p.child("crash"),
            n.crash.as_ref(),
            o.map(|o| o.crash.as_ref()),
        )
        .optional()
        .then(|p, n, o| {
            string(
                op,
                &p.child("message"),
                &n.message,
                o.map(|o| o.message.as_str()),
            )
            .optional()
            .max_len(4096)
            .finish()
        })
        .finish(),
        string(
            op,
            &p.child("assigned_node"),
            &n.assigned_node,
            o.map(|o| o.assigned_node.as_str()),
        )
        .optional()
        .format(Format::DnsSubdomain)
        .finish(),
        list(
            op,
            &p.child("snapshots"),
            &n.snapshots,
            o.map(|o| o.snapshots.as_slice()),
        )
        .max_items(16)
        .optional()
        .each(|p, n, o| snapshot(op, p, n, o))
        .unique_by(|s| s.generation)
        .finish(),
        int(
            op,
            &p.child("last_assigned_generation"),
            n.last_assigned_generation,
            o.map(|o| o.last_assigned_generation),
        )
        .no_unset()
        .optional()
        .monotonic()
        .min(0)
        .finish(),
    ])
}

/// An Actor resource.
pub fn actor(op: &Operation, p: &Path, n: &pb::Actor, o: Option<&pb::Actor>) -> ErrorList {
    rules::all([
        metadata_field(
            op,
            &p.child("metadata"),
            n.metadata.as_ref(),
            o.map(|o| o.metadata.as_ref()),
            HiveRule::Required,
        ),
        object_ref_field(
            op,
            &p.child("actor_template"),
            n.actor_template.as_ref(),
            o.map(|o| o.actor_template.as_ref()),
            HiveRule::Required,
        ),
        selector_field(
            op,
            &p.child("worker_selector"),
            n.worker_selector.as_ref(),
            o.map(|o| o.worker_selector.as_ref()),
        ),
        message(
            op,
            &p.child("source_tag"),
            n.source_tag.as_ref(),
            o.map(|o| o.source_tag.as_ref()),
        )
        .immutable()
        .optional()
        .then(|p, n, _| object_ref(op, p, n, HiveRule::Required))
        .finish(),
        message(
            op,
            &p.child("status"),
            n.status.as_ref(),
            o.map(|o| o.status.as_ref()),
        )
        .optional()
        .then(|p, n, o| actor_status(op, p, n, o))
        .finish(),
    ])
}

/// Validates an Actor against the stored one. With `require_status`, a
/// missing status is an error: it is optional on input but always set by the
/// server.
pub fn validate_actor_update(
    op: &Operation,
    p: &Path,
    new: &pb::Actor,
    old: &pb::Actor,
    require_status: bool,
) -> ErrorList {
    let op = op.as_update();
    let mut errs = actor(&op, p, new, Some(old));
    if require_status && new.status.is_none() {
        errs.push(FieldError::required(&p.child("status"), ""));
    }
    errs
}

fn actor_ref(op: &Operation, r: Option<&pb::ObjectRef>) -> ErrorList {
    object_ref_field(op, &Path::from("actor"), r, None, HiveRule::Required)
}

/// `CreateActor`.
pub fn validate_create_actor_request(op: &Operation, req: &pb::CreateActorRequest) -> ErrorList {
    let op = op.as_create();
    message(&op, &Path::from("actor"), req.actor.as_ref(), None)
        .required()
        .then(|p, n, _| actor(&op, p, n, None))
        .finish()
}

/// `GetActor`.
pub fn validate_get_actor_request(op: &Operation, req: &pb::GetActorRequest) -> ErrorList {
    actor_ref(&op.as_create(), req.actor.as_ref())
}

/// `ListActors`.
pub fn validate_list_actors_request(op: &Operation, req: &pb::ListActorsRequest) -> ErrorList {
    let op = op.as_create();
    rules::all([
        hive_filter(&op, &Path::from("hive"), &req.hive),
        list_options(&op, &Path::from("options"), req.options.as_ref()),
    ])
}

/// `UpdateActor`. Only the request shape and metadata are checked here; the
/// merged result is checked with [`validate_actor_update`].
pub fn validate_update_actor_request(op: &Operation, req: &pb::UpdateActorRequest) -> ErrorList {
    let op = op.as_create();
    message(&op, &Path::from("actor"), req.actor.as_ref(), None)
        .required()
        .then(|p, n, _| {
            metadata_field(
                &op,
                &p.child("metadata"),
                n.metadata.as_ref(),
                None,
                HiveRule::Required,
            )
        })
        .finish()
}

/// `DeleteActor`.
pub fn validate_delete_actor_request(op: &Operation, req: &pb::DeleteActorRequest) -> ErrorList {
    let op = op.as_create();
    rules::all([
        actor_ref(&op, req.actor.as_ref()),
        delete_options(&op, &Path::from("options"), req.options.as_ref()),
    ])
}

/// `ResumeActor`.
pub fn validate_resume_actor_request(op: &Operation, req: &pb::ResumeActorRequest) -> ErrorList {
    actor_ref(&op.as_create(), req.actor.as_ref())
}

/// `SuspendActor`.
pub fn validate_suspend_actor_request(op: &Operation, req: &pb::SuspendActorRequest) -> ErrorList {
    let op = op.as_create();
    rules::all([
        actor_ref(&op, req.actor.as_ref()),
        message(&op, &Path::from("options"), req.options.as_ref(), None)
            .optional()
            .then(|p, n, _| {
                int(&op, &p.child("mode"), n.mode, None::<i64>)
                    .optional()
                    .min(1)
                    .max(2)
                    .finish()
            })
            .finish(),
    ])
}

/// `RevertActor`.
pub fn validate_revert_actor_request(op: &Operation, req: &pb::RevertActorRequest) -> ErrorList {
    actor_ref(&op.as_create(), req.actor.as_ref())
}

fn audiences(op: &Operation, p: &Path, v: &[String], min_len: usize) -> ErrorList {
    list(op, p, v, None)
        .max_items(16)
        .required()
        .each(|p, s, _| {
            string(op, p, s, None)
                .min_len(min_len)
                .max_len(512)
                .finish()
        })
        .unique_values()
        .finish()
}

/// `MintActorJwt`.
pub fn validate_mint_actor_jwt_request(op: &Operation, req: &pb::MintActorJwtRequest) -> ErrorList {
    let op = op.as_create();
    rules::all([
        object_ref_field(
            &op,
            &Path::from("actor"),
            req.actor.as_ref(),
            None,
            HiveRule::Optional,
        ),
        audiences(&op, &Path::from("audiences"), &req.audiences, 0),
        int(
            &op,
            &Path::from("expiration_seconds"),
            req.expiration_seconds,
            None::<i64>,
        )
        .required()
        .min(300)
        .max(3600)
        .finish(),
    ])
}

/// Maximum size of a certificate signing request.
pub const MAX_CSR_BYTES: usize = 16_384;

/// `MintActorCertificate`.
pub fn validate_mint_actor_certificate_request(
    op: &Operation,
    req: &pb::MintActorCertificateRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        object_ref_field(
            &op,
            &Path::from("actor"),
            req.actor.as_ref(),
            None,
            HiveRule::Optional,
        ),
        string(&op, &Path::from("actor_uid"), &req.actor_uid, None)
            .required()
            .format(Format::Uuid)
            .finish(),
        bytes(
            &Path::from("certificate_signing_request"),
            &req.certificate_signing_request,
            true,
            MAX_CSR_BYTES,
        ),
    ])
}

/// `WorkerService.MintWorkerActorCertificate`.
pub fn validate_mint_worker_actor_certificate_request(
    op: &Operation,
    req: &pb::MintWorkerActorCertificateRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        actor_ref(&op, req.actor.as_ref()),
        string(&op, &Path::from("actor_uid"), &req.actor_uid, None)
            .required()
            .format(Format::Uuid)
            .finish(),
        bytes(
            &Path::from("certificate_signing_request"),
            &req.certificate_signing_request,
            true,
            MAX_CSR_BYTES,
        ),
        object_ref_field(
            &op,
            &Path::from("worker"),
            req.worker.as_ref(),
            None,
            HiveRule::Forbidden,
        ),
    ])
}

/// `WorkerService.MintWorkerActorJwt`.
pub fn validate_mint_worker_actor_jwt_request(
    op: &Operation,
    req: &pb::MintWorkerActorJwtRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        actor_ref(&op, req.actor.as_ref()),
        string(&op, &Path::from("actor_uid"), &req.actor_uid, None)
            .required()
            .format(Format::Uuid)
            .finish(),
        audiences(&op, &Path::from("audiences"), &req.audiences, 1),
    ])
}
