//! Validation of the keeper API (`hivekeep.keeper.v1`).
//!
//! Workload values arrive already validated by the control plane, so a
//! failure here means the two sides disagree; checking again at the RPC edge
//! keeps a bad spec from reaching the node's filesystem.

use hivekeep_proto::keeper::v1 as pb;

use crate::field::{ErrorList, Path};
use crate::validation::checks;
use crate::validation::rules::{self, bytes, int, list, map, message, string, Format, Operation};

/// Validators for the workload messages shared with the public API.
pub mod workload {
    use hivekeep_proto::keeper::v1 as pb;
    crate::validation::workload::workload_validators!();
}

pub use workload::*;

fn external_volume_source(op: &Operation, p: &Path, n: &pb::ExternalVolumeSource) -> ErrorList {
    rules::all([
        string(
            op,
            &p.child("storage_volume_id"),
            &n.storage_volume_id,
            None,
        )
        .required()
        .max_len(256)
        .custom(checks::storage_volume_id)
        .finish(),
        string(op, &p.child("volume_type"), &n.volume_type, None)
            .optional()
            .max_len(253)
            .custom(checks::volume_type)
            .finish(),
        map(op, &p.child("volume_context"), &n.volume_context, None)
            .optional()
            .custom(checks::csi_map)
            .finish(),
        map(op, &p.child("publish_context"), &n.publish_context, None)
            .optional()
            .custom(checks::csi_map)
            .finish(),
    ])
}

/// One workload volume; exactly one source is set.
pub fn volume(op: &Operation, p: &Path, n: &pb::Volume) -> ErrorList {
    rules::all([
        string(op, &p.child("name"), &n.name, None)
            .required()
            .format(Format::DnsLabel)
            .finish(),
        rules::union(
            op,
            p,
            &[
                ("durable_dir", n.durable_dir.is_some()),
                ("external", n.external.is_some()),
                ("system_info", n.system_info.is_some()),
                ("image", n.image.is_some()),
            ],
            None,
        ),
        message(op, &p.child("external"), n.external.as_ref(), None)
            .optional()
            .then(|p, n, _| external_volume_source(op, p, n))
            .finish(),
        message(op, &p.child("system_info"), n.system_info.as_ref(), None)
            .optional()
            .then(|p, n, o| system_info_volume_source(op, p, n, o))
            .finish(),
        message(op, &p.child("image"), n.image.as_ref(), None)
            .optional()
            .then(|p, n, o| image_volume_source(op, p, n, o))
            .finish(),
    ])
}

/// A workload spec: containers, volumes, and mounts that name declared
/// volumes.
pub fn workload_spec(op: &Operation, p: &Path, n: &pb::WorkloadSpec) -> ErrorList {
    rules::all([
        mounts_reference_volumes(p, &n.containers, &n.volumes, "spec"),
        list(op, &p.child("containers"), &n.containers, None)
            .max_items(10)
            .optional()
            .each(|p, n, o| container(op, p, n, o))
            .unique_by(|c| c.name.clone())
            .finish(),
        list(op, &p.child("volumes"), &n.volumes, None)
            .max_items(32)
            .optional()
            .each(|p, n, _| volume(op, p, n))
            .unique_by(|v| v.name.clone())
            .finish(),
    ])
}

fn egress_gateway(op: &Operation, p: &Path, n: &pb::EgressGateway) -> ErrorList {
    string(op, &p.child("address"), &n.address, None)
        .required()
        .max_len(261)
        .custom(checks::host_port)
        .finish()
}

/// A snapshot to take, restore or promote.
pub fn snapshot(op: &Operation, p: &Path, n: &pb::Snapshot) -> ErrorList {
    rules::all([
        string(op, &p.child("snapshot_uuid"), &n.snapshot_uuid, None)
            .required()
            .format(Format::DnsLabel)
            .finish(),
        int(op, &p.child("generation"), n.generation, None::<i64>)
            .required()
            .min(1)
            .finish(),
        int(op, &p.child("fidelity"), n.fidelity, None::<i64>)
            .required()
            .min(1)
            .max(3)
            .finish(),
        message(op, &p.child("object"), n.object.as_ref(), None)
            .required()
            .then(|p, n, _| {
                rules::all([
                    string(op, &p.child("snapshot_uri"), &n.snapshot_uri, None)
                        .required()
                        .max_len(2048)
                        .custom(checks::snapshot_uri)
                        .finish(),
                    int(op, &p.child("sharing"), n.sharing, None::<i64>)
                        .optional()
                        .max(2)
                        .finish(),
                ])
            })
            .finish(),
    ])
}

fn name(op: &Operation, field: &str, v: &str, required: bool) -> ErrorList {
    let r = string(op, &Path::from(field), v, None);
    let r = if required { r.required() } else { r.optional() };
    r.format(Format::DnsLabel).finish()
}

fn uid(op: &Operation, field: &str, v: &str, required: bool) -> ErrorList {
    let r = string(op, &Path::from(field), v, None);
    let r = if required { r.required() } else { r.optional() };
    r.format(Format::Uuid).finish()
}

/// The identity fields carried by every workload RPC.
struct Identity<'a> {
    hive: &'a str,
    actor_name: &'a str,
    actor_uid: &'a str,
    template_hive: &'a str,
    template_name: &'a str,
}

fn identity(op: &Operation, id: Identity<'_>) -> ErrorList {
    rules::all([
        name(op, "hive", id.hive, true),
        name(op, "actor_name", id.actor_name, true),
        uid(op, "actor_uid", id.actor_uid, true),
        name(op, "actor_template_hive", id.template_hive, false),
        name(op, "actor_template_name", id.template_name, false),
    ])
}

fn spec_field(op: &Operation, s: Option<&pb::WorkloadSpec>, required: bool) -> ErrorList {
    let m = message(op, &Path::from("spec"), s, None);
    let m = if required { m.required() } else { m.optional() };
    m.then(|p, n, _| workload_spec(op, p, n)).finish()
}

fn snapshot_field(op: &Operation, s: Option<&pb::Snapshot>) -> ErrorList {
    message(op, &Path::from("snapshot"), s, None)
        .required()
        .then(|p, n, _| snapshot(op, p, n))
        .finish()
}

fn gateway_field(op: &Operation, g: Option<&pb::EgressGateway>) -> ErrorList {
    message(op, &Path::from("egress_gateway"), g, None)
        .optional()
        .then(|p, n, _| egress_gateway(op, p, n))
        .finish()
}

fn resources_field(op: &Operation, r: Option<&pb::ResourceRequirements>) -> ErrorList {
    message(op, &Path::from("resources"), r, None)
        .optional()
        .then(|p, n, o| resource_requirements(op, p, n, o))
        .finish()
}

fn runtime_field(op: &Operation, r: Option<&pb::ActorSandboxRuntime>) -> ErrorList {
    message(op, &Path::from("sandbox_runtime"), r, None)
        .required()
        .then(|p, n, o| actor_sandbox_runtime(op, p, n, o))
        .finish()
}

/// `Keeper.Run`.
pub fn validate_run_request(op: &Operation, r: &pb::RunRequest) -> ErrorList {
    let op = op.as_create();
    rules::all([
        uid(&op, "worker_pod_uid", &r.worker_pod_uid, true),
        identity(
            &op,
            Identity {
                hive: &r.hive,
                actor_name: &r.actor_name,
                actor_uid: &r.actor_uid,
                template_hive: &r.actor_template_hive,
                template_name: &r.actor_template_name,
            },
        ),
        spec_field(&op, r.spec.as_ref(), true),
        gateway_field(&op, r.egress_gateway.as_ref()),
        resources_field(&op, r.resources.as_ref()),
        runtime_field(&op, r.sandbox_runtime.as_ref()),
    ])
}

/// `Keeper.Restore`.
pub fn validate_restore_request(op: &Operation, r: &pb::RestoreRequest) -> ErrorList {
    let op = op.as_create();
    rules::all([
        uid(&op, "worker_pod_uid", &r.worker_pod_uid, true),
        identity(
            &op,
            Identity {
                hive: &r.hive,
                actor_name: &r.actor_name,
                actor_uid: &r.actor_uid,
                template_hive: &r.actor_template_hive,
                template_name: &r.actor_template_name,
            },
        ),
        spec_field(&op, r.spec.as_ref(), true),
        snapshot_field(&op, r.snapshot.as_ref()),
        gateway_field(&op, r.egress_gateway.as_ref()),
        resources_field(&op, r.resources.as_ref()),
        runtime_field(&op, r.sandbox_runtime.as_ref()),
    ])
}

/// `Keeper.Checkpoint`.
pub fn validate_checkpoint_request(op: &Operation, r: &pb::CheckpointRequest) -> ErrorList {
    let op = op.as_create();
    rules::all([
        uid(&op, "worker_pod_uid", &r.worker_pod_uid, true),
        identity(
            &op,
            Identity {
                hive: &r.hive,
                actor_name: &r.actor_name,
                actor_uid: &r.actor_uid,
                template_hive: &r.actor_template_hive,
                template_name: &r.actor_template_name,
            },
        ),
        spec_field(&op, r.spec.as_ref(), true),
        snapshot_field(&op, r.snapshot.as_ref()),
        message(
            &op,
            &Path::from("upload_config"),
            r.upload_config.as_ref(),
            None,
        )
        .required()
        .then(|p, n, _| {
            int(&op, &p.child("mode"), n.mode, None::<i64>)
                .required()
                .min(1)
                .max(2)
                .finish()
        })
        .finish(),
    ])
}

/// `Keeper.Terminate`. A missing spec and worker pod UID are tolerated:
/// deletes of template-less Actors send a fallback.
pub fn validate_terminate_request(op: &Operation, r: &pb::TerminateRequest) -> ErrorList {
    let op = op.as_create();
    rules::all([
        uid(&op, "worker_pod_uid", &r.worker_pod_uid, false),
        identity(
            &op,
            Identity {
                hive: &r.hive,
                actor_name: &r.actor_name,
                actor_uid: &r.actor_uid,
                template_hive: &r.actor_template_hive,
                template_name: &r.actor_template_name,
            },
        ),
        spec_field(&op, r.spec.as_ref(), false),
    ])
}

/// `Keeper.PromoteSnapshot`.
pub fn validate_promote_snapshot_request(
    op: &Operation,
    r: &pb::PromoteSnapshotRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        identity(
            &op,
            Identity {
                hive: &r.hive,
                actor_name: &r.actor_name,
                actor_uid: &r.actor_uid,
                template_hive: &r.actor_template_hive,
                template_name: &r.actor_template_name,
            },
        ),
        snapshot_field(&op, r.snapshot.as_ref()),
    ])
}

/// `Keeper.DetachActor`.
pub fn validate_detach_actor_request(op: &Operation, r: &pb::DetachActorRequest) -> ErrorList {
    let op = op.as_create();
    rules::all([
        name(&op, "hive", &r.hive, true),
        name(&op, "actor_name", &r.actor_name, true),
        uid(&op, "actor_uid", &r.actor_uid, true),
    ])
}

fn hosted_actor(op: &Operation, hive: &str, actor: &str) -> ErrorList {
    rules::all([
        name(op, "actor_hive", hive, true),
        name(op, "actor_name", actor, true),
    ])
}

/// `WorkerSupport.RequestActorSuspend`.
pub fn validate_request_actor_suspend_request(
    op: &Operation,
    r: &pb::RequestActorSuspendRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        hosted_actor(&op, &r.actor_hive, &r.actor_name),
        uid(&op, "actor_uid", &r.actor_uid, true),
    ])
}

/// `WorkerSupport.ReportActorCrashed`.
pub fn validate_report_actor_crashed_request(
    op: &Operation,
    r: &pb::ReportActorCrashedRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        hosted_actor(&op, &r.actor_hive, &r.actor_name),
        uid(&op, "actor_uid", &r.actor_uid, true),
        string(&op, &Path::from("message"), &r.message, None)
            .required()
            .max_len(4096)
            .finish(),
    ])
}

/// `WorkerSupport.GetActorEgressPolicy`.
pub fn validate_get_actor_egress_policy_request(
    op: &Operation,
    r: &pb::GetActorEgressPolicyRequest,
) -> ErrorList {
    hosted_actor(&op.as_create(), &r.actor_hive, &r.actor_name)
}

/// `WorkerSupport.MintActorCertificate`.
pub fn validate_mint_actor_certificate_request(
    op: &Operation,
    r: &pb::MintActorCertificateRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        hosted_actor(&op, &r.actor_hive, &r.actor_name),
        uid(&op, "actor_uid", &r.actor_uid, true),
        bytes(
            &Path::from("certificate_signing_request"),
            &r.certificate_signing_request,
            true,
            16_384,
        ),
    ])
}

/// `WorkerSupport.RegisterWorker`.
pub fn validate_register_worker_request(
    op: &Operation,
    r: &pb::RegisterWorkerRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        int(&op, &Path::from("epoch"), r.epoch, None::<i64>)
            .required()
            .min(1)
            .finish(),
        list(&op, &Path::from("ips"), &r.ips, None)
            .max_items(2)
            .required()
            .finish(),
        message(&op, &Path::from("capacity"), r.capacity.as_ref(), None)
            .required()
            .then(|p, n, o| worker_resources(&op, p, n, o))
            .finish(),
    ])
}

/// `WorkerSupport.SetWorkerSandboxRuntimes`.
pub fn validate_set_worker_sandbox_runtimes_request(
    op: &Operation,
    r: &pb::SetWorkerSandboxRuntimesRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        int(&op, &Path::from("epoch"), r.epoch, None::<i64>)
            .required()
            .min(1)
            .finish(),
        worker_sandbox_classes(
            &op,
            &Path::from("sandbox_classes"),
            &r.sandbox_classes,
            None,
            true,
        ),
    ])
}
