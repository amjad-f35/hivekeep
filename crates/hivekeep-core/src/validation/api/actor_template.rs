//! ActorTemplate validation.

use hivekeep_proto::api::v1 as pb;

use super::common::*;
use super::egress_policy::egress_policy_template;
use crate::field::{ErrorList, FieldError, Path};
use crate::preview::{Gate, PREVIEW_HEADER};
use crate::validation::checks;
use crate::validation::rules::{self, int, list, message, string, Format, HiveRule, Operation};

fn external_volume_template(
    op: &Operation,
    p: &Path,
    n: &pb::ExternalVolumeTemplate,
    o: Option<&pb::ExternalVolumeTemplate>,
) -> ErrorList {
    rules::all([
        int(
            op,
            &p.child("capacity_bytes"),
            n.capacity_bytes,
            o.map(|o| o.capacity_bytes),
        )
        .required()
        .min(1)
        .finish(),
        string(
            op,
            &p.child("storage_class_name"),
            &n.storage_class_name,
            o.map(|o| o.storage_class_name.as_str()),
        )
        .required()
        .format(Format::DnsSubdomain)
        .finish(),
    ])
}

fn external_volume_source(
    op: &Operation,
    p: &Path,
    n: &pb::ExternalVolumeSource,
    o: Option<&pb::ExternalVolumeSource>,
) -> ErrorList {
    rules::all([
        rules::union(
            op,
            p,
            &[("template", n.template.is_some())],
            o.map(|o| [o.template.is_some()])
                .as_ref()
                .map(|a| a.as_slice()),
        ),
        message(
            op,
            &p.child("template"),
            n.template.as_ref(),
            o.map(|o| o.template.as_ref()),
        )
        .optional()
        .then(|p, n, o| external_volume_template(op, p, n, o))
        .finish(),
    ])
}

/// One template volume; exactly one source is set.
pub fn volume(op: &Operation, p: &Path, n: &pb::Volume, o: Option<&pb::Volume>) -> ErrorList {
    let set = |v: &pb::Volume| {
        [
            v.durable_dir.is_some(),
            v.external.is_some(),
            v.system_info.is_some(),
            v.image.is_some(),
        ]
    };
    let now = set(n);
    rules::all([
        string(op, &p.child("name"), &n.name, o.map(|o| o.name.as_str()))
            .required()
            .format(Format::DnsLabel)
            .finish(),
        rules::union(
            op,
            p,
            &[
                ("durable_dir", now[0]),
                ("external", now[1]),
                ("system_info", now[2]),
                ("image", now[3]),
            ],
            o.map(set).as_ref().map(|a| a.as_slice()),
        ),
        message(
            op,
            &p.child("external"),
            n.external.as_ref(),
            o.map(|o| o.external.as_ref()),
        )
        .needs_gate(Gate::ExternalVolumes)
        .optional()
        .then(|p, n, o| external_volume_source(op, p, n, o))
        .finish(),
        message(
            op,
            &p.child("system_info"),
            n.system_info.as_ref(),
            o.map(|o| o.system_info.as_ref()),
        )
        .optional()
        .then(|p, n, o| system_info_volume_source(op, p, n, o))
        .finish(),
        message(
            op,
            &p.child("image"),
            n.image.as_ref(),
            o.map(|o| o.image.as_ref()),
        )
        .optional()
        .then(|p, n, o| image_volume_source(op, p, n, o))
        .finish(),
    ])
}

/// The storage location is only honoured with the PerTemplateSnapshotLocation
/// gate. Rather than rejecting it on servers without the gate (which would
/// break templates that already set it), a server with the gate on forbids
/// it to requests that did not opt in to preview features.
fn storage_location(op: &Operation, p: &Path, value: &str) -> ErrorList {
    if op.server_gate_enabled(Gate::PerTemplateSnapshotLocation) && op.opted_out() {
        return FieldError::forbidden(
            p,
            format!(
                "requires a client that opts in to preview features (the {PREVIEW_HEADER} header) while the {} preview gate is enabled",
                Gate::PerTemplateSnapshotLocation
            ),
        )
        .into();
    }
    checks::snapshot_location(p, value)
}

fn snapshot_config(
    op: &Operation,
    p: &Path,
    n: &pb::SnapshotConfig,
    o: Option<&pb::SnapshotConfig>,
) -> ErrorList {
    rules::all([
        string(
            op,
            &p.child("storage_location"),
            &n.storage_location,
            o.map(|o| o.storage_location.as_str()),
        )
        .optional()
        .max_len(1024)
        .custom(|p, v| storage_location(op, p, v))
        .finish(),
        int(
            op,
            &p.child("preferred_fidelity"),
            n.preferred_fidelity,
            o.map(|o| o.preferred_fidelity),
        )
        .required()
        .min(1)
        .max(3)
        .finish(),
        message(
            op,
            &p.child("golden_snapshot_config"),
            n.golden_snapshot_config.as_ref(),
            o.map(|o| o.golden_snapshot_config.as_ref()),
        )
        .optional()
        .then(|p, n, o| {
            int(op, &p.child("mode"), n.mode, o.map(|o| o.mode))
                .required()
                .min(1)
                .max(2)
                .finish()
        })
        .finish(),
    ])
}

fn template_status(
    op: &Operation,
    p: &Path,
    n: &pb::ActorTemplateStatus,
    o: Option<&pb::ActorTemplateStatus>,
) -> ErrorList {
    message(
        op,
        &p.child("golden_snapshot_status"),
        n.golden_snapshot_status.as_ref(),
        o.map(|o| o.golden_snapshot_status.as_ref()),
    )
    .optional()
    .then(|p, n, o| {
        rules::all([
            message(
                op,
                &p.child("golden_tag"),
                n.golden_tag.as_ref(),
                o.map(|o| o.golden_tag.as_ref()),
            )
            .optional()
            .then(|p, n, _| object_ref(op, p, n, HiveRule::Required))
            .finish(),
            string(
                op,
                &p.child("error_message"),
                &n.error_message,
                o.map(|o| o.error_message.as_str()),
            )
            .optional()
            .max_len(4096)
            .finish(),
        ])
    })
    .finish()
}

/// Both a CPU and a memory limit are required: the scheduler books them
/// against worker capacity.
fn template_resources(
    op: &Operation,
    p: &Path,
    n: &pb::ResourceRequirements,
    o: Option<&pb::ResourceRequirements>,
) -> ErrorList {
    let limits = n.limits.unwrap_or_default();
    let mut errs = ErrorList::new();
    if limits.cpu_millicores == 0 {
        errs.push(FieldError::required(
            &p.child("limits").child("cpu_millicores"),
            "",
        ));
    }
    if limits.memory_bytes == 0 {
        errs.push(FieldError::required(
            &p.child("limits").child("memory_bytes"),
            "",
        ));
    }
    errs.extend(resource_requirements(op, p, n, o));
    errs
}

/// An ActorTemplate resource.
pub fn actor_template(
    op: &Operation,
    p: &Path,
    n: &pb::ActorTemplate,
    o: Option<&pb::ActorTemplate>,
) -> ErrorList {
    rules::all([
        metadata_field(
            op,
            &p.child("metadata"),
            n.metadata.as_ref(),
            o.map(|o| o.metadata.as_ref()),
            HiveRule::Required,
        ),
        selector_field(
            op,
            &p.child("worker_selector"),
            n.worker_selector.as_ref(),
            o.map(|o| o.worker_selector.as_ref()),
        ),
        list(
            op,
            &p.child("containers"),
            &n.containers,
            o.map(|o| o.containers.as_slice()),
        )
        .max_items(10)
        .required()
        .each_keyed(|c| c.name.clone(), |p, n, o| container(op, p, n, o))
        .unique_by(|c| c.name.clone())
        .finish(),
        list(
            op,
            &p.child("volumes"),
            &n.volumes,
            o.map(|o| o.volumes.as_slice()),
        )
        .max_items(32)
        .optional()
        .each_keyed(|v| v.name.clone(), |p, n, o| volume(op, p, n, o))
        .unique_by(|v| v.name.clone())
        .finish(),
        message(
            op,
            &p.child("snapshot_config"),
            n.snapshot_config.as_ref(),
            o.map(|o| o.snapshot_config.as_ref()),
        )
        .optional()
        .then(|p, n, o| snapshot_config(op, p, n, o))
        .finish(),
        message(
            op,
            &p.child("resources"),
            n.resources.as_ref(),
            o.map(|o| o.resources.as_ref()),
        )
        .required()
        .then(|p, n, o| template_resources(op, p, n, o))
        .finish(),
        message(
            op,
            &p.child("golden_egress_policy"),
            n.golden_egress_policy.as_ref(),
            o.map(|o| o.golden_egress_policy.as_ref()),
        )
        .needs_gate(Gate::GoldenEgressPolicy)
        .optional()
        .then(|p, n, o| egress_policy_template(op, p, n, o))
        .finish(),
        message(
            op,
            &p.child("status"),
            n.status.as_ref(),
            o.map(|o| o.status.as_ref()),
        )
        .optional()
        .then(|p, n, o| template_status(op, p, n, o))
        .finish(),
    ])
}

/// Rules spanning several fields of a template being created: a golden
/// egress policy needs golden snapshots, and every mount names a declared
/// volume.
fn cross_field(p: &Path, t: &pb::ActorTemplate) -> ErrorList {
    let mut errs = ErrorList::new();
    let golden_disabled = t
        .snapshot_config
        .as_ref()
        .and_then(|c| c.golden_snapshot_config.as_ref())
        .is_some_and(|g| g.mode == pb::GoldenSnapshotMode::Disabled as i32);
    if t.golden_egress_policy.is_some() && golden_disabled {
        errs.push(FieldError::forbidden(
            &p.child("golden_egress_policy"),
            "requires golden snapshots to be enabled",
        ));
    }
    errs.extend(mounts_reference_volumes(
        p,
        &t.containers,
        &t.volumes,
        "template",
    ));
    errs
}

/// `CreateActorTemplate`.
pub fn validate_create_actor_template_request(
    op: &Operation,
    req: &pb::CreateActorTemplateRequest,
) -> ErrorList {
    let op = op.as_create();
    message(
        &op,
        &Path::from("actor_template"),
        req.actor_template.as_ref(),
        None,
    )
    .required()
    .then(|p, n, _| rules::all([cross_field(p, n), actor_template(&op, p, n, None)]))
    .finish()
}

fn template_ref(op: &Operation, r: Option<&pb::ObjectRef>) -> ErrorList {
    object_ref_field(
        op,
        &Path::from("actor_template"),
        r,
        None,
        HiveRule::Required,
    )
}

/// `GetActorTemplate`.
pub fn validate_get_actor_template_request(
    op: &Operation,
    req: &pb::GetActorTemplateRequest,
) -> ErrorList {
    template_ref(&op.as_create(), req.actor_template.as_ref())
}

/// `ListActorTemplates`.
pub fn validate_list_actor_templates_request(
    op: &Operation,
    req: &pb::ListActorTemplatesRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        hive_filter(&op, &Path::from("hive"), &req.hive),
        list_options(&op, &Path::from("options"), req.options.as_ref()),
    ])
}

/// `DeleteActorTemplate`.
pub fn validate_delete_actor_template_request(
    op: &Operation,
    req: &pb::DeleteActorTemplateRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        template_ref(&op, req.actor_template.as_ref()),
        delete_options(&op, &Path::from("options"), req.options.as_ref()),
    ])
}

/// Validates a template against the stored one.
pub fn validate_actor_template_update(
    op: &Operation,
    p: &Path,
    new: &pb::ActorTemplate,
    old: &pb::ActorTemplate,
) -> ErrorList {
    actor_template(&op.as_update(), p, new, Some(old))
}
