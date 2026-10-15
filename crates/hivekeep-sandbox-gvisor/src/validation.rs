//! Validation of the worker API (`hivekeep.worker.v1`) at the RPC edge.
//!
//! The keeper builds these requests from values the control plane already
//! validated, so a failure here means the two sides disagree. Checking again
//! keeps a malformed spec from reaching runsc or the node's filesystem.
//! Requests are validated as creates: no worker RPC updates stored state, so
//! there is nothing to ratchet against.

use std::collections::HashMap;

use hivekeep_core::field::{ErrorList, FieldError, Path};
use hivekeep_core::validation::checks;
use hivekeep_core::validation::rules::{self, int, list, map, message, string, Format, Operation};
use hivekeep_proto::worker::v1 as pb;

/// Upper bound on any directory or binary path a request carries.
const PATH_MAX_LEN: usize = 4096;
/// Upper bound on `WorkloadSpec.containers`.
pub const MAX_CONTAINERS: usize = 10;
/// Upper bound on each of a container's mount lists.
const MAX_MOUNTS: usize = 32;
/// Upper bound on `runtime_asset_paths` entries.
const MAX_RUNTIME_ASSETS: usize = 16;
/// Upper bound on a runtime asset name.
const RUNTIME_ASSET_NAME_MAX_LEN: usize = 64;
/// Upper bound on an egress gateway address (a bracketed IPv6 or DNS name plus port).
const EGRESS_ADDRESS_MAX_LEN: usize = 261;

/// The identity fields every workload RPC carries.
struct Identity<'a> {
    hive: &'a str,
    actor_name: &'a str,
    actor_uid: &'a str,
    template_hive: &'a str,
    template_name: &'a str,
}

macro_rules! identity_of {
    ($req:expr) => {
        Identity {
            hive: &$req.hive,
            actor_name: &$req.actor_name,
            actor_uid: &$req.actor_uid,
            template_hive: &$req.actor_template_hive,
            template_name: &$req.actor_template_name,
        }
    };
}

fn op() -> Operation {
    Operation::create()
}

/// Validates a `RunWorkloadRequest`.
pub fn run_workload_request(req: &pb::RunWorkloadRequest) -> ErrorList {
    let op = op();
    rules::all([
        identity(&op, identity_of!(req)),
        runsc_path(&op, &req.runsc_path),
        spec_field(&op, req.spec.as_ref(), true),
        runtime_asset_paths(&op, &req.runtime_asset_paths),
        gateway_field(&op, req.egress_gateway.as_ref()),
        resources_field(&op, &Path::from("resources"), req.resources.as_ref()),
        actor_dirs_field(&op, req.actor_dirs.as_ref()),
    ])
}

/// Validates a `CheckpointWorkloadRequest`.
pub fn checkpoint_workload_request(req: &pb::CheckpointWorkloadRequest) -> ErrorList {
    let op = op();
    rules::all([
        identity(&op, identity_of!(req)),
        runsc_path(&op, &req.runsc_path),
        spec_field(&op, req.spec.as_ref(), false),
        fidelity(&op, req.fidelity),
        actor_dirs_field(&op, req.actor_dirs.as_ref()),
    ])
}

/// Validates a `RestoreWorkloadRequest`.
pub fn restore_workload_request(req: &pb::RestoreWorkloadRequest) -> ErrorList {
    let op = op();
    rules::all([
        identity(&op, identity_of!(req)),
        runsc_path(&op, &req.runsc_path),
        spec_field(&op, req.spec.as_ref(), true),
        runtime_asset_paths(&op, &req.runtime_asset_paths),
        fidelity(&op, req.fidelity),
        gateway_field(&op, req.egress_gateway.as_ref()),
        resources_field(&op, &Path::from("resources"), req.resources.as_ref()),
        actor_dirs_field(&op, req.actor_dirs.as_ref()),
    ])
}

/// Validates a `TerminateWorkloadRequest`. Its spec may have no containers:
/// that is the fallback the keeper sends for an Actor without a template.
pub fn terminate_workload_request(req: &pb::TerminateWorkloadRequest) -> ErrorList {
    let op = op();
    rules::all([
        identity(&op, identity_of!(req)),
        runsc_path(&op, &req.runsc_path),
        spec_field(&op, req.spec.as_ref(), false),
        actor_dirs_field(&op, req.actor_dirs.as_ref()),
    ])
}

/// Validates a `GetWorkloadStatsRequest`.
pub fn get_workload_stats_request(req: &pb::GetWorkloadStatsRequest) -> ErrorList {
    string(&op(), &Path::from("actor_uid"), &req.actor_uid, None)
        .required()
        .format(Format::Uuid)
        .finish()
}

/// Turns a validation result into the `INVALID_ARGUMENT` status a handler
/// answers with.
pub fn into_status(errs: ErrorList) -> Result<(), tonic::Status> {
    errs.into_result().map_err(Into::into)
}

// ------------------------------------------------------------- request parts

fn identity(op: &Operation, id: Identity<'_>) -> ErrorList {
    let name = |field: &str, v: &str, required: bool| {
        let r = string(op, &Path::from(field), v, None);
        let r = if required { r.required() } else { r.optional() };
        r.format(Format::DnsLabel).finish()
    };
    rules::all([
        name("hive", id.hive, true),
        name("actor_name", id.actor_name, true),
        string(op, &Path::from("actor_uid"), id.actor_uid, None)
            .required()
            .format(Format::Uuid)
            .finish(),
        name("actor_template_hive", id.template_hive, false),
        name("actor_template_name", id.template_name, false),
    ])
}

fn runsc_path(op: &Operation, p: &str) -> ErrorList {
    string(op, &Path::from("runsc_path"), p, None)
        .optional()
        .max_len(PATH_MAX_LEN)
        .finish()
}

fn fidelity(op: &Operation, f: i32) -> ErrorList {
    int(op, &Path::from("fidelity"), f, None::<i64>)
        .required()
        .min(1)
        .max(3)
        .finish()
}

fn spec_field(
    op: &Operation,
    s: Option<&pb::WorkloadSpec>,
    containers_required: bool,
) -> ErrorList {
    message(op, &Path::from("spec"), s, None)
        .required()
        .then(|p, n, _| workload_spec(op, p, n, containers_required))
        .finish()
}

fn runtime_asset_paths(op: &Operation, m: &HashMap<String, String>) -> ErrorList {
    map(op, &Path::from("runtime_asset_paths"), m, None)
        .optional()
        .max_entries(MAX_RUNTIME_ASSETS)
        .custom(|p, m| {
            let mut entries: Vec<_> = m.iter().collect();
            entries.sort();
            let mut errs = ErrorList::new();
            // Keys are reported at the map, values at their entry.
            for (k, _) in &entries {
                if k.len() > RUNTIME_ASSET_NAME_MAX_LEN {
                    errs.push(
                        FieldError::too_long(p, RUNTIME_ASSET_NAME_MAX_LEN)
                            .with_origin("maxLength"),
                    );
                }
            }
            for (k, v) in &entries {
                if v.len() > PATH_MAX_LEN {
                    errs.push(
                        FieldError::too_long(&p.key(k), PATH_MAX_LEN).with_origin("maxLength"),
                    );
                }
            }
            errs
        })
        .finish()
}

fn gateway_field(op: &Operation, g: Option<&pb::EgressGateway>) -> ErrorList {
    message(op, &Path::from("egress_gateway"), g, None)
        .optional()
        .then(|p, n, _| egress_gateway(op, p, n))
        .finish()
}

/// The gateway address: present, bounded, and the `host:port` shape the
/// tunnel dials. The length bound does not hide the shape check.
pub fn egress_gateway(op: &Operation, p: &Path, n: &pb::EgressGateway) -> ErrorList {
    let ap = p.child("address");
    let mut errs = string(op, &ap, &n.address, None).required().finish();
    if !errs.is_empty() {
        return errs;
    }
    if n.address.len() > EGRESS_ADDRESS_MAX_LEN {
        errs.push(FieldError::too_long(&ap, EGRESS_ADDRESS_MAX_LEN).with_origin("maxLength"));
    }
    errs.extend(checks::host_port(&ap, &n.address));
    errs
}

fn resources_field(op: &Operation, p: &Path, r: Option<&pb::ResourceRequirements>) -> ErrorList {
    message(op, p, r, None)
        .optional()
        .then(|p, n, _| {
            message(op, &p.child("limits"), n.limits.as_ref(), None)
                .optional()
                .then(|p, n, _| resources(op, p, n))
                .finish()
        })
        .finish()
}

/// An amount of compute: CPU within the keeper's bound, nothing negative.
pub fn resources(op: &Operation, p: &Path, n: &pb::Resources) -> ErrorList {
    rules::all([
        int(
            op,
            &p.child("cpu_millicores"),
            n.cpu_millicores,
            None::<i64>,
        )
        .min(0)
        .max(999_999)
        .finish(),
        int(op, &p.child("memory_bytes"), n.memory_bytes, None::<i64>)
            .min(0)
            .finish(),
    ])
}

fn actor_dirs_field(op: &Operation, d: Option<&pb::ActorDirs>) -> ErrorList {
    message(op, &Path::from("actor_dirs"), d, None)
        .required()
        .then(|p, n, _| actor_dirs(op, p, n))
        .finish()
}

/// Every Actor directory: present, bounded, and an absolute, clean path.
pub fn actor_dirs(op: &Operation, p: &Path, d: &pb::ActorDirs) -> ErrorList {
    let mut errs = ErrorList::new();
    for (name, dir) in actor_dir_fields(d) {
        let fp = p.child(name);
        errs.extend(
            string(op, &fp, dir, None)
                .required()
                .max_len(PATH_MAX_LEN)
                .custom(checks::abs_clean_dir)
                .finish(),
        );
    }
    errs
}

fn actor_dir_fields(d: &pb::ActorDirs) -> [(&'static str, &str); 8] {
    [
        ("root_dir", &d.root_dir),
        ("oci_bundle_dir", &d.oci_bundle_dir),
        ("checkpoint_dir", &d.checkpoint_dir),
        ("restore_dir", &d.restore_dir),
        (
            "durable_dir_volume_mounts_dir",
            &d.durable_dir_volume_mounts_dir,
        ),
        (
            "system_info_volume_roots_dir",
            &d.system_info_volume_roots_dir,
        ),
        ("external_volume_mounts_dir", &d.external_volume_mounts_dir),
        ("sandbox_assets_dir", &d.sandbox_assets_dir),
    ]
}

// ------------------------------------------------------------- workload spec

/// A workload spec: a bounded list of uniquely named containers.
pub fn workload_spec(
    op: &Operation,
    p: &Path,
    n: &pb::WorkloadSpec,
    containers_required: bool,
) -> ErrorList {
    let l = list(op, &p.child("containers"), &n.containers, None).max_items(MAX_CONTAINERS);
    let l = if containers_required {
        l.required()
    } else {
        l.optional()
    };
    l.unique_by(|c| c.name.clone())
        .each(|p, c, _| container(op, p, c))
        .finish()
}

/// One container: its name, probe, mounts and resources.
pub fn container(op: &Operation, p: &Path, c: &pb::Container) -> ErrorList {
    let mut errs = cross_list_mounts(p, c);
    errs.extend(
        string(op, &p.child("name"), &c.name, None)
            .required()
            .format(Format::DnsLabel)
            .finish(),
    );
    errs.extend(
        message(op, &p.child("wakeup_probe"), c.wakeup_probe.as_ref(), None)
            .optional()
            .then(|p, n, _| wakeup_probe(op, p, n))
            .finish(),
    );
    for (name, mounts) in mount_lists(c) {
        errs.extend(
            list(op, &p.child(name), mounts, None)
                .max_items(MAX_MOUNTS)
                .optional()
                .unique_by(|m| m.mount_path.clone())
                .each(|p, m, _| volume_mount(op, p, m))
                .finish(),
        );
    }
    errs.extend(
        message(
            op,
            &p.child("container_spec"),
            c.container_spec.as_ref(),
            None,
        )
        .optional()
        .then(|p, n, _| resources_field(op, &p.child("resources"), n.resources.as_ref()))
        .finish(),
    );
    errs
}

fn mount_lists(c: &pb::Container) -> [(&'static str, &[pb::VolumeMount]); 4] {
    [
        ("durable_dir_volume_mounts", &c.durable_dir_volume_mounts),
        ("external_volume_mounts", &c.external_volume_mounts),
        ("system_info_volume_mounts", &c.system_info_volume_mounts),
        ("image_volume_mounts", &c.image_volume_mounts),
    ]
}

/// Mount paths stay unique and non-nesting across a container's four mount
/// lists: a volume cannot be mounted onto another. A repeat within one list is
/// that list's uniqueness finding and is not reported again.
fn cross_list_mounts(p: &Path, c: &pb::Container) -> ErrorList {
    struct Mount<'a> {
        path: &'a str,
        list: &'static str,
        at: Path,
    }
    let mut mounts = Vec::new();
    for (list, ms) in mount_lists(c) {
        for (i, m) in ms.iter().enumerate() {
            mounts.push(Mount {
                path: &m.mount_path,
                list,
                at: p.child(list).index(i).child("mount_path"),
            });
        }
    }
    let mut errs = ErrorList::new();
    let mut first_list: HashMap<&str, &str> = HashMap::new();
    for m in &mounts {
        if m.path.is_empty() {
            continue;
        }
        match first_list.get(m.path) {
            Some(list) if *list != m.list => errs.push(FieldError::duplicate(&m.at, m.path)),
            Some(_) => {}
            None => {
                first_list.insert(m.path, m.list);
            }
        }
    }
    let paths: Vec<&str> = mounts.iter().map(|m| m.path).collect();
    errs.extend(checks::nested_mount_paths_at(&paths, |i| {
        mounts[i].at.clone()
    }));
    errs
}

/// A volume mount: a named volume at a clean absolute path.
pub fn volume_mount(op: &Operation, p: &Path, m: &pb::VolumeMount) -> ErrorList {
    rules::all([
        string(op, &p.child("name"), &m.name, None)
            .required()
            .format(Format::DnsLabel)
            .finish(),
        string(op, &p.child("mount_path"), &m.mount_path, None)
            .required()
            .max_len(PATH_MAX_LEN)
            .custom(checks::mount_path)
            .finish(),
    ])
}

/// A wakeup probe: an HTTP GET and a bounded timeout.
pub fn wakeup_probe(op: &Operation, p: &Path, w: &pb::WakeupProbe) -> ErrorList {
    rules::all([
        message(op, &p.child("http_get"), w.http_get.as_ref(), None)
            .required()
            .then(|p, h, _| http_get_action(op, p, h))
            .finish(),
        int(
            op,
            &p.child("timeout_seconds"),
            w.timeout_seconds,
            None::<i64>,
        )
        .required()
        .min(1)
        .max(3600)
        .finish(),
    ])
}

fn http_get_action(op: &Operation, p: &Path, h: &pb::HttpGetAction) -> ErrorList {
    rules::all([
        string(op, &p.child("path"), &h.path, None)
            .required()
            .max_len(1024)
            .custom(checks::http_get_path)
            .finish(),
        int(op, &p.child("port"), h.port, None::<i64>)
            .required()
            .min(1)
            .max(65535)
            .finish(),
    ])
}

#[cfg(test)]
mod tests;
