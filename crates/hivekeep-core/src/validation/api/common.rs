//! Validators shared by several public API resources: metadata, references,
//! selectors, list/delete options and snapshot records.

use hivekeep_proto::api::v1 as pb;

use crate::field::{ErrorList, FieldError, Path};
use crate::validation::checks;
use crate::validation::rules::{self, int, map, message, string, Format, HiveRule, Operation};

pub use super::workload::*;

/// Common resource metadata, including the server-stamped timestamps: each
/// must be valid, and `update_time` must not precede `create_time`.
pub fn resource_metadata(
    op: &Operation,
    p: &Path,
    n: &pb::ResourceMetadata,
    o: Option<&pb::ResourceMetadata>,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let mut create_ok = false;
    if let Some(ct) = &n.create_time {
        let e = checks::timestamp(&p.child("create_time"), ct);
        create_ok = e.is_empty();
        errs.extend(e);
    }
    if let Some(ut) = &n.update_time {
        let e = checks::timestamp(&p.child("update_time"), ut);
        if !e.is_empty() {
            errs.extend(e);
        } else if let (true, Some(ct)) = (create_ok, &n.create_time) {
            if (ut.seconds, ut.nanos) < (ct.seconds, ct.nanos) {
                errs.push(FieldError::invalid(
                    &p.child("update_time"),
                    format!("{}s", ut.seconds),
                    "must not precede create_time",
                ));
            }
        }
    }
    errs.extend(rules::all([
        string(op, &p.child("hive"), &n.hive, o.map(|o| o.hive.as_str()))
            .immutable()
            .optional()
            .format(Format::DnsLabel)
            .finish(),
        string(op, &p.child("name"), &n.name, o.map(|o| o.name.as_str()))
            .immutable()
            .required()
            .format(Format::DnsLabel)
            .finish(),
        string(op, &p.child("uid"), &n.uid, o.map(|o| o.uid.as_str()))
            .immutable()
            .optional()
            .format(Format::Uuid)
            .finish(),
        int(op, &p.child("version"), n.version, o.map(|o| o.version))
            .no_unset()
            .optional()
            .monotonic()
            .min(1)
            .finish(),
        message(
            op,
            &p.child("create_time"),
            n.create_time.as_ref(),
            o.map(|o| o.create_time.as_ref()),
        )
        .immutable()
        .finish(),
        message(
            op,
            &p.child("update_time"),
            n.update_time.as_ref(),
            o.map(|o| o.update_time.as_ref()),
        )
        .no_unset()
        .finish(),
        message(
            op,
            &p.child("delete_time"),
            n.delete_time.as_ref(),
            o.map(|o| o.delete_time.as_ref()),
        )
        .no_modify()
        .no_unset()
        .finish(),
    ]));
    errs
}

/// Applies a resource's scope to `metadata.hive`.
pub fn hive_scope(
    op: &Operation,
    p: &Path,
    hive: &str,
    old: Option<&str>,
    rule: HiveRule,
) -> ErrorList {
    let r = string(op, p, hive, old);
    match rule {
        HiveRule::Required => r.required().finish(),
        HiveRule::Forbidden => r.forbidden().finish(),
        HiveRule::Optional => r.finish(),
    }
}

/// A required metadata field of a resource with the given scope.
pub fn metadata_field(
    op: &Operation,
    p: &Path,
    n: Option<&pb::ResourceMetadata>,
    o: Option<Option<&pb::ResourceMetadata>>,
    scope: HiveRule,
) -> ErrorList {
    message(op, p, n, o)
        .required()
        .then(|p, n, o| {
            rules::all([
                hive_scope(
                    op,
                    &p.child("hive"),
                    &n.hive,
                    o.map(|o| o.hive.as_str()),
                    scope,
                ),
                resource_metadata(op, p, n, o),
            ])
        })
        .finish()
}

/// A reference to a resource of the given scope.
pub fn object_ref(op: &Operation, p: &Path, n: &pb::ObjectRef, scope: HiveRule) -> ErrorList {
    rules::hived_name(op, p, &n.hive, &n.name, scope)
}

/// A required reference field.
pub fn object_ref_field(
    op: &Operation,
    p: &Path,
    n: Option<&pb::ObjectRef>,
    o: Option<Option<&pb::ObjectRef>>,
    scope: HiveRule,
) -> ErrorList {
    message(op, p, n, o)
        .required()
        .then(|p, n, _| object_ref(op, p, n, scope))
        .finish()
}

/// A label selector: `match_labels` must hold 1 to 10 valid labels.
pub fn selector(op: &Operation, p: &Path, n: &pb::Selector, o: Option<&pb::Selector>) -> ErrorList {
    rules::all([
        rules::union(
            op,
            p,
            &[("match_labels", !n.match_labels.is_empty())],
            o.map(|o| [!o.match_labels.is_empty()])
                .as_ref()
                .map(|a| a.as_slice()),
        ),
        map(
            op,
            &p.child("match_labels"),
            &n.match_labels,
            o.map(|o| &o.match_labels),
        )
        .optional()
        .min_entries(1)
        .max_entries(10)
        .each_key(Format::LabelKey)
        .each_value(Format::LabelValue)
        .finish(),
    ])
}

/// An optional selector field.
pub fn selector_field(
    op: &Operation,
    p: &Path,
    n: Option<&pb::Selector>,
    o: Option<Option<&pb::Selector>>,
) -> ErrorList {
    message(op, p, n, o)
        .optional()
        .then(|p, n, o| selector(op, p, n, o))
        .finish()
}

/// Delete preconditions; zero values are ignored.
pub fn delete_options(op: &Operation, p: &Path, n: Option<&pb::DeleteOptions>) -> ErrorList {
    message(op, p, n, None)
        .optional()
        .then(|p, n, _| {
            rules::all([
                int(op, &p.child("version"), n.version, None::<i64>)
                    .optional()
                    .min(1)
                    .finish(),
                string(op, &p.child("uid"), &n.uid, None)
                    .optional()
                    .format(Format::Uuid)
                    .finish(),
            ])
        })
        .finish()
}

/// Pagination controls.
pub fn list_options(op: &Operation, p: &Path, n: Option<&pb::ListOptions>) -> ErrorList {
    message(op, p, n, None)
        .optional()
        .then(|p, n, _| {
            rules::all([
                int(op, &p.child("page_size"), n.page_size, None::<i64>)
                    .optional()
                    .min(1)
                    .finish(),
                string(op, &p.child("page_token"), &n.page_token, None)
                    .optional()
                    .max_len(256)
                    .finish(),
            ])
        })
        .finish()
}

/// An optional hive filter of a List request.
pub fn hive_filter(op: &Operation, p: &Path, hive: &str) -> ErrorList {
    string(op, p, hive, None)
        .optional()
        .format(Format::DnsLabel)
        .finish()
}

/// A snapshot's object-storage location.
pub fn object_snapshot(
    op: &Operation,
    p: &Path,
    n: &pb::ObjectSnapshot,
    o: Option<&pb::ObjectSnapshot>,
) -> ErrorList {
    string(
        op,
        &p.child("snapshot_uri"),
        &n.snapshot_uri,
        o.map(|o| o.snapshot_uri.as_str()),
    )
    .required()
    .max_len(2048)
    .finish()
}

/// A snapshot's durable copy.
pub fn snapshot_storage(
    op: &Operation,
    p: &Path,
    n: &pb::SnapshotStorage,
    o: Option<&pb::SnapshotStorage>,
) -> ErrorList {
    rules::all([
        int(op, &p.child("state"), n.state, o.map(|o| o.state))
            .required()
            .min(1)
            .max(2)
            .finish(),
        message(
            op,
            &p.child("object"),
            n.object.as_ref(),
            o.map(|o| o.object.as_ref()),
        )
        .required()
        .then(|p, n, o| object_snapshot(op, p, n, o))
        .finish(),
    ])
}

/// One snapshot generation of an Actor or Tag.
pub fn snapshot(op: &Operation, p: &Path, n: &pb::Snapshot, o: Option<&pb::Snapshot>) -> ErrorList {
    rules::all([
        string(op, &p.child("uuid"), &n.uuid, o.map(|o| o.uuid.as_str()))
            .required()
            .format(Format::DnsLabel)
            .finish(),
        int(
            op,
            &p.child("generation"),
            n.generation,
            o.map(|o| o.generation),
        )
        .required()
        .min(1)
        .finish(),
        int(op, &p.child("owner"), n.owner, o.map(|o| o.owner))
            .required()
            .min(1)
            .max(2)
            .finish(),
        int(op, &p.child("fidelity"), n.fidelity, o.map(|o| o.fidelity))
            .required()
            .min(1)
            .max(3)
            .finish(),
        string(
            op,
            &p.child("actor_template_uid"),
            &n.actor_template_uid,
            o.map(|o| o.actor_template_uid.as_str()),
        )
        .optional()
        .format(Format::Uuid)
        .finish(),
        string(
            op,
            &p.child("locality"),
            &n.locality,
            o.map(|o| o.locality.as_str()),
        )
        .optional()
        .format(Format::DnsSubdomain)
        .finish(),
        message(
            op,
            &p.child("durable_snapshot"),
            n.durable_snapshot.as_ref(),
            o.map(|o| o.durable_snapshot.as_ref()),
        )
        .optional()
        .then(|p, n, o| snapshot_storage(op, p, n, o))
        .finish(),
        message(
            op,
            &p.child("sandbox_runtime"),
            n.sandbox_runtime.as_ref(),
            o.map(|o| o.sandbox_runtime.as_ref()),
        )
        .optional()
        .then(|p, n, o| actor_sandbox_runtime(op, p, n, o))
        .finish(),
    ])
}

/// One snapshot a keeper reports.
pub fn snapshot_report(op: &Operation, p: &Path, n: &pb::SnapshotReport) -> ErrorList {
    rules::all([
        string(op, &p.child("uuid"), &n.uuid, None)
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
            .then(|p, n, o| object_snapshot(op, p, n, o))
            .finish(),
    ])
}

/// What a keeper holds for one Actor.
pub fn actor_snapshot_state(op: &Operation, p: &Path, n: &pb::ActorSnapshotState) -> ErrorList {
    let report = |name: &str, r: Option<&pb::SnapshotReport>| {
        message(op, &p.child(name), r, None)
            .optional()
            .then(|p, n, _| snapshot_report(op, p, n))
            .finish()
    };
    rules::all([
        report("latest_local", n.latest_local.as_ref()),
        report("uploading", n.uploading.as_ref()),
        report("latest_durable", n.latest_durable.as_ref()),
    ])
}
