//! Actor request validation and the check of an Actor against the stored
//! one.

mod common;

use std::collections::HashMap;

use common::*;
use hivekeep_core::field::{ErrorKind, ErrorList, Path};
use hivekeep_core::preview::PreviewGates;
use hivekeep_core::validation::api::*;
use hivekeep_core::validation::Operation;
use hivekeep_proto::api::v1 as pb;

const ACTOR_UID: &str = "6b1f9d0c-4a2e-4d38-9c77-5e0a1b2c3d4e";

fn run(cases: Vec<(&str, ErrorList, Vec<Want>)>) {
    for (name, got, want) in cases {
        assert_errs(name, got, &want);
    }
}

fn oref(hive: &str, name: &str) -> Option<pb::ObjectRef> {
    Some(pb::ObjectRef {
        hive: hive.into(),
        name: name.into(),
    })
}

/// A minimal Actor that passes input validation.
fn valid_actor() -> pb::Actor {
    pb::Actor {
        metadata: Some(pb::ResourceMetadata {
            hive: "ns1".into(),
            name: "id1".into(),
            ..Default::default()
        }),
        actor_template: oref("ns1", "tmpl1"),
        ..Default::default()
    }
}

fn actor(f: impl FnOnce(&mut pb::Actor)) -> pb::Actor {
    with(valid_actor(), f)
}

fn meta(f: impl FnOnce(&mut pb::ResourceMetadata)) -> pb::Actor {
    actor(|a| f(a.metadata.as_mut().unwrap()))
}

fn valid_status() -> pb::ActorStatus {
    pb::ActorStatus {
        state: pb::ActorState::Suspended as i32,
        ..Default::default()
    }
}

/// A valid Actor with a valid status, edited by `f`.
fn with_status(f: impl FnOnce(&mut pb::ActorStatus)) -> pb::Actor {
    actor(|a| a.status = Some(with(valid_status(), f)))
}

fn valid_assignment() -> pb::WorkerAssignment {
    pb::WorkerAssignment {
        worker: oref("", "worker"),
        worker_namespace: "ns".into(),
        worker_pool: "pool".into(),
        worker_pod: "pod".into(),
        worker_pod_uid: "12345678-1234-1234-1234-123456789abc".into(),
        worker_pod_ips: vec!["1.2.3.4".into()],
        node_name: "node1".into(),
        ..Default::default()
    }
}

/// A valid Actor whose status carries a valid assignment edited by `f`.
fn with_assignment(f: impl FnOnce(&mut pb::WorkerAssignment)) -> pb::Actor {
    with_status(|s| s.worker_assignment = Some(with(valid_assignment(), f)))
}

fn selector(labels: HashMap<String, String>) -> Option<pb::Selector> {
    Some(pb::Selector {
        match_labels: labels,
    })
}

fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn labels_of_size(n: usize) -> HashMap<String, String> {
    (0..n).map(|i| (format!("k{i}"), "v".to_string())).collect()
}

fn snapshot(f: impl FnOnce(&mut pb::Snapshot)) -> pb::Snapshot {
    with(
        pb::Snapshot {
            uuid: "snap-1".into(),
            generation: 1,
            owner: pb::SnapshotOwner::Actor as i32,
            fidelity: pb::SnapshotFidelity::Memory as i32,
            locality: "node-1".into(),
            durable_snapshot: Some(pb::SnapshotStorage {
                state: pb::SnapshotStorageState::Completed as i32,
                object: Some(pb::ObjectSnapshot {
                    snapshot_uri: format!(
                        "gs://private/hives/as/actors/{ACTOR_UID}/snapshots/snap-1"
                    ),
                }),
            }),
            ..Default::default()
        },
        f,
    )
}

fn with_snapshots(snaps: Vec<pb::Snapshot>) -> pb::Actor {
    with_status(|s| s.snapshots = snaps)
}

fn volume(name: &str) -> pb::ExternalVolumeStatus {
    pb::ExternalVolumeStatus {
        name: name.into(),
        volume_type: "hivekeep.io/mock".into(),
        ..Default::default()
    }
}

fn with_volumes(volumes: Vec<pb::ExternalVolumeStatus>) -> pb::Actor {
    with_status(|s| s.external_volumes = Some(pb::ExternalVolumes { volumes }))
}

#[test]
fn create_actor_request() {
    // Status is scrubbed on input, so it only needs to be shown optional
    // here; actor_update covers its rules.
    let c = |a: Option<pb::Actor>| {
        validate_create_actor_request(&Operation::create(), &pb::CreateActorRequest { actor: a })
    };
    let s = |l: HashMap<String, String>| Some(actor(|a| a.worker_selector = selector(l)));
    run(vec![
        ("valid", c(Some(valid_actor())), vec![]),
        ("valid with status", c(Some(with_status(|_| {}))), vec![]),
        ("missing actor", c(None), vec![required("actor")]),
        (
            "missing actor.metadata",
            c(Some(actor(|a| a.metadata = None))),
            vec![required("actor.metadata")],
        ),
        (
            "missing actor.metadata.hive",
            c(Some(meta(|m| m.hive.clear()))),
            vec![required("actor.metadata.hive")],
        ),
        (
            "invalid actor.metadata.hive",
            c(Some(meta(|m| m.hive = "NS1".into()))),
            vec![dns_label("actor.metadata.hive")],
        ),
        (
            "missing actor.metadata.name",
            c(Some(meta(|m| m.name.clear()))),
            vec![required("actor.metadata.name")],
        ),
        (
            "invalid actor.metadata.name",
            c(Some(meta(|m| m.name = "ID1".into()))),
            vec![dns_label("actor.metadata.name")],
        ),
        (
            "valid actor.actor_template",
            c(Some(actor(|a| a.actor_template = oref("as", "tmpl")))),
            vec![],
        ),
        (
            "missing actor.actor_template",
            c(Some(actor(|a| a.actor_template = None))),
            vec![required("actor.actor_template")],
        ),
        (
            "missing actor.actor_template.hive",
            c(Some(actor(|a| a.actor_template = oref("", "tmpl")))),
            vec![required("actor.actor_template.hive")],
        ),
        (
            "invalid actor.actor_template.hive",
            c(Some(actor(|a| {
                a.actor_template = oref("invalid value", "tmpl")
            }))),
            vec![dns_label("actor.actor_template.hive")],
        ),
        (
            "missing actor.actor_template.name",
            c(Some(actor(|a| a.actor_template = oref("as", "")))),
            vec![required("actor.actor_template.name")],
        ),
        (
            "invalid actor.actor_template.name",
            c(Some(actor(|a| {
                a.actor_template = oref("as", "invalid value")
            }))),
            vec![dns_label("actor.actor_template.name")],
        ),
        (
            "valid worker_selector",
            c(s(labels(&[("tier", "1")]))),
            vec![],
        ),
        (
            "worker_selector with no match_labels",
            c(Some(actor(|a| {
                a.worker_selector = Some(pb::Selector::default())
            }))),
            vec![invalid_by("actor.worker_selector", "union")],
        ),
        (
            "worker_selector with empty match_labels",
            c(s(HashMap::new())),
            vec![invalid_by("actor.worker_selector", "union")],
        ),
        (
            "worker_selector with exactly max match_labels",
            c(s(labels_of_size(10))),
            vec![],
        ),
        (
            "too many worker_selector.match_labels",
            c(s(labels_of_size(11))),
            vec![(
                ErrorKind::TooMany,
                "actor.worker_selector.match_labels",
                Some("maxProperties"),
            )],
        ),
        (
            "invalid worker_selector label key",
            c(s(labels(&[("bad key!", "1")]))),
            vec![invalid_by(
                "actor.worker_selector.match_labels",
                "format=label-key",
            )],
        ),
        (
            "invalid worker_selector label value",
            c(s(labels(&[("tier", "not valid!")]))),
            vec![invalid_by(
                "actor.worker_selector.match_labels[tier]",
                "format=label-value",
            )],
        ),
        (
            "valid actor.source_tag",
            c(Some(actor(|a| a.source_tag = oref("as", "tag")))),
            vec![],
        ),
        (
            "missing actor.source_tag.hive",
            c(Some(actor(|a| a.source_tag = oref("", "tag")))),
            vec![required("actor.source_tag.hive")],
        ),
        (
            "invalid actor.source_tag.hive",
            c(Some(actor(|a| a.source_tag = oref("invalid value", "tag")))),
            vec![dns_label("actor.source_tag.hive")],
        ),
        (
            "missing actor.source_tag.name",
            c(Some(actor(|a| a.source_tag = oref("as", "")))),
            vec![required("actor.source_tag.name")],
        ),
        (
            "invalid actor.source_tag.name",
            c(Some(actor(|a| a.source_tag = oref("as", "invalid value")))),
            vec![dns_label("actor.source_tag.name")],
        ),
    ]);
}

/// Validates `new` against `old` as the server does before a write, with no
/// preview gates and again with all of them: status is server-written, so
/// no Actor rule depends on a gate.
fn update(old: &pb::Actor, new: &pb::Actor) -> ErrorList {
    let plain = validate_actor_update(&Operation::update(), &Path::root(), new, old, true);
    let gated = validate_actor_update(
        &Operation::update().with_gates(PreviewGates::all()),
        &Path::root(),
        new,
        old,
        true,
    );
    assert_eq!(
        plain.message(),
        gated.message(),
        "preview gates changed an Actor result"
    );
    plain
}

#[test]
fn actor_update() {
    // Inputs carry no status; outputs always do.
    let input = valid_actor;
    let out = |f: &dyn Fn(&mut pb::Actor)| with(with_status(|_| {}), f);
    let out_meta =
        |f: &dyn Fn(&mut pb::ResourceMetadata)| out(&|a| f(a.metadata.as_mut().unwrap()));
    let tmpl = |h: &str, n: &str| actor(|a| a.actor_template = oref(h, n));
    let tag = |h: &str, n: &str| actor(|a| a.source_tag = oref(h, n));
    let out_tag = |h: &'static str, n: &'static str| out(&move |a| a.source_tag = oref(h, n));
    let out_sel =
        |l: HashMap<String, String>| out(&move |a| a.worker_selector = selector(l.clone()));
    let lag = |g: i32| with_status(|s| s.last_assigned_generation = g);
    let wa = "status.worker_assignment";
    let ip = |i: usize| -> &'static str {
        Box::leak(format!("{wa}.worker_pod_ips[{i}]").into_boxed_str())
    };
    let runtime = |class: &str| pb::ActorSandboxRuntime {
        sandbox_class: class.into(),
        version: Some(pb::VersionedSandboxCompat {
            schema_version: "v1".into(),
            attributes: vec![pb::AttributeEntry {
                key: "architecture".into(),
                value: "amd64".into(),
            }],
        }),
    };
    let tuid = |u: &str| with_snapshots(vec![snapshot(|s| s.actor_template_uid = u.into())]);
    let cases: Vec<(&str, pb::Actor, pb::Actor, Vec<Want>)> = vec![
        ("valid", input(), out(&|_| {}), vec![]),
        (
            "missing actor.metadata",
            input(),
            out(&|a| a.metadata = None),
            vec![required("metadata")],
        ),
        (
            "missing actor.metadata.hive",
            input(),
            out_meta(&|m| m.hive.clear()),
            vec![
                required("metadata.hive"),
                invalid_by("metadata.hive", "immutable"),
            ],
        ),
        (
            "invalid actor.metadata.hive",
            input(),
            out_meta(&|m| m.hive = "invalid value".into()),
            vec![invalid_by("metadata.hive", "immutable")],
        ),
        (
            "missing actor.metadata.name",
            input(),
            out_meta(&|m| m.name.clear()),
            vec![
                required("metadata.name"),
                invalid_by("metadata.name", "immutable"),
            ],
        ),
        (
            "invalid actor.metadata.name",
            input(),
            out_meta(&|m| m.name = "invalid value".into()),
            vec![invalid_by("metadata.name", "immutable")],
        ),
        (
            "change actor.actor_template is allowed",
            tmpl("as1", "nm1"),
            out(&|a| a.actor_template = oref("as2", "nm2")),
            vec![],
        ),
        (
            "clear actor.actor_template",
            tmpl("as", "nm"),
            out(&|a| a.actor_template = None),
            vec![required("actor_template")],
        ),
        (
            "add actor.source_tag",
            input(),
            out_tag("as", "nm"),
            vec![invalid_by("source_tag", "immutable")],
        ),
        (
            "clear actor.source_tag",
            tag("as", "nm"),
            out(&|a| a.source_tag = None),
            vec![invalid_by("source_tag", "immutable")],
        ),
        (
            "change actor.source_tag",
            tag("as1", "nm1"),
            out_tag("as2", "nm2"),
            vec![invalid_by("source_tag", "immutable")],
        ),
        (
            "set valid worker_selector",
            input(),
            out_sel(labels(&[("tier", "1")])),
            vec![],
        ),
        (
            "clear worker_selector",
            actor(|a| a.worker_selector = selector(labels(&[("tier", "1")]))),
            out(&|_| {}),
            vec![],
        ),
        (
            "modify worker_selector",
            actor(|a| a.worker_selector = selector(labels(&[("tier", "1")]))),
            out_sel(labels(&[("tier", "2")])),
            vec![],
        ),
        (
            "invalid worker_selector with no match_labels",
            input(),
            out(&|a| a.worker_selector = Some(pb::Selector::default())),
            vec![invalid_by("worker_selector", "union")],
        ),
        (
            "invalid worker_selector label key",
            input(),
            out_sel(labels(&[("bad key", "2")])),
            vec![invalid_by(
                "worker_selector.match_labels",
                "format=label-key",
            )],
        ),
        (
            "invalid worker_selector label value",
            input(),
            out_sel(labels(&[("tier", "bad value")])),
            vec![invalid_by(
                "worker_selector.match_labels[tier]",
                "format=label-value",
            )],
        ),
        (
            "too many worker_selector.match_labels",
            input(),
            out_sel(labels_of_size(11)),
            vec![(
                ErrorKind::TooMany,
                "worker_selector.match_labels",
                Some("maxProperties"),
            )],
        ),
        (
            "unspecified actor.status",
            with_status(|_| {}),
            out(&|a| a.status = None),
            vec![required("status")],
        ),
        (
            "unspecified actor.status.state",
            input(),
            with_status(|s| s.state = 0),
            vec![required("status.state")],
        ),
        (
            "change actor.status.state",
            with_status(|s| s.state = pb::ActorState::Running as i32),
            with_status(|s| s.state = pb::ActorState::Crashed as i32),
            vec![],
        ),
        (
            "negative actor.status.state",
            input(),
            with_status(|s| s.state = -1),
            vec![invalid_by("status.state", "minimum")],
        ),
        (
            "just out of bounds actor.status.state",
            input(),
            with_status(|s| s.state = 10),
            vec![invalid_by("status.state", "maximum")],
        ),
        (
            "invalid actor.status.state",
            input(),
            with_status(|s| s.state = 1234567890),
            vec![invalid_by("status.state", "maximum")],
        ),
        (
            "set valid worker_assignment, IPv4",
            with_status(|_| {}),
            with_assignment(|w| w.worker_pod_ips = vec!["1.2.3.4".into()]),
            vec![],
        ),
        (
            "set valid worker_assignment, IPv6",
            with_status(|_| {}),
            with_assignment(|w| w.worker_pod_ips = vec!["1234::5678".into()]),
            vec![],
        ),
        (
            "set valid worker_assignment, dual-stack",
            with_status(|_| {}),
            with_assignment(|w| w.worker_pod_ips = vec!["1234::5678".into(), "1.2.3.4".into()]),
            vec![],
        ),
        (
            "invalid worker_pod_ips: two IPv6",
            with_status(|_| {}),
            with_assignment(|w| w.worker_pod_ips = vec!["1234::5678".into(), "1234::9".into()]),
            vec![invalid(ip(1))],
        ),
        (
            "invalid worker_pod_ips: two IPv4",
            with_status(|_| {}),
            with_assignment(|w| w.worker_pod_ips = vec!["1.2.3.4".into(), "5.6.7.8".into()]),
            vec![invalid(ip(1))],
        ),
        (
            "invalid worker_pod_ips: too many",
            with_status(|_| {}),
            with_assignment(|w| {
                w.worker_pod_ips = vec!["1.2.3.4".into(), "1234::5678".into(), "1.2.3.5".into()]
            }),
            vec![too_many("status.worker_assignment.worker_pod_ips")],
        ),
        (
            "set valid worker_spiffe_id",
            with_status(|_| {}),
            with_assignment(|w| {
                w.worker_spiffe_id = "spiffe://cluster.local/ns/ns/sa/worker".into()
            }),
            vec![],
        ),
        (
            "invalid worker_spiffe_id",
            with_status(|_| {}),
            with_assignment(|w| w.worker_spiffe_id = "ns/ns/sa/worker".into()),
            vec![invalid("status.worker_assignment.worker_spiffe_id")],
        ),
        (
            "set valid worker_assignment.sandbox_runtime",
            with_status(|_| {}),
            with_assignment(|w| w.sandbox_runtime = Some(runtime("gvisor"))),
            vec![],
        ),
        (
            "invalid worker_assignment.sandbox_runtime.sandbox_class",
            with_status(|_| {}),
            with_assignment(|w| w.sandbox_runtime = Some(runtime("plain"))),
            vec![not_supported(
                "status.worker_assignment.sandbox_runtime.sandbox_class",
            )],
        ),
        (
            "clear actor.status.worker_assignment",
            with_assignment(|_| {}),
            with_status(|s| s.worker_assignment = None),
            vec![],
        ),
        (
            "set actor.status.worker_assignment after it was cleared",
            with_status(|_| {}),
            with_assignment(|_| {}),
            vec![],
        ),
        (
            "modify worker_assignment identity",
            with_assignment(|_| {}),
            with_assignment(|w| w.worker_pod = "pod2".into()),
            vec![invalid_by("status.worker_assignment.worker_pod", "update")],
        ),
        (
            "modify worker_assignment node",
            with_assignment(|_| {}),
            with_assignment(|w| w.node_name = "node2".into()),
            vec![invalid_by("status.worker_assignment.node_name", "update")],
        ),
        (
            "modify worker_assignment worker process",
            with_assignment(|_| {}),
            with_assignment(|w| {
                w.worker_epoch = 2;
                w.worker_pod_ips = vec!["10.9.8.7".into()];
            }),
            vec![],
        ),
        (
            "empty actor.status.worker_assignment",
            input(),
            with_status(|s| s.worker_assignment = Some(pb::WorkerAssignment::default())),
            vec![
                required("status.worker_assignment.worker"),
                required("status.worker_assignment.worker_namespace"),
                required("status.worker_assignment.worker_pool"),
                required("status.worker_assignment.worker_pod"),
                required("status.worker_assignment.worker_pod_uid"),
                required("status.worker_assignment.worker_pod_ips"),
                required("status.worker_assignment.node_name"),
            ],
        ),
        (
            "invalid actor.status.worker_assignment",
            input(),
            with_assignment(|w| {
                w.worker = oref("not-allowed", "bad value");
                w.worker_namespace = "invalid namespace".into();
                w.worker_pool = "invalid pool".into();
                w.worker_pod = "invalid pod".into();
                w.worker_pod_uid = "invalid UUID".into();
                w.worker_pod_ips = vec!["invalid IP".into()];
                w.node_name = "invalid node".into();
            }),
            vec![
                forbidden("status.worker_assignment.worker.hive"),
                dns_label("status.worker_assignment.worker.name"),
                dns_label("status.worker_assignment.worker_namespace"),
                invalid_by(
                    "status.worker_assignment.worker_pool",
                    "format=dns-subdomain",
                ),
                invalid_by(
                    "status.worker_assignment.worker_pod",
                    "format=dns-subdomain",
                ),
                invalid_by("status.worker_assignment.worker_pod_uid", "format=uuid"),
                invalid(ip(0)),
                invalid_by("status.worker_assignment.node_name", "format=dns-subdomain"),
            ],
        ),
        (
            // IPs are checked by hand, so make sure the strict form is
            // enforced.
            "invalid worker_pod_ips: leading 0s",
            input(),
            with_assignment(|w| w.worker_pod_ips = vec!["001.002.003.004".into()]),
            vec![invalid(ip(0))],
        ),
        (
            "invalid worker_pod_ips: non-canonical",
            input(),
            with_assignment(|w| w.worker_pod_ips = vec!["0012::0034".into()]),
            vec![invalid(ip(0))],
        ),
        (
            "valid actor.status.assigned_node",
            input(),
            with_status(|s| s.assigned_node = "node-1".into()),
            vec![],
        ),
        (
            "invalid actor.status.assigned_node",
            input(),
            with_status(|s| s.assigned_node = "NOT A NODE".into()),
            vec![invalid_by("status.assigned_node", "format=dns-subdomain")],
        ),
        (
            "crash message too long",
            input(),
            with_status(|s| {
                s.crash = Some(pb::ActorCrash {
                    message: "x".repeat(4097),
                    crash_time: None,
                })
            }),
            vec![too_long("status.crash.message")],
        ),
        ("valid last_assigned_generation", input(), lag(1), vec![]),
        (
            "increasing last_assigned_generation",
            lag(2),
            lag(3),
            vec![],
        ),
        ("unchanged last_assigned_generation", lag(3), lag(3), vec![]),
        (
            "decreasing last_assigned_generation",
            lag(3),
            lag(1),
            vec![invalid_by("status.last_assigned_generation", "monotonic")],
        ),
        (
            "unsetting last_assigned_generation",
            lag(3),
            lag(0),
            vec![invalid_by("status.last_assigned_generation", "update")],
        ),
        (
            "negative last_assigned_generation",
            input(),
            lag(-1),
            vec![invalid_by("status.last_assigned_generation", "minimum")],
        ),
        (
            "valid snapshots with durable object snapshot",
            input(),
            with_snapshots(vec![snapshot(|_| {})]),
            vec![],
        ),
        (
            "valid snapshots actor_template_uid",
            input(),
            tuid("01234567-89ab-cdef-0123-456789abcdef"),
            vec![],
        ),
        (
            // Each suspend restamps the template the snapshot was taken under.
            "changing snapshots.actor_template_uid is allowed",
            tuid("01234567-89ab-cdef-0123-456789abcdef"),
            tuid("fedcba98-7654-3210-fedc-ba9876543210"),
            vec![],
        ),
        (
            "invalid snapshots.actor_template_uid",
            input(),
            tuid("not-a-uuid"),
            vec![invalid_by(
                "status.snapshots[0].actor_template_uid",
                "format=uuid",
            )],
        ),
        (
            "valid snapshots uuid and locality",
            input(),
            with_snapshots(vec![snapshot(|s| {
                s.uuid = "snap-1".into();
                s.locality = "node-1".into();
            })]),
            vec![],
        ),
        (
            "missing snapshots.uuid",
            input(),
            with_snapshots(vec![snapshot(|s| s.uuid.clear())]),
            vec![required("status.snapshots[0].uuid")],
        ),
        (
            "invalid snapshots.uuid",
            input(),
            with_snapshots(vec![snapshot(|s| s.uuid = "SNAP 1".into())]),
            vec![dns_label("status.snapshots[0].uuid")],
        ),
        (
            "invalid snapshots.locality",
            input(),
            with_snapshots(vec![snapshot(|s| s.locality = "NODE 1".into())]),
            vec![invalid_by(
                "status.snapshots[0].locality",
                "format=dns-subdomain",
            )],
        ),
        (
            "duplicate snapshots generation",
            input(),
            with_snapshots(vec![snapshot(|_| {}), snapshot(|_| {})]),
            vec![duplicate("status.snapshots[1]")],
        ),
        (
            "missing snapshots generation",
            input(),
            with_snapshots(vec![snapshot(|s| s.generation = 0)]),
            vec![required("status.snapshots[0].generation")],
        ),
        (
            "valid snapshots.fidelity",
            input(),
            with_snapshots(vec![snapshot(|s| {
                s.fidelity = pb::SnapshotFidelity::Volumes as i32
            })]),
            vec![],
        ),
        (
            "missing snapshots.fidelity",
            input(),
            with_snapshots(vec![snapshot(|s| s.fidelity = 0)]),
            vec![required("status.snapshots[0].fidelity")],
        ),
        (
            "negative snapshots.fidelity",
            input(),
            with_snapshots(vec![snapshot(|s| s.fidelity = -1)]),
            vec![invalid_by("status.snapshots[0].fidelity", "minimum")],
        ),
        (
            "invalid snapshots.fidelity",
            input(),
            with_snapshots(vec![snapshot(|s| s.fidelity = 4)]),
            vec![invalid_by("status.snapshots[0].fidelity", "maximum")],
        ),
        (
            "no volumes",
            with_status(|_| {}),
            with_status(|_| {}),
            vec![],
        ),
        (
            // Set-once fields permit unset -> set, so a volume added in an
            // update validates like one added at creation.
            "adding a volume",
            with_status(|_| {}),
            with_volumes(vec![volume("vol-a")]),
            vec![],
        ),
        (
            "adding a volume to an actor with no status",
            input(),
            with_volumes(vec![volume("vol-a")]),
            vec![],
        ),
        (
            "provisioning transition on an existing volume",
            with_volumes(vec![volume("vol-a")]),
            with_volumes(vec![with(volume("vol-a"), |v| {
                v.storage_volume_id = "csi-426d29b7".into();
                v.volume_context = [(
                    "attachment".to_string(),
                    "iqn.2026-08.io.hivekeep:vol-a".to_string(),
                )]
                .into();
            })]),
            vec![],
        ),
        (
            "duplicate external_volumes volume_name",
            with_status(|_| {}),
            with_volumes(vec![volume("vol-a"), volume("vol-a")]),
            vec![duplicate("status.external_volumes.volumes[1]")],
        ),
        (
            "too many external_volumes",
            with_status(|_| {}),
            with_volumes((0..33).map(|i| volume(&format!("vol-{i}"))).collect()),
            vec![too_many("status.external_volumes.volumes")],
        ),
    ];
    for (name, old, new, want) in cases {
        assert_errs(name, update(&old, &new), &want);
    }
    // A missing status is only an error when the caller requires one.
    assert!(validate_actor_update(
        &Operation::update(),
        &Path::root(),
        &valid_actor(),
        &valid_actor(),
        false
    )
    .is_empty());
}

/// The ref cases shared by every request naming one Actor.
fn ref_cases(f: impl Fn(Option<pb::ObjectRef>) -> ErrorList) {
    run(vec![
        ("valid", f(oref("ns1", "id1")), vec![]),
        ("missing actor", f(None), vec![required("actor")]),
        (
            "missing actor.hive",
            f(oref("", "id1")),
            vec![required("actor.hive")],
        ),
        (
            "invalid actor.hive",
            f(oref("NS1", "id1")),
            vec![dns_label("actor.hive")],
        ),
        (
            "missing actor.name",
            f(oref("ns1", "")),
            vec![required("actor.name")],
        ),
        (
            "invalid actor.name",
            f(oref("ns1", "ID1")),
            vec![dns_label("actor.name")],
        ),
    ]);
}

#[test]
fn get_actor_request() {
    ref_cases(|actor| {
        validate_get_actor_request(&Operation::create(), &pb::GetActorRequest { actor })
    });
}

#[test]
fn list_actors_request() {
    let c = |hive: &str, options: Option<pb::ListOptions>| {
        validate_list_actors_request(
            &Operation::create(),
            &pb::ListActorsRequest {
                hive: hive.into(),
                options,
            },
        )
    };
    let size = |page_size| {
        Some(pb::ListOptions {
            page_size,
            ..Default::default()
        })
    };
    let token = |n: usize| {
        Some(pb::ListOptions {
            page_token: "x".repeat(n),
            ..Default::default()
        })
    };
    run(vec![
        ("valid, hive scoped", c("ns1", None), vec![]),
        ("valid, empty hive means all hives", c("", None), vec![]),
        ("invalid hive", c("NS1", None), vec![dns_label("hive")]),
        ("valid, positive page_size", c("ns1", size(10)), vec![]),
        (
            "negative page_size",
            c("ns1", size(-1)),
            vec![invalid_by("options.page_size", "minimum")],
        ),
        ("valid page_token", c("ns1", token(256)), vec![]),
        (
            "too-large page_token",
            c("ns1", token(257)),
            vec![too_long("options.page_token")],
        ),
    ]);
}

#[test]
fn update_actor_request() {
    // Status is scrubbed on input; actor_update covers its rules.
    let stored = || {
        meta(|m| {
            m.uid = "12345678-1234-1234-1234-123456789abc".into();
            m.version = 1;
        })
    };
    let s = |f: fn(&mut pb::ResourceMetadata)| {
        Some(with(stored(), |a| f(a.metadata.as_mut().unwrap())))
    };
    let c = |a: Option<pb::Actor>| {
        validate_update_actor_request(&Operation::create(), &pb::UpdateActorRequest { actor: a })
    };
    run(vec![
        ("valid", c(Some(stored())), vec![]),
        (
            "valid with status",
            c(Some(with(stored(), |a| a.status = Some(valid_status())))),
            vec![],
        ),
        ("missing actor", c(None), vec![required("actor")]),
        (
            "missing actor.metadata",
            c(Some(with(stored(), |a| a.metadata = None))),
            vec![required("actor.metadata")],
        ),
        (
            "missing actor.metadata.hive",
            c(s(|m| m.hive.clear())),
            vec![required("actor.metadata.hive")],
        ),
        (
            "invalid actor.metadata.hive",
            c(s(|m| m.hive = "NS1".into())),
            vec![dns_label("actor.metadata.hive")],
        ),
        (
            "missing actor.metadata.name",
            c(s(|m| m.name.clear())),
            vec![required("actor.metadata.name")],
        ),
        (
            "invalid actor.metadata.name",
            c(s(|m| m.name = "ID1".into())),
            vec![dns_label("actor.metadata.name")],
        ),
        (
            "missing actor.metadata.uid precondition",
            c(s(|m| m.uid.clear())),
            vec![],
        ),
        (
            "invalid actor.metadata.uid precondition",
            c(s(|m| m.uid = "not-a-uuid".into())),
            vec![invalid_by("actor.metadata.uid", "format=uuid")],
        ),
        (
            "missing actor.metadata.version precondition",
            c(s(|m| m.version = 0)),
            vec![],
        ),
        (
            "negative actor.metadata.version precondition",
            c(s(|m| m.version = -1)),
            vec![invalid_by("actor.metadata.version", "minimum")],
        ),
        (
            "missing actor.metadata.version and actor.metadata.uid",
            c(s(|m| {
                m.uid.clear();
                m.version = 0;
            })),
            vec![],
        ),
        (
            // The request does not descend into the rest of the Actor.
            "actor body is not checked against the request",
            c(Some(with(stored(), |a| a.actor_template = None))),
            vec![],
        ),
    ]);
}

#[test]
fn delete_actor_request() {
    ref_cases(|actor| {
        validate_delete_actor_request(
            &Operation::create(),
            &pb::DeleteActorRequest {
                actor,
                options: None,
            },
        )
    });
    assert_errs(
        "invalid delete options",
        validate_delete_actor_request(
            &Operation::create(),
            &pb::DeleteActorRequest {
                actor: oref("ns", "a"),
                options: Some(pb::DeleteOptions {
                    version: -1,
                    uid: "x".into(),
                }),
            },
        ),
        &[
            invalid_by("options.version", "minimum"),
            invalid_by("options.uid", "format=uuid"),
        ],
    );
}

#[test]
fn resume_actor_request() {
    ref_cases(|actor| {
        validate_resume_actor_request(&Operation::create(), &pb::ResumeActorRequest { actor })
    });
}

#[test]
fn revert_actor_request() {
    ref_cases(|actor| {
        validate_revert_actor_request(&Operation::create(), &pb::RevertActorRequest { actor })
    });
}

#[test]
fn suspend_actor_request() {
    ref_cases(|actor| {
        validate_suspend_actor_request(
            &Operation::create(),
            &pb::SuspendActorRequest {
                actor,
                options: None,
            },
        )
    });
    let c = |mode| {
        validate_suspend_actor_request(
            &Operation::create(),
            &pb::SuspendActorRequest {
                actor: oref("ns1", "id1"),
                options: Some(pb::SuspendOptions { mode }),
            },
        )
    };
    run(vec![
        (
            "synchronous mode",
            c(pb::DurabilityMode::Synchronous as i32),
            vec![],
        ),
        (
            "asynchronous mode",
            c(pb::DurabilityMode::Asynchronous as i32),
            vec![],
        ),
        ("mode unspecified", c(0), vec![]),
        (
            "mode negative",
            c(-1),
            vec![invalid_by("options.mode", "minimum")],
        ),
        (
            "mode unknown",
            c(3),
            vec![invalid_by("options.mode", "maximum")],
        ),
    ]);
}

#[test]
fn mint_actor_certificate_request() {
    const VALID_UID: &str = "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21";
    let c = |actor: Option<pb::ObjectRef>, uid: &str, csr: Vec<u8>| {
        validate_mint_actor_certificate_request(
            &Operation::create(),
            &pb::MintActorCertificateRequest {
                actor,
                actor_uid: uid.into(),
                certificate_signing_request: csr,
            },
        )
    };
    let csr = || b"csr".to_vec();
    run(vec![
        ("valid", c(oref("ns1", "id1"), VALID_UID, csr()), vec![]),
        (
            "missing actor",
            c(None, VALID_UID, csr()),
            vec![required("actor")],
        ),
        (
            "missing actor_uid",
            c(oref("ns1", "id1"), "", csr()),
            vec![required("actor_uid")],
        ),
        (
            "invalid actor_uid",
            c(oref("ns1", "id1"), "x", csr()),
            vec![invalid_by("actor_uid", "format=uuid")],
        ),
        (
            "missing csr",
            c(oref("ns1", "id1"), VALID_UID, vec![]),
            vec![required("certificate_signing_request")],
        ),
        (
            "csr too large",
            c(oref("ns1", "id1"), VALID_UID, vec![0; 16_385]),
            vec![(
                ErrorKind::TooLong,
                "certificate_signing_request",
                Some("maxBytes"),
            )],
        ),
    ]);
}

#[test]
fn mint_actor_jwt_request() {
    let req = |secs: i64| pb::MintActorJwtRequest {
        actor: oref("ns1", "id1"),
        audiences: vec!["https://example.com".into()],
        expiration_seconds: secs,
    };
    let c = |r: pb::MintActorJwtRequest| validate_mint_actor_jwt_request(&Operation::create(), &r);
    let p = "expiration_seconds";
    run(vec![
        ("minimum expiration", c(req(300)), vec![]),
        ("maximum expiration", c(req(3600)), vec![]),
        ("missing expiration", c(req(0)), vec![required(p)]),
        (
            "negative expiration",
            c(req(-1)),
            vec![invalid_by(p, "minimum")],
        ),
        (
            "expiration below minimum",
            c(req(299)),
            vec![invalid_by(p, "minimum")],
        ),
        (
            "expiration above maximum",
            c(req(3601)),
            vec![invalid_by(p, "maximum")],
        ),
        (
            "no audiences",
            c(with(req(300), |r| r.audiences.clear())),
            vec![required("audiences")],
        ),
        (
            "duplicate audiences",
            c(with(req(300), |r| {
                r.audiences = vec!["a".into(), "a".into()]
            })),
            vec![duplicate("audiences[1]")],
        ),
        (
            "too many audiences",
            c(with(req(300), |r| {
                r.audiences = (0..17).map(|i| i.to_string()).collect()
            })),
            vec![too_many("audiences")],
        ),
    ]);
}
