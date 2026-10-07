use std::collections::{BTreeMap, BTreeSet};

use super::*;

fn to_map(kvs: Vec<KeyValue>) -> BTreeMap<&'static str, AttrValue> {
    let n = kvs.len();
    let m: BTreeMap<_, _> = kvs.into_iter().map(|kv| (kv.key, kv.value)).collect();
    assert_eq!(m.len(), n, "duplicate keys");
    m
}

fn s(v: &str) -> AttrValue {
    AttrValue::Str(v.into())
}

fn assert_attrs(got: Vec<KeyValue>, want: &[(&'static str, AttrValue)]) {
    let got = to_map(got);
    let want: BTreeMap<_, _> = want.iter().cloned().collect();
    assert_eq!(got, want);
}

fn attribution() -> ActorAttribution {
    ActorAttribution {
        actor: ActorRef::new("team-a", "support-agent-42"),
        uid: "uid-abc".into(),
        template_hive: "hk-agents".into(),
        template_name: "support-agent".into(),
    }
}

#[test]
fn actor_attributes_cases() {
    let full = api::Actor {
        metadata: Some(api::ResourceMetadata {
            hive: "team-a".into(),
            name: "support-agent-42".into(),
            uid: "uid-abc".into(),
            version: 7,
            ..Default::default()
        }),
        actor_template: Some(api::ObjectRef {
            hive: "hk-agents".into(),
            name: "support-agent".into(),
        }),
        ..Default::default()
    };
    assert_attrs(
        actor_attributes(Some(&full)),
        &[
            (HIVE_KEY, s("team-a")),
            (ACTOR_NAME_KEY, s("support-agent-42")),
            (ACTOR_UID_KEY, s("uid-abc")),
            (TEMPLATE_NAME_KEY, s("support-agent")),
            (TEMPLATE_HIVE_KEY, s("hk-agents")),
            // Versions stay integers.
            (ACTOR_VERSION_KEY, AttrValue::I64(7)),
        ],
    );
    // No Actor yields zero values rather than nothing.
    assert_attrs(
        actor_attributes(None),
        &[
            (HIVE_KEY, s("")),
            (ACTOR_NAME_KEY, s("")),
            (ACTOR_UID_KEY, s("")),
            (TEMPLATE_NAME_KEY, s("")),
            (TEMPLATE_HIVE_KEY, s("")),
            (ACTOR_VERSION_KEY, AttrValue::I64(0)),
        ],
    );
}

#[test]
fn actor_ref_attributes_cases() {
    assert_attrs(
        actor_ref_attributes(&ActorRef::new("team-a", "support-agent-42")),
        &[
            (HIVE_KEY, s("team-a")),
            (ACTOR_NAME_KEY, s("support-agent-42")),
        ],
    );
    assert_attrs(
        actor_ref_attributes(&ActorRef::new("", "")),
        &[(HIVE_KEY, s("")), (ACTOR_NAME_KEY, s(""))],
    );
}

/// Renaming a key silently breaks dashboards and the contract between
/// components, so every spelling is pinned.
#[test]
fn key_spellings() {
    for (got, want) in [
        (HIVE_KEY, "hk.hive"),
        (ACTOR_NAME_KEY, "hk.actor.name"),
        (ACTOR_UID_KEY, "hk.actor.uid"),
        (ACTOR_CONTAINER_NAME_KEY, "hk.actor.container.name"),
        (TEMPLATE_NAME_KEY, "hk.template.name"),
        (TEMPLATE_HIVE_KEY, "hk.template.hive"),
        (ACTOR_VERSION_KEY, "hk.actor.version"),
        (ACTOR_STATE_KEY, "hk.actor.state"),
        (ACTOR_OPERATION_NAME_KEY, "hk.actor.operation.name"),
        (WORKER_POOL_NAMESPACE_KEY, "hk.workerpool.namespace"),
        (WORKER_POOL_NAME_KEY, "hk.workerpool.name"),
        (WORKER_STATE_KEY, "hk.worker.state"),
        (SANDBOX_CLASS_KEY, "hk.sandbox.class"),
        (SNAPSHOT_KIND_KEY, "hk.snapshot.kind"),
        (SNAPSHOT_FIDELITY_KEY, "hk.snapshot.fidelity"),
        (SNAPSHOT_PHASE_KEY, "hk.snapshot.phase"),
        (IMAGE_CACHE_OUTCOME_KEY, "hk.imagecache.outcome"),
        (SCHEDULER_OUTCOME_KEY, "hk.scheduler.outcome"),
        (ERROR_TYPE_KEY, "error.type"),
        (OTLP_RELAY_KEY, "hk.otlp.relay"),
    ] {
        assert_eq!(got, want);
    }
    // Every Hivekeep key lives in the reserved namespace.
    assert!(HIVE_KEY.starts_with(RESERVED_NAMESPACE));
}

/// Fixed by the OpenTelemetry spec, not ours to choose.
#[test]
fn log_field_spellings() {
    assert_eq!(LOG_TRACE_ID_FIELD, "trace_id");
    assert_eq!(LOG_SPAN_ID_FIELD, "span_id");
    assert_eq!(LOG_TRACE_FLAGS_FIELD, "trace_flags");
}

#[test]
fn actor_log_labels_cases() {
    let full = |container: Option<&str>| {
        let mut m: BTreeMap<String, String> = [
            ("hk.hive", "team-a"),
            ("hk.actor.name", "support-agent-42"),
            ("hk.actor.uid", "uid-abc"),
            ("hk.template.hive", "hk-agents"),
            ("hk.template.name", "support-agent"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        if let Some(c) = container {
            m.insert("hk.actor.container.name".into(), c.into());
        }
        m
    };
    // Container output carries the container name.
    assert_eq!(
        actor_log_labels(&attribution(), "counter"),
        full(Some("counter"))
    );
    // No container name omits the key rather than emitting it empty.
    assert_eq!(actor_log_labels(&attribution(), ""), full(None));
    // A zero attribution still produces the five identity keys.
    let zero = ActorAttribution {
        actor: ActorRef::new("", ""),
        uid: String::new(),
        template_hive: String::new(),
        template_name: String::new(),
    };
    let got = actor_log_labels(&zero, "");
    assert_eq!(got.len(), 5);
    assert!(got.values().all(String::is_empty));
}

#[test]
fn actor_log_attrs_cases() {
    let got: BTreeMap<_, _> = actor_log_attrs(&attribution()).into_iter().collect();
    let want: BTreeMap<_, _> = [
        ("hk.hive", "team-a"),
        ("hk.actor.name", "support-agent-42"),
        ("hk.actor.uid", "uid-abc"),
        ("hk.template.hive", "hk-agents"),
        ("hk.template.name", "support-agent"),
    ]
    .into_iter()
    .map(|(k, v)| (k, v.to_string()))
    .collect();
    assert_eq!(got, want);
    let zero = ActorAttribution {
        actor: ActorRef::new("", ""),
        uid: String::new(),
        template_hive: String::new(),
        template_name: String::new(),
    };
    let got = actor_log_attrs(&zero);
    assert_eq!(got.len(), 5);
    assert!(got.iter().all(|(_, v)| v.is_empty()));
}

/// The two log shapes must join: a consumer filtering on `hk.actor.uid`
/// finds the Actor in both.
#[test]
fn actor_log_attrs_match_actor_log_labels() {
    let a = attribution();
    let labels = actor_log_labels(&a, "");
    let attrs = actor_log_attrs(&a);
    assert_eq!(attrs.len(), labels.len());
    for (k, v) in attrs {
        assert_eq!(labels.get(k), Some(&v), "{k}");
    }
}

/// The exact strings dashboards group by.
#[test]
fn metric_label_values() {
    for (got, want) in [
        (worker_state::IDLE, "idle"),
        (worker_state::PARTIAL, "partial"),
        (worker_state::AT_CAPACITY, "at_capacity"),
        (worker_state::UNSCHEDULABLE, "unschedulable"),
        (operation::CREATE, "create"),
        (operation::RESUME, "resume"),
        (operation::SUSPEND, "suspend"),
        (operation::DELETE, "delete"),
        (operation::REVERT, "revert"),
        (operation::UNKNOWN, "unknown"),
        (image_cache_outcome::HIT, "hit"),
        (image_cache_outcome::MISS, "miss"),
        (image_cache_outcome::ERROR, "error"),
        (image_cache_outcome::CANCELLED, "cancelled"),
        (image_cache_outcome::TIMEOUT, "timeout"),
        (scheduler_outcome::ASSIGNED, "assigned"),
        (scheduler_outcome::NO_CAPACITY, "no_capacity"),
        (scheduler_outcome::ERROR, "error"),
        (snapshot_kind::LATEST, "latest"),
        (snapshot_kind::LOCAL, "local"),
        (snapshot_kind::BOOT, "boot"),
        (snapshot_fidelity::MEMORY, "memory"),
        (snapshot_fidelity::VOLUMES, "volumes"),
        (snapshot_fidelity::ROOTFS, "rootfs"),
        (snapshot_fidelity::UNKNOWN, "unknown"),
        (snapshot_phase::VOLUME_MOUNT, "volume_mount"),
        (snapshot_phase::MANIFEST_FETCH, "manifest_fetch"),
        (snapshot_phase::SANDBOX_ASSETS, "sandbox_assets"),
        (snapshot_phase::DOWNLOAD, "download"),
        (snapshot_phase::OCI_UNPACK, "oci_unpack"),
        (snapshot_phase::WORKER_RESTORE, "worker_restore"),
        (snapshot_phase::WORKER_CHECKPOINT, "worker_checkpoint"),
        (snapshot_phase::PERSIST, "persist"),
        (snapshot_phase::CLEANUP, "cleanup"),
        (snapshot_phase::TOTAL, "total"),
        (SANDBOX_CLASS_UNKNOWN, "unknown"),
        (TEMPLATE_UNKNOWN, "unknown"),
        (router_resume::NONE, "none"),
        (router_resume::TRIGGERED, "triggered"),
        (router_resume::JOINED, "joined"),
        (router_resume::UNKNOWN, "unknown"),
    ] {
        assert_eq!(got, want);
    }
}

#[test]
fn actor_metric_attributes_cases() {
    let assignment = Some(api::ActorStatus {
        worker_assignment: Some(api::WorkerAssignment {
            worker_namespace: "hk-workers".into(),
            worker_pool: "default-pool".into(),
            ..Default::default()
        }),
        ..Default::default()
    });
    let actor = api::Actor {
        actor_template: Some(api::ObjectRef {
            hive: "default".into(),
            name: "counter-template".into(),
        }),
        status: assignment.clone(),
        ..Default::default()
    };
    let want = |tmpl: (&str, &str), class: &str, op: &str, pool: bool| {
        let mut w = vec![
            (TEMPLATE_HIVE_KEY, s(tmpl.0)),
            (TEMPLATE_NAME_KEY, s(tmpl.1)),
            (SANDBOX_CLASS_KEY, s(class)),
            (ACTOR_OPERATION_NAME_KEY, s(op)),
        ];
        if pool {
            w.push((WORKER_POOL_NAMESPACE_KEY, s("hk-workers")));
            w.push((WORKER_POOL_NAME_KEY, s("default-pool")));
        }
        w
    };
    let t = ("default", "counter-template");
    // An explicit operation.
    assert_attrs(
        actor_metric_attributes(Some(&actor), "gvisor", operation::RESUME),
        &want(t, "gvisor", "resume", true),
    );
    // A missing operation is unknown.
    assert_attrs(
        actor_metric_attributes(Some(&actor), "gvisor", ""),
        &want(t, "gvisor", "unknown", true),
    );
    // An empty sandbox class is unknown, never empty.
    assert_attrs(
        actor_metric_attributes(Some(&actor), "", operation::RESUME),
        &want(t, "unknown", "resume", true),
    );
    // An out-of-range operation is unknown.
    assert_attrs(
        actor_metric_attributes(Some(&actor), "gvisor", "invalid_op"),
        &want(t, "gvisor", "unknown", true),
    );
    // No template reference reports empty template labels.
    let no_template = api::Actor {
        status: assignment,
        ..Default::default()
    };
    assert_attrs(
        actor_metric_attributes(Some(&no_template), "gvisor", operation::RESUME),
        &want(("", ""), "gvisor", "resume", true),
    );
    // An unassigned Actor omits both pool keys.
    let unassigned = api::Actor {
        actor_template: actor.actor_template.clone(),
        ..Default::default()
    };
    assert_attrs(
        actor_metric_attributes(Some(&unassigned), "gvisor", operation::CREATE),
        &want(t, "gvisor", "create", false),
    );
    assert!(actor_metric_attributes(None, "gvisor", "create").is_empty());
}

/// Both or neither: half the pair identifies no pool.
#[test]
fn worker_pool_attributes_cases() {
    assert_attrs(
        worker_pool_attributes("hk-workers", "pool-a"),
        &[
            (WORKER_POOL_NAMESPACE_KEY, s("hk-workers")),
            (WORKER_POOL_NAME_KEY, s("pool-a")),
        ],
    );
    for ns in ["", "hk-workers"] {
        assert!(worker_pool_attributes(ns, "").is_empty(), "{ns:?}");
    }
    assert!(worker_pool_attributes("", "pool-a").is_empty());
}

#[test]
fn snapshot_fidelity_values() {
    use keeper::SnapshotFidelity as F;
    for (f, want) in [
        (F::Memory as i32, "memory"),
        (F::Volumes as i32, "volumes"),
        (F::Rootfs as i32, "rootfs"),
        (F::Unspecified as i32, "unknown"),
        (9999, "unknown"),
    ] {
        assert_eq!(snapshot_fidelity_value(f), want, "{f}");
    }
}

#[test]
fn actor_state_spellings() {
    for (got, want) in [
        (actor_state::RESUMING, "resuming"),
        (actor_state::RUNNING, "running"),
        (actor_state::SUSPENDING, "suspending"),
        (actor_state::SUSPENDED, "suspended"),
        (actor_state::CRASHED, "crashed"),
        (actor_state::DELETING, "deleting"),
        (actor_state::DELETED, "deleted"),
        (actor_state::UNKNOWN, "unknown"),
    ] {
        assert_eq!(got, want);
    }
}

#[test]
fn actor_state_value_cases() {
    use api::ActorState as S;
    for (st, want) in [
        (S::Resuming as i32, actor_state::RESUMING),
        (S::Running as i32, actor_state::RUNNING),
        (S::Suspending as i32, actor_state::SUSPENDING),
        (S::Suspended as i32, actor_state::SUSPENDED),
        (S::Crashed as i32, actor_state::CRASHED),
        (S::Deleting as i32, actor_state::DELETING),
        (S::Reverting as i32, actor_state::REVERTING),
        (S::Unspecified as i32, actor_state::UNKNOWN),
        (9999, actor_state::UNKNOWN),
    ] {
        assert_eq!(actor_state_value(st), want, "{st}");
    }
}

/// The log vocabulary follows the control plane's state machine: a state
/// added to the enum without a value here fails.
#[test]
fn actor_state_values_mirror_actor_state() {
    let no_enum_counterpart: BTreeSet<&str> = [actor_state::DELETED, actor_state::UNKNOWN].into();
    let got: BTreeSet<&str> = [
        actor_state::RESUMING,
        actor_state::RUNNING,
        actor_state::SUSPENDING,
        actor_state::SUSPENDED,
        actor_state::CRASHED,
        actor_state::DELETING,
        actor_state::REVERTING,
        actor_state::DELETED,
        actor_state::UNKNOWN,
    ]
    .into();
    let want: BTreeSet<String> = (1..64)
        .filter_map(|v| api::ActorState::try_from(v).ok())
        .map(|s| {
            s.as_str_name()
                .trim_start_matches("ACTOR_STATE_")
                .to_lowercase()
        })
        .collect();
    for st in &want {
        assert!(
            got.contains(st.as_str()),
            "ActorState has {st:?} with no attr value"
        );
    }
    for st in &got {
        assert!(
            want.contains(*st) || no_enum_counterpart.contains(st),
            "attr value {st:?} has no ActorState"
        );
    }
}

/// Anything unrecognized collapses onto one value.
#[test]
fn normalize_sandbox_class_cases() {
    for (class, want) in [
        ("gvisor", "gvisor"),
        ("microvm", "microvm"),
        ("", "unknown"),
        ("kvm", "unknown"),
        ("GVISOR", "unknown"),
        ("gvisor\";evil=\"1", "unknown"),
    ] {
        assert_eq!(normalize_sandbox_class(class), want, "{class:?}");
    }
    assert_eq!(
        sandbox_class_attribute("kvm"),
        KeyValue::str(SANDBOX_CLASS_KEY, "unknown")
    );
}

#[test]
fn normalize_operation_name_cases() {
    for (op, want) in [
        ("create", "create"),
        ("resume", "resume"),
        ("suspend", "suspend"),
        ("pause", "unknown"),
        ("delete", "delete"),
        ("revert", "revert"),
        ("", "unknown"),
        ("invalid", "unknown"),
        ("crash", "unknown"),
    ] {
        assert_eq!(normalize_operation_name(op), want, "{op:?}");
    }
}

#[test]
fn actor_ref_log_attrs_cases() {
    let got: BTreeMap<_, _> = actor_ref_log_attrs(&ActorRef::new("space-1", "actor-1"))
        .into_iter()
        .collect();
    let want: BTreeMap<_, _> = [
        (HIVE_KEY, "space-1".to_string()),
        (ACTOR_NAME_KEY, "actor-1".to_string()),
    ]
    .into();
    assert_eq!(got, want);
}

#[test]
fn normalize_template_dimension_cases() {
    assert_eq!(normalize_template_dimension(""), TEMPLATE_UNKNOWN);
    assert_eq!(normalize_template_dimension("counter"), "counter");
    assert_eq!(normalize_template_dimension("hk-demo"), "hk-demo");
}
