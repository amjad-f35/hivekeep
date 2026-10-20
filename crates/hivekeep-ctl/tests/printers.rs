//! Rendering of every resource in each output format, against a fixed
//! clock.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hivekeep_ctl::age::{format_age, timestamp_before};
use hivekeep_ctl::io::SharedBuf;
use hivekeep_ctl::output::{OutputFormat, Printer};
use hivekeep_ctl::print;
use hivekeep_ctl::print::worker_top::WorkerTopItem;
use hivekeep_proto::api::v1 as pb;

/// 2026-01-01T12:00:00Z.
fn now() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_767_268_800)
}

fn ago(secs: u64) -> Option<prost_types::Timestamp> {
    Some(timestamp_before(now(), Duration::from_secs(secs)))
}

fn p(format: OutputFormat) -> Printer {
    Printer::at(format, now())
}

fn meta(hive: &str, name: &str, version: i64) -> Option<pb::ResourceMetadata> {
    Some(pb::ResourceMetadata {
        hive: hive.into(),
        name: name.into(),
        version,
        ..Default::default()
    })
}

fn render(f: impl FnOnce(&mut SharedBuf)) -> String {
    let mut buf = SharedBuf::default();
    f(&mut buf);
    buf.contents()
}

const MIN: u64 = 60;
const HOUR: u64 = 3600;

#[test]
fn format_age_cases() {
    for (secs, want) in [(5 * MIN, "5m"), (5 * HOUR, "5h"), (72 * HOUR, "3d")] {
        assert_eq!(format_age(ago(secs).as_ref(), now()), want);
    }
}

#[test]
fn invalid_formats_are_rejected() {
    // Every printer takes an `OutputFormat`, so an unknown format is turned
    // away when the flag is parsed.
    for bad in ["xml", "invalid", ""] {
        let err = bad.parse::<OutputFormat>().unwrap_err();
        assert!(err.contains("Must be one of: table, json, yaml"), "{err}");
    }
}

// Actors.

#[test]
fn actors_table() {
    let a = pb::Actor {
        metadata: Some(pb::ResourceMetadata {
            name: "id-1".into(),
            hive: "team-a".into(),
            version: 2,
            create_time: ago(5 * MIN),
            ..Default::default()
        }),
        actor_template: Some(pb::ObjectRef {
            hive: "default".into(),
            name: "template-1".into(),
        }),
        status: Some(pb::ActorStatus {
            state: pb::ActorState::Running as i32,
            worker_assignment: Some(pb::WorkerAssignment {
                worker_namespace: "worker-ns".into(),
                worker_pod: "pod-1".into(),
                worker_pod_ips: vec!["1.2.3.4".into(), "fd00::1".into()],
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(
        render(|b| print::actor::print_list(&p(OutputFormat::Table), b, vec![a]).unwrap()),
        "HIVE     NAME   TEMPLATE             STATE                 WORKER POD        WORKER IPS        VERSION   AGE\n\
         team-a   id-1   default/template-1   ACTOR_STATE_RUNNING   worker-ns/pod-1   1.2.3.4,fd00::1   2         5m\n"
    );
}

fn minimal_actor() -> pb::Actor {
    pb::Actor {
        metadata: meta("", "id-1", 2),
        ..Default::default()
    }
}

#[test]
fn actors_json() {
    assert_eq!(
        render(|b| print::actor::print_list(&p(OutputFormat::Json), b, vec![minimal_actor()])
            .unwrap()),
        "{\n  \"actors\": [\n    {\n      \"metadata\": {\n        \"name\": \"id-1\",\n        \"version\": \"2\"\n      }\n    }\n  ]\n}\n"
    );
}

#[test]
fn actors_yaml() {
    assert_eq!(
        render(
            |b| print::actor::print_list(&p(OutputFormat::Yaml), b, vec![minimal_actor()]).unwrap()
        ),
        "actors:\n- metadata:\n    name: id-1\n    version: \"2\"\n"
    );
}

fn templated_actor(
    hive: &str,
    name: &str,
    tpl: (&str, &str),
    state: pb::ActorState,
    secs: u64,
) -> pb::Actor {
    pb::Actor {
        metadata: Some(pb::ResourceMetadata {
            name: name.into(),
            hive: hive.into(),
            create_time: ago(secs),
            ..Default::default()
        }),
        actor_template: Some(pb::ObjectRef {
            hive: tpl.0.into(),
            name: tpl.1.into(),
        }),
        status: Some(pb::ActorStatus {
            state: state as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn actors_table_sorted() {
    use pb::ActorState::{Running, Suspended};
    let actors = vec![
        templated_actor(
            "team-b",
            "zebra",
            ("default", "template-1"),
            Suspended,
            72 * HOUR,
        ),
        templated_actor(
            "team-a",
            "alpha",
            ("default", "template-1"),
            Running,
            5 * MIN,
        ),
        templated_actor(
            "team-a",
            "beta",
            ("other", "template-2"),
            Suspended,
            5 * HOUR,
        ),
    ];
    // Sorted by hive, then template, then name.
    assert_eq!(
        render(|b| print::actor::print_list(&p(OutputFormat::Table), b, actors).unwrap()),
        "HIVE     NAME    TEMPLATE             STATE                   WORKER POD   WORKER IPS   VERSION   AGE\n\
         team-a   alpha   default/template-1   ACTOR_STATE_RUNNING     <none>                    0         5m\n\
         team-a   beta    other/template-2     ACTOR_STATE_SUSPENDED   <none>                    0         5h\n\
         team-b   zebra   default/template-1   ACTOR_STATE_SUSPENDED   <none>                    0         3d\n"
    );
}

#[test]
fn actors_table_template_ref() {
    let a = templated_actor(
        "team-a",
        "id-1",
        ("demo-counter-shared", "counter"),
        pb::ActorState::Suspended,
        5 * MIN,
    );
    assert_eq!(
        render(|b| print::actor::print_list(&p(OutputFormat::Table), b, vec![a]).unwrap()),
        "HIVE     NAME   TEMPLATE                      STATE                   WORKER POD   WORKER IPS   VERSION   AGE\n\
         team-a   id-1   demo-counter-shared/counter   ACTOR_STATE_SUSPENDED   <none>                    0         5m\n"
    );
}

#[test]
fn actor_json() {
    assert_eq!(
        render(|b| print::actor::print_one(&p(OutputFormat::Json), b, &minimal_actor()).unwrap()),
        "{\n  \"metadata\": {\n    \"name\": \"id-1\",\n    \"version\": \"2\"\n  }\n}\n"
    );
}

#[test]
fn actor_yaml() {
    assert_eq!(
        render(|b| print::actor::print_one(&p(OutputFormat::Yaml), b, &minimal_actor()).unwrap()),
        "metadata:\n  name: id-1\n  version: \"2\"\n"
    );
}

// Workers.

fn bare_worker(name: Option<&str>, ns: &str, pool: &str, pod: &str, secs: u64) -> pb::Worker {
    pb::Worker {
        metadata: name.map(|n| pb::ResourceMetadata {
            name: n.into(),
            create_time: ago(secs),
            ..Default::default()
        }),
        worker_namespace: ns.into(),
        worker_pool: pool.into(),
        worker_pod: pod.into(),
        ..Default::default()
    }
}

#[test]
fn workers_table() {
    let mut w = bare_worker(Some("worker-1"), "default", "pool-1", "pod-1", 72 * HOUR);
    w.status = Some(pb::WorkerStatus {
        state: pb::WorkerState::Active as i32,
        capacity: Some(pb::WorkerResources {
            actors: 4,
            cpu_millicores: 2000,
            memory_bytes: 4 << 30,
        }),
        allocated: Some(pb::WorkerResources {
            actors: 1,
            cpu_millicores: 500,
            memory_bytes: 1 << 30,
        }),
        ..Default::default()
    });
    assert_eq!(
        render(|b| print::worker::print_list(&p(OutputFormat::Table), b, vec![w]).unwrap()),
        "NAME       POOL     STATE                 ACTORS   CPU(MILLICORES)   MEMORY    POD             AGE\n\
         worker-1   pool-1   WORKER_STATE_ACTIVE   1/4      500/2000          1Gi/4Gi   default/pod-1   3d\n"
    );
}

#[test]
fn workers_table_free() {
    // No metadata and no status: zero counts, "-" for the resources and
    // the age of the epoch.
    let w = bare_worker(None, "default", "pool-1", "pod-1", 0);
    assert_eq!(
        render(|b| print::worker::print_list(&p(OutputFormat::Table), b, vec![w]).unwrap()),
        "NAME   POOL     STATE                      ACTORS   CPU(MILLICORES)   MEMORY   POD             AGE\n\
         \x20      pool-1   WORKER_STATE_UNSPECIFIED   0/0      -                 -        default/pod-1   56y\n"
    );
}

#[test]
fn workers_table_sorted() {
    let workers = vec![
        bare_worker(Some("worker-z"), "default", "pool-1", "pod-z", 5 * MIN),
        bare_worker(Some("worker-a"), "default", "pool-1", "pod-a", 5 * MIN),
        bare_worker(Some("worker-o"), "other", "pool-2", "pod-1", 5 * MIN),
    ];
    assert_eq!(
        render(|b| print::worker::print_list(&p(OutputFormat::Table), b, workers).unwrap()),
        "NAME       POOL     STATE                      ACTORS   CPU(MILLICORES)   MEMORY   POD             AGE\n\
         worker-a   pool-1   WORKER_STATE_UNSPECIFIED   0/0      -                 -        default/pod-a   5m\n\
         worker-z   pool-1   WORKER_STATE_UNSPECIFIED   0/0      -                 -        default/pod-z   5m\n\
         worker-o   pool-2   WORKER_STATE_UNSPECIFIED   0/0      -                 -        other/pod-1     5m\n"
    );
}

fn named_worker() -> pb::Worker {
    pb::Worker {
        metadata: meta("", "worker-1", 0),
        ..Default::default()
    }
}

#[test]
fn worker_json() {
    assert_eq!(
        render(|b| print::worker::print_one(&p(OutputFormat::Json), b, &named_worker()).unwrap()),
        "{\n  \"metadata\": {\n    \"name\": \"worker-1\"\n  }\n}\n"
    );
}

#[test]
fn worker_yaml() {
    assert_eq!(
        render(|b| print::worker::print_one(&p(OutputFormat::Yaml), b, &named_worker()).unwrap()),
        "metadata:\n  name: worker-1\n"
    );
}

// Actor templates.

fn template(hive: &str, name: &str, version: i64, secs: u64) -> pb::ActorTemplate {
    pb::ActorTemplate {
        metadata: Some(pb::ResourceMetadata {
            hive: hive.into(),
            name: name.into(),
            version,
            create_time: ago(secs),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn actor_templates_table() {
    let mut golden = template("demo-counter", "counter", 1, 5 * MIN);
    golden.status = Some(pb::ActorTemplateStatus {
        golden_snapshot_status: Some(pb::GoldenSnapshotStatus {
            golden_tag: Some(pb::ObjectRef {
                hive: "demo-counter".into(),
                name: "golden-tag".into(),
            }),
            ..Default::default()
        }),
    });
    let mut failed = template("demo-counter-microvm", "counter-microvm", 1, 5 * HOUR);
    failed.status = Some(pb::ActorTemplateStatus {
        golden_snapshot_status: Some(pb::GoldenSnapshotStatus {
            error_message: "golden actor failed to start".into(),
            ..Default::default()
        }),
    });
    let plain = template("demo-counter", "counter-2", 1, 72 * HOUR);
    let mut disabled = template("demo-counter", "counter-3", 0, 15 * MIN);
    disabled.snapshot_config = Some(pb::SnapshotConfig {
        golden_snapshot_config: Some(pb::GoldenSnapshotConfig {
            mode: pb::GoldenSnapshotMode::Disabled as i32,
        }),
        ..Default::default()
    });
    // Sorted by hive, then name; ERROR only flags that a message exists.
    assert_eq!(
        render(|b| print::actor_template::print_list(
            &p(OutputFormat::Table),
            b,
            vec![golden, failed, plain, disabled]
        )
        .unwrap()),
        "HIVE                   NAME              GOLDEN TAG   ERROR   AGE\n\
         demo-counter           counter           golden-tag           5m\n\
         demo-counter           counter-2                              3d\n\
         demo-counter           counter-3         DISABLED             15m\n\
         demo-counter-microvm   counter-microvm                ERROR   5h\n"
    );
}

fn minimal_template() -> pb::ActorTemplate {
    pb::ActorTemplate {
        metadata: meta("team-a", "counter", 1),
        ..Default::default()
    }
}

#[test]
fn actor_templates_json() {
    assert_eq!(
        render(|b| print::actor_template::print_list(
            &p(OutputFormat::Json),
            b,
            vec![minimal_template()]
        )
        .unwrap()),
        "{\n  \"actorTemplates\": [\n    {\n      \"metadata\": {\n        \"hive\": \"team-a\",\n        \"name\": \"counter\",\n        \"version\": \"1\"\n      }\n    }\n  ]\n}\n"
    );
}

#[test]
fn actor_templates_yaml() {
    assert_eq!(
        render(|b| print::actor_template::print_list(
            &p(OutputFormat::Yaml),
            b,
            vec![minimal_template()]
        )
        .unwrap()),
        "actorTemplates:\n- metadata:\n    hive: team-a\n    name: counter\n    version: \"1\"\n"
    );
}

#[test]
fn actor_template_json() {
    assert_eq!(
        render(|b| print::actor_template::print_one(
            &p(OutputFormat::Json),
            b,
            &minimal_template()
        )
        .unwrap()),
        "{\n  \"metadata\": {\n    \"hive\": \"team-a\",\n    \"name\": \"counter\",\n    \"version\": \"1\"\n  }\n}\n"
    );
}

#[test]
fn actor_template_yaml() {
    assert_eq!(
        render(|b| print::actor_template::print_one(
            &p(OutputFormat::Yaml),
            b,
            &minimal_template()
        )
        .unwrap()),
        "metadata:\n  hive: team-a\n  name: counter\n  version: \"1\"\n"
    );
}

// Tags.

fn tag(name: &str, scope: pb::TagScope, state: pb::SnapshotStorageState, secs: u64) -> pb::Tag {
    pb::Tag {
        metadata: Some(pb::ResourceMetadata {
            hive: "team-a".into(),
            name: name.into(),
            create_time: ago(secs),
            ..Default::default()
        }),
        scope: scope as i32,
        status: Some(pb::TagStatus {
            snapshot: Some(pb::Snapshot {
                fidelity: pb::SnapshotFidelity::Memory as i32,
                durable_snapshot: Some(pb::SnapshotStorage {
                    state: state as i32,
                    object: Some(pb::ObjectSnapshot {
                        snapshot_uri: format!("gs://private/hives/team-a/tags/{name}"),
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn tags_table() {
    use pb::SnapshotStorageState::{Completed, Pending};
    let tags = vec![
        tag("v2", pb::TagScope::Published, Completed, 5 * MIN),
        tag("v1", pb::TagScope::Hive, Completed, 5 * HOUR),
        // A create that never finished names nothing yet.
        tag("v3", pb::TagScope::Hive, Pending, 30),
    ];
    assert_eq!(
        render(|b| print::tag::print_list(&p(OutputFormat::Table), b, tags).unwrap()),
        "HIVE     NAME   SCOPE                 STATE     SNAPSHOT                            FIDELITY                   AGE\n\
         team-a   v1     TAG_SCOPE_HIVE        Ready     gs://private/hives/team-a/tags/v1   SNAPSHOT_FIDELITY_MEMORY   5h\n\
         team-a   v2     TAG_SCOPE_PUBLISHED   Ready     gs://private/hives/team-a/tags/v2   SNAPSHOT_FIDELITY_MEMORY   5m\n\
         team-a   v3     TAG_SCOPE_HIVE        Pending   <none>                              <none>                     30s\n"
    );
}

fn minimal_tag() -> pb::Tag {
    pb::Tag {
        metadata: meta("team-a", "v1", 0),
        ..Default::default()
    }
}

#[test]
fn tag_json() {
    assert_eq!(
        render(|b| print::tag::print_one(&p(OutputFormat::Json), b, &minimal_tag()).unwrap()),
        "{\n  \"metadata\": {\n    \"hive\": \"team-a\",\n    \"name\": \"v1\"\n  }\n}\n"
    );
}

#[test]
fn tag_yaml() {
    assert_eq!(
        render(|b| print::tag::print_one(&p(OutputFormat::Yaml), b, &minimal_tag()).unwrap()),
        "metadata:\n  hive: team-a\n  name: v1\n"
    );
}

// Hives.

fn hive(name: &str, secs: Option<u64>) -> pb::Hive {
    pb::Hive {
        metadata: Some(pb::ResourceMetadata {
            name: name.into(),
            create_time: secs.and_then(ago),
            ..Default::default()
        }),
    }
}

#[test]
fn hives_table() {
    assert_eq!(
        render(|b| print::hive::print_list(
            &p(OutputFormat::Table),
            b,
            vec![hive("team-a", Some(5 * MIN))]
        )
        .unwrap()),
        "NAME     AGE\nteam-a   5m\n"
    );
}

#[test]
fn hives_json() {
    assert_eq!(
        render(|b| print::hive::print_list(&p(OutputFormat::Json), b, vec![hive("team-a", None)])
            .unwrap()),
        "{\n  \"hives\": [\n    {\n      \"metadata\": {\n        \"name\": \"team-a\"\n      }\n    }\n  ]\n}\n"
    );
}

#[test]
fn hives_yaml() {
    assert_eq!(
        render(
            |b| print::hive::print_list(&p(OutputFormat::Yaml), b, vec![hive("team-a", None)])
                .unwrap()
        ),
        "hives:\n- metadata:\n    name: team-a\n"
    );
}

#[test]
fn hives_table_sorted() {
    let hives = vec![
        hive("team-c", Some(72 * HOUR)),
        hive("team-a", Some(5 * MIN)),
        hive("team-b", Some(5 * HOUR)),
    ];
    assert_eq!(
        render(|b| print::hive::print_list(&p(OutputFormat::Table), b, hives).unwrap()),
        "NAME     AGE\nteam-a   5m\nteam-b   5h\nteam-c   3d\n"
    );
}

#[test]
fn hive_json() {
    assert_eq!(
        render(
            |b| print::hive::print_one(&p(OutputFormat::Json), b, &hive("team-a", None)).unwrap()
        ),
        "{\n  \"metadata\": {\n    \"name\": \"team-a\"\n  }\n}\n"
    );
}

#[test]
fn hive_yaml() {
    assert_eq!(
        render(
            |b| print::hive::print_one(&p(OutputFormat::Yaml), b, &hive("team-a", None)).unwrap()
        ),
        "metadata:\n  name: team-a\n"
    );
}

// Worker usage.

#[test]
fn worker_top_table() {
    let items = vec![
        WorkerTopItem {
            pod: "counter-worker-pool-7b9f8-x123".into(),
            pool: "counter".into(),
            class: "gvisor".into(),
            status: "ASSIGNED".into(),
            cpu: "342m".into(),
            memory: "412Mi".into(),
            namespace: "demo-counter".into(),
        },
        WorkerTopItem {
            pod: "counter-worker-pool-7b9f8-y456".into(),
            pool: "counter".into(),
            class: "microvm".into(),
            status: "FREE".into(),
            cpu: "2m".into(),
            memory: "64Mi".into(),
            namespace: "demo-counter".into(),
        },
    ];
    assert_eq!(
        render(|b| print::worker_top::print(&p(OutputFormat::Table), b, items).unwrap()),
        "NAME                             POOL      CLASS     STATUS     CPU(CORES)   MEMORY(bytes)\n\
         counter-worker-pool-7b9f8-x123   counter   gvisor    ASSIGNED   342m         412Mi\n\
         counter-worker-pool-7b9f8-y456   counter   microvm   FREE       2m           64Mi\n"
    );
}

fn top_item() -> WorkerTopItem {
    WorkerTopItem {
        pod: "worker-1".into(),
        pool: "pool-1".into(),
        class: String::new(),
        status: "ASSIGNED".into(),
        cpu: "100m".into(),
        memory: "128Mi".into(),
        namespace: String::new(),
    }
}

#[test]
fn worker_top_json() {
    assert_eq!(
        render(|b| print::worker_top::print(&p(OutputFormat::Json), b, vec![top_item()]).unwrap()),
        "{\n  \"workers\": [\n    {\n      \"pod\": \"worker-1\",\n      \"pool\": \"pool-1\",\n      \"status\": \"ASSIGNED\",\n      \"cpu\": \"100m\",\n      \"memory\": \"128Mi\"\n    }\n  ]\n}\n"
    );
}

#[test]
fn worker_top_yaml() {
    assert_eq!(
        render(|b| print::worker_top::print(&p(OutputFormat::Yaml), b, vec![top_item()]).unwrap()),
        "workers:\n- cpu: 100m\n  memory: 128Mi\n  pod: worker-1\n  pool: pool-1\n  status: ASSIGNED\n"
    );
}

// Policies.

#[test]
fn access_policy_formats() {
    let policy = pb::AccessPolicy {
        metadata: Some(pb::ResourceMetadata {
            name: "default".into(),
            uid: "u-1".into(),
            version: 2,
            ..Default::default()
        }),
        bindings: vec![
            pb::Binding {
                role: "owner".into(),
                members: vec!["user:alice".into(), "user:bob".into()],
            },
            pb::Binding {
                role: "viewer".into(),
                members: vec!["user:carol".into()],
            },
        ],
    };
    for (name, hive, policy, format, wants) in [
        (
            "table",
            "team-a",
            policy.clone(),
            OutputFormat::Table,
            &[
                "SCOPE",
                "team-a   owner    user:alice,user:bob",
                "team-a   viewer   user:carol",
            ][..],
        ),
        (
            "global table",
            "",
            policy.clone(),
            OutputFormat::Table,
            &["<global>   owner"],
        ),
        (
            "no bindings",
            "team-a",
            pb::AccessPolicy::default(),
            OutputFormat::Table,
            &["team-a   <none>   <none>"],
        ),
        (
            "yaml carries preconditions",
            "team-a",
            policy.clone(),
            OutputFormat::Yaml,
            &["uid: u-1", "version: \"2\"", "role: owner"],
        ),
    ] {
        let got = render(|b| print::access_policy::print(&p(format), b, hive, &policy).unwrap());
        for want in wants {
            assert!(got.contains(want), "{name}: {got:?} lacks {want:?}");
        }
    }
}

fn egress_policy() -> pb::EgressPolicy {
    pb::EgressPolicy {
        metadata: Some(pb::ResourceMetadata {
            hive: "team-a".into(),
            name: "default".into(),
            uid: "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d".into(),
            version: 2,
            create_time: ago(5 * MIN),
            ..Default::default()
        }),
        rules: vec![
            pb::EgressRule {
                http: Some(pb::HttpRule {
                    hostnames: vec!["api.example.com".into()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            pb::EgressRule {
                https: Some(pb::HttpsRule {
                    hostnames: vec!["www.example.com".into()],
                    ports: Some(pb::Ports {
                        numbers: vec![8443],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ],
    }
}

#[test]
fn egress_policy_formats() {
    let empty = pb::EgressPolicy {
        metadata: Some(pb::ResourceMetadata {
            hive: "team-a".into(),
            name: "default".into(),
            version: 1,
            create_time: ago(30),
            ..Default::default()
        }),
        rules: vec![],
    };
    for (name, format, actor, policy, want) in [
        (
            "yaml bare document",
            OutputFormat::Yaml,
            "c1",
            egress_policy(),
            "metadata:\n  createTime: \"2026-01-01T11:55:00Z\"\n  hive: team-a\n  name: default\n  uid: 3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d\n  version: \"2\"\nrules:\n- http:\n    hostnames:\n    - api.example.com\n- https:\n    hostnames:\n    - www.example.com\n    ports:\n      numbers:\n      - 8443\n",
        ),
        (
            "json bare document",
            OutputFormat::Json,
            "c1",
            egress_policy(),
            "{\n  \"metadata\": {\n    \"createTime\": \"2026-01-01T11:55:00Z\",\n    \"hive\": \"team-a\",\n    \"name\": \"default\",\n    \"uid\": \"3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d\",\n    \"version\": \"2\"\n  },\n  \"rules\": [\n    {\n      \"http\": {\n        \"hostnames\": [\n          \"api.example.com\"\n        ]\n      }\n    },\n    {\n      \"https\": {\n        \"hostnames\": [\n          \"www.example.com\"\n        ],\n        \"ports\": {\n          \"numbers\": [\n            8443\n          ]\n        }\n      }\n    }\n  ]\n}\n",
        ),
        (
            "table one row",
            OutputFormat::Table,
            "c1",
            egress_policy(),
            "HIVE     ACTOR   RULES   VERSION   AGE\nteam-a   c1      2       2         5m\n",
        ),
        (
            "table no rules",
            OutputFormat::Table,
            "c2",
            empty.clone(),
            "HIVE     ACTOR   RULES   VERSION   AGE\nteam-a   c2      0       1         30s\n",
        ),
    ] {
        let got = render(|b| print::egress_policy::print(&p(format), b, actor, &policy).unwrap());
        assert_eq!(got, want, "{name}");
    }
}
