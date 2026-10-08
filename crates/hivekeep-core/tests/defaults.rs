//! Resource defaults: each resource's defaulted form, and idempotency.

use hivekeep_core::defaults::{apply, Defaults};
use hivekeep_proto::api::v1 as pb;
use prost::Message;

const FULL: i32 = pb::SnapshotFidelity::Memory as i32;
const DATA: i32 = pb::SnapshotFidelity::Volumes as i32;

fn golden(mode: pb::GoldenSnapshotMode) -> Option<pb::GoldenSnapshotConfig> {
    Some(pb::GoldenSnapshotConfig { mode: mode as i32 })
}

/// What an omitted snapshot_config defaults to.
fn default_snapshot_config() -> Option<pb::SnapshotConfig> {
    Some(pb::SnapshotConfig {
        preferred_fidelity: FULL,
        golden_snapshot_config: golden(pb::GoldenSnapshotMode::Enabled),
        ..Default::default()
    })
}

fn container(name: &str, probe: Option<pb::WakeupProbe>) -> pb::Container {
    pb::Container {
        name: name.into(),
        wakeup_probe: probe,
        ..Default::default()
    }
}

fn probe(port: Option<(i32, &str)>, timeout_seconds: i32) -> Option<pb::WakeupProbe> {
    Some(pb::WakeupProbe {
        http_get: port.map(|(port, path)| pb::HttpGetAction {
            port,
            path: path.into(),
        }),
        timeout_seconds,
    })
}

fn ports(n: &[i32]) -> Option<pb::Ports> {
    Some(pb::Ports {
        all: None,
        numbers: n.to_vec(),
    })
}

fn all_ports() -> Option<pb::Ports> {
    Some(pb::Ports {
        all: Some(pb::AllPorts {}),
        numbers: vec![],
    })
}

fn http(host: &str, p: Option<pb::Ports>) -> pb::EgressRule {
    pb::EgressRule {
        http: Some(pb::HttpRule {
            hostnames: vec![host.into()],
            ports: p,
            effects: None,
        }),
        ..Default::default()
    }
}

fn https(host: &str, p: Option<pb::Ports>) -> pb::EgressRule {
    pb::EgressRule {
        https: Some(pb::HttpsRule {
            hostnames: vec![host.into()],
            ports: p,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn passthrough(host: &str) -> pb::EgressRule {
    pb::EgressRule {
        tls_passthrough: Some(pb::TlsPassthroughRule {
            hostnames: vec![host.into()],
            ports: None,
        }),
        ..Default::default()
    }
}

/// Defaults `input`, compares it with `want`, and checks that defaulting
/// again changes nothing.
#[track_caller]
fn check<T: Defaults + Clone + PartialEq + std::fmt::Debug>(name: &str, input: T, want: T) {
    let mut got = input;
    apply(&mut got);
    assert_eq!(got, want, "{name}: defaulted form");
    let mut again = got.clone();
    apply(&mut again);
    assert_eq!(again, got, "{name}: not idempotent");
}

#[test]
fn apply_defaults() {
    let t = |f: fn(&mut pb::ActorTemplate)| {
        let mut t = pb::ActorTemplate::default();
        f(&mut t);
        t
    };
    check(
        "missing snapshot_config gets every default",
        pb::ActorTemplate::default(),
        t(|t| t.snapshot_config = default_snapshot_config()),
    );
    check(
        "empty snapshot_config gets every default",
        t(|t| t.snapshot_config = Some(pb::SnapshotConfig::default())),
        t(|t| t.snapshot_config = default_snapshot_config()),
    );
    check(
        "set fidelity is kept",
        t(|t| {
            t.snapshot_config = Some(pb::SnapshotConfig {
                preferred_fidelity: DATA,
                ..Default::default()
            })
        }),
        t(|t| {
            t.snapshot_config = Some(pb::SnapshotConfig {
                preferred_fidelity: DATA,
                golden_snapshot_config: golden(pb::GoldenSnapshotMode::Enabled),
                ..Default::default()
            })
        }),
    );
    check(
        "set snapshot_config fields are kept",
        t(|t| {
            t.snapshot_config = Some(pb::SnapshotConfig {
                preferred_fidelity: DATA,
                golden_snapshot_config: golden(pb::GoldenSnapshotMode::Disabled),
                storage_location: "gs://b/r".into(),
            })
        }),
        t(|t| {
            t.snapshot_config = Some(pb::SnapshotConfig {
                preferred_fidelity: DATA,
                golden_snapshot_config: golden(pb::GoldenSnapshotMode::Disabled),
                storage_location: "gs://b/r".into(),
            })
        }),
    );
    check(
        "container without wakeup probe stays without one",
        t(|t| t.containers = vec![container("main", None)]),
        t(|t| {
            t.snapshot_config = default_snapshot_config();
            t.containers = vec![container("main", None)];
        }),
    );
    check(
        "wakeup probe gets timeout and path",
        t(|t| t.containers = vec![container("main", probe(Some((8080, "")), 0))]),
        t(|t| {
            t.snapshot_config = default_snapshot_config();
            t.containers = vec![container("main", probe(Some((8080, "/")), 30))];
        }),
    );
    check(
        "wakeup probe without http_get gets only the timeout",
        t(|t| t.containers = vec![container("main", probe(None, 0))]),
        t(|t| {
            t.snapshot_config = default_snapshot_config();
            t.containers = vec![container("main", probe(None, 30))];
        }),
    );
    check(
        "set wakeup probe fields are kept",
        t(|t| t.containers = vec![container("main", probe(Some((8080, "/healthz")), 5))]),
        t(|t| {
            t.snapshot_config = default_snapshot_config();
            t.containers = vec![container("main", probe(Some((8080, "/healthz")), 5))];
        }),
    );
    check(
        "every container is defaulted",
        t(|t| {
            t.containers = vec![
                container("a", probe(Some((1, "")), 0)),
                container("b", None),
                container("c", probe(Some((3, "")), 0)),
            ]
        }),
        t(|t| {
            t.snapshot_config = default_snapshot_config();
            t.containers = vec![
                container("a", probe(Some((1, "/")), 30)),
                container("b", None),
                container("c", probe(Some((3, "/")), 30)),
            ];
        }),
    );
    let actor = pb::Actor {
        worker_selector: Some(pb::Selector {
            match_labels: [("tier".to_string(), "1".to_string())].into(),
        }),
        ..Default::default()
    };
    check("actor has no defaults", actor.clone(), actor);
    let hive = pb::Hive {
        metadata: Some(pb::ResourceMetadata {
            name: "team-a".into(),
            ..Default::default()
        }),
    };
    check("hive has no defaults", hive.clone(), hive);
    check(
        "egress policy defaults http and https ports only",
        pb::EgressPolicy {
            rules: vec![
                http("a.example.com", None),
                https("b.example.com", None),
                http("c.example.com", all_ports()),
                passthrough("d.example.com"),
                pb::EgressRule::default(),
            ],
            ..Default::default()
        },
        pb::EgressPolicy {
            rules: vec![
                http("a.example.com", ports(&[80])),
                https("b.example.com", ports(&[443])),
                http("c.example.com", all_ports()),
                passthrough("d.example.com"),
                pb::EgressRule::default(),
            ],
            ..Default::default()
        },
    );
    check(
        "template defaults egress ports",
        t(|t| {
            t.golden_egress_policy = Some(pb::EgressPolicyTemplate {
                rules: vec![
                    http("api.example.com", None),
                    https("api.example.com", None),
                    https("custom.example.com", ports(&[8443])),
                    passthrough("tcp.example.com"),
                ],
            })
        }),
        t(|t| {
            t.snapshot_config = default_snapshot_config();
            t.golden_egress_policy = Some(pb::EgressPolicyTemplate {
                rules: vec![
                    http("api.example.com", ports(&[80])),
                    https("api.example.com", ports(&[443])),
                    https("custom.example.com", ports(&[8443])),
                    passthrough("tcp.example.com"),
                ],
            });
        }),
    );
    let tag = pb::Tag {
        scope: pb::TagScope::Published as i32,
        ..Default::default()
    };
    check("tag has no defaults", tag.clone(), tag);
    let worker = pb::Worker {
        metadata: Some(pb::ResourceMetadata {
            name: "worker-1".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    check("worker has no defaults", worker.clone(), worker);
    let policy = pb::AccessPolicy {
        metadata: Some(pb::ResourceMetadata {
            name: "default".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    check("access policy has no defaults", policy.clone(), policy);
}

#[test]
fn golden_snapshot_defaults() {
    // The mode is an enum value on the wire, not a presence bit, so an
    // unspecified (zero) mode and an omitted one decode alike.
    let mode = |m: i32| {
        Some(pb::SnapshotConfig {
            golden_snapshot_config: Some(pb::GoldenSnapshotConfig { mode: m }),
            ..Default::default()
        })
    };
    let cases: Vec<(&str, Option<pb::SnapshotConfig>, pb::GoldenSnapshotMode)> = vec![
        (
            "omitted config",
            Some(pb::SnapshotConfig::default()),
            pb::GoldenSnapshotMode::Enabled,
        ),
        ("omitted mode", mode(0), pb::GoldenSnapshotMode::Enabled),
        (
            "unspecified mode",
            mode(pb::GoldenSnapshotMode::Unspecified as i32),
            pb::GoldenSnapshotMode::Enabled,
        ),
        (
            "enabled",
            mode(pb::GoldenSnapshotMode::Enabled as i32),
            pb::GoldenSnapshotMode::Enabled,
        ),
        (
            "disabled",
            mode(pb::GoldenSnapshotMode::Disabled as i32),
            pb::GoldenSnapshotMode::Disabled,
        ),
    ];
    for (name, sc, want) in cases {
        let sent = pb::ActorTemplate {
            snapshot_config: sc,
            ..Default::default()
        };
        let mut tmpl = pb::ActorTemplate::decode(sent.encode_to_vec().as_slice()).unwrap();
        apply(&mut tmpl);
        assert_eq!(
            tmpl.snapshot_config
                .as_ref()
                .and_then(|c| c.golden_snapshot_config.as_ref()),
            Some(&pb::GoldenSnapshotConfig { mode: want as i32 }),
            "{name}"
        );
        let mut again = tmpl.clone();
        apply(&mut again);
        assert_eq!(again, tmpl, "{name}: not idempotent");
    }
}
