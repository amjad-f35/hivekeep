//! Manifests the commands read: egress policies, access policies and actor
//! templates, plus the flag checks made alongside them.

use std::time::{Duration, UNIX_EPOCH};

use hivekeep_ctl::cli::{DeleteGuards, GlobalArgs};
use hivekeep_ctl::commands::access_policy::{self, PolicyScope};
use hivekeep_ctl::commands::egress_policy::{
    delete_options, from_manifest, override_metadata, require_preconditions,
};
use hivekeep_ctl::context::Ctx;
use hivekeep_ctl::io::{Io, SharedBuf};
use hivekeep_ctl::manifest;
use hivekeep_ctl::output::{OutputFormat, Printer};
use hivekeep_ctl::print;
use hivekeep_proto::api::v1 as pb;

fn ts(secs: i64) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp {
        seconds: secs,
        nanos: 0,
    })
}

/// 2026-01-01T11:55:00Z and 12:00:00Z.
const T_1155: i64 = 1_767_268_500;
const T_1200: i64 = 1_767_268_800;

fn meta(hive: &str, name: &str) -> Option<pb::ResourceMetadata> {
    Some(pb::ResourceMetadata {
        hive: hive.into(),
        name: name.into(),
        ..Default::default()
    })
}

fn http(hosts: &[&str]) -> pb::EgressRule {
    pb::EgressRule {
        http: Some(pb::HttpRule {
            hostnames: hosts.iter().map(|h| h.to_string()).collect(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn ports(numbers: &[i32]) -> Option<pb::Ports> {
    Some(pb::Ports {
        numbers: numbers.to_vec(),
        all: None,
    })
}

fn tls(hosts: &[&str], numbers: &[i32]) -> pb::EgressRule {
    pb::EgressRule {
        tls_passthrough: Some(pb::TlsPassthroughRule {
            hostnames: hosts.iter().map(|h| h.to_string()).collect(),
            ports: ports(numbers),
        }),
        ..Default::default()
    }
}

#[test]
fn egress_policy_from_manifest() {
    let full = meta("team-a", "default");
    let ok = |m: Option<pb::ResourceMetadata>, rules: Vec<pb::EgressRule>| {
        Ok::<_, Option<&str>>(pb::EgressPolicy { metadata: m, rules })
    };
    let more = Err(Some("manifest holds more than one document"));
    type Want = Result<pb::EgressPolicy, Option<&'static str>>;
    let cases: Vec<(&str, &str, Want)> = vec![
        (
            "camel case http",
            "metadata:\n  hive: team-a\n  name: default\nrules:\n- http:\n    hostnames:\n    - api.example.com\n",
            ok(full.clone(), vec![http(&["api.example.com"])]),
        ),
        (
            "snake case tls_passthrough",
            "metadata: {hive: team-a, name: default}\nrules:\n- tls_passthrough: {hostnames: [\"*.example.com\"], ports: {numbers: [443]}}\n",
            ok(full.clone(), vec![tls(&["*.example.com"], &[443])]),
        ),
        (
            "star pattern",
            "metadata: {hive: team-a, name: default}\nrules:\n- http: {hostnames: [\"*\"]}\n",
            ok(full.clone(), vec![http(&["*"])]),
        ),
        (
            "metadata omitted is left unset",
            "rules:\n- http: {hostnames: [api.example.com]}\n",
            ok(None, vec![http(&["api.example.com"])]),
        ),
        (
            "uid version and timestamps preserved",
            "metadata:\n  hive: team-a\n  name: default\n  uid: 3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d\n  version: \"2\"\n  createTime: \"2026-01-01T11:55:00Z\"\nrules:\n- http: {hostnames: [api.example.com]}\n",
            ok(
                Some(pb::ResourceMetadata {
                    hive: "team-a".into(),
                    name: "default".into(),
                    uid: "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d".into(),
                    version: 2,
                    create_time: ts(T_1155),
                    ..Default::default()
                }),
                vec![http(&["api.example.com"])],
            ),
        ),
        (
            "json input",
            r#"{"metadata": {"hive": "team-a", "name": "default"}, "rules": [{"tls_passthrough": {"hostnames": ["*.example.com"], "ports": {"numbers": [443]}}}]}"#,
            ok(full.clone(), vec![tls(&["*.example.com"], &[443])]),
        ),
        ("empty", "", Err(Some("manifest is empty"))),
        ("unknown field", "rulez: []", Err(Some("invalid EgressPolicy"))),
        ("rules not a list", "rules: {http: {}}", Err(None)),
        (
            "crd shape",
            "apiVersion: hivekeep.io/v1\nkind: EgressPolicy\nmetadata: {name: default}\n",
            Err(None),
        ),
        ("not yaml", "\t{", Err(Some("invalid YAML"))),
        (
            "leading document separator",
            "---\nrules:\n- http: {hostnames: [\"*\"]}\n",
            ok(None, vec![http(&["*"])]),
        ),
        (
            "document end marker",
            "rules:\n- http: {hostnames: [\"*\"]}\n...\n",
            ok(None, vec![http(&["*"])]),
        ),
        (
            "trailing document separator",
            "rules:\n- http: {hostnames: [\"*\"]}\n---\n",
            more.clone(),
        ),
        (
            "empty second document",
            "rules:\n- http: {hostnames: [\"*\"]}\n---\n# nothing here\n",
            more.clone(),
        ),
        (
            "empty first document",
            "---\n# nothing\n---\nrules:\n- http: {hostnames: [\"*\"]}\n",
            more.clone(),
        ),
        (
            "two leading separators",
            "---\n---\nrules:\n- http: {hostnames: [\"*\"]}\n",
            more.clone(),
        ),
        (
            "document end marker then separator",
            "rules:\n- http: {hostnames: [\"*\"]}\n...\n---\n",
            more.clone(),
        ),
        (
            "two egress policies separated by ---",
            "metadata:\n  hive: team-a\n  name: default\nrules:\n- http:\n    hostnames:\n    - api.example.com\n---\nmetadata:\n  hive: team-b\n  name: default\nrules:\n- tls_passthrough:\n    hostnames:\n    - \"*.example.com\"\n    ports:\n      numbers:\n      - 443\n",
            more.clone(),
        ),
        (
            "three egress policies separated by ---",
            "metadata: {hive: team-a, name: default}\nrules:\n- http: {hostnames: [api.example.com]}\n---\nmetadata: {hive: team-b, name: default}\nrules:\n- tls_passthrough: {hostnames: [\"*.example.com\"], ports: {numbers: [443]}}\n---\nmetadata: {hive: team-c, name: default}\nrules:\n- http: {hostnames: [\"*\"]}\n",
            more.clone(),
        ),
        (
            "two policies with an empty document between",
            "metadata: {hive: team-a, name: default}\nrules:\n- http: {hostnames: [api.example.com]}\n---\n# just a comment\n---\nmetadata: {hive: team-b, name: default}\nrules:\n- tls_passthrough: {hostnames: [\"*.example.com\"], ports: {numbers: [443]}}\n",
            more.clone(),
        ),
        (
            "two json documents separated by ---",
            "{\"metadata\": {\"hive\": \"team-a\", \"name\": \"default\"}, \"rules\": [{\"http\": {\"hostnames\": [\"*\"]}}]}\n---\n{\"metadata\": {\"hive\": \"team-b\", \"name\": \"default\"}, \"rules\": [{\"http\": {\"hostnames\": [\"*\"]}}]}\n",
            more.clone(),
        ),
        ("only a separator", "---\n", Err(Some("manifest is empty"))),
        ("comment only", "# nothing\n", Err(Some("manifest is empty"))),
    ];
    for (name, data, want) in cases {
        match (from_manifest(data), want) {
            (Ok(got), Ok(want)) => assert_eq!(got, want, "{name}"),
            (Err(e), Err(want)) => {
                if let Some(w) = want {
                    assert!(e.contains(w), "{name}: {e}");
                }
            }
            (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
        }
    }
}

#[test]
fn override_egress_policy_metadata() {
    let rules = vec![http(&["*"])];
    let filled = pb::EgressPolicy {
        metadata: meta("team-a", "default"),
        rules: rules.clone(),
    };
    let pinned = Some(pb::ResourceMetadata {
        hive: "team-a".into(),
        name: "default".into(),
        uid: "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d".into(),
        version: 2,
        ..Default::default()
    });
    let with = |m: Option<pb::ResourceMetadata>| pb::EgressPolicy {
        metadata: m,
        rules: rules.clone(),
    };
    for (name, policy, want) in [
        ("metadata omitted is filled", with(None), Ok(filled.clone())),
        (
            "empty metadata object is filled",
            with(Some(Default::default())),
            Ok(filled.clone()),
        ),
        (
            "name omitted is filled",
            with(meta("team-a", "")),
            Ok(filled.clone()),
        ),
        (
            "hive omitted is filled",
            with(meta("", "default")),
            Ok(filled.clone()),
        ),
        (
            "matching metadata is kept",
            with(pinned.clone()),
            Ok(with(pinned.clone())),
        ),
        (
            "hive mismatch",
            with(meta("dev", "")),
            Err("metadata.hive \"dev\" does not match --hive \"team-a\""),
        ),
        (
            "name mismatch",
            with(meta("", "other")),
            Err("metadata.name \"other\" must be \"default\""),
        ),
    ] {
        let mut got = policy;
        match (override_metadata(&mut got, "team-a"), want) {
            (Ok(()), Ok(want)) => assert_eq!(got, want, "{name}"),
            (Err(e), Err(want)) => assert!(e.message().contains(want), "{name}: {e}"),
            (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
        }
    }
}

#[test]
fn egress_policy_manifest_round_trip() {
    let policy = pb::EgressPolicy {
        metadata: Some(pb::ResourceMetadata {
            hive: "team-a".into(),
            name: "default".into(),
            uid: "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d".into(),
            version: 2,
            create_time: ts(T_1155),
            update_time: ts(T_1200),
            ..Default::default()
        }),
        rules: vec![
            pb::EgressRule {
                http: Some(pb::HttpRule {
                    hostnames: vec!["*.example.com".into()],
                    ports: ports(&[80, 8080]),
                    ..Default::default()
                }),
                ..Default::default()
            },
            pb::EgressRule {
                https: Some(pb::HttpsRule {
                    hostnames: vec!["api.example.com".into()],
                    effects: Some(pb::HttpRuleEffects {
                        replace_headers: vec![pb::CredentialHeader {
                            header: "authorization".into(),
                            prefix: "Bearer ".into(),
                            credential_uri: "hk-secret://k8s/default/token".into(),
                            ..Default::default()
                        }],
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            pb::EgressRule {
                tls_passthrough: Some(pb::TlsPassthroughRule {
                    hostnames: vec!["*".into()],
                    ports: Some(pb::Ports {
                        all: Some(pb::AllPorts {}),
                        numbers: vec![],
                    }),
                }),
                ..Default::default()
            },
        ],
    };
    let now = UNIX_EPOCH + Duration::from_secs(T_1200 as u64);
    for format in [OutputFormat::Yaml, OutputFormat::Json] {
        let mut buf = SharedBuf::default();
        print::egress_policy::print(&Printer::at(format, now), &mut buf, "c1", &policy).unwrap();
        let text = buf.contents();
        let got = from_manifest(&text).unwrap_or_else(|e| panic!("{format}: {e}\n{text}"));
        assert_eq!(got, policy, "{format}");
    }
}

#[test]
fn require_egress_policy_preconditions() {
    let actor = pb::ObjectRef {
        hive: "team-a".into(),
        name: "c1".into(),
    };
    let want = "manifest for actor \"c1\" in hive \"team-a\" lacks the metadata.uid and metadata.version preconditions that \"get egress-policy -o yaml\" prints";
    for (name, uid, version, fails) in [
        ("uid and version present", "u", 1, false),
        ("uid missing", "", 1, true),
        ("version missing", "u", 0, true),
        ("both missing", "", 0, true),
    ] {
        let policy = pb::EgressPolicy {
            metadata: Some(pb::ResourceMetadata {
                hive: "team-a".into(),
                name: "default".into(),
                uid: uid.into(),
                version,
                ..Default::default()
            }),
            rules: vec![],
        };
        match require_preconditions(&policy, &actor) {
            Ok(()) => assert!(!fails, "{name}"),
            Err(e) => {
                assert!(fails, "{name}");
                assert_eq!(e.message(), want, "{name}");
            }
        }
    }
}

#[test]
fn delete_options_from_flags() {
    let g = |uid: Option<&str>, version: Option<i64>| DeleteGuards {
        uid: uid.map(String::from),
        version,
    };
    let opts = |uid: &str, version: i64| {
        Some(pb::DeleteOptions {
            uid: uid.into(),
            version,
        })
    };
    for (name, guards, want) in [
        ("neither set leaves options unset", g(None, None), Ok(None)),
        ("uid alone", g(Some("u"), None), Ok(opts("u", 0))),
        ("version alone", g(None, Some(3)), Ok(opts("", 3))),
        ("uid and version", g(Some("u"), Some(3)), Ok(opts("u", 3))),
        (
            "empty uid rejected",
            g(Some(""), None),
            Err("--uid must not be empty"),
        ),
        (
            "zero version rejected",
            g(None, Some(0)),
            Err("--version must be at least 1, got 0"),
        ),
        (
            "negative version rejected",
            g(None, Some(-1)),
            Err("--version must be at least 1, got -1"),
        ),
        (
            "empty uid with valid version rejected",
            g(Some(""), Some(3)),
            Err("--uid must not be empty"),
        ),
    ] {
        match (delete_options(&guards), want) {
            (Ok(got), Ok(want)) => assert_eq!(got, want, "{name}"),
            (Err(e), Err(want)) => assert_eq!(e.message(), want, "{name}"),
            (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
        }
    }
}

/// A policy for load-generating Actors that may reach any host on the
/// common web ports.
const OPEN_WEB_EGRESS: &str = r#"# EgressPolicy for traffic-driver Actors in the `loadgen` hive.
metadata:
  hive: loadgen
  name: default
rules:
- http:
    hostnames:
    - "*"
    ports:
      numbers:
      - 80
- https:
    hostnames:
    - "*"
    ports:
      numbers:
      - 443
- tlsPassthrough:
    hostnames:
    - "*"
    ports:
      numbers:
      - 8443
"#;

#[test]
fn egress_policy_from_sample_manifest() {
    let got = from_manifest(OPEN_WEB_EGRESS).unwrap();
    let want = pb::EgressPolicy {
        metadata: meta("loadgen", "default"),
        rules: vec![
            pb::EgressRule {
                http: Some(pb::HttpRule {
                    hostnames: vec!["*".into()],
                    ports: ports(&[80]),
                    ..Default::default()
                }),
                ..Default::default()
            },
            pb::EgressRule {
                https: Some(pb::HttpsRule {
                    hostnames: vec!["*".into()],
                    ports: ports(&[443]),
                    ..Default::default()
                }),
                ..Default::default()
            },
            tls(&["*"], &[8443]),
        ],
    };
    assert_eq!(got, want);
}

// Access policies.

#[test]
fn scope_from_flags() {
    for (name, hive, global, want) in [
        (
            "hive",
            Some("team-a"),
            false,
            Ok(PolicyScope::Hive("team-a".into())),
        ),
        ("global", None, true, Ok(PolicyScope::Global)),
        (
            "neither",
            None,
            false,
            Err("one of --hive or --global is required"),
        ),
        ("both", Some("team-a"), true, Err("mutually exclusive")),
    ] {
        match (PolicyScope::from_flags(hive, global), want) {
            (Ok(got), Ok(want)) => assert_eq!(got, want, "{name}"),
            (Err(e), Err(want)) => {
                assert!(e.is_usage() && e.message().contains(want), "{name}: {e}")
            }
            (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
        }
    }
}

#[test]
fn load_access_policy_manifest() {
    let owner_alice = vec![pb::Binding {
        role: "owner".into(),
        members: vec!["user:alice".into()],
    }];
    for (name, data, want) in [
        (
            "metadata omitted is filled in",
            "bindings:\n- role: owner\n  members: [\"user:alice\"]\n",
            Ok(pb::AccessPolicy {
                metadata: meta("", "default"),
                bindings: owner_alice.clone(),
            }),
        ),
        (
            "preconditions preserved",
            "metadata: {name: default, uid: u-1, version: \"3\"}\nbindings: [{role: owner, members: [\"user:alice\"]}]\n",
            Ok(pb::AccessPolicy {
                metadata: Some(pb::ResourceMetadata {
                    name: "default".into(),
                    uid: "u-1".into(),
                    version: 3,
                    ..Default::default()
                }),
                bindings: owner_alice.clone(),
            }),
        ),
        (
            "no bindings",
            "metadata: {name: default}\n",
            Ok(pb::AccessPolicy {
                metadata: meta("", "default"),
                bindings: vec![],
            }),
        ),
        ("wrong name", "metadata: {name: other}\n", Err("must be \"default\"")),
        (
            "hive set",
            "metadata: {hive: team-a}\n",
            Err("metadata.hive must be empty"),
        ),
        ("unknown field", "bindingz: []\n", Err("invalid AccessPolicy")),
        ("empty", "", Err("manifest is empty")),
    ] {
        match (access_policy::from_manifest(data), want) {
            (Ok(got), Ok(want)) => assert_eq!(got, want, "{name}"),
            (Err(e), Err(want)) => assert!(e.contains(want), "{name}: {e}"),
            (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
        }
    }
}

// Actor templates.

const COUNTER_TEMPLATE: &str = r#"metadata:
  hive: demo-counter
  name: counter
workerSelector:
  matchLabels:
    workload: counter
containers:
- name: counter
  image: registry.example/demos/counter:latest
  command: ["/app/counter", "--extra-port=9090"]
  wakeupProbe:
    httpGet:
      path: /readyz
      port: 80
  volumeMounts:
  - name: data
    mountPath: /home/counter
resources:
  limits:
    cpuMillicores: 1000
    memoryBytes: 536870912
snapshotConfig:
  preferredFidelity: SNAPSHOT_FIDELITY_MEMORY
  storageLocation: gs://hk-snapshots/demo-counter/
volumes:
- name: data
  durableDir: {}
"#;

fn template(data: &str) -> Result<pb::ActorTemplate, String> {
    manifest::parse(data, "ActorTemplate")
}

#[test]
fn actor_template_from_manifest() {
    let got = template(COUNTER_TEMPLATE).unwrap();
    let want = pb::ActorTemplate {
        metadata: meta("demo-counter", "counter"),
        worker_selector: Some(pb::Selector {
            match_labels: [("workload".to_string(), "counter".to_string())].into(),
        }),
        containers: vec![pb::Container {
            name: "counter".into(),
            image: "registry.example/demos/counter:latest".into(),
            command: vec!["/app/counter".into(), "--extra-port=9090".into()],
            wakeup_probe: Some(pb::WakeupProbe {
                http_get: Some(pb::HttpGetAction {
                    path: "/readyz".into(),
                    port: 80,
                }),
                ..Default::default()
            }),
            volume_mounts: vec![pb::VolumeMount {
                name: "data".into(),
                mount_path: "/home/counter".into(),
            }],
            ..Default::default()
        }],
        resources: Some(pb::ResourceRequirements {
            limits: Some(pb::Resources {
                cpu_millicores: 1000,
                memory_bytes: 512 << 20,
            }),
        }),
        snapshot_config: Some(pb::SnapshotConfig {
            preferred_fidelity: pb::SnapshotFidelity::Memory as i32,
            storage_location: "gs://hk-snapshots/demo-counter/".into(),
            ..Default::default()
        }),
        volumes: vec![pb::Volume {
            name: "data".into(),
            durable_dir: Some(pb::DurableDirVolumeSource {}),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_eq!(got, want);
}

#[test]
fn actor_template_from_manifest_snake_case() {
    let got = template(
        "metadata:\n  hive: demo-counter\n  name: counter\nsnapshot_config:\n  storage_location: gs://hk-snapshots/demo-counter/\n",
    )
    .unwrap();
    assert_eq!(
        got.snapshot_config.unwrap().storage_location,
        "gs://hk-snapshots/demo-counter/"
    );
}

#[test]
fn actor_template_from_manifest_errors() {
    for (name, data) in [
        ("empty", ""),
        (
            "unknown field",
            "metadata: {hive: a, name: n}\nsandboxClass: gvisor\n",
        ),
        ("bad enum", "snapshotConfig: {preferredFidelity: memory}\n"),
        (
            "resources limits list",
            "resources: {limits: [{name: cpu, quantity: \"1\"}]}\n",
        ),
        (
            "crd shape",
            "apiVersion: hivekeep.io/v1\nkind: ActorTemplate\nmetadata: {name: counter}\n",
        ),
        ("not yaml", "\t{"),
    ] {
        assert!(template(data).is_err(), "{name}: parsed");
    }
}

/// Sample template manifests as an install script ships them: with a bucket
/// placeholder to substitute and optional placeholder lines to drop.
const SAMPLE_TEMPLATES: [(&str, &str, &str); 2] = [
    (
        r#"metadata:
  hive: demo-counter
  name: counter
workerSelector:
  matchLabels:
    workload: counter
containers:
- name: counter
  image: registry.example/demos/counter:latest
  command:
  - /app/counter
  # A second listener, reachable by CONNECTing to <actor-dns>:9090.
  - --extra-port=9090
  - --tcp-port=9091
${VALIDATE_EXISTING_FILE_PATH_ARG}
  wakeupProbe:
    httpGet:
      path: /readyz
      port: 80
  volumeMounts:
  - name: data
    mountPath: /home/counter
${EXTERNAL_VOLUME_MOUNTS}
resources:
  limits:
    cpuMillicores: 1000
    memoryBytes: 536870912 # 512Mi
snapshotConfig:
  preferredFidelity: SNAPSHOT_FIDELITY_MEMORY
  storageLocation: gs://${BUCKET_NAME}/demo-counter/
volumes:
- name: data
  durableDir: {}
${EXTERNAL_VOLUMES}
"#,
        "demo-counter",
        "counter",
    ),
    (
        r#"metadata:
  hive: demo-counter-microvm
  name: counter-microvm
workerSelector:
  matchLabels:
    workload: counter-microvm
containers:
- name: counter
  image: registry.example/demos/counter:latest
  command:
  - /app/counter
  - --extra-port=9090
  wakeupProbe:
    httpGet:
      path: /readyz
      port: 80
    # A guest boot is slow on a contended node.
    timeoutSeconds: 120
  volumeMounts:
  - name: data
    mountPath: /home/counter
resources:
  limits:
    cpuMillicores: 1000
    memoryBytes: 536870912 # 512Mi
snapshotConfig:
  preferredFidelity: SNAPSHOT_FIDELITY_MEMORY
  storageLocation: gs://${BUCKET_NAME}/demo-counter-microvm/
volumes:
- name: data
  durableDir: {}
"#,
        "demo-counter-microvm",
        "counter-microvm",
    ),
];

#[test]
fn actor_template_from_sample_manifests() {
    for (data, hive, name) in SAMPLE_TEMPLATES {
        let substituted = data.replace("${BUCKET_NAME}", "hk-snapshots");
        let rendered: Vec<&str> = substituted.lines().filter(|l| !l.contains("${")).collect();
        let got = template(&rendered.join("\n")).unwrap_or_else(|e| panic!("{name}: {e}"));
        let m = got.metadata.clone().unwrap();
        assert_eq!((m.hive.as_str(), m.name.as_str()), (hive, name));
        assert!(!got.containers.is_empty(), "{name}");
        assert!(
            !got.snapshot_config
                .unwrap_or_default()
                .storage_location
                .is_empty(),
            "{name}"
        );
    }
}

#[test]
fn read_file_or_stdin() {
    #[derive(clap::Parser)]
    struct G {
        #[command(flatten)]
        g: GlobalArgs,
    }
    use clap::Parser;
    let (io, _, _) = Io::captured_with_stdin(b"metadata: {name: n}".to_vec());
    let mut ctx = Ctx::new(G::parse_from(["x"]).g, io);
    assert_eq!(ctx.read_manifest("-").unwrap(), "metadata: {name: n}");
    assert!(ctx.read_manifest("/does/not/exist.yaml").is_err());
}
