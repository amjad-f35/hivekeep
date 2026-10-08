//! ActorTemplate request and resource validation, including the fields
//! behind preview gates.

mod common;

use common::*;
use hivekeep_core::field::{ErrorKind, ErrorList, Path};
use hivekeep_core::preview::{Gate, PreviewGates};
use hivekeep_core::validation::api::*;
use hivekeep_core::validation::Operation;
use hivekeep_proto::api::v1 as pb;

/// The smallest template that passes create validation. Snapshot settings
/// are explicit because resource validation runs without defaulting.
fn valid_template() -> pb::ActorTemplate {
    pb::ActorTemplate {
        metadata: Some(pb::ResourceMetadata {
            hive: "ns1".into(),
            name: "tmpl-a".into(),
            ..Default::default()
        }),
        containers: vec![container("main", PINNED)],
        snapshot_config: Some(pb::SnapshotConfig {
            storage_location: "gs://my-bucket/snapshots".into(),
            preferred_fidelity: pb::SnapshotFidelity::Memory as i32,
            ..Default::default()
        }),
        resources: Some(limits(1000, 512 << 20)),
        ..Default::default()
    }
}

fn container(name: &str, image: &str) -> pb::Container {
    pb::Container {
        name: name.into(),
        image: image.into(),
        ..Default::default()
    }
}

fn limits(cpu_millicores: i64, memory_bytes: i64) -> pb::ResourceRequirements {
    pb::ResourceRequirements {
        limits: Some(pb::Resources {
            cpu_millicores,
            memory_bytes,
        }),
    }
}

fn t(f: impl FnOnce(&mut pb::ActorTemplate)) -> pb::ActorTemplate {
    with(valid_template(), f)
}

fn c0(f: impl FnOnce(&mut pb::Container)) -> pb::ActorTemplate {
    t(|t| f(&mut t.containers[0]))
}

fn sc(f: impl FnOnce(&mut pb::SnapshotConfig)) -> pb::ActorTemplate {
    t(|t| f(t.snapshot_config.as_mut().unwrap()))
}

fn mount(name: &str, path: &str) -> pb::VolumeMount {
    pb::VolumeMount {
        name: name.into(),
        mount_path: path.into(),
    }
}

fn durable(name: &str) -> pb::Volume {
    pb::Volume {
        name: name.into(),
        durable_dir: Some(pb::DurableDirVolumeSource {}),
        ..Default::default()
    }
}

fn external(capacity_bytes: i64, storage_class_name: &str) -> pb::Volume {
    pb::Volume {
        name: "data".into(),
        external: Some(pb::ExternalVolumeSource {
            template: Some(pb::ExternalVolumeTemplate {
                capacity_bytes,
                storage_class_name: storage_class_name.into(),
            }),
        }),
        ..Default::default()
    }
}

fn image_volume(reference: &str) -> pb::Volume {
    pb::Volume {
        name: "tools".into(),
        image: Some(pb::ImageVolumeSource {
            reference: reference.into(),
        }),
        ..Default::default()
    }
}

fn probe(path: &str, port: i32, timeout_seconds: i32) -> Option<pb::WakeupProbe> {
    Some(pb::WakeupProbe {
        http_get: Some(pb::HttpGetAction {
            path: path.into(),
            port,
        }),
        timeout_seconds,
    })
}

fn caps(add: &[&str], drop: &[&str]) -> Option<pb::SecurityContext> {
    let v = |s: &[&str]| s.iter().map(|x| x.to_string()).collect();
    Some(pb::SecurityContext {
        capabilities: Some(pb::Capabilities {
            add: v(add),
            drop: v(drop),
        }),
    })
}

fn http_rule(host: &str, port: i32) -> pb::EgressRule {
    pb::EgressRule {
        http: Some(pb::HttpRule {
            hostnames: vec![host.into()],
            ports: Some(pb::Ports {
                all: None,
                numbers: vec![port],
            }),
            effects: None,
        }),
        ..Default::default()
    }
}

fn golden(rules: Vec<pb::EgressRule>) -> pb::ActorTemplate {
    t(|t| t.golden_egress_policy = Some(pb::EgressPolicyTemplate { rules }))
}

fn create(op: &Operation, actor_template: Option<pb::ActorTemplate>) -> ErrorList {
    validate_create_actor_template_request(op, &pb::CreateActorTemplateRequest { actor_template })
}

fn gates(gs: &[Gate]) -> Operation {
    let mut g = PreviewGates::none();
    for gate in gs {
        g = g.with(*gate);
    }
    Operation::create().with_gates(g)
}

fn all_gates() -> Operation {
    Operation::create().with_gates(PreviewGates::all())
}

fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

#[test]
fn create_actor_template_request() {
    let a = |s: &str| leak(format!("actor_template.{s}"));
    let tie_rules = vec![
        pb::EgressRule {
            https: Some(pb::HttpsRule {
                hostnames: vec!["api.example.com".into()],
                ports: Some(pb::Ports {
                    all: None,
                    numbers: vec![443],
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        pb::EgressRule {
            tls_passthrough: Some(pb::TlsPassthroughRule {
                hostnames: vec!["api.example.com".into()],
                ports: Some(pb::Ports {
                    all: None,
                    numbers: vec![443],
                }),
            }),
            ..Default::default()
        },
    ];
    let cases: Vec<(&str, Option<pb::ActorTemplate>, Vec<Want>)> = vec![
        ("valid", Some(valid_template()), vec![]),
        (
            "valid golden egress rules",
            Some(golden(vec![http_rule("api.example.com", 80)])),
            vec![],
        ),
        (
            "missing golden egress rules",
            Some(golden(vec![])),
            vec![required(a("golden_egress_policy.rules"))],
        ),
        (
            "invalid golden egress hostname",
            Some(golden(vec![http_rule("bad/host", 80)])),
            vec![invalid(a(
                "golden_egress_policy.rules[0].http.hostnames[0]",
            ))],
        ),
        (
            "golden egress rules tie across protocols",
            Some(golden(tie_rules)),
            vec![invalid(a(
                "golden_egress_policy.rules[1].tls_passthrough.hostnames[0]",
            ))],
        ),
        (
            "too many golden egress rules",
            Some(golden(
                (0..257)
                    .map(|i| http_rule(&format!("host-{i}.example.com"), 80))
                    .collect(),
            )),
            vec![too_many(a("golden_egress_policy.rules"))],
        ),
        (
            "missing actor_template",
            None,
            vec![required("actor_template")],
        ),
        (
            "missing metadata.hive",
            Some(t(|t| t.metadata.as_mut().unwrap().hive.clear())),
            vec![required(a("metadata.hive"))],
        ),
        (
            "invalid metadata.hive",
            Some(t(|t| t.metadata.as_mut().unwrap().hive = "NS_1".into())),
            vec![dns_label(a("metadata.hive"))],
        ),
        (
            "missing metadata.name",
            Some(t(|t| t.metadata.as_mut().unwrap().name.clear())),
            vec![required(a("metadata.name"))],
        ),
        (
            "invalid metadata.name",
            Some(t(|t| t.metadata.as_mut().unwrap().name = "Tmpl_A".into())),
            vec![dns_label(a("metadata.name"))],
        ),
        (
            "valid volumes fidelity",
            Some(sc(|s| {
                s.preferred_fidelity = pb::SnapshotFidelity::Volumes as i32
            })),
            vec![],
        ),
        (
            "valid rootfs fidelity",
            Some(sc(|s| {
                s.preferred_fidelity = pb::SnapshotFidelity::Rootfs as i32
            })),
            vec![],
        ),
        (
            "invalid worker_selector label key",
            Some(t(|t| {
                t.worker_selector = Some(pb::Selector {
                    match_labels: [("bad key".into(), "v".into())].into(),
                })
            })),
            vec![invalid_by(
                a("worker_selector.match_labels"),
                "format=label-key",
            )],
        ),
        (
            "no containers",
            Some(t(|t| t.containers.clear())),
            vec![required(a("containers"))],
        ),
        (
            "container missing name",
            Some(c0(|c| c.name.clear())),
            vec![required(a("containers[0].name"))],
        ),
        (
            "container invalid name",
            Some(c0(|c| c.name = "Main_1".into())),
            vec![dns_label(a("containers[0].name"))],
        ),
        (
            "container missing image",
            Some(c0(|c| c.image.clear())),
            vec![required(a("containers[0].image"))],
        ),
        (
            "volume mount referencing a declared volume",
            Some(t(|t| {
                t.volumes = vec![durable("data")];
                t.containers[0].volume_mounts = vec![mount("data", "/var/data")];
            })),
            vec![],
        ),
        (
            "volume mount referencing an undeclared volume",
            Some(c0(|c| {
                c.volume_mounts = vec![mount("ghost-vol", "/var/data")]
            })),
            vec![invalid(a("containers[0].volume_mounts[0].name"))],
        ),
        (
            "missing snapshot_config is allowed",
            Some(t(|t| t.snapshot_config = None)),
            vec![],
        ),
        (
            "storage_location is not needed when the preview gate is off",
            Some(sc(|s| s.storage_location.clear())),
            vec![],
        ),
        (
            "storage_location without a bucket",
            Some(sc(|s| s.storage_location = "my-bucket/snapshots".into())),
            vec![invalid(a("snapshot_config.storage_location"))],
        ),
        (
            "storage_location with a query",
            Some(sc(|s| {
                s.storage_location = "gs://my-bucket/snapshots?versions=true".into()
            })),
            vec![invalid(a("snapshot_config.storage_location"))],
        ),
        (
            // preferred_fidelity has no default at this layer.
            "preferred_fidelity unset",
            Some(sc(|s| s.preferred_fidelity = 0)),
            vec![required(a("snapshot_config.preferred_fidelity"))],
        ),
        (
            "missing resources",
            Some(t(|t| t.resources = None)),
            vec![required(a("resources"))],
        ),
        (
            "missing cpu",
            Some(t(|t| t.resources = Some(limits(0, 512 << 20)))),
            vec![required(a("resources.limits.cpu_millicores"))],
        ),
        (
            "missing memory",
            Some(t(|t| t.resources = Some(limits(1000, 0)))),
            vec![required(a("resources.limits.memory_bytes"))],
        ),
        (
            "empty resources",
            Some(t(|t| {
                t.resources = Some(pb::ResourceRequirements::default())
            })),
            vec![
                required(a("resources.limits.cpu_millicores")),
                required(a("resources.limits.memory_bytes")),
            ],
        ),
    ];
    for (name, tmpl, want) in cases {
        let gated = tmpl
            .as_ref()
            .is_some_and(|t| t.golden_egress_policy.is_some());
        if gated {
            // Without its gate a golden egress policy is rejected outright;
            // with it (alone or with every gate), its own rules apply.
            assert_errs(
                name,
                create(&Operation::create(), tmpl.clone()),
                &[(
                    ErrorKind::Forbidden,
                    a("golden_egress_policy"),
                    Some("ifDisabled"),
                )],
            );
            assert_errs(
                name,
                create(&gates(&[Gate::GoldenEgressPolicy]), tmpl.clone()),
                &want,
            );
            assert_errs(name, create(&all_gates(), tmpl), &want);
        } else {
            assert_errs(name, create(&Operation::create(), tmpl), &want);
        }
    }
}

#[test]
fn create_actor_template_request_preview_restricted() {
    let tmpl = t(|t| t.volumes = vec![external(10 << 30, "fast-ssd")]);
    assert_errs("allowed", create(&all_gates(), Some(tmpl.clone())), &[]);
    assert_errs(
        "restricted",
        create(&all_gates().with_opt_out(true), Some(tmpl)),
        &[
            (
                ErrorKind::Forbidden,
                "actor_template.volumes[0].external",
                Some("ifDisabled"),
            ),
            forbidden("actor_template.snapshot_config.storage_location"),
        ],
    );
}

#[test]
fn create_actor_template_request_per_template_snapshot_location_opt_in() {
    let on = || gates(&[Gate::PerTemplateSnapshotLocation]);
    let no_location = || sc(|s| s.storage_location.clear());
    let loc = "actor_template.snapshot_config.storage_location";
    let cases: Vec<(&str, Operation, pb::ActorTemplate, Vec<Want>)> = vec![
        (
            "gate off, opted out, location set",
            Operation::create().with_opt_out(true),
            valid_template(),
            vec![],
        ),
        (
            "gate off, opted out, no location",
            Operation::create().with_opt_out(true),
            no_location(),
            vec![],
        ),
        (
            "gate on, opted out, location set",
            on().with_opt_out(true),
            valid_template(),
            vec![forbidden(loc)],
        ),
        (
            "gate on, opted out, no location",
            on().with_opt_out(true),
            no_location(),
            vec![],
        ),
        (
            "gate on, opted in, location set",
            on(),
            valid_template(),
            vec![],
        ),
        (
            "gate on, opted in, no location",
            on(),
            no_location(),
            vec![],
        ),
    ];
    for (name, op, tmpl, want) in cases {
        assert_errs(name, create(&op, Some(tmpl)), &want);
    }
}

fn r(hive: &str, name: &str) -> Option<pb::ObjectRef> {
    Some(pb::ObjectRef {
        hive: hive.into(),
        name: name.into(),
    })
}

/// The ref cases shared by Get and Delete.
fn ref_cases(f: impl Fn(Option<pb::ObjectRef>) -> ErrorList) {
    let cases: Vec<(&str, Option<pb::ObjectRef>, Vec<Want>)> = vec![
        ("valid", r("ns1", "tmpl-a"), vec![]),
        (
            "missing actor_template",
            None,
            vec![required("actor_template")],
        ),
        (
            "missing hive",
            r("", "tmpl-a"),
            vec![required("actor_template.hive")],
        ),
        (
            "missing name",
            r("ns1", ""),
            vec![required("actor_template.name")],
        ),
    ];
    for (name, req, want) in cases {
        assert_errs(name, f(req), &want);
    }
}

#[test]
fn get_actor_template_request() {
    ref_cases(|actor_template| {
        validate_get_actor_template_request(
            &Operation::create(),
            &pb::GetActorTemplateRequest { actor_template },
        )
    });
}

#[test]
fn list_actor_templates_request() {
    let c = |hive: &str, options: Option<pb::ListOptions>| {
        validate_list_actor_templates_request(
            &Operation::create(),
            &pb::ListActorTemplatesRequest {
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
    assert_errs("valid", c("", size(10)), &[]);
    assert_errs("zero page size", c("", None), &[]);
    assert_errs("valid hive filter", c("ns1", None), &[]);
    assert_errs("invalid hive filter", c("NS_1", None), &[dns_label("hive")]);
    assert_errs(
        "negative page size",
        c("", size(-1)),
        &[invalid_by("options.page_size", "minimum")],
    );
}

#[test]
fn delete_actor_template_request() {
    ref_cases(|actor_template| {
        validate_delete_actor_template_request(
            &Operation::create(),
            &pb::DeleteActorTemplateRequest {
                actor_template,
                options: None,
            },
        )
    });
}

/// Resource validation of a template, without and with every preview gate.
/// `with_preview` is `None` when the gates make no difference.
fn template_case(
    name: &str,
    tmpl: pb::ActorTemplate,
    want: Vec<Want>,
    with_preview: Option<Vec<Want>>,
) {
    assert_errs(
        name,
        actor_template(&Operation::create(), &Path::root(), &tmpl, None),
        &want,
    );
    let gated = with_preview.unwrap_or(want);
    assert_errs(
        &format!("{name} (preview=ALL)"),
        actor_template(&all_gates(), &Path::root(), &tmpl, None),
        &gated,
    );
}

/// `(name, template, errors, errors with every preview gate)`.
type TemplateCase = (
    &'static str,
    pb::ActorTemplate,
    Vec<Want>,
    Option<Vec<Want>>,
);

#[test]
fn actor_template_resource() {
    let p = |s: &str| leak(s.to_string());
    let c0p = |s: &str| leak(format!("containers[0].{s}"));
    let gmode =
        |mode: i32| sc(|s| s.golden_snapshot_config = Some(pb::GoldenSnapshotConfig { mode }));
    let other = |name: &str| {
        container(
            name,
            "example.com/other:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
    };
    let digest = "@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let pinned_ref = format!("example.com/app{digest}");
    let ext_forbidden = (
        ErrorKind::Forbidden,
        "volumes[0].external",
        Some("ifDisabled"),
    );
    let mut cases: Vec<TemplateCase> = vec![
        ("valid", valid_template(), vec![], None),
        (
            "golden snapshot enabled without wakeup probe",
            gmode(pb::GoldenSnapshotMode::Enabled as i32),
            vec![],
            None,
        ),
        (
            "golden snapshot disabled",
            gmode(pb::GoldenSnapshotMode::Disabled as i32),
            vec![],
            None,
        ),
        (
            "golden snapshot mode unspecified",
            gmode(0),
            vec![required("snapshot_config.golden_snapshot_config.mode")],
            None,
        ),
        (
            "golden snapshot mode negative",
            gmode(-1),
            vec![invalid_by(
                "snapshot_config.golden_snapshot_config.mode",
                "minimum",
            )],
            None,
        ),
        (
            "golden snapshot mode unknown",
            gmode(3),
            vec![invalid_by(
                "snapshot_config.golden_snapshot_config.mode",
                "maximum",
            )],
            None,
        ),
        (
            "missing metadata",
            t(|t| t.metadata = None),
            vec![required("metadata")],
            None,
        ),
        (
            "missing metadata.hive",
            t(|t| t.metadata.as_mut().unwrap().hive.clear()),
            vec![required("metadata.hive")],
            None,
        ),
        (
            "invalid metadata.hive",
            t(|t| t.metadata.as_mut().unwrap().hive = "NS1".into()),
            vec![dns_label("metadata.hive")],
            None,
        ),
        (
            "invalid metadata.name",
            t(|t| t.metadata.as_mut().unwrap().name = "TMPL A".into()),
            vec![dns_label("metadata.name")],
            None,
        ),
        (
            "worker_selector with an invalid label value",
            t(|t| {
                t.worker_selector = Some(pb::Selector {
                    match_labels: [("tier".into(), "Not Valid".into())].into(),
                })
            }),
            vec![invalid_by(
                "worker_selector.match_labels[tier]",
                "format=label-value",
            )],
            None,
        ),
        (
            "missing snapshot_config is allowed",
            t(|t| t.snapshot_config = None),
            vec![],
            None,
        ),
        (
            "storage_location too long",
            sc(|s| s.storage_location = format!("gs://{}", "x".repeat(1020))),
            vec![too_long("snapshot_config.storage_location")],
            None,
        ),
        (
            // Optional with or without the gate: an empty field falls back
            // to the server's default location.
            "missing storage_location",
            sc(|s| s.storage_location.clear()),
            vec![],
            None,
        ),
        (
            "unspecified preferred_fidelity",
            sc(|s| s.preferred_fidelity = 0),
            vec![required("snapshot_config.preferred_fidelity")],
            None,
        ),
        (
            "preferred_fidelity outside the enum",
            sc(|s| s.preferred_fidelity = 99),
            vec![invalid_by("snapshot_config.preferred_fidelity", "maximum")],
            None,
        ),
        (
            "negative preferred_fidelity",
            sc(|s| s.preferred_fidelity = -1),
            vec![invalid_by("snapshot_config.preferred_fidelity", "minimum")],
            None,
        ),
        (
            "no containers",
            t(|t| t.containers.clear()),
            vec![required("containers")],
            None,
        ),
        (
            "too many containers",
            t(|t| {
                for i in 0..10 {
                    t.containers.push(container(&format!("c-{i}"), PINNED));
                }
            }),
            vec![too_many("containers")],
            None,
        ),
        (
            "duplicate container name",
            t(|t| t.containers.push(other("main"))),
            vec![duplicate("containers[1]")],
            None,
        ),
        (
            "duplicate env name",
            c0(|c| {
                c.env = vec![
                    pb::EnvVar {
                        name: "PORT".into(),
                        value: "1".into(),
                    },
                    pb::EnvVar {
                        name: "PORT".into(),
                        value: "2".into(),
                    },
                ]
            }),
            vec![duplicate("containers[0].env[1]")],
            None,
        ),
        (
            "the same volume mounted at two paths is allowed",
            c0(|c| c.volume_mounts = vec![mount("data", "/var/data"), mount("data", "/mnt/data")]),
            vec![],
            None,
        ),
        (
            "two volumes at the same path are rejected",
            c0(|c| c.volume_mounts = vec![mount("data", "/var/data"), mount("other", "/var/data")]),
            vec![duplicate("containers[0].volume_mounts[1]")],
            None,
        ),
        (
            "nested mount paths are rejected",
            c0(|c| c.volume_mounts = vec![mount("data", "/data"), mount("config", "/data/config")]),
            vec![invalid("containers[0].volume_mounts[1].mount_path")],
            None,
        ),
        (
            "the same path in different containers is allowed",
            t(|t| {
                t.containers.push(other("sidecar"));
                t.containers[0].volume_mounts = vec![mount("data", "/var/data")];
                t.containers[1].volume_mounts = vec![mount("data", "/var/data")];
            }),
            vec![],
            None,
        ),
        (
            "duplicate volume name",
            t(|t| t.volumes = vec![durable("scratch"), durable("scratch")]),
            vec![duplicate("volumes[1]")],
            None,
        ),
        (
            "too many command entries",
            c0(|c| c.command = vec![String::new(); 65]),
            vec![too_many("containers[0].command")],
            None,
        ),
        (
            "command entry too long",
            c0(|c| c.command = vec!["x".repeat(4097)]),
            vec![too_long("containers[0].command[0]")],
            None,
        ),
        (
            // argv lists are atomic, not sets: repeats are legitimate.
            "valid command and args",
            c0(|c| {
                c.command = vec!["/bin/app".into()];
                c.args = vec!["--serve".into(), "-v".into(), "-v".into()];
            }),
            vec![],
            None,
        ),
        (
            "too many args entries",
            c0(|c| c.args = vec![String::new(); 65]),
            vec![too_many("containers[0].args")],
            None,
        ),
        (
            "args entry too long",
            c0(|c| c.args = vec!["x".repeat(4097)]),
            vec![too_long("containers[0].args[0]")],
            None,
        ),
        (
            "too many volume_mounts",
            c0(|c| {
                c.volume_mounts = (0..33)
                    .map(|i| mount("data", &format!("/mnt/p{i}")))
                    .collect()
            }),
            vec![too_many("containers[0].volume_mounts")],
            None,
        ),
        (
            "too many env entries",
            c0(|c| {
                c.env = (0..33)
                    .map(|i| pb::EnvVar {
                        name: format!("VAR_{i}"),
                        ..Default::default()
                    })
                    .collect()
            }),
            vec![too_many("containers[0].env")],
            None,
        ),
        (
            "image too long",
            c0(|c| c.image = format!("{}{digest}", "x".repeat(513))),
            vec![
                invalid("containers[0].image"),
                too_long("containers[0].image"),
            ],
            None,
        ),
        (
            "valid image: pinned by digest",
            c0(|c| c.image = pinned_ref.clone()),
            vec![],
            None,
        ),
        (
            "container image missing digest",
            c0(|c| c.image = "example.com/app:v1".into()),
            vec![invalid("containers[0].image")],
            None,
        ),
        (
            "container missing name",
            c0(|c| c.name.clear()),
            vec![required("containers[0].name")],
            None,
        ),
        (
            "container invalid name",
            c0(|c| c.name = "Main_1".into()),
            vec![dns_label("containers[0].name")],
            None,
        ),
        (
            "container missing image",
            c0(|c| c.image.clear()),
            vec![required("containers[0].image")],
            None,
        ),
        (
            "valid env",
            c0(|c| {
                c.env = vec![
                    pb::EnvVar {
                        name: "PORT".into(),
                        value: "8080".into(),
                    },
                    pb::EnvVar {
                        name: "DEBUG".into(),
                        ..Default::default()
                    },
                ]
            }),
            vec![],
            None,
        ),
        (
            "env name with an equals sign",
            c0(|c| {
                c.env = vec![pb::EnvVar {
                    name: "FOO=BAR".into(),
                    ..Default::default()
                }]
            }),
            vec![invalid("containers[0].env[0].name")],
            None,
        ),
        (
            "env missing name",
            c0(|c| {
                c.env = vec![pb::EnvVar {
                    value: "8080".into(),
                    ..Default::default()
                }]
            }),
            vec![required("containers[0].env[0].name")],
            None,
        ),
        (
            "valid security_context capabilities",
            c0(|c| c.security_context = caps(&["NET_BIND_SERVICE"], &["ALL"])),
            vec![],
            None,
        ),
        (
            "capabilities add rejects ALL",
            c0(|c| c.security_context = caps(&["ALL"], &[])),
            vec![invalid(
                "containers[0].security_context.capabilities.add[0]",
            )],
            None,
        ),
        (
            "lowercase capability",
            c0(|c| c.security_context = caps(&[], &["net_raw"])),
            vec![invalid(
                "containers[0].security_context.capabilities.drop[0]",
            )],
            None,
        ),
        (
            "duplicate capability",
            c0(|c| c.security_context = caps(&["NET_BIND_SERVICE", "NET_BIND_SERVICE"], &[])),
            vec![duplicate(
                "containers[0].security_context.capabilities.add[1]",
            )],
            None,
        ),
        (
            "too many capabilities",
            c0(|c| {
                let names: Vec<String> = (0..65).map(|i| format!("CAP{i}")).collect();
                let refs: Vec<&str> = names.iter().map(String::as_str).collect();
                c.security_context = caps(&refs, &[]);
            }),
            vec![too_many("containers[0].security_context.capabilities.add")],
            None,
        ),
        (
            "valid wakeup probe",
            c0(|c| c.wakeup_probe = probe("/healthz", 8080, 60)),
            vec![],
            None,
        ),
        (
            "wakeup probe missing http_get",
            c0(|c| {
                c.wakeup_probe = Some(pb::WakeupProbe {
                    http_get: None,
                    timeout_seconds: 60,
                })
            }),
            vec![required("containers[0].wakeup_probe.http_get")],
            None,
        ),
        (
            "missing wakeup probe timeout_seconds",
            c0(|c| c.wakeup_probe = probe("/healthz", 8080, 0)),
            vec![required("containers[0].wakeup_probe.timeout_seconds")],
            None,
        ),
        (
            "missing wakeup probe http_get.path",
            c0(|c| c.wakeup_probe = probe("", 8080, 60)),
            vec![required("containers[0].wakeup_probe.http_get.path")],
            None,
        ),
        (
            "wakeup probe timeout_seconds out of range",
            c0(|c| c.wakeup_probe = probe("/healthz", 8080, 3601)),
            vec![invalid_by(
                "containers[0].wakeup_probe.timeout_seconds",
                "maximum",
            )],
            None,
        ),
        (
            "negative wakeup probe timeout_seconds",
            c0(|c| c.wakeup_probe = probe("/healthz", 8080, -1)),
            vec![invalid_by(
                "containers[0].wakeup_probe.timeout_seconds",
                "minimum",
            )],
            None,
        ),
        (
            "wakeup probe missing port",
            c0(|c| c.wakeup_probe = probe("/healthz", 0, 60)),
            vec![required("containers[0].wakeup_probe.http_get.port")],
            None,
        ),
        (
            "negative wakeup probe port",
            c0(|c| c.wakeup_probe = probe("/healthz", -1, 60)),
            vec![invalid_by(
                "containers[0].wakeup_probe.http_get.port",
                "minimum",
            )],
            None,
        ),
        (
            "wakeup probe port out of range",
            c0(|c| c.wakeup_probe = probe("/healthz", 65536, 60)),
            vec![invalid_by(
                "containers[0].wakeup_probe.http_get.port",
                "maximum",
            )],
            None,
        ),
        (
            "wakeup probe path with query string",
            c0(|c| c.wakeup_probe = probe("/readyz?verbose=1", 8080, 60)),
            vec![invalid("containers[0].wakeup_probe.http_get.path")],
            None,
        ),
        (
            "wakeup probe path not starting with slash",
            c0(|c| c.wakeup_probe = probe("readyz", 8080, 60)),
            vec![invalid("containers[0].wakeup_probe.http_get.path")],
            None,
        ),
        (
            "valid volume_mount",
            c0(|c| c.volume_mounts = vec![mount("data", "/var/data")]),
            vec![],
            None,
        ),
        (
            "volume_mount missing name",
            c0(|c| c.volume_mounts = vec![mount("", "/var/data")]),
            vec![required("containers[0].volume_mounts[0].name")],
            None,
        ),
        (
            "volume_mount invalid name",
            c0(|c| c.volume_mounts = vec![mount("Data_1", "/var/data")]),
            vec![dns_label("containers[0].volume_mounts[0].name")],
            None,
        ),
        (
            "volume_mount missing mount_path",
            c0(|c| c.volume_mounts = vec![mount("data", "")]),
            vec![required("containers[0].volume_mounts[0].mount_path")],
            None,
        ),
        (
            "relative mount_path",
            c0(|c| c.volume_mounts = vec![mount("data", "var/data")]),
            vec![invalid("containers[0].volume_mounts[0].mount_path")],
            None,
        ),
        (
            "valid durable_dir volume",
            t(|t| t.volumes = vec![durable("scratch")]),
            vec![],
            None,
        ),
        (
            "too many volumes",
            t(|t| t.volumes = (0..33).map(|i| durable(&format!("vol-{i}"))).collect()),
            vec![too_many("volumes")],
            None,
        ),
        (
            "volume missing name",
            t(|t| t.volumes = vec![durable("")]),
            vec![required("volumes[0].name")],
            None,
        ),
        (
            "volume invalid name",
            t(|t| t.volumes = vec![durable("Scratch_1")]),
            vec![dns_label("volumes[0].name")],
            None,
        ),
        (
            "volume with no source",
            t(|t| {
                t.volumes = vec![pb::Volume {
                    name: "scratch".into(),
                    ..Default::default()
                }]
            }),
            vec![invalid_by("volumes[0]", "union")],
            None,
        ),
        (
            "volume with two sources",
            t(|t| {
                t.volumes = vec![with(durable("scratch"), |v| {
                    v.image = Some(pb::ImageVolumeSource {
                        reference: pinned_ref.clone(),
                    })
                })]
            }),
            vec![invalid_by("volumes[0]", "union")],
            None,
        ),
        (
            "valid image volume",
            t(|t| t.volumes = vec![image_volume(&pinned_ref)]),
            vec![],
            None,
        ),
        (
            "image volume missing reference",
            t(|t| t.volumes = vec![image_volume("")]),
            vec![required("volumes[0].image.reference")],
            None,
        ),
        (
            "image volume reference missing digest",
            t(|t| t.volumes = vec![image_volume("example.com/app:v1")]),
            vec![invalid("volumes[0].image.reference")],
            None,
        ),
        (
            "valid external volume template",
            t(|t| t.volumes = vec![external(10 << 30, "fast-ssd")]),
            vec![ext_forbidden],
            Some(vec![]),
        ),
        (
            "external volume template missing capacity",
            t(|t| t.volumes = vec![external(0, "fast-ssd")]),
            vec![ext_forbidden],
            Some(vec![required(
                "volumes[0].external.template.capacity_bytes",
            )]),
        ),
        (
            "external volume template negative capacity",
            t(|t| t.volumes = vec![external(-1, "fast-ssd")]),
            vec![ext_forbidden],
            Some(vec![invalid_by(
                "volumes[0].external.template.capacity_bytes",
                "minimum",
            )]),
        ),
        (
            "external volume template missing storage_class_name",
            t(|t| t.volumes = vec![external(10 << 30, "")]),
            vec![ext_forbidden],
            Some(vec![required(
                "volumes[0].external.template.storage_class_name",
            )]),
        ),
        (
            "external volume template invalid storage_class_name",
            t(|t| t.volumes = vec![external(10 << 30, "Fast SSD")]),
            vec![ext_forbidden],
            Some(vec![invalid_by(
                "volumes[0].external.template.storage_class_name",
                "format=dns-subdomain",
            )]),
        ),
        (
            "valid resources",
            c0(|c| c.resources = Some(limits(500, 2 << 30))),
            vec![],
            None,
        ),
        (
            "negative cpu",
            c0(|c| c.resources = Some(limits(-1, 0))),
            vec![invalid_by(
                c0p("resources.limits.cpu_millicores"),
                "minimum",
            )],
            None,
        ),
        (
            "negative memory",
            c0(|c| c.resources = Some(limits(0, -1))),
            vec![invalid_by(c0p("resources.limits.memory_bytes"), "minimum")],
            None,
        ),
        (
            "cpu above maximum",
            c0(|c| c.resources = Some(limits(1_000_000, 0))),
            vec![invalid_by(
                c0p("resources.limits.cpu_millicores"),
                "maximum",
            )],
            None,
        ),
        (
            "template-level resources validated too",
            t(|t| t.resources = Some(limits(1000, -1))),
            vec![invalid_by(p("resources.limits.memory_bytes"), "minimum")],
            None,
        ),
    ];
    for (name, tmpl, want, with_preview) in cases.drain(..) {
        template_case(name, tmpl, want, with_preview);
    }
}

#[test]
fn actor_template_update_ratchets_unchanged_containers() {
    let mut old = valid_template();
    old.containers[0].image = "example.com/legacy:v1".into();
    let mut new = old.clone();
    new.worker_selector = Some(pb::Selector {
        match_labels: [("tier".into(), "1".into())].into(),
    });
    assert_errs(
        "unchanged container",
        validate_actor_template_update(&Operation::update(), &Path::root(), &new, &old),
        &[],
    );
    // Ratcheting is per field: other changes to the container keep the
    // unchanged image exempt, a new image is checked.
    new.containers[0].args = vec!["--x".into()];
    assert_errs(
        "changed args",
        validate_actor_template_update(&Operation::update(), &Path::root(), &new, &old),
        &[],
    );
    new.containers[0].image = "example.com/legacy:v2".into();
    assert_errs(
        "changed image",
        validate_actor_template_update(&Operation::update(), &Path::root(), &new, &old),
        &[invalid("containers[0].image")],
    );
}
