//! Keeper API validation.

mod common;

use common::*;
use hivekeep_core::field::{ErrorKind, ErrorList, Path};
use hivekeep_core::validation::keeper::*;
use hivekeep_core::validation::Operation;
use hivekeep_proto::keeper::v1 as pb;

const URI: &str = "gs://bucket/root/hives/ns1/actors/actor-uid/snapshots/snap-1";

fn spec() -> pb::WorkloadSpec {
    pb::WorkloadSpec {
        containers: vec![pb::Container {
            name: "main".into(),
            image: PINNED.into(),
            volume_mounts: vec![pb::VolumeMount {
                name: "data".into(),
                mount_path: "/data".into(),
            }],
            ..Default::default()
        }],
        volumes: vec![pb::Volume {
            name: "data".into(),
            durable_dir: Some(pb::DurableDirVolumeSource {}),
            ..Default::default()
        }],
    }
}

fn runtime() -> pb::ActorSandboxRuntime {
    pb::ActorSandboxRuntime {
        sandbox_class: "gvisor".into(),
        version: Some(pb::VersionedSandboxCompat {
            schema_version: "v1".into(),
            attributes: vec![pb::AttributeEntry {
                key: "k".into(),
                value: "v".into(),
            }],
        }),
    }
}

fn run() -> pb::RunRequest {
    pb::RunRequest {
        worker_pod_uid: UID.into(),
        hive: "ns1".into(),
        actor_name: "a1".into(),
        actor_uid: UID.into(),
        spec: Some(spec()),
        sandbox_runtime: Some(runtime()),
        ..Default::default()
    }
}

fn snapshot(uri: &str) -> pb::Snapshot {
    pb::Snapshot {
        snapshot_uuid: "snap-1".into(),
        generation: 1,
        fidelity: 3,
        object: Some(pb::ObjectStorage {
            snapshot_uri: uri.into(),
            sharing: 1,
        }),
    }
}

#[test]
fn run_requests() {
    let op = Operation::create();
    let cases: Vec<(&str, pb::RunRequest, Vec<Want>)> = vec![
        ("valid", run(), vec![]),
        (
            "no worker pod",
            with(run(), |r| r.worker_pod_uid.clear()),
            vec![required("worker_pod_uid")],
        ),
        (
            "bad hive",
            with(run(), |r| r.hive = "NS".into()),
            vec![dns_label("hive")],
        ),
        (
            "template identity optional",
            with(run(), |r| {
                r.actor_template_hive = "ns1".into();
                r.actor_template_name = "t".into();
            }),
            vec![],
        ),
        (
            "bad template name",
            with(run(), |r| r.actor_template_name = "T_1".into()),
            vec![dns_label("actor_template_name")],
        ),
        (
            "no spec",
            with(run(), |r| r.spec = None),
            vec![required("spec")],
        ),
        (
            "no runtime",
            with(run(), |r| r.sandbox_runtime = None),
            vec![required("sandbox_runtime")],
        ),
        (
            "bad runtime class",
            with(run(), |r| {
                r.sandbox_runtime.as_mut().unwrap().sandbox_class = "kata".into()
            }),
            vec![not_supported("sandbox_runtime.sandbox_class")],
        ),
        (
            "gateway",
            with(run(), |r| {
                r.egress_gateway = Some(pb::EgressGateway {
                    address: "gw.example.com:15001".into(),
                })
            }),
            vec![],
        ),
        (
            "gateway without port",
            with(run(), |r| {
                r.egress_gateway = Some(pb::EgressGateway {
                    address: "gw.example.com".into(),
                })
            }),
            vec![invalid("egress_gateway.address")],
        ),
        (
            "gateway empty",
            with(run(), |r| {
                r.egress_gateway = Some(pb::EgressGateway::default())
            }),
            vec![required("egress_gateway.address")],
        ),
        (
            "dangling mount",
            with(run(), |r| r.spec.as_mut().unwrap().volumes.clear()),
            vec![invalid("spec.containers[0].volume_mounts[0].name")],
        ),
        (
            "unpinned image",
            with(run(), |r| {
                r.spec.as_mut().unwrap().containers[0].image = "example.com/app:v1".into()
            }),
            vec![invalid("spec.containers[0].image")],
        ),
        (
            "external volume",
            with(run(), |r| {
                r.spec.as_mut().unwrap().volumes.push(pb::Volume {
                    name: "ext".into(),
                    external: Some(pb::ExternalVolumeSource {
                        storage_volume_id: "vol-1".into(),
                        volume_type: "hivekeep.io/pd".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
            }),
            vec![],
        ),
        (
            "external volume without id",
            with(run(), |r| {
                r.spec.as_mut().unwrap().volumes.push(pb::Volume {
                    name: "ext".into(),
                    external: Some(pb::ExternalVolumeSource::default()),
                    ..Default::default()
                })
            }),
            vec![required("spec.volumes[1].external.storage_volume_id")],
        ),
        (
            "negative cpu",
            with(run(), |r| {
                r.resources = Some(pb::ResourceRequirements {
                    limits: Some(pb::Resources {
                        cpu_millicores: -1,
                        memory_bytes: 0,
                    }),
                })
            }),
            vec![invalid_by("resources.limits.cpu_millicores", "minimum")],
        ),
    ];
    for (name, req, want) in cases {
        assert_errs(name, validate_run_request(&op, &req), &want);
    }
}

#[test]
fn snapshot_requests() {
    let op = Operation::create();
    let cp = |s: Option<pb::Snapshot>, mode| pb::CheckpointRequest {
        worker_pod_uid: UID.into(),
        hive: "ns1".into(),
        actor_name: "a1".into(),
        actor_uid: UID.into(),
        spec: Some(spec()),
        snapshot: s,
        upload_config: Some(pb::UploadConfig { mode }),
        ..Default::default()
    };
    assert_errs(
        "checkpoint",
        validate_checkpoint_request(&op, &cp(Some(snapshot(URI)), 1)),
        &[],
    );
    assert_errs(
        "checkpoint no snapshot",
        validate_checkpoint_request(&op, &cp(None, 1)),
        &[required("snapshot")],
    );
    assert_errs(
        "checkpoint no mode",
        validate_checkpoint_request(&op, &cp(Some(snapshot(URI)), 0)),
        &[required("upload_config.mode")],
    );
    assert_errs(
        "checkpoint bad uri",
        validate_checkpoint_request(&op, &cp(Some(snapshot("gs://bucket/elsewhere")), 2)),
        &[invalid("snapshot.object.snapshot_uri")],
    );
    let bad_uuid = pb::Snapshot {
        snapshot_uuid: "a/b".into(),
        ..snapshot(URI)
    };
    assert_errs(
        "checkpoint bad uuid",
        validate_checkpoint_request(&op, &cp(Some(bad_uuid), 1)),
        &[dns_label("snapshot.snapshot_uuid")],
    );
    let bad_sharing = pb::Snapshot {
        object: Some(pb::ObjectStorage {
            snapshot_uri: URI.into(),
            sharing: 3,
        }),
        ..snapshot(URI)
    };
    assert_errs(
        "checkpoint bad sharing",
        validate_checkpoint_request(&op, &cp(Some(bad_sharing), 1)),
        &[invalid_by("snapshot.object.sharing", "maximum")],
    );

    let restore = pb::RestoreRequest {
        worker_pod_uid: UID.into(),
        hive: "ns1".into(),
        actor_name: "a1".into(),
        actor_uid: UID.into(),
        spec: Some(spec()),
        snapshot: Some(snapshot(URI)),
        sandbox_runtime: Some(runtime()),
        ..Default::default()
    };
    assert_errs("restore", validate_restore_request(&op, &restore), &[]);
    let promote = pb::PromoteSnapshotRequest {
        hive: "ns1".into(),
        actor_name: "a1".into(),
        actor_uid: UID.into(),
        snapshot: Some(snapshot(URI)),
        ..Default::default()
    };
    assert_errs(
        "promote",
        validate_promote_snapshot_request(&op, &promote),
        &[],
    );
    assert_errs(
        "detach",
        validate_detach_actor_request(
            &op,
            &pb::DetachActorRequest {
                hive: "ns1".into(),
                actor_name: "a1".into(),
                actor_uid: UID.into(),
            },
        ),
        &[],
    );
    assert_errs(
        "detach empty",
        validate_detach_actor_request(&op, &pb::DetachActorRequest::default()),
        &[
            required("hive"),
            required("actor_name"),
            required("actor_uid"),
        ],
    );
    let term = pb::TerminateRequest {
        hive: "ns1".into(),
        actor_name: "a1".into(),
        actor_uid: UID.into(),
        ..Default::default()
    };
    assert_errs(
        "terminate without spec",
        validate_terminate_request(&op, &term),
        &[],
    );
}

#[test]
fn worker_support_requests() {
    let op = Operation::create();
    let crash = |msg: &str| pb::ReportActorCrashedRequest {
        actor_hive: "ns".into(),
        actor_name: "a".into(),
        actor_uid: UID.into(),
        message: msg.into(),
    };
    assert_errs(
        "crash",
        validate_report_actor_crashed_request(&op, &crash("boom")),
        &[],
    );
    assert_errs(
        "crash no message",
        validate_report_actor_crashed_request(&op, &crash("")),
        &[required("message")],
    );
    let suspend = pb::RequestActorSuspendRequest {
        actor_hive: String::new(),
        actor_name: "a".into(),
        actor_uid: UID.into(),
    };
    assert_errs(
        "suspend no hive",
        validate_request_actor_suspend_request(&op, &suspend),
        &[required("actor_hive")],
    );
    let egress = pb::GetActorEgressPolicyRequest {
        actor_hive: "ns".into(),
        actor_name: "A".into(),
    };
    assert_errs(
        "egress bad name",
        validate_get_actor_egress_policy_request(&op, &egress),
        &[dns_label("actor_name")],
    );
    let cert = pb::MintActorCertificateRequest {
        actor_hive: "ns".into(),
        actor_name: "a".into(),
        actor_uid: UID.into(),
        certificate_signing_request: vec![],
    };
    assert_errs(
        "cert no csr",
        validate_mint_actor_certificate_request(&op, &cert),
        &[required("certificate_signing_request")],
    );
    let reg = pb::RegisterWorkerRequest {
        epoch: 1,
        ips: vec!["10.0.0.1".into()],
        capacity: Some(pb::WorkerResources {
            cpu_millicores: 1,
            memory_bytes: 1,
            actors: 1,
        }),
    };
    assert_errs("register", validate_register_worker_request(&op, &reg), &[]);
    assert_errs(
        "register no capacity",
        validate_register_worker_request(
            &op,
            &pb::RegisterWorkerRequest {
                capacity: None,
                ..reg
            },
        ),
        &[required("capacity")],
    );
    let set = pb::SetWorkerSandboxRuntimesRequest {
        epoch: 0,
        sandbox_classes: vec![],
    };
    assert_errs(
        "runtimes empty",
        validate_set_worker_sandbox_runtimes_request(&op, &set),
        &[required("epoch"), required("sandbox_classes")],
    );
}

// ------------------------------------------------------------------------
// Table tests, one per keeper RPC and per nested workload message.

const ACTOR_UID: &str = "01234567-89ab-cdef-0123-456789abcdef";
const POD_UID: &str = "0f9a3b1c-2d4e-5f60-7182-93a4b5c6d7e8";
const DIGEST_IMAGE: &str =
    "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const SNAP_URI: &str =
    "gs://bucket/root/hives/team-a/actors/01234567-89ab-cdef-0123-456789abcdef/snapshots/snap-1";

fn uuid(path: &'static str) -> Want {
    invalid_by(path, "format=uuid")
}
fn minimum(path: &'static str) -> Want {
    invalid_by(path, "minimum")
}
fn maximum(path: &'static str) -> Want {
    invalid_by(path, "maximum")
}
fn union(path: &'static str) -> Want {
    invalid_by(path, "union")
}

fn compat_with_attributes(n: usize) -> pb::VersionedSandboxCompat {
    pb::VersionedSandboxCompat {
        schema_version: "v1".into(),
        attributes: (0..n)
            .map(|i| pb::AttributeEntry {
                key: format!("k{i}"),
                value: "v".into(),
            })
            .collect(),
    }
}

fn runtime_with_attributes(n: usize) -> pb::ActorSandboxRuntime {
    pb::ActorSandboxRuntime {
        sandbox_class: "gvisor".into(),
        version: Some(compat_with_attributes(n)),
    }
}

fn digest_container(name: &str) -> pb::Container {
    pb::Container {
        name: name.into(),
        image: DIGEST_IMAGE.into(),
        ..Default::default()
    }
}

fn limits(cpu: i64, mem: i64) -> Option<pb::ResourceRequirements> {
    Some(pb::ResourceRequirements {
        limits: Some(pb::Resources {
            cpu_millicores: cpu,
            memory_bytes: mem,
        }),
    })
}

fn snap(uuid: &str) -> pb::Snapshot {
    pb::Snapshot {
        snapshot_uuid: uuid.into(),
        generation: 1,
        fidelity: pb::SnapshotFidelity::Memory as i32,
        object: Some(pb::ObjectStorage {
            snapshot_uri: SNAP_URI.into(),
            sharing: 0,
        }),
    }
}

fn run_cases<T>(cases: Vec<(&str, T, Vec<Want>)>, f: impl Fn(&Operation, &T) -> ErrorList) {
    let op = Operation::create();
    for (name, obj, want) in cases {
        assert_errs(name, f(&op, &obj), &want);
    }
}

#[test]
fn request_actor_suspend_request() {
    let v = || pb::RequestActorSuspendRequest {
        actor_hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: ACTOR_UID.into(),
    };
    run_cases(
        vec![
            ("valid", v(), vec![]),
            (
                "missing actor_hive",
                with(v(), |r| r.actor_hive.clear()),
                vec![required("actor_hive")],
            ),
            (
                "invalid actor_hive: uppercase",
                with(v(), |r| r.actor_hive = "Team-A".into()),
                vec![dns_label("actor_hive")],
            ),
            (
                "missing actor_name",
                with(v(), |r| r.actor_name.clear()),
                vec![required("actor_name")],
            ),
            (
                "invalid actor_name: trailing dash",
                with(v(), |r| r.actor_name = "actor-".into()),
                vec![dns_label("actor_name")],
            ),
            (
                "missing actor_uid",
                with(v(), |r| r.actor_uid.clear()),
                vec![required("actor_uid")],
            ),
            (
                "invalid actor_uid: not a uuid",
                with(v(), |r| r.actor_uid = "not-a-uuid".into()),
                vec![uuid("actor_uid")],
            ),
        ],
        validate_request_actor_suspend_request,
    );
}

#[test]
fn get_actor_egress_policy_request() {
    let v = || pb::GetActorEgressPolicyRequest {
        actor_hive: "team-a".into(),
        actor_name: "actor-1".into(),
    };
    run_cases(
        vec![
            ("valid", v(), vec![]),
            (
                "missing actor_hive",
                with(v(), |r| r.actor_hive.clear()),
                vec![required("actor_hive")],
            ),
            (
                "invalid actor_hive: uppercase",
                with(v(), |r| r.actor_hive = "Team-A".into()),
                vec![dns_label("actor_hive")],
            ),
            (
                "missing actor_name",
                with(v(), |r| r.actor_name.clear()),
                vec![required("actor_name")],
            ),
            (
                "invalid actor_name: trailing dash",
                with(v(), |r| r.actor_name = "actor-".into()),
                vec![dns_label("actor_name")],
            ),
        ],
        validate_get_actor_egress_policy_request,
    );
}

#[test]
fn mint_actor_certificate_request() {
    let v = || pb::MintActorCertificateRequest {
        actor_hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: ACTOR_UID.into(),
        certificate_signing_request: b"der-bytes".to_vec(),
    };
    run_cases(
        vec![
            ("valid", v(), vec![]),
            (
                "missing actor_hive",
                with(v(), |r| r.actor_hive.clear()),
                vec![required("actor_hive")],
            ),
            (
                "invalid actor_hive: uppercase",
                with(v(), |r| r.actor_hive = "Team-A".into()),
                vec![dns_label("actor_hive")],
            ),
            (
                "missing actor_name",
                with(v(), |r| r.actor_name.clear()),
                vec![required("actor_name")],
            ),
            (
                "invalid actor_name: trailing dash",
                with(v(), |r| r.actor_name = "actor-".into()),
                vec![dns_label("actor_name")],
            ),
            (
                "missing actor_uid",
                with(v(), |r| r.actor_uid.clear()),
                vec![required("actor_uid")],
            ),
            (
                "invalid actor_uid: not a uuid",
                with(v(), |r| r.actor_uid = "not-a-uuid".into()),
                vec![uuid("actor_uid")],
            ),
            (
                "missing certificate_signing_request",
                with(v(), |r| r.certificate_signing_request.clear()),
                vec![required("certificate_signing_request")],
            ),
            (
                "certificate_signing_request at the bound",
                with(v(), |r| r.certificate_signing_request = vec![0; 16384]),
                vec![],
            ),
            (
                "certificate_signing_request too large",
                with(v(), |r| r.certificate_signing_request = vec![0; 16385]),
                vec![(
                    ErrorKind::TooLong,
                    "certificate_signing_request",
                    Some("maxBytes"),
                )],
            ),
        ],
        validate_mint_actor_certificate_request,
    );
}

fn run_req() -> pb::RunRequest {
    pb::RunRequest {
        worker_pod_uid: POD_UID.into(),
        hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: ACTOR_UID.into(),
        actor_template_hive: "team-a".into(),
        actor_template_name: "tmpl-1".into(),
        spec: Some(pb::WorkloadSpec {
            containers: vec![digest_container("worker")],
            ..Default::default()
        }),
        sandbox_runtime: Some(runtime_with_attributes(1)),
        resources: limits(500, 1 << 30),
        ..Default::default()
    }
}

fn gw(addr: &str) -> Option<pb::EgressGateway> {
    Some(pb::EgressGateway {
        address: addr.into(),
    })
}

#[test]
fn run_request() {
    let v = run_req;
    run_cases(
        vec![
            ("valid", v(), vec![]),
            (
                "missing worker_pod_uid",
                with(v(), |r| r.worker_pod_uid.clear()),
                vec![required("worker_pod_uid")],
            ),
            (
                "invalid worker_pod_uid: path escape",
                with(v(), |r| r.worker_pod_uid = "../escape".into()),
                vec![uuid("worker_pod_uid")],
            ),
            (
                "invalid worker_pod_uid: not a pod UID",
                with(v(), |r| r.worker_pod_uid = "worker-1".into()),
                vec![uuid("worker_pod_uid")],
            ),
            (
                "missing hive",
                with(v(), |r| r.hive.clear()),
                vec![required("hive")],
            ),
            (
                "invalid hive: path escape",
                with(v(), |r| r.hive = "../escape".into()),
                vec![dns_label("hive")],
            ),
            (
                "missing actor_name",
                with(v(), |r| r.actor_name.clear()),
                vec![required("actor_name")],
            ),
            (
                "invalid actor_name: path escape",
                with(v(), |r| r.actor_name = "../escape".into()),
                vec![dns_label("actor_name")],
            ),
            (
                "missing actor_uid",
                with(v(), |r| r.actor_uid.clear()),
                vec![required("actor_uid")],
            ),
            (
                "invalid actor_uid: path escape",
                with(v(), |r| r.actor_uid = "../escape".into()),
                vec![uuid("actor_uid")],
            ),
            (
                "invalid actor_template_hive: uppercase",
                with(v(), |r| r.actor_template_hive = "Team-A".into()),
                vec![dns_label("actor_template_hive")],
            ),
            (
                "invalid actor_template_name: trailing dash",
                with(v(), |r| r.actor_template_name = "tmpl-".into()),
                vec![dns_label("actor_template_name")],
            ),
            (
                "unset template identity is allowed",
                with(v(), |r| {
                    r.actor_template_hive.clear();
                    r.actor_template_name.clear();
                }),
                vec![],
            ),
            (
                "missing spec",
                with(v(), |r| r.spec = None),
                vec![required("spec")],
            ),
            (
                "invalid container name: path escape",
                with(v(), |r| {
                    r.spec.as_mut().unwrap().containers[0].name = "../escape".into()
                }),
                vec![dns_label("spec.containers[0].name")],
            ),
            (
                "duplicate container name",
                with(v(), |r| {
                    r.spec
                        .as_mut()
                        .unwrap()
                        .containers
                        .push(digest_container("worker"))
                }),
                vec![duplicate("spec.containers[1]")],
            ),
            (
                "egress gateway with a DNS name",
                with(v(), |r| {
                    r.egress_gateway = gw("gate-egress.hk-system.svc:443")
                }),
                vec![],
            ),
            (
                "egress gateway with an IPv6 address",
                with(v(), |r| r.egress_gateway = gw("[fd00::1]:443")),
                vec![],
            ),
            (
                "egress gateway missing address",
                with(v(), |r| r.egress_gateway = gw("")),
                vec![required("egress_gateway.address")],
            ),
            (
                "egress gateway address without a port",
                with(v(), |r| r.egress_gateway = gw("gate-egress.hk-system.svc")),
                vec![invalid("egress_gateway.address")],
            ),
            (
                "egress gateway address too long",
                with(v(), |r| {
                    r.egress_gateway = gw(&format!("{}:443", "a".repeat(258)))
                }),
                vec![
                    too_long("egress_gateway.address"),
                    invalid("egress_gateway.address"),
                ],
            ),
            (
                "negative cpu_millicores",
                with(v(), |r| r.resources = limits(-1, 0)),
                vec![minimum("resources.limits.cpu_millicores")],
            ),
            (
                "cpu_millicores at the bound",
                with(v(), |r| r.resources = limits(999_999, 0)),
                vec![],
            ),
            (
                "cpu_millicores over the bound",
                with(v(), |r| r.resources = limits(1_000_000, 0)),
                vec![maximum("resources.limits.cpu_millicores")],
            ),
            (
                "negative memory_bytes",
                with(v(), |r| r.resources = limits(0, -1)),
                vec![minimum("resources.limits.memory_bytes")],
            ),
            (
                "unset sizes are allowed",
                with(v(), |r| r.resources = None),
                vec![],
            ),
            (
                "missing sandbox_runtime",
                with(v(), |r| r.sandbox_runtime = None),
                vec![required("sandbox_runtime")],
            ),
            (
                "sandbox_runtime errors surface at their path",
                with(v(), |r| {
                    r.sandbox_runtime.as_mut().unwrap().sandbox_class = "kata".into()
                }),
                vec![not_supported("sandbox_runtime.sandbox_class")],
            ),
        ],
        validate_run_request,
    );
}

fn bad_spec() -> Option<pb::WorkloadSpec> {
    Some(pb::WorkloadSpec {
        containers: vec![digest_container("_sandbox")],
        ..Default::default()
    })
}

#[test]
fn checkpoint_request() {
    let v = || pb::CheckpointRequest {
        worker_pod_uid: POD_UID.into(),
        hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: ACTOR_UID.into(),
        actor_template_hive: "team-a".into(),
        actor_template_name: "tmpl-1".into(),
        spec: Some(pb::WorkloadSpec::default()),
        snapshot: Some(snap("snap-1")),
        upload_config: Some(pb::UploadConfig {
            mode: pb::UploadMode::Immediate as i32,
        }),
    };
    let deferred = |name: &str| {
        let name = name.to_string();
        move |r: &mut pb::CheckpointRequest| {
            r.snapshot.as_mut().unwrap().snapshot_uuid = name;
            r.upload_config = Some(pb::UploadConfig {
                mode: pb::UploadMode::Deferred as i32,
            });
        }
    };
    let s = snap_of_checkpoint;
    run_cases(
        vec![
            ("valid immediate", v(), vec![]),
            (
                "valid deferred",
                with(v(), deferred("pause-snap-1")),
                vec![],
            ),
            (
                "valid volumes fidelity",
                with(v(), |r| {
                    s(r).fidelity = pb::SnapshotFidelity::Volumes as i32
                }),
                vec![],
            ),
            (
                "missing worker_pod_uid",
                with(v(), |r| r.worker_pod_uid.clear()),
                vec![required("worker_pod_uid")],
            ),
            (
                "invalid worker_pod_uid: path escape",
                with(v(), |r| r.worker_pod_uid = "../escape".into()),
                vec![uuid("worker_pod_uid")],
            ),
            (
                "invalid worker_pod_uid: not a pod UID",
                with(v(), |r| r.worker_pod_uid = "worker-1".into()),
                vec![uuid("worker_pod_uid")],
            ),
            (
                "invalid hive: path escape",
                with(v(), |r| r.hive = "../escape".into()),
                vec![dns_label("hive")],
            ),
            (
                "invalid actor_name: uppercase",
                with(v(), |r| r.actor_name = "UPPER".into()),
                vec![dns_label("actor_name")],
            ),
            (
                "invalid actor_uid: not a UUID",
                with(v(), |r| r.actor_uid = "../escape".into()),
                vec![uuid("actor_uid")],
            ),
            (
                "invalid actor_template_name: trailing dash",
                with(v(), |r| r.actor_template_name = "tmpl-".into()),
                vec![dns_label("actor_template_name")],
            ),
            (
                "unset template identity is allowed",
                with(v(), |r| {
                    r.actor_template_hive.clear();
                    r.actor_template_name.clear();
                }),
                vec![],
            ),
            (
                "missing spec",
                with(v(), |r| r.spec = None),
                vec![required("spec")],
            ),
            (
                "spec errors surface at the spec's path",
                with(v(), |r| r.spec = bad_spec()),
                vec![dns_label("spec.containers[0].name")],
            ),
            (
                "missing snapshot",
                with(v(), |r| r.snapshot = None),
                vec![required("snapshot")],
            ),
            (
                "missing upload_config",
                with(v(), |r| r.upload_config = None),
                vec![required("upload_config")],
            ),
            (
                "unspecified upload mode",
                with(v(), |r| {
                    r.upload_config = Some(pb::UploadConfig { mode: 0 })
                }),
                vec![required("upload_config.mode")],
            ),
            (
                "unknown upload mode",
                with(v(), |r| {
                    r.upload_config = Some(pb::UploadConfig { mode: 3 })
                }),
                vec![maximum("upload_config.mode")],
            ),
            (
                "immediate upload without object storage",
                with(v(), |r| s(r).object = None),
                vec![required("snapshot.object")],
            ),
            (
                "deferred upload without object storage",
                with(with(v(), deferred("pause-snap-1")), |r| s(r).object = None),
                vec![required("snapshot.object")],
            ),
            (
                "missing snapshot_uuid",
                with(v(), deferred("")),
                vec![required("snapshot.snapshot_uuid")],
            ),
            (
                "invalid snapshot_uuid: path escape",
                with(v(), deferred("../escape")),
                vec![dns_label("snapshot.snapshot_uuid")],
            ),
            (
                "missing generation",
                with(v(), |r| s(r).generation = 0),
                vec![required("snapshot.generation")],
            ),
            (
                "negative generation",
                with(v(), |r| s(r).generation = -1),
                vec![minimum("snapshot.generation")],
            ),
            (
                "missing snapshot_uri",
                with(v(), |r| s(r).object.as_mut().unwrap().snapshot_uri.clear()),
                vec![required("snapshot.object.snapshot_uri")],
            ),
            (
                "invalid snapshot_uri: no bucket",
                with(v(), |r| {
                    s(r).object.as_mut().unwrap().snapshot_uri = "relative/path".into()
                }),
                vec![invalid("snapshot.object.snapshot_uri")],
            ),
            (
                "snapshot_uri too long",
                with(v(), |r| {
                    s(r).object.as_mut().unwrap().snapshot_uri =
                        format!("gs://bucket/{}", "p".repeat(2048))
                }),
                vec![
                    too_long("snapshot.object.snapshot_uri"),
                    invalid("snapshot.object.snapshot_uri"),
                ],
            ),
            (
                "unspecified fidelity",
                with(v(), |r| s(r).fidelity = 0),
                vec![required("snapshot.fidelity")],
            ),
            (
                "unknown fidelity",
                with(v(), |r| s(r).fidelity = 4),
                vec![maximum("snapshot.fidelity")],
            ),
            (
                "rootfs fidelity is forwarded for the runtime to judge",
                with(v(), |r| s(r).fidelity = pb::SnapshotFidelity::Rootfs as i32),
                vec![],
            ),
        ],
        validate_checkpoint_request,
    );
}

#[test]
fn restore_request() {
    let v = || pb::RestoreRequest {
        worker_pod_uid: POD_UID.into(),
        hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: ACTOR_UID.into(),
        actor_template_hive: "team-a".into(),
        actor_template_name: "tmpl-1".into(),
        spec: Some(pb::WorkloadSpec::default()),
        snapshot: Some(snap("snap-1")),
        sandbox_runtime: Some(runtime_with_attributes(1)),
        ..Default::default()
    };
    let s = snap_of_restore;
    run_cases(
        vec![
            ("valid", v(), vec![]),
            (
                "missing object storage",
                with(v(), |r| s(r).object = None),
                vec![required("snapshot.object")],
            ),
            (
                "valid volumes fidelity",
                with(v(), |r| {
                    s(r).fidelity = pb::SnapshotFidelity::Volumes as i32
                }),
                vec![],
            ),
            (
                "valid size and egress",
                with(v(), |r| {
                    r.resources = limits(999_999, 1 << 30);
                    r.egress_gateway = gw("10.0.0.1:15001");
                }),
                vec![],
            ),
            (
                "missing worker_pod_uid",
                with(v(), |r| r.worker_pod_uid.clear()),
                vec![required("worker_pod_uid")],
            ),
            (
                "invalid worker_pod_uid: path escape",
                with(v(), |r| r.worker_pod_uid = "../escape".into()),
                vec![uuid("worker_pod_uid")],
            ),
            (
                "invalid worker_pod_uid: not a pod UID",
                with(v(), |r| r.worker_pod_uid = "worker-1".into()),
                vec![uuid("worker_pod_uid")],
            ),
            (
                "invalid hive: path escape",
                with(v(), |r| r.hive = "../escape".into()),
                vec![dns_label("hive")],
            ),
            (
                "invalid actor_name: uppercase",
                with(v(), |r| r.actor_name = "UPPER".into()),
                vec![dns_label("actor_name")],
            ),
            (
                "invalid actor_uid: not a UUID",
                with(v(), |r| r.actor_uid = "../escape".into()),
                vec![uuid("actor_uid")],
            ),
            (
                "invalid actor_template_hive: slash",
                with(v(), |r| r.actor_template_hive = "no/slashes".into()),
                vec![dns_label("actor_template_hive")],
            ),
            (
                "unset template identity is allowed",
                with(v(), |r| {
                    r.actor_template_hive.clear();
                    r.actor_template_name.clear();
                }),
                vec![],
            ),
            (
                "missing spec",
                with(v(), |r| r.spec = None),
                vec![required("spec")],
            ),
            (
                "spec errors surface at the spec's path",
                with(v(), |r| r.spec = bad_spec()),
                vec![dns_label("spec.containers[0].name")],
            ),
            (
                "missing snapshot",
                with(v(), |r| r.snapshot = None),
                vec![required("snapshot")],
            ),
            (
                "missing snapshot_uuid",
                with(v(), |r| s(r).snapshot_uuid.clear()),
                vec![required("snapshot.snapshot_uuid")],
            ),
            (
                "invalid snapshot_uuid: path escape",
                with(v(), |r| s(r).snapshot_uuid = "../escape".into()),
                vec![dns_label("snapshot.snapshot_uuid")],
            ),
            (
                "missing generation",
                with(v(), |r| s(r).generation = 0),
                vec![required("snapshot.generation")],
            ),
            (
                "negative generation",
                with(v(), |r| s(r).generation = -1),
                vec![minimum("snapshot.generation")],
            ),
            (
                "missing snapshot_uri",
                with(v(), |r| s(r).object.as_mut().unwrap().snapshot_uri.clear()),
                vec![required("snapshot.object.snapshot_uri")],
            ),
            (
                "invalid snapshot_uri: no bucket",
                with(v(), |r| {
                    s(r).object.as_mut().unwrap().snapshot_uri = "relative/path".into()
                }),
                vec![invalid("snapshot.object.snapshot_uri")],
            ),
            (
                "unspecified fidelity",
                with(v(), |r| s(r).fidelity = 0),
                vec![required("snapshot.fidelity")],
            ),
            (
                "unknown fidelity",
                with(v(), |r| s(r).fidelity = 4),
                vec![maximum("snapshot.fidelity")],
            ),
            (
                "rootfs fidelity is forwarded for the runtime to judge",
                with(v(), |r| s(r).fidelity = pb::SnapshotFidelity::Rootfs as i32),
                vec![],
            ),
            (
                "negative cpu_millicores",
                with(v(), |r| r.resources = limits(-1, 0)),
                vec![minimum("resources.limits.cpu_millicores")],
            ),
            (
                "cpu_millicores too large",
                with(v(), |r| r.resources = limits(1_000_000, 0)),
                vec![maximum("resources.limits.cpu_millicores")],
            ),
            (
                "negative memory_bytes",
                with(v(), |r| r.resources = limits(0, -1)),
                vec![minimum("resources.limits.memory_bytes")],
            ),
            (
                "empty egress gateway address",
                with(v(), |r| r.egress_gateway = gw("")),
                vec![required("egress_gateway.address")],
            ),
            (
                "missing sandbox_runtime",
                with(v(), |r| r.sandbox_runtime = None),
                vec![required("sandbox_runtime")],
            ),
            (
                "sandbox_runtime errors surface at their path",
                with(v(), |r| {
                    r.sandbox_runtime.as_mut().unwrap().sandbox_class = "kata".into()
                }),
                vec![not_supported("sandbox_runtime.sandbox_class")],
            ),
        ],
        validate_restore_request,
    );
}

#[test]
fn promote_snapshot_request() {
    let v = || pb::PromoteSnapshotRequest {
        hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: ACTOR_UID.into(),
        actor_template_hive: "team-a".into(),
        actor_template_name: "tmpl-1".into(),
        snapshot: Some(snap("pause-snap-1")),
    };
    let s = snap_of_promote;
    run_cases(
        vec![
            ("valid", v(), vec![]),
            (
                "valid volumes fidelity",
                with(v(), |r| {
                    s(r).fidelity = pb::SnapshotFidelity::Volumes as i32
                }),
                vec![],
            ),
            (
                "missing hive",
                with(v(), |r| r.hive.clear()),
                vec![required("hive")],
            ),
            (
                "invalid hive: path escape",
                with(v(), |r| r.hive = "../escape".into()),
                vec![dns_label("hive")],
            ),
            (
                "invalid actor_name: uppercase",
                with(v(), |r| r.actor_name = "UPPER".into()),
                vec![dns_label("actor_name")],
            ),
            (
                "missing actor_uid",
                with(v(), |r| r.actor_uid.clear()),
                vec![required("actor_uid")],
            ),
            (
                "invalid actor_uid: not a UUID",
                with(v(), |r| r.actor_uid = "actor-uid-1".into()),
                vec![uuid("actor_uid")],
            ),
            (
                "invalid actor_template_hive: slash",
                with(v(), |r| r.actor_template_hive = "no/slashes".into()),
                vec![dns_label("actor_template_hive")],
            ),
            (
                "unset template identity is allowed",
                with(v(), |r| {
                    r.actor_template_hive.clear();
                    r.actor_template_name.clear();
                }),
                vec![],
            ),
            (
                "missing snapshot",
                with(v(), |r| r.snapshot = None),
                vec![required("snapshot")],
            ),
            (
                "missing snapshot_uuid",
                with(v(), |r| s(r).snapshot_uuid.clear()),
                vec![required("snapshot.snapshot_uuid")],
            ),
            (
                "invalid snapshot_uuid: path escape",
                with(v(), |r| s(r).snapshot_uuid = "../escape".into()),
                vec![dns_label("snapshot.snapshot_uuid")],
            ),
            (
                "missing generation",
                with(v(), |r| s(r).generation = 0),
                vec![required("snapshot.generation")],
            ),
            (
                "negative generation",
                with(v(), |r| s(r).generation = -1),
                vec![minimum("snapshot.generation")],
            ),
            (
                "missing snapshot.object",
                with(v(), |r| s(r).object = None),
                vec![required("snapshot.object")],
            ),
            (
                "missing snapshot_uri",
                with(v(), |r| s(r).object.as_mut().unwrap().snapshot_uri.clear()),
                vec![required("snapshot.object.snapshot_uri")],
            ),
            (
                "invalid snapshot_uri",
                with(v(), |r| {
                    s(r).object.as_mut().unwrap().snapshot_uri = "not-a-uri".into()
                }),
                vec![invalid("snapshot.object.snapshot_uri")],
            ),
            (
                "snapshot_uri too long",
                with(v(), |r| {
                    s(r).object.as_mut().unwrap().snapshot_uri =
                        format!("gs://bucket/{}", "p".repeat(2048))
                }),
                vec![
                    too_long("snapshot.object.snapshot_uri"),
                    invalid("snapshot.object.snapshot_uri"),
                ],
            ),
            (
                "unspecified fidelity",
                with(v(), |r| s(r).fidelity = 0),
                vec![required("snapshot.fidelity")],
            ),
            (
                "unknown fidelity",
                with(v(), |r| s(r).fidelity = 4),
                vec![maximum("snapshot.fidelity")],
            ),
            (
                "rootfs fidelity is forwarded for the runtime to judge",
                with(v(), |r| s(r).fidelity = pb::SnapshotFidelity::Rootfs as i32),
                vec![],
            ),
        ],
        validate_promote_snapshot_request,
    );
}

#[test]
fn terminate_request() {
    let v = || pb::TerminateRequest {
        worker_pod_uid: POD_UID.into(),
        hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: ACTOR_UID.into(),
        actor_template_hive: "team-a".into(),
        actor_template_name: "tmpl-1".into(),
        spec: Some(pb::WorkloadSpec::default()),
    };
    run_cases(
        vec![
            ("valid", v(), vec![]),
            (
                "unset worker_pod_uid is allowed: node-only cleanup",
                with(v(), |r| r.worker_pod_uid.clear()),
                vec![],
            ),
            (
                "invalid worker_pod_uid: path escape",
                with(v(), |r| r.worker_pod_uid = "../escape".into()),
                vec![uuid("worker_pod_uid")],
            ),
            (
                "invalid worker_pod_uid: not a pod UID",
                with(v(), |r| r.worker_pod_uid = "worker-1".into()),
                vec![uuid("worker_pod_uid")],
            ),
            (
                "missing hive",
                with(v(), |r| r.hive.clear()),
                vec![required("hive")],
            ),
            (
                "invalid hive: uppercase",
                with(v(), |r| r.hive = "Team-A".into()),
                vec![dns_label("hive")],
            ),
            (
                "missing actor_name",
                with(v(), |r| r.actor_name.clear()),
                vec![required("actor_name")],
            ),
            (
                "invalid actor_name: path escape",
                with(v(), |r| r.actor_name = "../escape".into()),
                vec![dns_label("actor_name")],
            ),
            (
                "missing actor_uid",
                with(v(), |r| r.actor_uid.clear()),
                vec![required("actor_uid")],
            ),
            (
                "invalid actor_uid: not a uuid",
                with(v(), |r| r.actor_uid = "not-a-uuid".into()),
                vec![uuid("actor_uid")],
            ),
            (
                "invalid actor_template_hive: uppercase",
                with(v(), |r| r.actor_template_hive = "Team-A".into()),
                vec![dns_label("actor_template_hive")],
            ),
            (
                "invalid actor_template_name: trailing dash",
                with(v(), |r| r.actor_template_name = "tmpl-".into()),
                vec![dns_label("actor_template_name")],
            ),
            (
                "spec errors surface at the spec's path",
                with(v(), |r| r.spec = bad_spec()),
                vec![dns_label("spec.containers[0].name")],
            ),
            (
                "unset template identity is allowed",
                with(v(), |r| {
                    r.actor_template_hive.clear();
                    r.actor_template_name.clear();
                }),
                vec![],
            ),
            (
                "unset spec is allowed",
                with(v(), |r| r.spec = None),
                vec![],
            ),
        ],
        validate_terminate_request,
    );
}

#[test]
fn detach_actor_request() {
    let v = || pb::DetachActorRequest {
        hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: ACTOR_UID.into(),
    };
    run_cases(
        vec![
            ("valid", v(), vec![]),
            (
                "missing hive",
                with(v(), |r| r.hive.clear()),
                vec![required("hive")],
            ),
            (
                "invalid hive: uppercase",
                with(v(), |r| r.hive = "Team-A".into()),
                vec![dns_label("hive")],
            ),
            (
                "missing actor_name",
                with(v(), |r| r.actor_name.clear()),
                vec![required("actor_name")],
            ),
            (
                "invalid actor_name: path escape",
                with(v(), |r| r.actor_name = "../escape".into()),
                vec![dns_label("actor_name")],
            ),
            (
                "missing actor_uid",
                with(v(), |r| r.actor_uid.clear()),
                vec![required("actor_uid")],
            ),
            (
                "invalid actor_uid: not a uuid",
                with(v(), |r| r.actor_uid = "not-a-uuid".into()),
                vec![uuid("actor_uid")],
            ),
        ],
        validate_detach_actor_request,
    );
}

#[test]
fn set_worker_sandbox_runtimes_request() {
    let compat = || pb::VersionedSandboxCompat {
        schema_version: "v1".into(),
        attributes: vec![pb::AttributeEntry {
            key: "architecture".into(),
            value: "amd64".into(),
        }],
    };
    let rt = |c: pb::VersionedSandboxCompat| pb::WorkerSandboxRuntime {
        version: Some(c),
        ..Default::default()
    };
    let class = || pb::WorkerSandboxClass {
        sandbox_class: "gvisor".into(),
        default_runtime: Some(rt(compat())),
        ..Default::default()
    };
    let with_class = |c: pb::WorkerSandboxClass| pb::SetWorkerSandboxRuntimesRequest {
        epoch: 1,
        sandbox_classes: vec![c],
    };
    let with_default = |c: pb::VersionedSandboxCompat| {
        with_class(pb::WorkerSandboxClass {
            sandbox_class: "gvisor".into(),
            default_runtime: Some(rt(c)),
            ..Default::default()
        })
    };
    let with_restorable = |cs: Vec<pb::VersionedSandboxCompat>| {
        let mut c = class();
        c.restorable_runtimes = cs.into_iter().map(rt).collect();
        with_class(c)
    };
    let attrs = |a: Vec<(&str, &str)>| pb::VersionedSandboxCompat {
        schema_version: "v1".into(),
        attributes: a
            .into_iter()
            .map(|(k, v)| pb::AttributeEntry {
                key: k.into(),
                value: v.into(),
            })
            .collect(),
    };
    let k129 = "k".repeat(129);
    let v257 = "v".repeat(257);
    run_cases(
        vec![
            ("valid", with_class(class()), vec![]),
            (
                "valid with restorable runtimes",
                with_restorable(vec![compat()]),
                vec![],
            ),
            (
                "missing epoch",
                pb::SetWorkerSandboxRuntimesRequest {
                    epoch: 0,
                    sandbox_classes: vec![class()],
                },
                vec![required("epoch")],
            ),
            (
                "missing sandbox_classes",
                pb::SetWorkerSandboxRuntimesRequest {
                    epoch: 1,
                    sandbox_classes: vec![],
                },
                vec![required("sandbox_classes")],
            ),
            (
                "too many sandbox_classes",
                pb::SetWorkerSandboxRuntimesRequest {
                    epoch: 1,
                    sandbox_classes: vec![
                        class(),
                        pb::WorkerSandboxClass {
                            sandbox_class: "microvm".into(),
                            default_runtime: Some(rt(compat())),
                            ..Default::default()
                        },
                    ],
                },
                vec![too_many("sandbox_classes")],
            ),
            (
                "missing sandbox_classes.sandbox_class",
                with_class(pb::WorkerSandboxClass {
                    default_runtime: Some(rt(compat())),
                    ..Default::default()
                }),
                vec![required("sandbox_classes[0].sandbox_class")],
            ),
            (
                "sandbox_classes.sandbox_class too long",
                with_class(pb::WorkerSandboxClass {
                    sandbox_class: "c".repeat(64),
                    default_runtime: Some(rt(compat())),
                    ..Default::default()
                }),
                vec![
                    too_long("sandbox_classes[0].sandbox_class"),
                    not_supported("sandbox_classes[0].sandbox_class"),
                ],
            ),
            (
                "unsupported sandbox_classes.sandbox_class",
                with_class(pb::WorkerSandboxClass {
                    sandbox_class: "kata".into(),
                    default_runtime: Some(rt(compat())),
                    ..Default::default()
                }),
                vec![not_supported("sandbox_classes[0].sandbox_class")],
            ),
            (
                "missing sandbox_classes.default_runtime",
                with_class(pb::WorkerSandboxClass {
                    sandbox_class: "gvisor".into(),
                    ..Default::default()
                }),
                vec![required("sandbox_classes[0].default_runtime")],
            ),
            (
                "missing default_runtime.version",
                with_class(pb::WorkerSandboxClass {
                    sandbox_class: "gvisor".into(),
                    default_runtime: Some(pb::WorkerSandboxRuntime::default()),
                    ..Default::default()
                }),
                vec![required("sandbox_classes[0].default_runtime.version")],
            ),
            (
                "missing default_runtime.schema_version",
                with_default(pb::VersionedSandboxCompat {
                    schema_version: String::new(),
                    ..compat()
                }),
                vec![required(
                    "sandbox_classes[0].default_runtime.version.schema_version",
                )],
            ),
            (
                "default_runtime.schema_version too long",
                with_default(pb::VersionedSandboxCompat {
                    schema_version: "v".repeat(65),
                    ..compat()
                }),
                vec![too_long(
                    "sandbox_classes[0].default_runtime.version.schema_version",
                )],
            ),
            (
                "missing default_runtime.attributes",
                with_default(attrs(vec![])),
                vec![required(
                    "sandbox_classes[0].default_runtime.version.attributes",
                )],
            ),
            (
                "duplicate attribute key",
                with_default(attrs(vec![
                    ("architecture", "amd64"),
                    ("architecture", "arm64"),
                ])),
                vec![duplicate(
                    "sandbox_classes[0].default_runtime.version.attributes[1]",
                )],
            ),
            (
                "missing attribute key",
                with_default(attrs(vec![("", "v")])),
                vec![required(
                    "sandbox_classes[0].default_runtime.version.attributes[0].key",
                )],
            ),
            (
                "attribute key too long",
                with_default(attrs(vec![(&k129, "v")])),
                vec![too_long(
                    "sandbox_classes[0].default_runtime.version.attributes[0].key",
                )],
            ),
            (
                "missing attribute value",
                with_default(attrs(vec![("k", "")])),
                vec![required(
                    "sandbox_classes[0].default_runtime.version.attributes[0].value",
                )],
            ),
            (
                "attribute value too long",
                with_default(attrs(vec![("k", &v257)])),
                vec![too_long(
                    "sandbox_classes[0].default_runtime.version.attributes[0].value",
                )],
            ),
            (
                "attributes at the bound",
                with_default(compat_with_attributes(32)),
                vec![],
            ),
            (
                "too many attributes",
                with_default(compat_with_attributes(33)),
                vec![too_many(
                    "sandbox_classes[0].default_runtime.version.attributes",
                )],
            ),
            (
                "invalid restorable_runtimes entry",
                with_restorable(vec![pb::VersionedSandboxCompat {
                    schema_version: String::new(),
                    ..compat()
                }]),
                vec![required(
                    "sandbox_classes[0].restorable_runtimes[0].version.schema_version",
                )],
            ),
            (
                "restorable_runtimes at the bound",
                with_restorable(vec![compat(); 32]),
                vec![],
            ),
            (
                "too many restorable_runtimes",
                with_restorable(vec![compat(); 33]),
                vec![too_many("sandbox_classes[0].restorable_runtimes")],
            ),
        ],
        validate_set_worker_sandbox_runtimes_request,
    );
}

#[test]
fn register_worker_request() {
    let ips = || vec!["10.0.0.1".to_string()];
    let req = |epoch: i64, ips: Vec<String>, cap: Option<pb::WorkerResources>| {
        pb::RegisterWorkerRequest {
            epoch,
            ips,
            capacity: cap,
        }
    };
    let cap = |actors: i32, cpu: i64, mem: i64| {
        Some(pb::WorkerResources {
            actors,
            cpu_millicores: cpu,
            memory_bytes: mem,
        })
    };
    let empty = || Some(pb::WorkerResources::default());
    run_cases(
        vec![
            ("valid", req(1, ips(), empty()), vec![]),
            (
                "dual-stack IPs",
                req(1, vec!["10.0.0.1".into(), "fd00::1".into()], empty()),
                vec![],
            ),
            (
                "missing capacity",
                req(1, ips(), None),
                vec![required("capacity")],
            ),
            (
                "missing epoch",
                req(0, ips(), empty()),
                vec![required("epoch")],
            ),
            (
                "negative epoch",
                req(-1, ips(), empty()),
                vec![minimum("epoch")],
            ),
            (
                "missing ips",
                req(1, vec![], empty()),
                vec![required("ips")],
            ),
            (
                "too many ips",
                req(
                    1,
                    vec!["10.0.0.1".into(), "fd00::1".into(), "10.0.0.2".into()],
                    empty(),
                ),
                vec![too_many("ips")],
            ),
            (
                "full capacity",
                req(1, ips(), cap(4, 4000, 8 << 30)),
                vec![],
            ),
            (
                "negative actors",
                req(1, ips(), cap(-1, 0, 0)),
                vec![minimum("capacity.actors")],
            ),
            (
                "negative cpu_millicores",
                req(1, ips(), cap(0, -1, 0)),
                vec![minimum("capacity.cpu_millicores")],
            ),
            (
                "cpu_millicores above the cap",
                req(1, ips(), cap(0, 1_000_000, 0)),
                vec![maximum("capacity.cpu_millicores")],
            ),
            (
                "negative memory_bytes",
                req(1, ips(), cap(0, 0, -1)),
                vec![minimum("capacity.memory_bytes")],
            ),
        ],
        validate_register_worker_request,
    );
}

#[test]
fn workload_spec_rules() {
    let v = || pb::WorkloadSpec {
        volumes: vec![pb::Volume {
            name: "data".into(),
            durable_dir: Some(pb::DurableDirVolumeSource {}),
            ..Default::default()
        }],
        containers: vec![digest_container("main"), digest_container("sidecar")],
    };
    let mounts = |ms: &[(&str, &str)]| -> Vec<pb::VolumeMount> {
        ms.iter()
            .map(|(n, p)| pb::VolumeMount {
                name: n.to_string(),
                mount_path: p.to_string(),
            })
            .collect()
    };
    run_cases(
        vec![
            ("valid", v(), vec![]),
            ("empty spec", pb::WorkloadSpec::default(), vec![]),
            (
                "no volumes: the delete flow may send containers only",
                with(v(), |s| s.volumes.clear()),
                vec![],
            ),
            (
                "duplicate volume names",
                with(v(), |s| {
                    s.volumes.push(pb::Volume {
                        name: "data".into(),
                        image: Some(pb::ImageVolumeSource {
                            reference: DIGEST_IMAGE.into(),
                        }),
                        ..Default::default()
                    })
                }),
                vec![duplicate("volumes[1]")],
            ),
            (
                "too many volumes",
                with(v(), |s| {
                    s.volumes = (0..33)
                        .map(|i| pb::Volume {
                            name: format!("v{i}"),
                            durable_dir: Some(pb::DurableDirVolumeSource {}),
                            ..Default::default()
                        })
                        .collect()
                }),
                vec![too_many("volumes")],
            ),
            (
                "volume errors surface at the volume's path",
                with(v(), |s| s.volumes[0].durable_dir = None),
                vec![union("volumes[0]")],
            ),
            (
                "duplicate container names",
                with(v(), |s| {
                    s.containers = vec![digest_container("main"), digest_container("main")]
                }),
                vec![duplicate("containers[1]")],
            ),
            (
                "too many containers",
                with(v(), |s| {
                    s.containers = "abcdefghijk"
                        .chars()
                        .map(|c| digest_container(&c.to_string()))
                        .collect()
                }),
                vec![too_many("containers")],
            ),
            (
                "container errors surface at the container's path",
                with(v(), |s| s.containers[0].resources = limits(-1, 0)),
                vec![minimum("containers[0].resources.limits.cpu_millicores")],
            ),
            (
                "mount references a declared volume",
                with(v(), |s| {
                    s.containers[0].volume_mounts = mounts(&[("data", "/data")])
                }),
                vec![],
            ),
            (
                "mount references an undeclared volume",
                with(v(), |s| {
                    s.containers[1].volume_mounts =
                        mounts(&[("data", "/data"), ("ghost", "/ghost")])
                }),
                vec![invalid("containers[1].volume_mounts[1].name")],
            ),
        ],
        |op, s| workload_spec(op, &Path::root(), s),
    );
}

#[test]
fn volume_rules() {
    let durable = || pb::Volume {
        name: "data".into(),
        durable_dir: Some(pb::DurableDirVolumeSource {}),
        ..Default::default()
    };
    let external = |f: &dyn Fn(&mut pb::ExternalVolumeSource)| {
        let mut e = pb::ExternalVolumeSource {
            storage_volume_id: "projects/p/zones/z/disks/vol-1".into(),
            volume_type: "hivekeep.io/mock".into(),
            volume_context: [("fsType".to_string(), "ext4".to_string())].into(),
            ..Default::default()
        };
        f(&mut e);
        pb::Volume {
            name: "data".into(),
            external: Some(e),
            ..Default::default()
        }
    };
    let image = |r: &str| pb::Volume {
        name: "data".into(),
        image: Some(pb::ImageVolumeSource {
            reference: r.into(),
        }),
        ..Default::default()
    };
    let system_info = |ds: Vec<pb::SystemInfoDataSource>| pb::Volume {
        name: "data".into(),
        system_info: Some(pb::SystemInfoVolumeSource { data_sources: ds }),
        ..Default::default()
    };
    let bundle_names = |names: Vec<&str>, path: &str| pb::SystemInfoDataSource {
        trust_bundle: Some(pb::TrustBundleDataSource {
            names: names.into_iter().map(String::from).collect(),
            path: path.into(),
        }),
        ..Default::default()
    };
    let bundle = |name: &str, path: &str| bundle_names(vec![name], path);
    let item = |f: i32, path: &str| pb::ActorMetadataItem {
        field: f,
        path: path.into(),
    };
    let metadata = |items: Vec<pb::ActorMetadataItem>| pb::SystemInfoDataSource {
        actor_metadata: Some(pb::ActorMetadataDataSource { items }),
        ..Default::default()
    };
    let name_f = pb::ActorMetadataField::Name as i32;
    let n254 = "n".repeat(254);
    let p256 = "p".repeat(256);
    run_cases(
        vec![
            // Volume.
            ("valid durable dir", durable(), vec![]),
            (
                "missing name",
                with(durable(), |v| v.name.clear()),
                vec![required("name")],
            ),
            (
                "invalid name: uppercase",
                with(durable(), |v| v.name = "Data".into()),
                vec![dns_label("name")],
            ),
            (
                "no source set",
                with(durable(), |v| v.durable_dir = None),
                vec![union("")],
            ),
            (
                "two sources set",
                with(durable(), |v| {
                    v.external = Some(pb::ExternalVolumeSource {
                        storage_volume_id: "vol-1".into(),
                        ..Default::default()
                    })
                }),
                vec![union("")],
            ),
            // ExternalVolumeSource.
            ("valid external", external(&|_| {}), vec![]),
            (
                "external: missing storage_volume_id",
                external(&|e| e.storage_volume_id.clear()),
                vec![required("external.storage_volume_id")],
            ),
            (
                "external: storage_volume_id with a control character",
                external(&|e| e.storage_volume_id = "vol\x01".into()),
                vec![invalid("external.storage_volume_id")],
            ),
            (
                "external: storage_volume_id too long",
                external(&|e| e.storage_volume_id = "x".repeat(257)),
                vec![too_long("external.storage_volume_id")],
            ),
            (
                "external: unset volume_type is allowed",
                external(&|e| e.volume_type.clear()),
                vec![],
            ),
            (
                "external: invalid volume_type: uppercase",
                external(&|e| e.volume_type = "hivekeep.io/Mock".into()),
                vec![invalid("external.volume_type")],
            ),
            (
                "external: volume_context with one long value, as a CSI driver may return",
                external(&|e| e.volume_context = [("k".to_string(), "v".repeat(4000))].into()),
                vec![],
            ),
            (
                "external: volume_context over the size bound",
                external(&|e| e.volume_context = [("k".to_string(), "v".repeat(40 * 1024))].into()),
                vec![(ErrorKind::TooLong, "external.volume_context", None)],
            ),
            (
                "external: publish_context with many entries, as a CSI driver may return",
                external(&|e| {
                    e.publish_context = (0..100)
                        .map(|i| (format!("key-{i:03}"), "v".to_string()))
                        .collect()
                }),
                vec![],
            ),
            (
                "external: publish_context over the size bound",
                external(&|e| {
                    e.publish_context = [
                        ("a".to_string(), "v".repeat(20 * 1024)),
                        ("b".to_string(), "v".repeat(20 * 1024)),
                    ]
                    .into()
                }),
                vec![(ErrorKind::TooLong, "external.publish_context", None)],
            ),
            // ImageVolumeSource.
            ("valid image", image(DIGEST_IMAGE), vec![]),
            (
                "image: missing reference",
                image(""),
                vec![required("image.reference")],
            ),
            (
                "image: reference not pinned by digest",
                image("example.com/app:v1"),
                vec![invalid("image.reference")],
            ),
            // SystemInfoVolumeSource and SystemInfoDataSource.
            (
                "valid system info",
                system_info(vec![
                    bundle("podcert", "a.pem"),
                    bundle("podcert", "b.pem"),
                    metadata(vec![item(name_f, "name")]),
                ]),
                vec![],
            ),
            (
                "system info: data source with neither set",
                system_info(vec![pb::SystemInfoDataSource::default()]),
                vec![union("system_info.data_sources[0]")],
            ),
            (
                "system info: data source with both set",
                system_info(vec![pb::SystemInfoDataSource {
                    actor_metadata: Some(pb::ActorMetadataDataSource {
                        items: vec![item(name_f, "name")],
                    }),
                    trust_bundle: Some(pb::TrustBundleDataSource {
                        names: vec!["podcert".into()],
                        path: "p".into(),
                    }),
                }]),
                vec![union("system_info.data_sources[0]")],
            ),
            (
                "system info: duplicate path across trust bundles",
                system_info(vec![bundle("podcert", "a.pem"), bundle("podcert", "a.pem")]),
                vec![duplicate("system_info.data_sources[1].trust_bundle.path")],
            ),
            (
                "system info: duplicate path between bundle and metadata item",
                system_info(vec![
                    bundle("podcert", "name"),
                    metadata(vec![item(name_f, "name")]),
                ]),
                vec![duplicate(
                    "system_info.data_sources[1].actor_metadata.items[0].path",
                )],
            ),
            (
                "system info: second actor_metadata entry",
                system_info(vec![
                    metadata(vec![item(name_f, "name")]),
                    metadata(vec![item(name_f, "name-2")]),
                ]),
                vec![forbidden("system_info.data_sources[1].actor_metadata")],
            ),
            (
                "system info: too many data sources",
                system_info(
                    "abcdefghi"
                        .chars()
                        .map(|c| bundle("podcert", &c.to_string()))
                        .collect(),
                ),
                vec![too_many("system_info.data_sources")],
            ),
            // TrustBundleDataSource.
            (
                "trust bundle: missing path",
                system_info(vec![bundle("podcert", "")]),
                vec![required("system_info.data_sources[0].trust_bundle.path")],
            ),
            (
                "trust bundle: absolute path",
                system_info(vec![bundle("podcert", "/trust/bundle.pem")]),
                vec![invalid("system_info.data_sources[0].trust_bundle.path")],
            ),
            (
                "trust bundle: path too long",
                system_info(vec![bundle("podcert", &p256)]),
                vec![too_long("system_info.data_sources[0].trust_bundle.path")],
            ),
            (
                "trust bundle: no names",
                system_info(vec![bundle_names(vec![], "trust/bundle.pem")]),
                vec![required("system_info.data_sources[0].trust_bundle.names")],
            ),
            (
                "trust bundle: several names",
                system_info(vec![bundle_names(
                    vec!["egress-mitm.hivekeep.io", "system-roots.hivekeep.io"],
                    "trust/bundle.pem",
                )]),
                vec![],
            ),
            (
                "trust bundle: too many names",
                system_info(vec![bundle_names(
                    vec!["a", "b", "c", "d", "e", "f", "g", "h", "i"],
                    "trust/bundle.pem",
                )]),
                vec![too_many("system_info.data_sources[0].trust_bundle.names")],
            ),
            (
                "trust bundle: duplicate names",
                system_info(vec![bundle_names(vec!["a", "a"], "trust/bundle.pem")]),
                vec![duplicate(
                    "system_info.data_sources[0].trust_bundle.names[1]",
                )],
            ),
            (
                "trust bundle: empty name",
                system_info(vec![bundle("", "trust/bundle.pem")]),
                vec![invalid_by(
                    "system_info.data_sources[0].trust_bundle.names[0]",
                    "minLength",
                )],
            ),
            (
                "trust bundle: name too long",
                system_info(vec![bundle(&n254, "trust/bundle.pem")]),
                vec![too_long(
                    "system_info.data_sources[0].trust_bundle.names[0]",
                )],
            ),
            // ActorMetadataDataSource.
            (
                "actor metadata: several fields",
                system_info(vec![metadata(vec![
                    item(name_f, "name"),
                    item(pb::ActorMetadataField::Uid as i32, "ids/uid"),
                ])]),
                vec![],
            ),
            (
                "actor metadata: empty items",
                system_info(vec![metadata(vec![])]),
                vec![required("system_info.data_sources[0].actor_metadata.items")],
            ),
            (
                "actor metadata: same field projected twice",
                system_info(vec![metadata(vec![item(name_f, "a"), item(name_f, "b")])]),
                vec![duplicate(
                    "system_info.data_sources[0].actor_metadata.items[1]",
                )],
            ),
            (
                "actor metadata: unspecified field",
                system_info(vec![metadata(vec![item(0, "a")])]),
                vec![required(
                    "system_info.data_sources[0].actor_metadata.items[0].field",
                )],
            ),
            (
                "actor metadata: field outside the enum",
                system_info(vec![metadata(vec![item(4, "a")])]),
                vec![maximum(
                    "system_info.data_sources[0].actor_metadata.items[0].field",
                )],
            ),
            (
                "actor metadata: escaping item path",
                system_info(vec![metadata(vec![item(name_f, "../name")])]),
                vec![invalid(
                    "system_info.data_sources[0].actor_metadata.items[0].path",
                )],
            ),
        ],
        |op, v| volume(op, &Path::root(), v),
    );
}

#[test]
fn container_rules() {
    let v = || pb::Container {
        name: "main".into(),
        image: DIGEST_IMAGE.into(),
        command: vec!["/bin/app".into()],
        args: vec!["--serve".into()],
        env: vec![pb::EnvVar {
            name: "PORT".into(),
            value: "8080".into(),
        }],
        volume_mounts: vec![
            pb::VolumeMount {
                name: "data".into(),
                mount_path: "/data".into(),
            },
            pb::VolumeMount {
                name: "data".into(),
                mount_path: "/mnt/data".into(),
            },
        ],
        ..Default::default()
    };
    let env = |f: &dyn Fn(&mut pb::EnvVar)| with(v(), |c| f(&mut c.env[0]));
    let mount = |name: &str, path: &str| {
        with(v(), |c| {
            c.volume_mounts = vec![pb::VolumeMount {
                name: name.into(),
                mount_path: path.into(),
            }]
        })
    };
    let probe = |f: &dyn Fn(&mut pb::WakeupProbe)| {
        let mut p = pb::WakeupProbe {
            http_get: Some(pb::HttpGetAction {
                path: "/healthz".into(),
                port: 8080,
            }),
            timeout_seconds: 30,
        };
        f(&mut p);
        with(v(), |c| c.wakeup_probe = Some(p))
    };
    let hg = http_get_of;
    let caps = |add: &[&str], drop: &[&str]| {
        let add = add.iter().map(|s| s.to_string()).collect();
        let drop = drop.iter().map(|s| s.to_string()).collect();
        with(v(), |c| {
            c.security_context = Some(pb::SecurityContext {
                capabilities: Some(pb::Capabilities { add, drop }),
            })
        })
    };
    let res = |r: Option<pb::ResourceRequirements>| with(v(), |c| c.resources = r);
    run_cases(
        vec![
            ("valid: the same volume mounted at two paths", v(), vec![]),
            (
                "missing name",
                with(v(), |c| c.name.clear()),
                vec![required("name")],
            ),
            (
                "invalid name: uppercase",
                with(v(), |c| c.name = "Main".into()),
                vec![dns_label("name")],
            ),
            (
                "missing image",
                with(v(), |c| c.image.clear()),
                vec![required("image")],
            ),
            (
                "image not pinned by digest",
                with(v(), |c| c.image = "example.com/app:v1".into()),
                vec![invalid("image")],
            ),
            (
                "empty command and args are allowed",
                with(v(), |c| {
                    c.command.clear();
                    c.args.clear();
                }),
                vec![],
            ),
            (
                "too many command items",
                with(v(), |c| c.command = vec![String::new(); 65]),
                vec![too_many("command")],
            ),
            (
                "arg over the length guardrail",
                with(v(), |c| c.args = vec!["a".repeat(4097)]),
                vec![too_long("args[0]")],
            ),
            // EnvVar.
            (
                "duplicate env names",
                with(v(), |c| {
                    c.env.push(pb::EnvVar {
                        name: "PORT".into(),
                        value: "9".into(),
                    })
                }),
                vec![duplicate("env[1]")],
            ),
            (
                "env: missing name",
                env(&|e| e.name.clear()),
                vec![required("env[0].name")],
            ),
            (
                "env: name with equals sign",
                env(&|e| e.name = "A=B".into()),
                vec![invalid("env[0].name")],
            ),
            (
                "env: name too long",
                env(&|e| e.name = "N".repeat(257)),
                vec![too_long("env[0].name")],
            ),
            (
                "env: empty value is allowed",
                env(&|e| e.value.clear()),
                vec![],
            ),
            (
                "env: value too long",
                env(&|e| e.value = "v".repeat(32769)),
                vec![too_long("env[0].value")],
            ),
            // VolumeMount.
            (
                "duplicate mount paths",
                with(v(), |c| c.volume_mounts[1].mount_path = "/data".into()),
                vec![duplicate("volume_mounts[1]")],
            ),
            (
                "nested mount paths",
                with(v(), |c| {
                    c.volume_mounts[1].mount_path = "/data/nested".into()
                }),
                vec![invalid("volume_mounts[1].mount_path")],
            ),
            (
                "mount: missing name",
                mount("", "/var/data"),
                vec![required("volume_mounts[0].name")],
            ),
            (
                "mount: invalid name: uppercase",
                mount("Data", "/var/data"),
                vec![dns_label("volume_mounts[0].name")],
            ),
            (
                "mount: missing mount_path",
                mount("data", ""),
                vec![required("volume_mounts[0].mount_path")],
            ),
            (
                "mount: relative mount_path",
                mount("data", "var/data"),
                vec![invalid("volume_mounts[0].mount_path")],
            ),
            // WakeupProbe and HttpGetAction.
            ("valid probe", probe(&|_| {}), vec![]),
            (
                "probe: missing http_get",
                probe(&|p| p.http_get = None),
                vec![required("wakeup_probe.http_get")],
            ),
            (
                "probe: missing timeout",
                probe(&|p| p.timeout_seconds = 0),
                vec![required("wakeup_probe.timeout_seconds")],
            ),
            (
                "probe: timeout above the bound",
                probe(&|p| p.timeout_seconds = 3601),
                vec![maximum("wakeup_probe.timeout_seconds")],
            ),
            (
                "probe: missing path",
                probe(&|p| hg(p).path.clear()),
                vec![required("wakeup_probe.http_get.path")],
            ),
            (
                "probe: path without a leading slash",
                probe(&|p| hg(p).path = "healthz".into()),
                vec![invalid("wakeup_probe.http_get.path")],
            ),
            (
                "probe: missing port",
                probe(&|p| hg(p).port = 0),
                vec![required("wakeup_probe.http_get.port")],
            ),
            (
                "probe: port above the range",
                probe(&|p| hg(p).port = 65536),
                vec![maximum("wakeup_probe.http_get.port")],
            ),
            // SecurityContext.
            (
                "valid capabilities",
                caps(&["NET_BIND_SERVICE"], &["ALL"]),
                vec![],
            ),
            (
                "empty security context",
                with(v(), |c| {
                    c.security_context = Some(pb::SecurityContext::default())
                }),
                vec![],
            ),
            (
                "capabilities: add does not accept ALL",
                caps(&["ALL"], &[]),
                vec![invalid("security_context.capabilities.add[0]")],
            ),
            (
                "capabilities: lowercase rejected",
                caps(&[], &["net_bind_service"]),
                vec![invalid("security_context.capabilities.drop[0]")],
            ),
            (
                "capabilities: duplicate rejected by the set",
                caps(&["SYS_TIME", "SYS_TIME"], &[]),
                vec![duplicate("security_context.capabilities.add[1]")],
            ),
            // Resources.
            ("valid limits", res(limits(500, 1 << 30)), vec![]),
            ("limits: zero means unset", res(limits(0, 0)), vec![]),
            (
                "limits: negative memory_bytes",
                res(limits(0, -1)),
                vec![minimum("resources.limits.memory_bytes")],
            ),
            (
                "limits: negative cpu_millis",
                res(limits(-1, 0)),
                vec![minimum("resources.limits.cpu_millicores")],
            ),
            (
                "limits: cpu_millis at the cap",
                res(limits(999_999, 0)),
                vec![],
            ),
            (
                "limits: cpu_millis above the cap",
                res(limits(1_000_000, 0)),
                vec![maximum("resources.limits.cpu_millicores")],
            ),
        ],
        |op, c| container(op, &Path::root(), c, None),
    );
}

fn snap_of_checkpoint(r: &mut pb::CheckpointRequest) -> &mut pb::Snapshot {
    r.snapshot.as_mut().unwrap()
}
fn snap_of_restore(r: &mut pb::RestoreRequest) -> &mut pb::Snapshot {
    r.snapshot.as_mut().unwrap()
}
fn snap_of_promote(r: &mut pb::PromoteSnapshotRequest) -> &mut pb::Snapshot {
    r.snapshot.as_mut().unwrap()
}
fn http_get_of(p: &mut pb::WakeupProbe) -> &mut pb::HttpGetAction {
    p.http_get.as_mut().unwrap()
}
