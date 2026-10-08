//! Worker resource validation and the WorkerService requests keepers send.

mod common;

use common::*;
use hivekeep_core::field::{ErrorKind, ErrorList, Path};
use hivekeep_core::validation::api::*;
use hivekeep_core::validation::Operation;
use hivekeep_proto::api::v1 as pb;

// Worker names are pod UIDs, opaque to everything above the syncer.
const WORKER: &str = "5f2c1a90-7b34-4e6d-8a11-0c3e9d5b7f42";
const OTHER: &str = "1a7e4c83-6d20-4f95-b3c8-9e0a2f6d4b17";

fn run(cases: Vec<(&str, ErrorList, Vec<Want>)>) {
    for (name, got, want) in cases {
        assert_errs(name, got, &want);
    }
}

fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn oref(hive: &str, name: &str) -> Option<pb::ObjectRef> {
    Some(pb::ObjectRef {
        hive: hive.into(),
        name: name.into(),
    })
}

fn wref(name: &str) -> Option<pb::ObjectRef> {
    oref("", name)
}

/// A Worker in the shape CreateWorker accepts: named, with its pod
/// coordinates and no status (status is output-only).
fn valid_worker() -> pb::Worker {
    pb::Worker {
        metadata: Some(pb::ResourceMetadata {
            name: WORKER.into(),
            ..Default::default()
        }),
        worker_namespace: "hivekeep-system".into(),
        worker_pool: "pool-1".into(),
        worker_pod: "worker-pod-1".into(),
        worker_pod_uid: WORKER.into(),
        node_name: "node-1".into(),
        ..Default::default()
    }
}

fn wk(f: impl FnOnce(&mut pb::Worker)) -> pb::Worker {
    with(valid_worker(), f)
}

fn with_status(f: impl FnOnce(&mut pb::WorkerStatus)) -> pb::Worker {
    wk(|w| {
        w.status = Some(with(
            pb::WorkerStatus {
                state: pb::WorkerState::Active as i32,
                ..Default::default()
            },
            f,
        ))
    })
}

fn compat() -> pb::VersionedSandboxCompat {
    pb::VersionedSandboxCompat {
        schema_version: "v1".into(),
        attributes: vec![pb::AttributeEntry {
            key: "architecture".into(),
            value: "amd64".into(),
        }],
    }
}

/// A sandbox class with a default runtime and nothing restorable.
fn valid_class() -> pb::WorkerSandboxClass {
    pb::WorkerSandboxClass {
        sandbox_class: "gvisor".into(),
        default_runtime: Some(pb::WorkerSandboxRuntime {
            version: Some(compat()),
            actor_overhead: None,
        }),
        restorable_runtimes: vec![],
    }
}

/// `n` valid attributes with distinct keys.
fn attributes(n: usize) -> Vec<pb::AttributeEntry> {
    (0..n)
        .map(|i| pb::AttributeEntry {
            key: format!("k{i}"),
            value: "v".into(),
        })
        .collect()
}

/// `n` valid runtimes.
fn runtimes(n: usize) -> Vec<pb::WorkerSandboxRuntime> {
    (0..n)
        .map(|_| pb::WorkerSandboxRuntime {
            version: Some(compat()),
            actor_overhead: None,
        })
        .collect()
}

fn version(c: &mut pb::WorkerSandboxClass) -> &mut pb::VersionedSandboxCompat {
    c.default_runtime
        .as_mut()
        .unwrap()
        .version
        .as_mut()
        .unwrap()
}

#[test]
fn create_worker_request() {
    // The RPC scrubs status before validating; a status that is present is
    // still validated like any other field.
    let c = |w: Option<pb::Worker>| {
        validate_create_worker_request(&Operation::create(), &pb::CreateWorkerRequest { worker: w })
    };
    let m = |f: fn(&mut pb::ResourceMetadata)| Some(wk(|w| f(w.metadata.as_mut().unwrap())));
    let s = |f: fn(&mut pb::WorkerStatus)| Some(with_status(f));
    run(vec![
        ("valid unassigned worker", c(Some(valid_worker())), vec![]),
        ("valid with status", c(s(|_| {})), vec![]),
        ("missing worker", c(None), vec![required("worker")]),
        (
            "missing metadata",
            c(Some(wk(|w| w.metadata = None))),
            vec![required("worker.metadata")],
        ),
        (
            "missing metadata.name",
            c(m(|m| m.name.clear())),
            vec![required("worker.metadata.name")],
        ),
        (
            "invalid metadata.name",
            c(m(|m| m.name = "not a name".into())),
            vec![dns_label("worker.metadata.name")],
        ),
        (
            "metadata.hive set on a global-scoped Worker",
            c(m(|m| m.hive = "team-a".into())),
            vec![forbidden("worker.metadata.hive")],
        ),
        (
            "missing worker_namespace",
            c(Some(wk(|w| w.worker_namespace.clear()))),
            vec![required("worker.worker_namespace")],
        ),
        (
            "invalid worker_namespace",
            c(Some(wk(|w| w.worker_namespace = "NS-1".into()))),
            vec![dns_label("worker.worker_namespace")],
        ),
        (
            "missing worker_pool",
            c(Some(wk(|w| w.worker_pool.clear()))),
            vec![required("worker.worker_pool")],
        ),
        (
            "invalid worker_pool",
            c(Some(wk(|w| w.worker_pool = "POOL_1".into()))),
            vec![invalid_by("worker.worker_pool", "format=dns-subdomain")],
        ),
        (
            "missing worker_pod",
            c(Some(wk(|w| w.worker_pod.clear()))),
            vec![required("worker.worker_pod")],
        ),
        (
            "invalid worker_pod",
            c(Some(wk(|w| w.worker_pod = "POD_1".into()))),
            vec![invalid_by("worker.worker_pod", "format=dns-subdomain")],
        ),
        (
            "missing worker_pod_uid",
            c(Some(wk(|w| w.worker_pod_uid.clear()))),
            vec![required("worker.worker_pod_uid")],
        ),
        (
            "invalid worker_pod_uid",
            c(Some(wk(|w| w.worker_pod_uid = "INVALID-UUID".into()))),
            vec![invalid_by("worker.worker_pod_uid", "format=uuid")],
        ),
        (
            "missing node_name",
            c(Some(wk(|w| w.node_name.clear()))),
            vec![required("worker.node_name")],
        ),
        (
            "invalid node_name",
            c(Some(wk(|w| w.node_name = "NODE_NAME".into()))),
            vec![invalid_by("worker.node_name", "format=dns-subdomain")],
        ),
        (
            "valid dual-stack status.ips",
            c(s(|s| s.ips = vec!["fd00::1".into(), "10.1.2.3".into()])),
            vec![],
        ),
        (
            "invalid status.ips",
            c(s(|s| s.ips = vec!["not-an-ip".into()])),
            vec![invalid("worker.status.ips[0]")],
        ),
        (
            "non-canonical status.ips",
            c(s(|s| s.ips = vec!["fd00:0::1".into()])),
            vec![invalid("worker.status.ips[0]")],
        ),
        (
            "two status.ips in one family",
            c(s(|s| s.ips = vec!["10.1.2.3".into(), "10.1.2.4".into()])),
            vec![invalid("worker.status.ips[1]")],
        ),
        (
            "too many status.ips",
            c(s(|s| {
                s.ips = vec!["10.1.2.3".into(), "fd00::1".into(), "10.1.2.4".into()]
            })),
            vec![too_many("worker.status.ips")],
        ),
        (
            "negative status epochs",
            c(s(|s| {
                s.issued_epoch = -1;
                s.registered_epoch = -1;
            })),
            vec![
                invalid_by("worker.status.issued_epoch", "minimum"),
                invalid_by("worker.status.registered_epoch", "minimum"),
            ],
        ),
        (
            "valid worker_spiffe_id",
            c(Some(wk(|w| {
                w.worker_spiffe_id = "spiffe://cluster.local/ns/hivekeep-system/sa/worker".into()
            }))),
            vec![],
        ),
        (
            "invalid worker_spiffe_id",
            c(Some(wk(|w| {
                w.worker_spiffe_id = "https://cluster.local/ns/hivekeep-system/sa/worker".into()
            }))),
            vec![invalid("worker.worker_spiffe_id")],
        ),
        (
            // The SPIFFE ID grammar caps IDs at 2048 bytes too, so the
            // format check also reports the overlong ID.
            "worker_spiffe_id too long",
            c(Some(wk(|w| {
                w.worker_spiffe_id = format!("spiffe://cluster.local/{}", "a".repeat(2048))
            }))),
            vec![
                too_long("worker.worker_spiffe_id"),
                invalid("worker.worker_spiffe_id"),
            ],
        ),
        (
            "valid labels",
            c(Some(wk(|w| {
                w.labels = [
                    ("tier".to_string(), "batch".to_string()),
                    (
                        "pool.hivekeep.io/zone".to_string(),
                        "us-west1-c".to_string(),
                    ),
                ]
                .into()
            }))),
            vec![],
        ),
        (
            "too many labels",
            c(Some(wk(|w| {
                w.labels = (0..65)
                    .map(|i| (format!("key-{i}"), "v".to_string()))
                    .collect()
            }))),
            vec![(ErrorKind::TooMany, "worker.labels", Some("maxProperties"))],
        ),
        (
            "invalid label key",
            c(Some(wk(|w| {
                w.labels = [("bad key!".to_string(), "batch".to_string())].into()
            }))),
            vec![invalid_by("worker.labels", "format=label-key")],
        ),
        (
            "invalid label value",
            c(Some(wk(|w| {
                w.labels = [("tier".to_string(), "not valid!".to_string())].into()
            }))),
            vec![invalid_by("worker.labels[tier]", "format=label-value")],
        ),
        (
            "status needs a state",
            c(s(|s| s.state = 0)),
            vec![required("worker.status.state")],
        ),
        (
            "status invalid state (too small)",
            c(s(|s| s.state = -1)),
            vec![invalid_by("worker.status.state", "minimum")],
        ),
        (
            "status invalid state (too large)",
            c(s(|s| s.state = 99)),
            vec![invalid_by("worker.status.state", "maximum")],
        ),
    ]);
}

#[test]
fn delete_worker_request() {
    let c = |worker: Option<pb::ObjectRef>, options: Option<pb::DeleteOptions>| {
        validate_delete_worker_request(
            &Operation::create(),
            &pb::DeleteWorkerRequest { worker, options },
        )
    };
    let opts = |uid: &str, version: i64| {
        Some(pb::DeleteOptions {
            uid: uid.into(),
            version,
        })
    };
    run(vec![
        ("valid, no options", c(wref(WORKER), None), vec![]),
        (
            "valid, both guards",
            c(wref(WORKER), opts(OTHER, 3)),
            vec![],
        ),
        ("missing worker", c(None, None), vec![required("worker")]),
        (
            "missing worker.name",
            c(wref(""), None),
            vec![required("worker.name")],
        ),
        (
            "worker.hive must be empty",
            c(oref("team-a", WORKER), None),
            vec![forbidden("worker.hive")],
        ),
        (
            "invalid options.uid",
            c(wref(WORKER), opts("not-a-uuid", 0)),
            vec![invalid_by("options.uid", "format=uuid")],
        ),
        (
            "negative options.version",
            c(wref(WORKER), opts("", -1)),
            vec![invalid_by("options.version", "minimum")],
        ),
        (
            // Zero values waive the guards, so they are never shape-checked.
            "zero options are waived, not validated",
            c(wref(WORKER), opts("", 0)),
            vec![],
        ),
    ]);
}

#[test]
fn update_worker_request() {
    // Only the metadata that addresses the resource is checked; the body is
    // validated against the stored worker afterwards.
    let stored = || {
        wk(|w| {
            let m = w.metadata.as_mut().unwrap();
            m.uid = OTHER.into();
            m.version = 3;
        })
    };
    let m = |f: fn(&mut pb::ResourceMetadata)| {
        Some(with(stored(), |w| f(w.metadata.as_mut().unwrap())))
    };
    let c = |w: Option<pb::Worker>| {
        validate_update_worker_request(&Operation::create(), &pb::UpdateWorkerRequest { worker: w })
    };
    run(vec![
        ("valid", c(Some(stored())), vec![]),
        (
            // The store insists on the preconditions, not the request check.
            "missing uid and version pass request validation",
            c(m(|m| {
                m.uid.clear();
                m.version = 0;
            })),
            vec![],
        ),
        ("missing worker", c(None), vec![required("worker")]),
        (
            "missing metadata",
            c(Some(with(stored(), |w| w.metadata = None))),
            vec![required("worker.metadata")],
        ),
        (
            "missing metadata.name",
            c(m(|m| m.name.clear())),
            vec![required("worker.metadata.name")],
        ),
        (
            "invalid metadata.name",
            c(m(|m| m.name = "Not A Name".into())),
            vec![dns_label("worker.metadata.name")],
        ),
        (
            "invalid metadata.uid",
            c(m(|m| m.uid = "not-a-uuid".into())),
            vec![invalid_by("worker.metadata.uid", "format=uuid")],
        ),
        (
            "metadata.hive set on a global-scoped Worker",
            c(m(|m| m.hive = "team-a".into())),
            vec![forbidden("worker.metadata.hive")],
        ),
    ]);
}

#[test]
fn worker_update_require_status() {
    // The server always sets status before storing, so only a direct call
    // shows the guard catching a worker without one.
    let old = with_status(|_| {});
    let new = wk(|_| {});
    let p = Path::from("worker");
    assert_errs(
        "required",
        validate_worker_update(&Operation::update(), &p, &new, &old, true),
        &[required("worker.status")],
    );
    // Status is optional in the schema, and clearing it is otherwise free.
    assert_errs(
        "not required",
        validate_worker_update(&Operation::update(), &p, &new, &old, false),
        &[],
    );
}

#[test]
fn worker_update_identity_is_immutable() {
    let old = with_status(|_| {});
    let up = |new: pb::Worker| {
        validate_worker_update(&Operation::update(), &Path::root(), &new, &old, true)
    };
    run(vec![
        ("unchanged", up(old.clone()), vec![]),
        (
            "labels change",
            up(with(old.clone(), |w| {
                w.labels = [("a".to_string(), "b".to_string())].into()
            })),
            vec![],
        ),
        (
            "pod moved",
            up(with(old.clone(), |w| w.worker_pod = "pod2".into())),
            vec![invalid_by("worker_pod", "immutable")],
        ),
        (
            "node moved",
            up(with(old.clone(), |w| w.node_name = "node2".into())),
            vec![invalid_by("node_name", "immutable")],
        ),
    ]);
}

/// The ref cases shared by every request naming only a Worker.
fn worker_ref_cases(f: impl Fn(Option<pb::ObjectRef>) -> ErrorList) {
    run(vec![
        ("valid", f(wref(WORKER)), vec![]),
        ("missing worker", f(None), vec![required("worker")]),
        (
            "worker.hive must be empty",
            f(oref("team-a", WORKER)),
            vec![forbidden("worker.hive")],
        ),
        (
            "missing worker.name",
            f(wref("")),
            vec![required("worker.name")],
        ),
    ]);
}

#[test]
fn acquire_worker_epoch_request() {
    worker_ref_cases(|worker| {
        validate_acquire_worker_epoch_request(
            &Operation::create(),
            &pb::AcquireWorkerEpochRequest { worker },
        )
    });
}

#[test]
fn worker_ref_requests() {
    let op = Operation::create();
    worker_ref_cases(|worker| validate_get_worker_request(&op, &pb::GetWorkerRequest { worker }));
    worker_ref_cases(|worker| {
        validate_drain_worker_request(&op, &pb::DrainWorkerRequest { worker })
    });
    worker_ref_cases(|worker| {
        validate_mark_worker_available_request(&op, &pb::MarkWorkerAvailableRequest { worker })
    });
    worker_ref_cases(|worker| {
        validate_mark_worker_unavailable_request(&op, &pb::MarkWorkerUnavailableRequest { worker })
    });
}

fn valid_register() -> pb::RegisterWorkerRequest {
    pb::RegisterWorkerRequest {
        worker: wref(WORKER),
        epoch: 1,
        ips: vec!["10.1.2.3".into()],
        capacity: Some(pb::WorkerResources {
            actors: 10,
            cpu_millicores: 2000,
            memory_bytes: 4 << 30,
        }),
    }
}

#[test]
fn register_worker_request() {
    let c = |f: fn(&mut pb::RegisterWorkerRequest)| {
        validate_register_worker_request(&Operation::create(), &with(valid_register(), f))
    };
    run(vec![
        ("valid", c(|_| {}), vec![]),
        (
            "valid empty capacity",
            c(|r| r.capacity = Some(pb::WorkerResources::default())),
            vec![],
        ),
        (
            "valid dual-stack ips",
            c(|r| r.ips = vec!["fd00::1".into(), "10.1.2.3".into()]),
            vec![],
        ),
        (
            "missing worker",
            c(|r| r.worker = None),
            vec![required("worker")],
        ),
        (
            "worker.hive must be empty",
            c(|r| r.worker.as_mut().unwrap().hive = "team-a".into()),
            vec![forbidden("worker.hive")],
        ),
        (
            "missing worker.name",
            c(|r| r.worker.as_mut().unwrap().name.clear()),
            vec![required("worker.name")],
        ),
        (
            "invalid worker.name",
            c(|r| r.worker.as_mut().unwrap().name = "UPPER".into()),
            vec![dns_label("worker.name")],
        ),
        ("missing epoch", c(|r| r.epoch = 0), vec![required("epoch")]),
        (
            "negative epoch",
            c(|r| r.epoch = -1),
            vec![invalid_by("epoch", "minimum")],
        ),
        ("missing ips", c(|r| r.ips.clear()), vec![required("ips")]),
        (
            "invalid ip",
            c(|r| r.ips = vec!["not-an-ip".into()]),
            vec![invalid("ips[0]")],
        ),
        (
            "two ips in one family",
            c(|r| r.ips = vec!["10.1.2.3".into(), "10.1.2.4".into()]),
            vec![invalid("ips[1]")],
        ),
        (
            "too many ips",
            c(|r| r.ips = vec!["10.1.2.3".into(), "fd00::1".into(), "10.1.2.4".into()]),
            vec![too_many("ips")],
        ),
        (
            "missing capacity",
            c(|r| r.capacity = None),
            vec![required("capacity")],
        ),
        (
            "negative capacity.actors",
            c(|r| r.capacity.as_mut().unwrap().actors = -1),
            vec![invalid_by("capacity.actors", "minimum")],
        ),
        (
            "negative capacity.memory_bytes",
            c(|r| r.capacity.as_mut().unwrap().memory_bytes = -1),
            vec![invalid_by("capacity.memory_bytes", "minimum")],
        ),
        (
            "negative capacity.cpu_millicores",
            c(|r| r.capacity.as_mut().unwrap().cpu_millicores = -1),
            vec![invalid_by("capacity.cpu_millicores", "minimum")],
        ),
    ]);
}

#[test]
fn set_worker_sandbox_runtimes_request() {
    let c = |f: &dyn Fn(&mut pb::SetWorkerSandboxRuntimesRequest)| {
        let mut r = pb::SetWorkerSandboxRuntimesRequest {
            worker: wref(WORKER),
            epoch: 1,
            sandbox_classes: vec![valid_class()],
        };
        f(&mut r);
        validate_set_worker_sandbox_runtimes_request(&Operation::create(), &r)
    };
    let cl = "sandbox_classes[0]";
    let dv = "sandbox_classes[0].default_runtime.version";
    let at = |s: &str| leak(format!("{dv}.{s}"));
    let rr = "sandbox_classes[0].restorable_runtimes";
    run(vec![
        ("valid", c(&|_| {}), vec![]),
        (
            "valid with restorable runtimes",
            c(&|r| r.sandbox_classes[0].restorable_runtimes = runtimes(1)),
            vec![],
        ),
        (
            "missing worker",
            c(&|r| r.worker = None),
            vec![required("worker")],
        ),
        (
            "worker.hive must be empty",
            c(&|r| r.worker.as_mut().unwrap().hive = "team-a".into()),
            vec![forbidden("worker.hive")],
        ),
        (
            "missing epoch",
            c(&|r| r.epoch = 0),
            vec![required("epoch")],
        ),
        (
            "negative epoch",
            c(&|r| r.epoch = -1),
            vec![invalid_by("epoch", "minimum")],
        ),
        (
            // An empty and an absent list are the same on the wire.
            "missing sandbox_classes",
            c(&|r| r.sandbox_classes.clear()),
            vec![required("sandbox_classes")],
        ),
        (
            "too many sandbox_classes",
            c(&|r| {
                r.sandbox_classes
                    .push(with(valid_class(), |c| c.sandbox_class = "microvm".into()))
            }),
            vec![too_many("sandbox_classes")],
        ),
        (
            "missing sandbox_classes.sandbox_class",
            c(&|r| r.sandbox_classes[0].sandbox_class.clear()),
            vec![required(leak(format!("{cl}.sandbox_class")))],
        ),
        (
            "sandbox_classes.sandbox_class too long",
            c(&|r| r.sandbox_classes[0].sandbox_class = "c".repeat(64)),
            vec![
                too_long(leak(format!("{cl}.sandbox_class"))),
                not_supported(leak(format!("{cl}.sandbox_class"))),
            ],
        ),
        (
            "unsupported sandbox_classes.sandbox_class",
            c(&|r| r.sandbox_classes[0].sandbox_class = "kata".into()),
            vec![not_supported(leak(format!("{cl}.sandbox_class")))],
        ),
        (
            "missing sandbox_classes.default_runtime",
            c(&|r| r.sandbox_classes[0].default_runtime = None),
            vec![required(leak(format!("{cl}.default_runtime")))],
        ),
        (
            "missing default_runtime.version",
            c(&|r| {
                r.sandbox_classes[0]
                    .default_runtime
                    .as_mut()
                    .unwrap()
                    .version = None
            }),
            vec![required(dv)],
        ),
        (
            "missing default_runtime.schema_version",
            c(&|r| version(&mut r.sandbox_classes[0]).schema_version.clear()),
            vec![required(at("schema_version"))],
        ),
        (
            "default_runtime.schema_version too long",
            c(&|r| version(&mut r.sandbox_classes[0]).schema_version = "v".repeat(65)),
            vec![too_long(at("schema_version"))],
        ),
        (
            "missing default_runtime.attributes",
            c(&|r| version(&mut r.sandbox_classes[0]).attributes.clear()),
            vec![required(at("attributes"))],
        ),
        (
            "duplicate attribute key",
            c(&|r| {
                version(&mut r.sandbox_classes[0]).attributes = vec![
                    pb::AttributeEntry {
                        key: "architecture".into(),
                        value: "amd64".into(),
                    },
                    pb::AttributeEntry {
                        key: "architecture".into(),
                        value: "arm64".into(),
                    },
                ]
            }),
            vec![duplicate(at("attributes[1]"))],
        ),
        (
            "missing attribute key",
            c(&|r| {
                version(&mut r.sandbox_classes[0]).attributes = vec![pb::AttributeEntry {
                    key: String::new(),
                    value: "amd64".into(),
                }]
            }),
            vec![required(at("attributes[0].key"))],
        ),
        (
            "attribute key too long",
            c(&|r| {
                version(&mut r.sandbox_classes[0]).attributes = vec![pb::AttributeEntry {
                    key: "k".repeat(129),
                    value: "v".into(),
                }]
            }),
            vec![too_long(at("attributes[0].key"))],
        ),
        (
            "missing attribute value",
            c(&|r| {
                version(&mut r.sandbox_classes[0]).attributes = vec![pb::AttributeEntry {
                    key: "k".into(),
                    value: String::new(),
                }]
            }),
            vec![required(at("attributes[0].value"))],
        ),
        (
            "attribute value too long",
            c(&|r| {
                version(&mut r.sandbox_classes[0]).attributes = vec![pb::AttributeEntry {
                    key: "k".into(),
                    value: "v".repeat(257),
                }]
            }),
            vec![too_long(at("attributes[0].value"))],
        ),
        (
            "attributes at the bound",
            c(&|r| version(&mut r.sandbox_classes[0]).attributes = attributes(32)),
            vec![],
        ),
        (
            "too many attributes",
            c(&|r| version(&mut r.sandbox_classes[0]).attributes = attributes(33)),
            vec![too_many(at("attributes"))],
        ),
        (
            "invalid restorable_runtimes entry",
            c(&|r| {
                r.sandbox_classes[0].restorable_runtimes = vec![pb::WorkerSandboxRuntime {
                    version: Some(pb::VersionedSandboxCompat {
                        schema_version: String::new(),
                        attributes: attributes(1),
                    }),
                    actor_overhead: None,
                }]
            }),
            vec![required(leak(format!("{rr}[0].version.schema_version")))],
        ),
        (
            "restorable_runtimes entry without a version",
            c(&|r| {
                r.sandbox_classes[0].restorable_runtimes = vec![pb::WorkerSandboxRuntime::default()]
            }),
            vec![required(leak(format!("{rr}[0].version")))],
        ),
        (
            "restorable_runtimes at the bound",
            c(&|r| r.sandbox_classes[0].restorable_runtimes = runtimes(32)),
            vec![],
        ),
        (
            "too many restorable_runtimes",
            c(&|r| r.sandbox_classes[0].restorable_runtimes = runtimes(33)),
            vec![too_many(rr)],
        ),
        (
            "actor overhead above its maximum",
            c(&|r| {
                r.sandbox_classes[0]
                    .default_runtime
                    .as_mut()
                    .unwrap()
                    .actor_overhead = Some(pb::RuntimeOverhead {
                    memory_bytes: (1 << 40) + 1,
                    memory_bytes_per_gib: 1 << 30,
                })
            }),
            vec![invalid_by(
                "sandbox_classes[0].default_runtime.actor_overhead.memory_bytes",
                "maximum",
            )],
        ),
    ]);
}

#[test]
fn worker_update_sandbox_classes() {
    // Sandbox classes reach the store as status, which no request check
    // sees. The old worker already reports a class, so every mutation
    // (clearing the list included) differs from it and is validated.
    let old = with_status(|s| s.sandbox_classes = vec![valid_class()]);
    let c = |f: &dyn Fn(&mut pb::WorkerStatus)| {
        let mut new = old.clone();
        f(new.status.as_mut().unwrap());
        validate_worker_update(
            &Operation::update(),
            &Path::from("worker"),
            &new,
            &old,
            true,
        )
    };
    let cl = "worker.status.sandbox_classes[0]";
    let at = |s: &str| leak(format!("{cl}.{s}"));
    run(vec![
        (
            "valid runtimes",
            c(&|s| s.sandbox_classes[0].restorable_runtimes = runtimes(32)),
            vec![],
        ),
        (
            "empty sandbox_classes",
            c(&|s| s.sandbox_classes.clear()),
            vec![],
        ),
        (
            "too many sandbox_classes",
            c(&|s| {
                s.sandbox_classes
                    .push(with(valid_class(), |c| c.sandbox_class = "microvm".into()))
            }),
            vec![too_many("worker.status.sandbox_classes")],
        ),
        (
            "missing sandbox_classes.sandbox_class",
            c(&|s| s.sandbox_classes[0].sandbox_class.clear()),
            vec![required(at("sandbox_class"))],
        ),
        (
            "unsupported sandbox_classes.sandbox_class",
            c(&|s| s.sandbox_classes[0].sandbox_class = "kata".into()),
            vec![not_supported(at("sandbox_class"))],
        ),
        (
            "missing sandbox_classes.default_runtime",
            c(&|s| s.sandbox_classes[0].default_runtime = None),
            vec![required(at("default_runtime"))],
        ),
        (
            "missing sandbox_classes.default_runtime.version",
            c(&|s| {
                s.sandbox_classes[0]
                    .default_runtime
                    .as_mut()
                    .unwrap()
                    .version = None
            }),
            vec![required(at("default_runtime.version"))],
        ),
        (
            "too many default_runtime attributes",
            c(&|s| version(&mut s.sandbox_classes[0]).attributes = attributes(33)),
            vec![too_many(at("default_runtime.version.attributes"))],
        ),
        (
            "invalid restorable_runtimes entry",
            c(&|s| {
                s.sandbox_classes[0].restorable_runtimes = vec![pb::WorkerSandboxRuntime {
                    version: Some(pb::VersionedSandboxCompat {
                        schema_version: String::new(),
                        attributes: attributes(1),
                    }),
                    actor_overhead: None,
                }]
            }),
            vec![required(at(
                "restorable_runtimes[0].version.schema_version",
            ))],
        ),
        (
            "too many restorable_runtimes",
            c(&|s| s.sandbox_classes[0].restorable_runtimes = runtimes(33)),
            vec![too_many(at("restorable_runtimes"))],
        ),
    ]);
}

#[test]
fn request_actor_suspend_request() {
    let c = |f: fn(&mut pb::RequestActorSuspendRequest)| {
        validate_request_actor_suspend_request(
            &Operation::create(),
            &with(
                pb::RequestActorSuspendRequest {
                    worker: wref(WORKER),
                    actor: oref("team-a", "actor-1"),
                    actor_uid: OTHER.into(),
                },
                f,
            ),
        )
    };
    run(vec![
        ("valid", c(|_| {}), vec![]),
        (
            "missing worker",
            c(|r| r.worker = None),
            vec![required("worker")],
        ),
        (
            "worker.hive must be empty",
            c(|r| r.worker.as_mut().unwrap().hive = "team-a".into()),
            vec![forbidden("worker.hive")],
        ),
        (
            "missing worker.name",
            c(|r| r.worker.as_mut().unwrap().name.clear()),
            vec![required("worker.name")],
        ),
        (
            "invalid worker.name",
            c(|r| r.worker.as_mut().unwrap().name = "UPPER".into()),
            vec![dns_label("worker.name")],
        ),
        (
            "missing actor",
            c(|r| r.actor = None),
            vec![required("actor")],
        ),
        (
            "missing actor.hive",
            c(|r| r.actor.as_mut().unwrap().hive.clear()),
            vec![required("actor.hive")],
        ),
        (
            "invalid actor.hive",
            c(|r| r.actor.as_mut().unwrap().hive = "../escape".into()),
            vec![dns_label("actor.hive")],
        ),
        (
            "missing actor.name",
            c(|r| r.actor.as_mut().unwrap().name.clear()),
            vec![required("actor.name")],
        ),
        (
            "invalid actor.name",
            c(|r| r.actor.as_mut().unwrap().name = "UPPER".into()),
            vec![dns_label("actor.name")],
        ),
        (
            "missing actor_uid",
            c(|r| r.actor_uid.clear()),
            vec![required("actor_uid")],
        ),
        (
            "invalid actor_uid",
            c(|r| r.actor_uid = "not-a-uuid".into()),
            vec![invalid_by("actor_uid", "format=uuid")],
        ),
    ]);
}

#[test]
fn report_actor_crashed_request() {
    let c = |msg: String| {
        validate_report_actor_crashed_request(
            &Operation::create(),
            &pb::ReportActorCrashedRequest {
                worker: wref(WORKER),
                actor: oref("team-a", "actor-1"),
                actor_uid: OTHER.into(),
                message: msg,
            },
        )
    };
    run(vec![
        ("valid", c("exited 1".into()), vec![]),
        (
            "missing message",
            c(String::new()),
            vec![required("message")],
        ),
        (
            "message too long",
            c("x".repeat(4097)),
            vec![too_long("message")],
        ),
    ]);
}

fn report(generation: i32) -> pb::SnapshotReport {
    pb::SnapshotReport {
        uuid: format!("snap-{generation}"),
        generation,
        fidelity: pb::SnapshotFidelity::Memory as i32,
        object: Some(pb::ObjectSnapshot {
            snapshot_uri: format!("gs://bucket/actors/a/snap-{generation}"),
        }),
        local_commit_time: Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        ..Default::default()
    }
}

fn valid_report_request() -> pb::ReportActorSnapshotStateRequest {
    let now = prost_types::Timestamp {
        seconds: 1_700_000_000,
        nanos: 0,
    };
    pb::ReportActorSnapshotStateRequest {
        actor: oref("team-a", "actor-1"),
        actor_uid: OTHER.into(),
        state: Some(pb::ActorSnapshotState {
            latest_local: Some(report(2)),
            uploading: Some(report(2)),
            latest_durable: Some(pb::SnapshotReport {
                durable_complete_time: Some(now),
                ..report(1)
            }),
            upload_due_time: Some(now),
        }),
    }
}

fn st(r: &mut pb::ReportActorSnapshotStateRequest) -> &mut pb::ActorSnapshotState {
    r.state.as_mut().unwrap()
}

fn local(r: &mut pb::ReportActorSnapshotStateRequest) -> &mut pb::SnapshotReport {
    st(r).latest_local.as_mut().unwrap()
}

#[test]
fn report_actor_snapshot_state_request() {
    let c = |f: fn(&mut pb::ReportActorSnapshotStateRequest)| {
        validate_report_actor_snapshot_state_request(
            &Operation::create(),
            &with(valid_report_request(), f),
        )
    };
    let l = "state.latest_local";
    let at = |s: &str| leak(format!("{l}.{s}"));
    run(vec![
        ("valid", c(|_| {}), vec![]),
        (
            "valid detach: no local snapshot",
            c(|r| {
                let s = st(r);
                s.latest_local = None;
                s.uploading = None;
                s.upload_due_time = None;
            }),
            vec![],
        ),
        (
            "empty state",
            c(|r| r.state = Some(pb::ActorSnapshotState::default())),
            vec![required("state.latest_durable")],
        ),
        (
            "valid upload in flight with no local snapshot",
            c(|r| {
                let up = st(r).uploading.take();
                r.state = Some(pb::ActorSnapshotState {
                    uploading: up,
                    ..Default::default()
                });
            }),
            vec![],
        ),
        (
            "missing actor",
            c(|r| r.actor = None),
            vec![required("actor")],
        ),
        (
            "missing actor.hive",
            c(|r| r.actor.as_mut().unwrap().hive.clear()),
            vec![required("actor.hive")],
        ),
        (
            "invalid actor.name",
            c(|r| r.actor.as_mut().unwrap().name = "UPPER".into()),
            vec![dns_label("actor.name")],
        ),
        (
            "missing actor_uid",
            c(|r| r.actor_uid.clear()),
            vec![required("actor_uid")],
        ),
        (
            "invalid actor_uid",
            c(|r| r.actor_uid = "not-a-uuid".into()),
            vec![invalid_by("actor_uid", "format=uuid")],
        ),
        (
            "missing state",
            c(|r| r.state = None),
            vec![required("state")],
        ),
        (
            "missing uuid",
            c(|r| local(r).uuid.clear()),
            vec![required(at("uuid"))],
        ),
        (
            "invalid uuid",
            c(|r| local(r).uuid = "../escape".into()),
            vec![dns_label(at("uuid"))],
        ),
        (
            "generation below 1",
            c(|r| local(r).generation = -1),
            vec![invalid_by(at("generation"), "minimum")],
        ),
        (
            "missing fidelity",
            c(|r| local(r).fidelity = 0),
            vec![required(at("fidelity"))],
        ),
        (
            "unknown fidelity",
            c(|r| local(r).fidelity = 4),
            vec![invalid_by(at("fidelity"), "maximum")],
        ),
        (
            "missing object",
            c(|r| local(r).object = None),
            vec![required(at("object"))],
        ),
        (
            "missing object.snapshot_uri",
            c(|r| local(r).object.as_mut().unwrap().snapshot_uri.clear()),
            vec![required(at("object.snapshot_uri"))],
        ),
        (
            "valid without local_commit_time",
            c(|r| st(r).latest_durable.as_mut().unwrap().local_commit_time = None),
            vec![],
        ),
        (
            "uploading is validated",
            c(|r| st(r).uploading.as_mut().unwrap().uuid.clear()),
            vec![required("state.uploading.uuid")],
        ),
        (
            "latest_durable is validated",
            c(|r| st(r).latest_durable.as_mut().unwrap().object = None),
            vec![required("state.latest_durable.object")],
        ),
    ]);
}
