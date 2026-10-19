//! Unit-level checks of pure helpers through the public API.

use std::sync::Arc;

use hivekeep_api::authz::checks;
use hivekeep_api::authz::model::{rel, Object};
use hivekeep_api::convert;
use hivekeep_api::error::Error;
use hivekeep_api::leases::Ctx;
use hivekeep_api::metrics::worker_state;
use hivekeep_api::scheduling::{self, Constraints, Scheduler, WorkerSource};
use hivekeep_api::snapshots;
use hivekeep_api::workflow::crash::{handle_keeper_error, keeper_crash_message, new_crash};
use hivekeep_api::workflow::resume::{
    can_relax_runtime, required_snapshot_runtime, scheduling_constraints, should_detach_from_node,
    snapshot_sharing, worker_assignment_from,
};
use hivekeep_api::workflow::revert::is_revertable;
use hivekeep_api::workflow::suspend::SuspendMode;
use hivekeep_api::workload_spec;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::keeper::v1 as kp;
use tonic::{Code, Status};

struct Fleet(Vec<Arc<pb::Worker>>);
impl WorkerSource for Fleet {
    fn workers(&self) -> Result<Vec<Arc<pb::Worker>>, String> {
        Ok(self.0.clone())
    }
}

fn worker(name: &str, alloc: i32, cap: i32) -> pb::Worker {
    pb::Worker {
        metadata: Some(pb::ResourceMetadata {
            name: name.into(),
            ..Default::default()
        }),
        node_name: "n".into(),
        status: Some(pb::WorkerStatus {
            state: pb::WorkerState::Active as i32,
            registered_epoch: 1,
            capacity: Some(pb::WorkerResources {
                cpu_millicores: 4000,
                memory_bytes: 1 << 33,
                actors: cap,
            }),
            allocated: Some(pb::WorkerResources {
                actors: alloc,
                ..Default::default()
            }),
            sandbox_classes: vec![pb::WorkerSandboxClass {
                sandbox_class: "gvisor".into(),
                default_runtime: Some(pb::WorkerSandboxRuntime {
                    version: Some(pb::VersionedSandboxCompat {
                        schema_version: "v1".into(),
                        attributes: vec![],
                    }),
                    actor_overhead: None,
                }),
                restorable_runtimes: vec![],
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn name(w: &pb::Worker) -> String {
    w.metadata.as_ref().unwrap().name.clone()
}

#[test]
fn schedule_power_of_two_choices_tie_breaking() {
    // Equal load: the first sample wins.
    let fleet = || {
        Arc::new(Fleet(vec![
            Arc::new(worker("a", 1, 4)),
            Arc::new(worker("b", 1, 4)),
        ]))
    };
    let s = Scheduler::with_intn(fleet(), |_| 0);
    assert_eq!(name(&s.schedule(&Constraints::default()).unwrap()), "a");
    // Equal ratio, more free slots wins.
    let s = Scheduler::with_intn(
        Arc::new(Fleet(vec![
            Arc::new(worker("a", 1, 2)),
            Arc::new(worker("b", 2, 4)),
        ])),
        |_| 0,
    );
    assert_eq!(name(&s.schedule(&Constraints::default()).unwrap()), "b");
}

#[test]
fn schedule_power_of_two_choices_shifted_second_sample() {
    let ws: Vec<_> = ["a", "b", "c"]
        .iter()
        .map(|n| Arc::new(worker(n, 0, 4)))
        .collect();
    let mut busy = (*ws[2]).clone();
    busy.status
        .as_mut()
        .unwrap()
        .allocated
        .as_mut()
        .unwrap()
        .actors = 3;
    let fleet = Arc::new(Fleet(vec![ws[0].clone(), ws[1].clone(), Arc::new(busy)]));
    let calls = std::sync::Mutex::new(vec![1usize, 1]);
    // i = 1, j = 1 -> shifted to 2.
    let s = Scheduler::with_intn(fleet, move |_| calls.lock().unwrap().remove(0));
    assert_eq!(name(&s.schedule(&Constraints::default()).unwrap()), "b");
}

#[test]
fn registered_and_applies_ignore_room() {
    let full = worker("a", 4, 4);
    assert!(scheduling::registered(&full));
    assert!(scheduling::applies(&full, &Constraints::default()));
    assert!(!scheduling::has_room(&full, &Constraints::default()));
    let mut fresh = worker("b", 0, 4);
    fresh.status.as_mut().unwrap().sandbox_classes.clear();
    assert!(!scheduling::registered(&fresh));
}

#[test]
fn snapshot_helpers() {
    let mk = |gen, state: pb::SnapshotStorageState, loc: &str| {
        let mut s = snapshots::new_durable(
            gen,
            pb::SnapshotOwner::Actor,
            3,
            "t",
            &format!("u{gen}"),
            &format!("gs://b/a/u{gen}"),
            state,
        );
        s.locality = loc.into();
        s
    };
    let st = pb::ActorStatus {
        last_assigned_generation: 3,
        snapshots: vec![
            mk(1, pb::SnapshotStorageState::Completed, ""),
            mk(3, pb::SnapshotStorageState::Pending, "n"),
        ],
        ..Default::default()
    };
    assert_eq!(
        snapshots::at_latest_generation(Some(&st))
            .unwrap()
            .generation,
        3
    );
    assert!(snapshots::by_generation(Some(&st), 2).is_none());
    assert_eq!(snapshots::latest_local(Some(&st)).unwrap().generation, 3);
    assert_eq!(
        snapshots::latest_completed(Some(&st)).unwrap().generation,
        1
    );
    let tag = pb::Tag {
        status: Some(pb::TagStatus {
            snapshot: Some(mk(1, pb::SnapshotStorageState::Pending, "")),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(snapshots::tag_durable_uri(&tag, None), "gs://b/a/u1");
    assert_eq!(
        snapshots::tag_durable_uri(&tag, Some(pb::SnapshotStorageState::Completed)),
        ""
    );
}

#[test]
fn crash_messages() {
    assert_eq!(
        keeper_crash_message("Run", &Status::internal("x")),
        "keeper Run: x"
    );
    assert_eq!(new_crash("suspend", "boom").message, "suspend failed: boom");
}

#[tokio::test]
async fn handle_keeper_error_transport_errors_do_not_crash() {
    let store = hivekeep_store::MemoryStore::new();
    let m = hivekeep_api::metrics::Metrics::new();
    let r = hivekeep_core::resource::ActorRef::new("h", "a");
    let e = handle_keeper_error(
        &Ctx::background(),
        &store,
        &m,
        &r,
        "resume",
        "Run",
        false,
        Status::unavailable("down"),
    )
    .await;
    assert_eq!(e.code(), Code::Unavailable);
    let e = handle_keeper_error(
        &Ctx::background(),
        &store,
        &m,
        &r,
        "delete",
        "Terminate",
        true,
        Status::internal("x"),
    )
    .await;
    assert_eq!(e.code(), Code::Internal);
    // A crash attempt on a missing actor surfaces the load error.
    let e = handle_keeper_error(
        &Ctx::background(),
        &store,
        &m,
        &r,
        "resume",
        "Run",
        false,
        Status::internal("x"),
    )
    .await;
    assert_eq!(e.code(), Code::NotFound);
}

#[test]
fn converters() {
    assert_eq!(
        convert::fidelity_label(kp::SnapshotFidelity::Rootfs),
        "rootfs"
    );
    assert!(convert::runtime_to_keeper(None).is_none());
    let rt = pb::ActorSandboxRuntime {
        sandbox_class: "gvisor".into(),
        version: Some(pb::VersionedSandboxCompat {
            schema_version: "v1".into(),
            attributes: vec![pb::AttributeEntry {
                key: "k".into(),
                value: "v".into(),
            }],
        }),
    };
    let k = convert::runtime_to_keeper(Some(&rt)).unwrap();
    assert_eq!(k.version.unwrap().attributes[0].key, "k");
}

#[test]
fn workload_spec_propagates_probe_security_and_resources() {
    let t = pb::ActorTemplate {
        containers: vec![pb::Container {
            name: "c".into(),
            wakeup_probe: Some(pb::WakeupProbe {
                http_get: Some(pb::HttpGetAction {
                    path: "/ready".into(),
                    port: 8080,
                }),
                timeout_seconds: 5,
            }),
            security_context: Some(pb::SecurityContext {
                capabilities: Some(pb::Capabilities {
                    add: vec!["NET_ADMIN".into()],
                    drop: vec!["ALL".into()],
                }),
            }),
            resources: Some(pb::ResourceRequirements {
                limits: Some(pb::Resources {
                    cpu_millicores: 500,
                    memory_bytes: 0,
                }),
            }),
            ..Default::default()
        }],
        volumes: vec![
            pb::Volume {
                name: "img".into(),
                image: Some(pb::ImageVolumeSource {
                    reference: "x@sha256:00".into(),
                }),
                ..Default::default()
            },
            pb::Volume {
                name: "si".into(),
                system_info: Some(pb::SystemInfoVolumeSource {
                    data_sources: vec![pb::SystemInfoDataSource {
                        actor_metadata: Some(pb::ActorMetadataDataSource {
                            items: vec![pb::ActorMetadataItem {
                                field: pb::ActorMetadataField::Hive as i32,
                                path: "hive".into(),
                            }],
                        }),
                        trust_bundle: None,
                    }],
                }),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let s = workload_spec::from_template(&t, &pb::Actor::default(), &Default::default()).unwrap();
    let c = &s.containers[0];
    assert_eq!(
        c.wakeup_probe
            .as_ref()
            .unwrap()
            .http_get
            .as_ref()
            .unwrap()
            .port,
        8080
    );
    assert_eq!(
        c.security_context
            .as_ref()
            .unwrap()
            .capabilities
            .as_ref()
            .unwrap()
            .drop,
        vec!["ALL"]
    );
    assert_eq!(c.resources.unwrap().limits.unwrap().cpu_millicores, 500);
    assert_eq!(
        s.volumes[1].system_info.as_ref().unwrap().data_sources[0]
            .actor_metadata
            .as_ref()
            .unwrap()
            .items[0]
            .field,
        kp::ActorMetadataField::Hive as i32
    );
}

#[test]
fn worker_states() {
    assert_eq!(worker_state(&worker("a", 0, 2)), "idle");
    assert_eq!(worker_state(&worker("a", 2, 2)), "at_capacity");
    let mut d = worker("a", 0, 2);
    d.status.as_mut().unwrap().state = pb::WorkerState::Draining as i32;
    assert_eq!(worker_state(&d), "unschedulable");
}

fn actor_with(state: pb::ActorState, node: &str, snaps: Vec<pb::Snapshot>) -> pb::Actor {
    pb::Actor {
        status: Some(pb::ActorStatus {
            state: state as i32,
            assigned_node: node.into(),
            last_assigned_generation: snaps.iter().map(|s| s.generation).max().unwrap_or(0),
            snapshots: snaps,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn mem_snap(locality: &str, state: pb::SnapshotStorageState) -> pb::Snapshot {
    let mut s = snapshots::new_durable(
        1,
        pb::SnapshotOwner::Actor,
        pb::SnapshotFidelity::Memory as i32,
        "t",
        "u",
        "gs://b/a/u",
        state,
    );
    s.locality = locality.into();
    s.sandbox_runtime = Some(pb::ActorSandboxRuntime {
        sandbox_class: "gvisor".into(),
        version: None,
    });
    s
}

#[test]
fn required_snapshot_runtime_and_relax() {
    let tmpl = pb::ActorTemplate {
        metadata: Some(pb::ResourceMetadata {
            uid: "t".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let s = mem_snap("", pb::SnapshotStorageState::Completed);
    assert!(required_snapshot_runtime(Some(&s), &tmpl).is_some());
    let other = pb::ActorTemplate {
        metadata: Some(pb::ResourceMetadata {
            uid: "x".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(
        required_snapshot_runtime(Some(&s), &other).is_none(),
        "a replaced template restores data only"
    );
    let mut rootfs = s.clone();
    rootfs.fidelity = pb::SnapshotFidelity::Rootfs as i32;
    assert!(required_snapshot_runtime(Some(&rootfs), &tmpl).is_none());

    // Durable, not pinned: relaxable.
    let a = actor_with(pb::ActorState::Suspended, "", vec![s.clone()]);
    assert!(can_relax_runtime(&a, &scheduling_constraints(&a, &tmpl)));
    // Pinned to a node: detach first instead.
    let p = actor_with(
        pb::ActorState::Suspended,
        "n",
        vec![mem_snap("n", pb::SnapshotStorageState::Pending)],
    );
    assert!(!can_relax_runtime(&p, &scheduling_constraints(&p, &tmpl)));
    assert!(should_detach_from_node(&p));
    assert_eq!(scheduling_constraints(&p, &tmpl).required_nodes, vec!["n"]);
    // Cold boot: nothing to relax.
    let c = actor_with(pb::ActorState::Suspended, "", vec![]);
    assert!(!can_relax_runtime(&c, &scheduling_constraints(&c, &tmpl)));
}

#[test]
fn revertable_states_and_modes() {
    assert!(is_revertable(&actor_with(
        pb::ActorState::Running,
        "",
        vec![]
    )));
    assert!(is_revertable(&actor_with(
        pb::ActorState::Crashed,
        "",
        vec![]
    )));
    assert!(!is_revertable(&actor_with(
        pb::ActorState::Suspended,
        "",
        vec![]
    )));
    assert!(is_revertable(&actor_with(
        pb::ActorState::Suspended,
        "n",
        vec![mem_snap("n", pb::SnapshotStorageState::Pending)]
    )));
    assert!(!is_revertable(&actor_with(
        pb::ActorState::Deleting,
        "",
        vec![]
    )));
    assert_eq!(SuspendMode::from_options(None), SuspendMode::Async);
    assert_eq!(
        SuspendMode::from_options(Some(&pb::SuspendOptions {
            mode: pb::DurabilityMode::Synchronous as i32
        })),
        SuspendMode::Sync
    );
    assert_eq!(
        snapshot_sharing(pb::SnapshotOwner::Tag as i32),
        kp::SnapshotSharing::Shared
    );
    assert_eq!(snapshot_sharing(0), kp::SnapshotSharing::Private);
}

#[test]
fn worker_assignment_copies_spiffe_id() {
    let mut w = worker("w", 0, 1);
    w.worker_spiffe_id = "spiffe://cluster.local/ns/x/sa/w".into();
    let a = worker_assignment_from(&w, &Default::default());
    assert_eq!(a.worker_spiffe_id, w.worker_spiffe_id);
    assert_eq!(
        a.worker_epoch, 0,
        "the epoch is stamped when RUNNING commits"
    );
    assert_eq!(a.sandbox_runtime.unwrap().sandbox_class, "gvisor");
}

#[test]
fn rpc_checks_cover_lifecycle_egress_worker_and_tags() {
    let a = Some(pb::ObjectRef {
        hive: "h".into(),
        name: "a".into(),
    });
    for (relation, c) in [
        (
            rel::CAN_RESUME,
            checks::actor_ref(rel::CAN_RESUME, a.as_ref()),
        ),
        (
            rel::CAN_GET_EGRESS_POLICY,
            checks::actor_ref(rel::CAN_GET_EGRESS_POLICY, a.as_ref()),
        ),
        (
            rel::CAN_DELETE,
            checks::tag_ref(rel::CAN_DELETE, a.as_ref()),
        ),
        (
            rel::CAN_DRAIN,
            checks::worker_ref(
                rel::CAN_DRAIN,
                Some(&pb::ObjectRef {
                    hive: String::new(),
                    name: "w".into(),
                }),
            ),
        ),
    ] {
        assert_eq!(c.unwrap()[0].relation, relation);
    }
    let u = checks::update_actor(&pb::UpdateActorRequest {
        actor: Some(pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: "h".into(),
                name: "a".into(),
                ..Default::default()
            }),
            actor_template: Some(pb::ObjectRef {
                hive: "t".into(),
                name: "tmpl".into(),
            }),
            ..Default::default()
        }),
    })
    .unwrap();
    assert_eq!(
        u[0].object,
        Object::Actor {
            hive: "h".into(),
            name: "a".into()
        }
    );
    assert_eq!(u[1].relation, rel::CAN_USE_TEMPLATE);
    assert!(
        checks::update_actor(&pb::UpdateActorRequest::default()).is_err(),
        "fails closed"
    );
}

#[test]
fn errors_convert() {
    let e: Status = Error::NoWorkerRoom.into();
    assert_eq!(e.code(), Code::ResourceExhausted);
    let e: Status = Error::NoKeeperOnNode("n".into()).into();
    assert_eq!(e.code(), Code::Unavailable);
}
