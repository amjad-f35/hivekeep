//! Fixtures and helpers shared by the contract tests.

use std::time::Duration;

use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{Resource, Store, WorkerEvent, WorkerWatch};

/// The Hive used by tests that create a single Actor.
pub const HIVE: &str = "test-hive";

/// The Actor uid an external snapshot's prefix is keyed on. The store treats
/// snapshot URIs as opaque beyond their shape.
pub const SNAPSHOT_OWNER_UID: &str = "6b1f9d0c-4a2e-4d38-9c77-5e0a1b2c3d4e";

/// Matches no stored object, for guards that must miss.
pub const FOREIGN_UID: &str = "0f0e0d0c-0b0a-4908-8706-050403020100";

/// Worker names; opaque to the store.
pub const WORKER: &str = "3f9a5c2e-1d47-4b8a-9f21-6c0e5a7d3b18";
pub const OTHER_WORKER: &str = "7c1b8d4a-92e6-4f30-a5c7-2b8f61d0e934";
/// Deliberately unlike either worker name.
pub const WORKER_POD_UID: &str = "d41b6e97-5a02-4c8d-b3f7-90e2c1a86d54";

pub fn md(hive: &str, name: &str) -> Option<pb::ResourceMetadata> {
    Some(pb::ResourceMetadata {
        hive: hive.into(),
        name: name.into(),
        ..Default::default()
    })
}

pub fn actor_snapshot_uri(location: &str, hive: &str, name: &str) -> String {
    format!("{location}/hives/{hive}/actors/{SNAPSHOT_OWNER_UID}/snapshots/{name}")
}

pub fn tag_snapshot_uri(location: &str, hive: &str, name: &str) -> String {
    format!("{location}/hives/{hive}/tags/{name}")
}

pub fn hive(name: &str) -> pb::Hive {
    pb::Hive {
        metadata: md("", name),
    }
}

pub async fn mk_hive(s: &dyn Store, name: &str) {
    s.create_hive(&hive(name))
        .await
        .unwrap_or_else(|e| panic!("create_hive({name}): {e}"));
}

/// A template as the API layer stores it: fully defaulted, so a read that
/// backfills defaults returns what was created.
pub fn template(hive: &str, name: &str) -> pb::ActorTemplate {
    pb::ActorTemplate {
        metadata: md(hive, name),
        containers: vec![pb::Container {
            name: "main".into(),
            image: "example.com/app@sha256:0000000000000000000000000000000000000000000000000000000000000000".into(),
            env: vec![pb::EnvVar {
                name: "MODE".into(),
                value: "VAL".into(),
            }],
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
        snapshot_config: Some(pb::SnapshotConfig {
            preferred_fidelity: pb::SnapshotFidelity::Memory as i32,
            golden_snapshot_config: Some(pb::GoldenSnapshotConfig {
                mode: pb::GoldenSnapshotMode::Enabled as i32,
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn worker(name: &str, pod: &str) -> pb::Worker {
    pb::Worker {
        metadata: md("", name),
        worker_namespace: "default".into(),
        worker_pool: "pool-1".into(),
        worker_pod: pod.into(),
        worker_pod_uid: WORKER_POD_UID.into(),
        status: Some(pb::WorkerStatus {
            capacity: Some(pb::WorkerResources {
                cpu_millicores: 2000,
                memory_bytes: 4 << 30,
                actors: 0,
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn actor(hive: &str, name: &str, state: pb::ActorState) -> pb::Actor {
    pb::Actor {
        metadata: md(hive, name),
        actor_template: Some(pb::ObjectRef {
            hive: "default".into(),
            name: "test-template".into(),
        }),
        status: Some(pb::ActorStatus {
            state: state as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// An Actor holding a durable snapshot, the state Tags are taken from.
pub fn suspended_actor(hive: &str, name: &str) -> pb::Actor {
    pb::Actor {
        metadata: md(hive, name),
        actor_template: Some(pb::ObjectRef {
            hive: "ns1".into(),
            name: "tmpl1".into(),
        }),
        status: Some(pb::ActorStatus {
            state: pb::ActorState::Suspended as i32,
            last_assigned_generation: 1,
            snapshots: vec![pb::Snapshot {
                generation: 1,
                owner: pb::SnapshotOwner::Actor as i32,
                fidelity: pb::SnapshotFidelity::Memory as i32,
                durable_snapshot: Some(pb::SnapshotStorage {
                    state: pb::SnapshotStorageState::Completed as i32,
                    object: Some(pb::ObjectSnapshot {
                        snapshot_uri: actor_snapshot_uri("gs://private", hive, name),
                    }),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The record a Tag create reserves: hive-scoped, with a PENDING snapshot
/// naming where the copy is going.
pub fn in_progress_tag(name: &str, actor: &pb::Actor) -> pb::Tag {
    let hive = actor.hive();
    pb::Tag {
        metadata: md(hive, name),
        scope: pb::TagScope::Hive as i32,
        status: Some(pb::TagStatus {
            actor_template_uid: "template-uid".into(),
            snapshot: Some(pb::Snapshot {
                generation: 1,
                owner: pb::SnapshotOwner::Tag as i32,
                fidelity: pb::SnapshotFidelity::Memory as i32,
                durable_snapshot: Some(pb::SnapshotStorage {
                    state: pb::SnapshotStorageState::Pending as i32,
                    object: Some(pb::ObjectSnapshot {
                        snapshot_uri: tag_snapshot_uri("gs://private", hive, name),
                    }),
                }),
                ..Default::default()
            }),
        }),
        ..Default::default()
    }
}

pub fn assignment(actor_uid: &str, cpu: i64, mem: i64) -> pb::ActorAssignment {
    pb::ActorAssignment {
        actor: Some(pb::ObjectRef {
            hive: HIVE.into(),
            name: format!("actor-{actor_uid}"),
        }),
        actor_uid: actor_uid.into(),
        resources: hivekeep_core::compute::cpu_memory(cpu, mem),
        ..Default::default()
    }
}

pub fn resources(actors: i32, cpu: i64, mem: i64) -> Option<pb::WorkerResources> {
    Some(pb::WorkerResources {
        cpu_millicores: cpu,
        memory_bytes: mem,
        actors,
    })
}

pub fn allocated(w: &pb::Worker) -> Option<pb::WorkerResources> {
    w.status.as_ref().and_then(|s| s.allocated)
}

/// Allocation totals that mean the same thing: no allocation equals an
/// all-zero one.
pub fn same_allocation(a: Option<pb::WorkerResources>, b: Option<pb::WorkerResources>) -> bool {
    a.unwrap_or_default() == b.unwrap_or_default()
}

/// `msg` with the server-owned metadata cleared, for comparing against input.
pub fn without_server_fields<T: Resource>(msg: &T) -> T {
    let mut out = msg.clone();
    let m = out.metadata_mut();
    m.uid.clear();
    m.version = 0;
    m.create_time = None;
    m.update_time = None;
    m.delete_time = None;
    out
}

pub async fn receive(watch: &mut WorkerWatch) -> WorkerEvent {
    tokio::time::timeout(Duration::from_secs(5), watch.recv())
        .await
        .expect("timed out waiting for a worker event")
        .expect("watch ended unexpectedly")
}

/// A deterministic generator, so a failing churn run reproduces.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        (z ^ (z >> 31)) % n
    }
}
