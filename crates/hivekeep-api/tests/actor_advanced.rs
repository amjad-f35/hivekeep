//! Lifecycle edge cases: local snapshots, detaches, runtimes, volumes.

mod common;

use common::*;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::keeper::v1 as kp;
use hivekeep_store::DeletePreconditions;
use tonic::{Code, Status};

#[tokio::test]
async fn suspend_from_paused_is_noop_and_revert_from_paused() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    let paused = h
        .suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    let again = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    assert_eq!(
        again.metadata.unwrap().version,
        paused.metadata.as_ref().unwrap().version
    );
    let pending = uri_of(latest_local(&paused).unwrap());
    // Reverting discards the local snapshot and its pending upload.
    h.objects.write_snapshot(&pending, &SNAPSHOT_OBJECTS);
    let r = h.revert("a1").await.unwrap();
    assert_eq!(state(&r), pb::ActorState::Suspended);
    let st = r.status.unwrap();
    assert!(st.snapshots.is_empty());
    assert!(st.assigned_node.is_empty());
    assert_eq!(
        st.last_assigned_generation, 1,
        "generations are never reused"
    );
    assert!(h.objects.list(&pending).is_empty());
}

#[tokio::test]
async fn revert_unreachable_node_with_durable_local_snapshot() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.ks().keep_checkpoint_local = true;
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    // A SUSPENDED Actor whose latest local snapshot is durable is revertable
    // only via its local copy; make the latest generation local.
    let a = h.get_actor("a1").await;
    assert!(latest_local(&a).is_some());
    h.dialer.remove(NODE);
    let r = h.revert("a1").await.unwrap();
    assert_eq!(state(&r), pb::ActorState::Suspended);
    assert!(r.status.as_ref().unwrap().assigned_node.is_empty());
    assert!(
        latest_durable(&r).is_some(),
        "the durable snapshot survives"
    );
}

#[tokio::test]
async fn resume_paused_restores_locally_on_same_node() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    h.wait_worker_free("w1").await;
    h.resume("a1").await.unwrap();
    let r = h.ks().restore.clone().unwrap();
    assert_eq!(r.snapshot.as_ref().unwrap().generation, 1);
    assert!(
        !r.snapshot.unwrap().object.unwrap().snapshot_uri.is_empty(),
        "a local restore still names its upload"
    );
}

#[tokio::test]
async fn resume_detaches_from_full_node() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.resume("a1").await.unwrap();
    h.suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    // Another actor takes the only slot on node1.
    h.create_actor("a2", "tmpl1").await;
    h.wait_worker_free("w1").await;
    h.resume("a2").await.unwrap();
    // A worker with room exists on node2.
    h.dialer.set("node2", h.keeper_addr.clone());
    h.create_worker("w2", "node2", "pool1", 1).await;
    let r = h.resume("a1").await.unwrap();
    assert!(h.ks().detach.is_some(), "the keeper uploaded and detached");
    let a = r.actor.unwrap();
    assert_eq!(
        a.status
            .as_ref()
            .unwrap()
            .worker_assignment
            .as_ref()
            .unwrap()
            .node_name,
        "node2"
    );
    assert!(latest_local(&a).is_none());
}

#[tokio::test]
async fn resume_full_node_without_alternative_stays_exhausted() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.resume("a1").await.unwrap();
    h.suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    h.create_actor("a2", "tmpl1").await;
    h.wait_worker_free("w1").await;
    h.resume("a2").await.unwrap();
    assert_code(h.resume("a1").await, Code::ResourceExhausted, "");
    assert!(h.ks().detach.is_none());
    assert_eq!(h.get_actor("a1").await.status.unwrap().assigned_node, NODE);
}

#[tokio::test]
async fn resume_node_gone_with_pending_upload_restores_elsewhere() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.resume("a1").await.unwrap();
    let paused = h
        .suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    let pending = uri_of(latest_local(&paused).unwrap());
    // The keeper uploaded before the node died, but never reported it.
    h.objects.write_snapshot(&pending, &SNAPSHOT_OBJECTS);
    h.store
        .delete_worker("w1", DeletePreconditions::none())
        .await
        .unwrap();
    h.dialer.mark_gone(NODE);
    h.dialer.set("node2", h.keeper_addr.clone());
    h.create_worker("w2", "node2", "pool1", 1).await;
    let r = h.resume("a1").await.unwrap();
    let a = r.actor.unwrap();
    assert_eq!(
        a.status
            .as_ref()
            .unwrap()
            .worker_assignment
            .as_ref()
            .unwrap()
            .node_name,
        "node2"
    );
    assert!(
        latest_durable(&a).is_some(),
        "the restore proved the upload complete"
    );
    assert!(latest_local(&a).is_none());
}

#[tokio::test]
async fn resume_local_restore_failure_on_gone_node_crashes() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.resume("a1").await.unwrap();
    h.suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    h.store
        .delete_worker("w1", DeletePreconditions::none())
        .await
        .unwrap();
    h.dialer.mark_gone(NODE);
    h.dialer.set("node2", h.keeper_addr.clone());
    h.create_worker("w2", "node2", "pool1", 1).await;
    h.ks().fail_restore = Some(Status::not_found("no upload"));
    assert_code(h.resume("a1").await, Code::FailedPrecondition, "CRASHED");
    let a = h.get_actor("a1").await;
    assert!(a
        .status
        .unwrap()
        .crash
        .unwrap()
        .message
        .contains("node holding the actor's latest snapshot no longer exists"));
}

#[tokio::test]
async fn delete_crashed_and_paused_actors() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 4).await;
    h.create_actor("c1", "tmpl1").await;
    h.ks().fail_run = Some(Status::internal("x"));
    let _ = h.resume("c1").await;
    h.ks().fail_run = None;
    h.reset_keeper();
    h.delete("c1").await.unwrap();
    assert!(
        h.ks().terminate.is_some(),
        "a crash keeps the node, which is cleaned"
    );

    h.create_actor("p1", "tmpl1").await;
    h.resume("p1").await.unwrap();
    let paused = h
        .suspend("p1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    let pending = uri_of(latest_local(&paused).unwrap());
    h.objects.write_snapshot(&pending, &SNAPSHOT_OBJECTS);
    h.delete("p1").await.unwrap();
    assert!(
        h.objects.list(&pending).is_empty(),
        "a pending upload is collected"
    );
}

#[tokio::test]
async fn delete_retry_after_worker_delete() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.ks().fail_terminate = Some(Status::internal("x"));
    assert!(h.delete("a1").await.is_err());
    h.c()
        .delete_worker(h.req(pb::DeleteWorkerRequest {
            worker: Some(wref("w1")),
            options: None,
        }))
        .await
        .unwrap();
    h.ks().fail_terminate = None;
    h.delete("a1").await.unwrap();
}

#[tokio::test]
async fn delete_node_gone_skips_terminate() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.c()
        .delete_worker(h.req(pb::DeleteWorkerRequest {
            worker: Some(wref("w1")),
            options: None,
        }))
        .await
        .unwrap();
    h.dialer.mark_gone(NODE);
    h.reset_keeper();
    h.delete("a1").await.unwrap();
    assert!(h.ks().terminate.is_none());
}

#[tokio::test]
async fn delete_releases_an_assignment_the_actor_does_not_reference() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    let a = h.create_actor("a1", "tmpl1").await;
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    h.store
        .bind_actor_to_worker(
            "w1",
            &pb::ActorAssignment {
                actor: Some(oref(HIVE, "a1")),
                actor_uid: uid.clone(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    h.delete("a1").await.unwrap();
    assert!(h.store.find_worker_hosting_actor(&uid).await.is_err());
}

#[tokio::test]
async fn resume_recovers_a_stale_claim() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    let a = h.create_actor("a1", "tmpl1").await;
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    h.store
        .bind_actor_to_worker(
            "w1",
            &pb::ActorAssignment {
                actor: Some(oref(HIVE, "a1")),
                actor_uid: uid,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    let r = h.resume("a1").await.unwrap();
    assert_eq!(
        r.actor
            .unwrap()
            .status
            .unwrap()
            .worker_assignment
            .unwrap()
            .worker
            .unwrap()
            .name,
        "w1"
    );
}

#[tokio::test]
async fn resume_lease_conflict() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_actor("a1", "tmpl1").await;
    let l = h
        .store
        .acquire_lease(&format!("lease:actor:{HIVE}:a1"))
        .await
        .unwrap();
    assert_code(
        h.resume("a1").await,
        Code::Aborted,
        "another operation is in progress for this actor",
    );
    l.close().await;
}

#[tokio::test]
async fn suspend_dangling_worker_still_finalizes() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.store
        .delete_worker("w1", DeletePreconditions::none())
        .await
        .unwrap();
    let s = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    assert_eq!(state(&s), pb::ActorState::Suspended);
}

#[tokio::test]
async fn suspend_replaces_and_releases_previous_snapshot() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    let first = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    let old = uri_of(latest_durable(&first).unwrap());
    h.resume("a1").await.unwrap();
    let second = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    let new = uri_of(latest_durable(&second).unwrap());
    assert_ne!(old, new);
    assert_eq!(second.status.as_ref().unwrap().snapshots.len(), 1);
    assert!(
        h.objects.list(&old).is_empty(),
        "the replaced snapshot is released"
    );
    assert!(!h.objects.list(&new).is_empty());
}

#[tokio::test]
async fn suspend_with_borrowed_tag_snapshot_keeps_tag_objects() {
    let h = Harness::new().await;
    let t = h.create_template_with_golden().await;
    let uid = t.metadata.as_ref().unwrap().uid.clone();
    h.create_worker("w1", NODE, "pool1", 2).await;
    h.create_actor("a1", "tmpl1").await;
    h.resume("a1").await.unwrap();
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    assert!(
        !h.objects.list(&golden_uri(&uid)).is_empty(),
        "the Tag owns its snapshot"
    );
    h.delete("a1").await.unwrap();
    assert!(!h.objects.list(&golden_uri(&uid)).is_empty());
}

async fn worker_with_runtimes(
    h: &Harness,
    name: &str,
    node: &str,
    default: pb::VersionedSandboxCompat,
    restorable: Vec<pb::VersionedSandboxCompat>,
) {
    {
        h.create_worker(name, node, "pool1", 2).await;
        let w = h.store.get_worker(name).await.unwrap();
        let epoch = w.status.as_ref().unwrap().registered_epoch;
        let class = pb::WorkerSandboxClass {
            sandbox_class: "gvisor".into(),
            default_runtime: Some(pb::WorkerSandboxRuntime {
                version: Some(default),
                actor_overhead: None,
            }),
            restorable_runtimes: restorable
                .into_iter()
                .map(|v| pb::WorkerSandboxRuntime {
                    version: Some(v),
                    actor_overhead: None,
                })
                .collect(),
        };
        h.ws()
            .set_worker_sandbox_runtimes(h.keeper_req(
                node,
                pb::SetWorkerSandboxRuntimesRequest {
                    worker: Some(wref(name)),
                    epoch,
                    sandbox_classes: vec![class],
                },
            ))
            .await
            .unwrap();
        h.wait_for_cache(name, |w| {
            w.status.as_ref().unwrap().sandbox_classes[0]
                .restorable_runtimes
                .len()
                < 9
        })
        .await;
    }
}

#[tokio::test]
async fn sandbox_runtime_placement_and_fallback() {
    let h = Harness::new().await;
    h.create_template().await;
    // w1 runs bundle a; the actor's snapshot is taken under it.
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.resume("a1").await.unwrap();
    let s = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    let snap_rt = latest_durable(&s).unwrap().sandbox_runtime.clone().unwrap();
    assert_eq!(
        snap_rt.version.as_ref().unwrap().attributes[0].value,
        "bundle-a"
    );
    h.c()
        .drain_worker(h.req(pb::DrainWorkerRequest {
            worker: Some(wref("w1")),
        }))
        .await
        .unwrap();
    h.wait_for_cache("w1", |w| {
        w.status.as_ref().unwrap().state == pb::WorkerState::Draining as i32
    })
    .await;

    // Only an incompatible worker: memory state is dropped (ROOTFS).
    h.dialer.set("node2", h.keeper_addr.clone());
    worker_with_runtimes(&h, "w2", "node2", compat("v1", "bundle-b"), vec![]).await;
    h.resume("a1").await.unwrap();
    assert_eq!(
        h.ks().restore.clone().unwrap().snapshot.unwrap().fidelity,
        kp::SnapshotFidelity::Rootfs as i32
    );
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();

    // A worker that can restore the new snapshot's runtime is preferred.
    h.c()
        .drain_worker(h.req(pb::DrainWorkerRequest {
            worker: Some(wref("w2")),
        }))
        .await
        .unwrap();
    h.dialer.set("node3", h.keeper_addr.clone());
    worker_with_runtimes(
        &h,
        "w3",
        "node3",
        compat("v1", "bundle-c"),
        vec![compat("v1", "bundle-b")],
    )
    .await;
    h.wait_for_cache("w2", |w| {
        w.status.as_ref().unwrap().state == pb::WorkerState::Draining as i32
    })
    .await;
    let r = h.resume("a1").await.unwrap();
    let wa = r.actor.unwrap().status.unwrap().worker_assignment.unwrap();
    assert_eq!(wa.worker.unwrap().name, "w3");
    assert_eq!(
        wa.sandbox_runtime.unwrap().version.unwrap().attributes[0].value,
        "bundle-b",
        "restores under the matching runtime"
    );
    assert_eq!(
        h.ks().restore.clone().unwrap().snapshot.unwrap().fidelity,
        kp::SnapshotFidelity::Memory as i32
    );
}

#[tokio::test]
async fn repoint_template_before_resume_restores_volumes_only() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    h.create_template_with("tmpl2", "pool1", vec![container()], vec![])
        .await;
    let mut a = h.get_actor("a1").await;
    a.actor_template = Some(oref(HIVE, "tmpl2"));
    h.c()
        .update_actor(h.req(pb::UpdateActorRequest { actor: Some(a) }))
        .await
        .unwrap();
    h.wait_worker_free("w1").await;
    h.resume("a1").await.unwrap();
    assert_eq!(
        h.ks().restore.clone().unwrap().snapshot.unwrap().fidelity,
        kp::SnapshotFidelity::Volumes as i32
    );
}

#[tokio::test]
async fn resume_rejects_snapshot_without_upload() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.set_actor_status("a1", |s| {
        s.last_assigned_generation = 1;
        s.snapshots.push(pb::Snapshot {
            uuid: uuid::Uuid::new_v4().to_string(),
            generation: 1,
            owner: pb::SnapshotOwner::Actor as i32,
            fidelity: pb::SnapshotFidelity::Memory as i32,
            locality: NODE.into(),
            ..Default::default()
        });
        s.assigned_node = NODE.into();
    })
    .await;
    assert_code(h.resume("a1").await, Code::DataLoss, "records no upload");
    let uid = h.get_actor("a1").await.metadata.unwrap().uid;
    assert!(
        h.store.find_worker_hosting_actor(&uid).await.is_err(),
        "no worker is claimed"
    );
}

fn ext_template_volumes() -> (Vec<pb::Container>, Vec<pb::Volume>) {
    let mut c = container();
    c.volume_mounts = vec![pb::VolumeMount {
        name: "data".into(),
        mount_path: "/data".into(),
    }];
    let v = pb::Volume {
        name: "data".into(),
        external: Some(pb::ExternalVolumeSource {
            template: Some(pb::ExternalVolumeTemplate {
                capacity_bytes: 1 << 30,
                storage_class_name: "standard".into(),
            }),
        }),
        ..Default::default()
    };
    (vec![c], vec![v])
}

#[tokio::test]
async fn actor_lifecycle_with_external_volumes() {
    let h = Harness::preview().await;
    let (c, v) = ext_template_volumes();
    h.create_template_with("tmpl1", "pool1", c, v).await;
    h.create_worker("w1", NODE, "pool1", 2).await;
    let a = h.create_actor("a1", "tmpl1").await;
    let vols = &a
        .status
        .as_ref()
        .unwrap()
        .external_volumes
        .as_ref()
        .unwrap()
        .volumes;
    assert_eq!(vols[0].volume_type, MOCK_DRIVER);
    assert!(
        vols[0].storage_volume_id.is_empty(),
        "provisioned on first resume"
    );
    h.resume("a1").await.unwrap();
    let a = h.get_actor("a1").await;
    let id = a
        .status
        .as_ref()
        .unwrap()
        .external_volumes
        .as_ref()
        .unwrap()
        .volumes[0]
        .storage_volume_id
        .clone();
    assert!(!id.is_empty());
    assert_eq!(
        h.volumes.attached.lock().as_slice(),
        &[(id.clone(), NODE.to_string())]
    );
    let run = h.ks().run.clone().unwrap();
    let ext = run
        .spec
        .unwrap()
        .volumes
        .into_iter()
        .find(|v| v.name == "data")
        .unwrap()
        .external
        .unwrap();
    assert_eq!(
        ext.publish_context.get("devicePath").map(String::as_str),
        Some(format!("/dev/{id}").as_str())
    );
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    assert_eq!(h.volumes.detached.lock().len(), 1);
    h.delete("a1").await.unwrap();
    assert_eq!(h.volumes.deleted.lock().as_slice(), &[id]);
}

#[tokio::test]
async fn create_actor_external_volumes_without_opt_in() {
    let h = Harness::preview().await;
    let (c, v) = ext_template_volumes();
    h.create_template_with("tmpl1", "pool1", c, v).await;
    let r = h
        .c()
        .create_actor(h.req_no_preview(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(md(HIVE, "a1")),
                actor_template: Some(oref(HIVE, "tmpl1")),
                ..Default::default()
            }),
        }))
        .await;
    assert_code(r, Code::InvalidArgument, "ExternalVolumes preview gate");
}

#[tokio::test]
async fn resume_volume_creation_failure_and_retry() {
    let h = Harness::preview().await;
    let (c, v) = ext_template_volumes();
    h.create_template_with("tmpl1", "pool1", c, v).await;
    h.create_worker("w1", NODE, "pool1", 2).await;
    h.create_actor("a1", "tmpl1").await;
    *h.volumes.fail_create.lock() = Some(Status::internal("quota"));
    assert_code(
        h.resume("a1").await,
        Code::Internal,
        "failed to create volume",
    );
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Suspended);
    *h.volumes.fail_create.lock() = None;
    h.resume("a1").await.unwrap();
}

#[tokio::test]
async fn resume_volume_attach_failure_and_retry() {
    let h = Harness::preview().await;
    let (c, v) = ext_template_volumes();
    h.create_template_with("tmpl1", "pool1", c, v).await;
    h.create_worker("w1", NODE, "pool1", 2).await;
    h.create_actor("a1", "tmpl1").await;
    *h.volumes.fail_attach.lock() = Some(Status::unavailable("controller down"));
    assert_code(
        h.resume("a1").await,
        Code::Internal,
        "failed to attach volume",
    );
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Resuming);
    *h.volumes.fail_attach.lock() = None;
    h.resume("a1").await.unwrap();
}

#[tokio::test]
async fn suspend_volume_detach_failure_retry_and_delete() {
    let h = Harness::preview().await;
    let (c, v) = ext_template_volumes();
    h.create_template_with("tmpl1", "pool1", c, v).await;
    h.create_worker("w1", NODE, "pool1", 2).await;
    h.create_actor("a1", "tmpl1").await;
    h.resume("a1").await.unwrap();
    *h.volumes.fail_detach.lock() = Some(Status::internal("busy"));
    assert!(h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .is_err());
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Suspending);
    // Delete also needs the detach, and leaves the Actor DELETING.
    assert!(h.delete("a1").await.is_err());
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Deleting);
    *h.volumes.fail_detach.lock() = None;
    h.delete("a1").await.unwrap();
}

#[tokio::test]
async fn delete_actor_volume_deletion_failure_then_retry() {
    let h = Harness::preview().await;
    let (c, v) = ext_template_volumes();
    h.create_template_with("tmpl1", "pool1", c, v).await;
    h.create_worker("w1", NODE, "pool1", 2).await;
    h.create_actor("a1", "tmpl1").await;
    h.resume("a1").await.unwrap();
    *h.volumes.fail_delete.lock() = Some(Status::internal("x"));
    assert_code(
        h.delete("a1").await,
        Code::Internal,
        "while deleting volumes",
    );
    *h.volumes.fail_delete.lock() = None;
    h.delete("a1").await.unwrap();
    // Not-found on delete is success.
    let _ = h
        .store
        .list_actors(HIVE, &Default::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn lifecycle_metrics_are_recorded() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    let text = h.metrics.encode();
    assert!(text.contains("hk_actor_operation_name=\"resume\""));
    assert!(text.contains("hk_actor_operation_name=\"suspend\""));
    assert!(text.contains("hk_actor_operation_name=\"create\""));
    assert!(text.contains("hk_workerpool_name=\"pool1\""));
    assert!(
        text.lines().any(|l| l
            .starts_with("hivekeep_scheduler_assignment_duration_seconds_count{error_type=\"\",")
            && l.contains("outcome=\"assigned\"")),
        "{text}"
    );
    for state in ["running", "suspended"] {
        assert!(
            text.contains(&format!(
                "hivekeep_actor_state_changes_total{{hk_actor_state=\"{state}\"}}"
            )),
            "{state}: {text}"
        );
    }
}

#[tokio::test]
async fn egress_gateway_address_is_sent() {
    let h = Harness::with(Options {
        egress_gateway: "gate.example:15001".into(),
        ..Default::default()
    })
    .await;
    h.running_actor("a1").await;
    assert_eq!(
        h.ks().run.clone().unwrap().egress_gateway.unwrap().address,
        "gate.example:15001"
    );
}
