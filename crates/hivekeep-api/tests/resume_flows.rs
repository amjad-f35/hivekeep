//! Resume workflow edge cases: assignment races, metrics, runtime fallback.

mod common;

use std::collections::HashMap;

use common::*;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::keeper::v1 as kp;
use hivekeep_store::{ActorStore, Precondition, StoreError, WorkerAssignmentStore, WorkerStore};
use pb::SnapshotStorageState::{Completed, Pending};
use tonic::{Code, Status};

fn assignments(h: &Harness, outcome: &str) -> f64 {
    metric_count(
        h,
        "hivekeep_scheduler_assignment_duration_seconds_count",
        &[("outcome", outcome)],
    )
}

fn ghost(hive: &str, name: &str) -> pb::ActorAssignment {
    pb::ActorAssignment {
        actor: Some(oref(hive, name)),
        actor_uid: uuid::Uuid::new_v4().to_string(),
        ..Default::default()
    }
}

async fn switch_runtime(h: &Harness, worker: &str, bundle: &str) {
    let w = h.store.get_worker(worker).await.unwrap();
    let epoch = w.status.as_ref().unwrap().registered_epoch;
    h.ws()
        .set_worker_sandbox_runtimes(h.keeper_req(
            &w.node_name,
            pb::SetWorkerSandboxRuntimesRequest {
                worker: Some(wref(worker)),
                epoch,
                sandbox_classes: vec![gvisor_class("v1", bundle)],
            },
        ))
        .await
        .unwrap();
    let b = bundle.to_string();
    h.wait_for_cache(worker, move |w| {
        w.status.as_ref().unwrap().sandbox_classes[0]
            .default_runtime
            .as_ref()
            .unwrap()
            .version
            .as_ref()
            .unwrap()
            .attributes[0]
            .value
            == b
    })
    .await;
}

#[tokio::test]
async fn assign_worker_attempt_conflict_refreshes_actor() {
    // A concurrent spec write: the retry uses the refreshed actor and wins.
    let h = Harness::new().await;
    h.ensure_base().await;
    h.create_actor("a1", "tmpl1").await;
    let mem = h.memory.clone();
    h.faults.before(
        "update_actor",
        hook(async move {
            let a = mem.get_actor(&ar("a1")).await.unwrap();
            mem.update_actor(&ar("a1"), Precondition::of(&a), &mut |a| {
                a.worker_selector = Some(pb::Selector {
                    match_labels: [("pool".to_string(), "pool1".to_string())].into(),
                });
                Ok(())
            })
            .await
            .unwrap();
        }),
    );
    let r = h.resume("a1").await.unwrap();
    assert!(
        r.actor.unwrap().worker_selector.is_some(),
        "the refreshed actor was used"
    );

    // A concurrent transition out of SUSPENDED aborts the resume.
    h.create_actor("a2", "tmpl1").await;
    let mem = h.memory.clone();
    h.faults.before(
        "update_actor",
        hook(async move {
            let a = mem.get_actor(&ar("a2")).await.unwrap();
            mem.update_actor(&ar("a2"), Precondition::of(&a), &mut |a| {
                a.status.as_mut().unwrap().state = pb::ActorState::Crashed as i32;
                Ok(())
            })
            .await
            .unwrap();
        }),
    );
    assert_code(
        h.resume("a2").await,
        Code::Aborted,
        "can no longer be resumed",
    );
}

#[tokio::test]
async fn assign_worker_attempt_retry_after_conflict_picks_fresh_worker() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    // A concurrent resume takes w1's only slot just before our bind; a second
    // worker appears.
    let mem = h.memory.clone();
    h.faults.before(
        "bind_actor_to_worker",
        hook(async move {
            mem.bind_actor_to_worker("w1", &ghost(HIVE, "other"), None)
                .await
                .unwrap();
        }),
    );
    let (h_ref, _) = (&h, ());
    let create_w2 = async {
        tokio::task::yield_now().await;
    };
    create_w2.await;
    h_ref.dialer.set("node2", h.keeper_addr.clone());
    let resume = h.resume("a1");
    let add = async {
        h.create_worker("w2", "node2", "pool1", 1).await;
    };
    let (r, _) = tokio::join!(resume, add);
    let a = r.unwrap().actor.unwrap();
    assert_eq!(
        a.status
            .unwrap()
            .worker_assignment
            .unwrap()
            .worker
            .unwrap()
            .name,
        "w2"
    );
}

#[tokio::test]
async fn assign_worker_attempt_skips_worker_assigned_in_other_hive() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.c().create_hive(h.req(hive("team-b"))).await.unwrap();
    let held = ghost("team-b", "shared");
    h.store
        .bind_actor_to_worker("w1", &held, None)
        .await
        .unwrap();
    h.wait_for_cache("w1", |w| {
        w.status.as_ref().unwrap().allocated.map(|a| a.actors) == Some(1)
    })
    .await;
    h.create_actor("shared", "tmpl1").await;
    assert_code(h.resume("shared").await, Code::ResourceExhausted, "");
    let left = h
        .store
        .list_worker_assignments("w1", &Default::default())
        .await
        .unwrap()
        .items;
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].actor_uid, held.actor_uid);
}

#[tokio::test]
async fn assign_worker_attempt_worker_without_node_rejects_volumes() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    // The cached worker lost its node name.
    let w = h.store.get_worker("w1").await.unwrap();
    h.memory
        .update_worker("w1", Precondition::of(&w), &mut |w| {
            w.node_name.clear();
            Ok(())
        })
        .await
        .unwrap();
    h.wait_for_cache("w1", |w| w.node_name.is_empty()).await;
    let a = h.create_actor("a1", "tmpl1").await;
    assert_code(
        h.resume("a1").await,
        Code::FailedPrecondition,
        "has no node name",
    );
    assert!(
        h.store
            .find_worker_hosting_actor(&a.metadata.unwrap().uid)
            .await
            .is_err(),
        "no claim"
    );
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Suspended);
}

#[tokio::test]
async fn ensure_worker_assigned_contention_records_one_observation() {
    let h = Harness::new().await;
    h.ensure_base().await;
    h.create_actor("a1", "tmpl1").await;
    h.faults
        .fail_next("update_actor", || StoreError::VersionConflict);
    h.resume("a1").await.unwrap();
    assert_eq!(assignments(&h, "assigned"), 1.0);
    assert_eq!(
        assignments(&h, "error"),
        0.0,
        "the retried conflict is not recorded"
    );
}

#[tokio::test]
async fn ensure_worker_assigned_exhausted_backoff_records_one_error() {
    let h = Harness::new().await;
    h.ensure_base().await;
    h.create_actor("a1", "tmpl1").await;
    h.faults
        .fail_always("update_actor", || StoreError::VersionConflict);
    assert_code(h.resume("a1").await, Code::Aborted, "conflict");
    h.faults.clear("update_actor");
    assert_eq!(
        metric_count(
            &h,
            "hivekeep_scheduler_assignment_duration_seconds_count",
            &[("outcome", "error"), ("error_type", "Aborted")]
        ),
        1.0
    );
    assert_eq!(assignments(&h, "assigned"), 0.0);
    assert_eq!(
        metric_count(
            &h,
            "hivekeep_actor_lifecycle_operation_duration_seconds_count",
            &[
                ("hk_actor_operation_name", "resume"),
                ("error_type", "Aborted")
            ]
        ),
        1.0
    );
}

#[tokio::test]
async fn ensure_worker_assigned_conflict_exhaustion_is_retryable() {
    let h = Harness::new().await;
    h.ensure_base().await;
    h.create_actor("a1", "tmpl1").await;
    h.faults
        .fail_always("update_actor", || StoreError::VersionConflict);
    let e = h.resume("a1").await.unwrap_err();
    assert_eq!(e.code(), Code::Aborted, "ABORTED tells the caller to retry");
    h.faults.clear("update_actor");
    h.resume("a1").await.unwrap();
}

#[tokio::test]
async fn ensure_worker_assigned_cancelled_before_first_attempt_records_nothing() {
    let h = Harness::new().await;
    h.ensure_base().await;
    h.create_actor("a1", "tmpl1").await;
    let faults = h.faults.clone();
    h.faults.before(
        "get_actor_template",
        hook(async move { faults.lose_lease(&format!("lease:actor:{HIVE}:a1")) }),
    );
    assert_eq!(h.resume("a1").await.unwrap_err().code(), Code::Cancelled);
    assert_eq!(assignments(&h, "error") + assignments(&h, "assigned"), 0.0);
}

#[tokio::test]
async fn ensure_worker_assigned_cancelled_during_backoff_records_one() {
    let h = Harness::new().await;
    h.ensure_base().await;
    h.create_actor("a1", "tmpl1").await;
    let faults = h.faults.clone();
    h.faults.before(
        "update_actor",
        hook(async move { faults.lose_lease(&format!("lease:actor:{HIVE}:a1")) }),
    );
    h.faults
        .fail_next("update_actor", || StoreError::VersionConflict);
    assert_eq!(h.resume("a1").await.unwrap_err().code(), Code::Cancelled);
    assert_eq!(
        metric_count(
            &h,
            "hivekeep_scheduler_assignment_duration_seconds_count",
            &[("outcome", "error"), ("error_type", "Cancelled")]
        ),
        1.0
    );
}

#[tokio::test]
async fn ensure_worker_assigned_cancelled_in_last_sleep_records_one() {
    let h = Harness::new().await;
    h.ensure_base().await;
    h.create_actor("a1", "tmpl1").await;
    h.faults
        .fail_always("update_actor", || StoreError::VersionConflict);
    // Five attempts pass; the lease is lost during the sixth.
    for _ in 0..5 {
        h.faults.before("update_actor", hook(async {}));
    }
    let faults = h.faults.clone();
    h.faults.before(
        "update_actor",
        hook(async move { faults.lose_lease(&format!("lease:actor:{HIVE}:a1")) }),
    );
    assert!(h.resume("a1").await.is_err());
    h.faults.clear("update_actor");
    assert_eq!(
        assignments(&h, "error"),
        1.0,
        "exactly one record for the whole loop"
    );
}

#[tokio::test]
async fn ensure_worker_assigned_context_ends_inside_attempt_records_one() {
    let h = Harness::new().await;
    h.ensure_base().await;
    h.create_actor("a1", "tmpl1").await;
    let faults = h.faults.clone();
    h.faults.before(
        "bind_actor_to_worker",
        hook(async move { faults.lose_lease(&format!("lease:actor:{HIVE}:a1")) }),
    );
    assert!(h.resume("a1").await.is_err());
    assert_eq!(assignments(&h, "error") + assignments(&h, "assigned"), 1.0);
}

#[tokio::test]
async fn finalize_running_unrecordable_upload_still_runs() {
    let h = Harness::new().await;
    h.ensure_base().await;
    let a = h.create_actor("a1", "tmpl1").await;
    let uri = h.actor_uri(&a, "s1");
    // A local snapshot on a deleted node that records no fidelity: the
    // restore works, but the upload cannot be recorded.
    h.set_actor_status("a1", move |s| {
        let mut snap = actor_snapshot(1, "s1", &uri, Pending, "gone-node");
        snap.fidelity = 0;
        s.snapshots = vec![snap];
        s.last_assigned_generation = 1;
    })
    .await;
    let r = h.resume("a1").await.unwrap();
    let a = r.actor.unwrap();
    assert_eq!(state(&a), pb::ActorState::Running);
    assert_eq!(
        a.status.unwrap().snapshots[0]
            .durable_snapshot
            .as_ref()
            .unwrap()
            .state,
        Pending as i32
    );
}

#[tokio::test]
async fn resume_actor_workflow_rejected_and_idempotent_paths() {
    for st in [
        pb::ActorState::Crashed,
        pb::ActorState::Deleting,
        pb::ActorState::Reverting,
        pb::ActorState::Suspending,
    ] {
        let h = Harness::new().await;
        h.seed_on_worker("a1", st, vec![]).await;
        assert_code(
            h.resume("a1").await,
            Code::FailedPrecondition,
            fields_name(st),
        );
    }
    let h = Harness::new().await;
    let seeded = h
        .seed_on_worker("a1", pb::ActorState::Running, vec![])
        .await;
    let r = h.resume("a1").await.unwrap();
    assert!(!r.resumed);
    assert_eq!(r.actor.unwrap(), seeded);
    assert_eq!(
        h.faults.calls("acquire_lease"),
        0,
        "the running fast path takes no lease"
    );
}

fn fields_name(s: pb::ActorState) -> &'static str {
    s.as_str_name()
}

#[tokio::test]
async fn resume_actor_crashes_on_missing_worker_assignment() {
    let h = Harness::new().await;
    h.seed_on_worker("a1", pb::ActorState::Resuming, vec![])
        .await;
    h.set_actor_status("a1", |s| s.worker_assignment = None)
        .await;
    assert_code(h.resume("a1").await, Code::Aborted, "crashed");
    let a = h.get_actor("a1").await;
    assert_eq!(state(&a), pb::ActorState::Crashed);
    assert!(a
        .status
        .unwrap()
        .crash
        .unwrap()
        .message
        .contains("no worker assignment"));
}

#[tokio::test]
async fn resume_checks_mounted_volumes_before_attach() {
    let h = Harness::preview().await;
    let mut c = container();
    c.volume_mounts = vec![
        pb::VolumeMount {
            name: "a".into(),
            mount_path: "/a".into(),
        },
        pb::VolumeMount {
            name: "b".into(),
            mount_path: "/b".into(),
        },
    ];
    let ext = |n: &str| pb::Volume {
        name: n.into(),
        external: Some(pb::ExternalVolumeSource {
            template: Some(pb::ExternalVolumeTemplate {
                capacity_bytes: 1,
                storage_class_name: "standard".into(),
            }),
        }),
        ..Default::default()
    };
    let t = pb::ActorTemplate {
        containers: vec![c],
        volumes: vec![ext("a"), ext("b")],
        ..Default::default()
    };
    let actor = pb::Actor {
        status: Some(pb::ActorStatus {
            external_volumes: Some(pb::ExternalVolumes {
                volumes: vec![
                    pb::ExternalVolumeStatus {
                        name: "a".into(),
                        storage_volume_id: "vol-a".into(),
                        volume_type: MOCK_DRIVER.into(),
                        ..Default::default()
                    },
                    pb::ExternalVolumeStatus {
                        name: "b".into(),
                        volume_type: MOCK_DRIVER.into(),
                        ..Default::default()
                    },
                ],
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let p = hivekeep_api::volumes::StaticVolumes::new();
    p.add_plugin(MOCK_DRIVER, h.volumes.clone());
    let e = hivekeep_api::volumes::attach_actor_volumes(&p, &actor, &t, NODE)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition);
    assert!(
        h.volumes.attached.lock().is_empty(),
        "nothing is published before the check"
    );
}

#[tokio::test]
async fn validate_assigned_worker_runtime_fallback() {
    // Re-entry accepts a worker bound relaxed for a durable memory snapshot.
    let h = Harness::new().await;
    let a = h
        .seed_on_worker("a1", pb::ActorState::Resuming, vec![])
        .await;
    let uri = h.actor_uri(&a, "s1");
    let other = pb::ActorSandboxRuntime {
        sandbox_class: "gvisor".into(),
        version: Some(compat("v1", "bundle-z")),
    };
    let rt = other.clone();
    h.set_actor_status("a1", move |s| {
        let mut snap = actor_snapshot(1, "s1", &uri, Completed, "");
        snap.sandbox_runtime = Some(rt.clone());
        s.snapshots = vec![snap];
        s.last_assigned_generation = 1;
    })
    .await;
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
    assert_eq!(
        h.ks().restore.clone().unwrap().snapshot.unwrap().fidelity,
        kp::SnapshotFidelity::Rootfs as i32
    );

    // An uploaded pause snapshot on the node is accepted too.
    let h = Harness::new().await;
    let a = h
        .seed_on_worker("a1", pb::ActorState::Resuming, vec![])
        .await;
    let uri = h.actor_uri(&a, "s1");
    let rt = other.clone();
    h.set_actor_status("a1", move |s| {
        let mut snap = actor_snapshot(1, "s1", &uri, Completed, NODE);
        snap.sandbox_runtime = Some(rt.clone());
        s.snapshots = vec![snap];
        s.last_assigned_generation = 1;
    })
    .await;
    h.resume("a1").await.unwrap();

    // A pending pause snapshot cannot be restored elsewhere: the worker is
    // ineligible and the actor crashes.
    let h = Harness::new().await;
    let a = h
        .seed_on_worker("a1", pb::ActorState::Resuming, vec![])
        .await;
    let uri = h.actor_uri(&a, "s1");
    let rt = other;
    h.set_actor_status("a1", move |s| {
        let mut snap = actor_snapshot(1, "s1", &uri, Pending, NODE);
        snap.sandbox_runtime = Some(rt.clone());
        s.snapshots = vec![snap];
        s.last_assigned_generation = 1;
    })
    .await;
    assert_code(h.resume("a1").await, Code::Aborted, "crashed");
    assert!(h
        .get_actor("a1")
        .await
        .status
        .unwrap()
        .crash
        .unwrap()
        .message
        .contains("placement constraints"));
}

#[tokio::test]
async fn resume_actor_paused_after_repoint_uses_local_provenance() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    let t2 = h
        .create_template_with("tmpl2", "pool1", vec![container()], vec![])
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
    let paused = h
        .suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    assert_eq!(
        latest_local(&paused).unwrap().actor_template_uid,
        t2.metadata.unwrap().uid
    );
    h.wait_worker_free("w1").await;
    h.resume("a1").await.unwrap();
    assert_eq!(
        h.ks().restore.clone().unwrap().snapshot.unwrap().fidelity,
        kp::SnapshotFidelity::Memory as i32,
        "the local checkpoint keeps its memory"
    );
}

#[tokio::test]
async fn resume_actor_paused_pending_snapshot_refuses_runtime_fallback() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    let synced = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    let durable = uri_of(latest_durable(&synced).unwrap());
    h.wait_worker_free("w1").await;
    h.resume("a1").await.unwrap();
    let paused = h
        .suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    let local = latest_local(&paused).unwrap().clone();
    h.wait_worker_free("w1").await;
    switch_runtime(&h, "w1", "bundle-b").await;
    h.reset_keeper();
    h.ks().fail_detach = Some(Status::unavailable("checkpoint upload unavailable"));
    assert_code(
        h.resume("a1").await,
        Code::Unavailable,
        "while calling keeper DetachActor",
    );
    assert!(h.ks().detach.is_some());
    assert!(
        h.ks().restore.is_none(),
        "neither the checkpoint nor the older generation is restored"
    );
    let after = h.get_actor("a1").await;
    assert_eq!(state(&after), pb::ActorState::Suspended);
    assert!(after.status.as_ref().unwrap().worker_assignment.is_none());
    let still = latest_local(&after).unwrap();
    assert_eq!(still.uuid, local.uuid);
    assert_eq!(
        still.durable_snapshot.as_ref().unwrap().state,
        Pending as i32
    );
    assert_eq!(uri_of(latest_durable(&after).unwrap()), durable);
}

#[tokio::test]
async fn resume_actor_retry_relaxed_pause_assignment_restores_same_snapshot() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let paused = h
        .suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    let local = latest_local(&paused).unwrap().clone();
    h.wait_worker_free("w1").await;
    let upload = pb::SnapshotReport {
        uuid: local.uuid.clone(),
        generation: local.generation,
        fidelity: local.fidelity,
        object: Some(pb::ObjectSnapshot {
            snapshot_uri: uri_of(&local),
        }),
        ..Default::default()
    };
    h.control
        .workflow()
        .apply_reported_snapshot_state(
            &ar("a1"),
            &a.metadata.as_ref().unwrap().uid,
            NODE,
            &pb::ActorSnapshotState {
                latest_local: Some(upload.clone()),
                latest_durable: Some(upload),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    switch_runtime(&h, "w1", "bundle-b").await;
    h.ks().fail_restore = Some(Status::unavailable("restore reply lost"));
    assert_code(h.resume("a1").await, Code::Unavailable, "");
    let interrupted = h.get_actor("a1").await;
    assert_eq!(state(&interrupted), pb::ActorState::Resuming);
    assert_eq!(
        interrupted
            .status
            .as_ref()
            .unwrap()
            .worker_assignment
            .as_ref()
            .unwrap()
            .worker_pod,
        "w1"
    );
    assert!(interrupted.status.unwrap().crash.is_none());
    h.ks().fail_restore = None;
    let r = h.resume("a1").await.unwrap();
    assert_eq!(
        r.actor
            .unwrap()
            .status
            .unwrap()
            .worker_assignment
            .unwrap()
            .worker_pod,
        "w1"
    );
    let req = h.ks().restore.clone().unwrap().snapshot.unwrap();
    assert_eq!(req.snapshot_uuid, local.uuid);
    assert_eq!(req.object.unwrap().snapshot_uri, uri_of(&local));
    assert_eq!(req.fidelity, kp::SnapshotFidelity::Rootfs as i32);
}

#[tokio::test]
async fn resume_actor_paused_local_snapshot_missing_crashes() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    h.wait_worker_free("w1").await;
    h.ks().fail_restore = Some(Status::not_found(
        "local checkpoint files missing on node: directory not found",
    ));
    assert_code(h.resume("a1").await, Code::Internal, "crashed");
    let a = h.get_actor("a1").await;
    assert_eq!(state(&a), pb::ActorState::Crashed);
    assert!(a.status.unwrap().worker_assignment.is_none());
    assert_code(
        h.resume("a1").await,
        Code::FailedPrecondition,
        "ACTOR_STATE_CRASHED",
    );
}

#[tokio::test]
async fn update_actor_reassigns_pool_across_suspend_resume() {
    let h = Harness::new().await;
    h.create_template_with("tmpl1", "shared", vec![container()], vec![])
        .await;
    for (name, tier, node) in [("wa", "a", NODE), ("wb", "b", "node2")] {
        h.dialer.set(node, h.keeper_addr.clone());
        h.create_worker(name, node, "shared", 2).await;
        let mut w = h.store.get_worker(name).await.unwrap();
        w.labels.insert("tier".into(), tier.into());
        h.c()
            .update_worker(h.req(pb::UpdateWorkerRequest { worker: Some(w) }))
            .await
            .unwrap();
        h.wait_for_cache(name, |w| w.labels.contains_key("tier"))
            .await;
    }
    let sel = |tier: &str| {
        Some(pb::Selector {
            match_labels: HashMap::from([("tier".to_string(), tier.to_string())]),
        })
    };
    h.c()
        .create_actor(h.req(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(md(HIVE, "a1")),
                actor_template: Some(oref(HIVE, "tmpl1")),
                worker_selector: sel("a"),
                ..Default::default()
            }),
        }))
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
        "wa"
    );
    let mut a = h.get_actor("a1").await;
    a.worker_selector = sel("b");
    h.c()
        .update_actor(h.req(pb::UpdateActorRequest { actor: Some(a) }))
        .await
        .unwrap();
    h.suspend("a1", pb::DurabilityMode::Synchronous)
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
        "wb"
    );
}

#[tokio::test]
async fn hosting_node_after_resume_onto_new_worker() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
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
    h.dialer.set("node2", h.keeper_addr.clone());
    h.create_worker("w2", "node2", "pool1", 1).await;
    let a = h.resume("a1").await.unwrap().actor.unwrap();
    assert_eq!(
        hivekeep_api::hosting::hosting_node(h.store.as_ref(), &a)
            .await
            .unwrap(),
        "node2"
    );
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    let mint = |node: &str| {
        h.ws().mint_worker_actor_jwt(h.keeper_req(
            node,
            pb::MintWorkerActorJwtRequest {
                actor: Some(oref(HIVE, "a1")),
                actor_uid: uid.clone(),
                audiences: vec!["x".into()],
            },
        ))
    };
    mint("node2").await.unwrap();
    assert_code(mint(NODE).await, Code::NotFound, "");
}
