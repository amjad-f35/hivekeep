//! Suspend and async-suspend workflow edge cases.

mod common;

use common::*;
use hivekeep_api::workflow::suspend::SuspendMode;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::keeper::v1 as kp;
use hivekeep_store::{ActorTemplateStore, StoreError};
use pb::SnapshotStorageState::{Completed, Pending};
use tonic::{Code, Status};

const MODES: [SuspendMode; 2] = [SuspendMode::Sync, SuspendMode::Async];

async fn suspend(h: &Harness, name: &str, mode: SuspendMode) -> Result<pb::Actor, Status> {
    h.control
        .workflow()
        .suspend_actor(&ar(name), mode)
        .await
        .map_err(Status::from)
}

fn crash_during_checkpoint(h: &Harness, name: &str, message: &'static str, clear_node: bool) {
    let mem = h.memory.clone();
    let name = name.to_string();
    h.ks().on_checkpoint = Some(hook(async move {
        use hivekeep_store::{ActorStore, Precondition};
        let a = mem.get_actor(&ar(&name)).await.unwrap();
        mem.update_actor(&ar(&name), Precondition::of(&a), &mut |a| {
            let s = a.status.as_mut().unwrap();
            s.state = pb::ActorState::Crashed as i32;
            s.crash = Some(pb::ActorCrash {
                message: message.into(),
                crash_time: None,
            });
            s.worker_assignment = None;
            if clear_node {
                s.assigned_node.clear();
            }
            Ok(())
        })
        .await
        .unwrap();
    }));
}

fn clobber_during_checkpoint(h: &Harness, name: &str, f: fn(&mut pb::ActorStatus)) {
    let mem = h.memory.clone();
    let name = name.to_string();
    h.ks().on_checkpoint = Some(hook(async move {
        use hivekeep_store::{ActorStore, Precondition};
        let a = mem.get_actor(&ar(&name)).await.unwrap();
        mem.update_actor(&ar(&name), Precondition::of(&a), &mut |a| {
            f(a.status.as_mut().unwrap());
            Ok(())
        })
        .await
        .unwrap();
    }));
}

#[tokio::test]
async fn snapshot_location_change_leaves_nothing_behind() {
    let h = Harness::new().await;
    let a = h
        .seed_on_worker("a1", pb::ActorState::Running, vec![])
        .await;
    let previous = actor_uri_at("gs://old-bucket/root", &a, "old");
    h.objects.write_snapshot(&previous, &["manifest.json"]);
    h.set_actor_status("a1", {
        let previous = previous.clone();
        move |s| {
            s.snapshots = vec![actor_snapshot(1, "old", &previous, Completed, "")];
            s.last_assigned_generation = 1;
        }
    })
    .await;
    let s = suspend(&h, "a1", SuspendMode::Sync).await.unwrap();
    assert!(
        h.objects.list(&previous).is_empty(),
        "the snapshot under the old location is released"
    );
    let fresh = uri_of(latest_durable(&s).unwrap());
    assert!(fresh.starts_with(LOCATION));
    assert!(!h.objects.list(&fresh).is_empty());
    h.delete("a1").await.unwrap();
    assert!(
        h.objects.list(&fresh).is_empty(),
        "deleting the actor collects the new one"
    );
}

#[tokio::test]
async fn suspend_actor_finalized_commits_despite_object_store_failure() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    let first = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    let old = uri_of(latest_durable(&first).unwrap());
    h.resume("a1").await.unwrap();
    *h.objects.fail_cleanup.lock() = true;
    let s = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    assert_eq!(state(&s), pb::ActorState::Suspended);
    assert_ne!(uri_of(latest_durable(&s).unwrap()), old);
    assert!(s.status.as_ref().unwrap().snapshots.iter().all(|x| x
        .durable_snapshot
        .as_ref()
        .unwrap()
        .state
        == Completed as i32));
    assert!(
        !h.objects.list(&old).is_empty(),
        "the replaced snapshot is left in storage"
    );
}

#[tokio::test]
async fn suspend_actor_replaced_snapshot_release_failure() {
    // Same contract through the RPC: the actor still reaches SUSPENDED and
    // resumes afterwards.
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    h.resume("a1").await.unwrap();
    *h.objects.fail_cleanup.lock() = true;
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    *h.objects.fail_cleanup.lock() = false;
    h.wait_worker_free("w1").await;
    h.resume("a1").await.unwrap();
}

async fn keeps_crash_from_worker_delete(mode: SuspendMode) {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    let mem = h.memory.clone();
    h.faults.before(
        "release_actor_from_worker",
        hook(async move {
            use hivekeep_store::{ActorStore, Precondition};
            let a = mem.get_actor(&ar("a1")).await.unwrap();
            mem.update_actor(&ar("a1"), Precondition::of(&a), &mut |a| {
                let s = a.status.as_mut().unwrap();
                s.state = pb::ActorState::Crashed as i32;
                s.crash = Some(pb::ActorCrash {
                    message: "suspend failed: worker pod went away while hosting the actor".into(),
                    crash_time: None,
                });
                s.worker_assignment = None;
                Ok(())
            })
            .await
            .unwrap();
        }),
    );
    let e = suspend(&h, "a1", mode).await.unwrap_err();
    assert_eq!(e.code(), Code::Aborted);
    let a = h.get_actor("a1").await;
    assert_eq!(state(&a), pb::ActorState::Crashed);
    assert_eq!(
        a.status.unwrap().crash.unwrap().message,
        "suspend failed: worker pod went away while hosting the actor"
    );
}

#[tokio::test]
async fn suspend_actor_finalized_keeps_crash_from_worker_delete() {
    keeps_crash_from_worker_delete(SuspendMode::Sync).await;
}

#[tokio::test]
async fn suspend_actor_async_finalized_keeps_crash_from_worker_delete() {
    keeps_crash_from_worker_delete(SuspendMode::Async).await;
}

#[tokio::test]
async fn suspend_actor_finalized_keeps_replaced_snapshot_on_conflict() {
    let h = Harness::new().await;
    let a = h
        .seed_on_worker("a1", pb::ActorState::Running, vec![])
        .await;
    let previous = h.actor_uri(&a, "old");
    let fresh = h.actor_uri(&a, "fresh");
    h.objects.write_snapshot(&previous, &["manifest.json"]);
    h.set_actor_status("a1", {
        let (p, f) = (previous.clone(), fresh.clone());
        move |s| {
            s.state = pb::ActorState::Suspending as i32;
            s.snapshots = vec![
                actor_snapshot(1, "old", &p, Completed, ""),
                actor_snapshot(2, "fresh", &f, Pending, ""),
            ];
            s.last_assigned_generation = 2;
        }
    })
    .await;
    h.faults
        .fail_always("update_actor", || StoreError::VersionConflict);
    let e = suspend(&h, "a1", SuspendMode::Sync).await.unwrap_err();
    assert_eq!(e.code(), Code::Aborted);
    assert_eq!(
        h.ks().checkpoint_calls,
        1,
        "the conflict came from finalize"
    );
    assert!(
        !h.objects.list(&previous).is_empty(),
        "nothing is released when the commit loses"
    );
    h.faults.clear("update_actor");
}

async fn missing_snapshot_crashes(
    mode: SuspendMode,
    clobber: fn(&mut pb::ActorStatus),
    want: &str,
) {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    clobber_during_checkpoint(&h, "a1", clobber);
    let e = suspend(&h, "a1", mode).await.unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition, "{e:?}");
    let a = h.get_actor("a1").await;
    assert_eq!(state(&a), pb::ActorState::Crashed);
    assert_eq!(
        a.status.as_ref().unwrap().crash.as_ref().unwrap().message,
        want
    );
    assert!(a.status.unwrap().worker_assignment.is_none());
    assert_eq!(h.crash_count(), 1.0, "one crash record");
}

#[tokio::test]
async fn suspend_actor_finalized_missing_durable_snapshot_clears_assignment() {
    missing_snapshot_crashes(
        SuspendMode::Sync,
        |s| s.snapshots.clear(),
        "suspend failed: actor has no durable snapshot",
    )
    .await;
}

#[tokio::test]
async fn suspend_actor_async_finalized_missing_snapshot_clears_assignment() {
    missing_snapshot_crashes(
        SuspendMode::Async,
        |s| s.snapshots.clear(),
        "suspend failed: actor has no local snapshot",
    )
    .await;
    missing_snapshot_crashes(
        SuspendMode::Async,
        |s| {
            for x in &mut s.snapshots {
                x.durable_snapshot = None;
            }
        },
        "suspend failed: actor has no durable snapshot",
    )
    .await;
}

#[tokio::test]
async fn suspend_actor_finalized_releases_only_own_worker() {
    for case in ["own", "other hive", "previous incarnation"] {
        let h = Harness::new().await;
        let a = h
            .seed_on_worker("shared", pb::ActorState::Running, vec![])
            .await;
        let uid = a.metadata.as_ref().unwrap().uid.clone();
        if case != "own" {
            // The worker holds someone else's assignment, not this actor's.
            h.store.release_actor_from_worker("w1", &uid).await.unwrap();
            let (hive, other_uid) = match case {
                "other hive" => ("team-b", uuid::Uuid::new_v4().to_string()),
                _ => (HIVE, uuid::Uuid::new_v4().to_string()),
            };
            h.c().create_hive(h.req(hive_req(hive))).await.ok();
            h.store
                .bind_actor_to_worker(
                    "w1",
                    &pb::ActorAssignment {
                        actor: Some(oref(hive, "shared")),
                        actor_uid: other_uid,
                        ..Default::default()
                    },
                    None,
                )
                .await
                .unwrap();
        }
        suspend(&h, "shared", SuspendMode::Sync).await.unwrap();
        let left = h
            .store
            .list_worker_assignments("w1", &Default::default())
            .await
            .unwrap()
            .items;
        assert_eq!(left.is_empty(), case == "own", "{case}");
    }
}

fn hive_req(name: &str) -> pb::CreateHiveRequest {
    hive(name)
}

#[tokio::test]
async fn suspend_actor_marked_suspending_reentry_keeps_persisted_snapshot_location() {
    for mode in MODES {
        let h = Harness::new().await;
        let a = h
            .seed_on_worker("a1", pb::ActorState::Running, vec![])
            .await;
        let first = h.actor_uri(&a, "first-attempt");
        h.set_actor_status("a1", {
            let first = first.clone();
            move |s| {
                s.state = pb::ActorState::Suspending as i32;
                s.snapshots = vec![actor_snapshot(1, "first-attempt", &first, Pending, "")];
                s.last_assigned_generation = 1;
            }
        })
        .await;
        let s = suspend(&h, "a1", mode).await.unwrap();
        let req = h.ks().checkpoint.clone().unwrap();
        assert_eq!(req.snapshot.unwrap().object.unwrap().snapshot_uri, first);
        let want_mode = if mode == SuspendMode::Sync {
            kp::UploadMode::Immediate
        } else {
            kp::UploadMode::Deferred
        };
        assert_eq!(req.upload_config.unwrap().mode, want_mode as i32);
        let snap = &s.status.as_ref().unwrap().snapshots[0];
        assert_eq!(uri_of(snap), first);
        match mode {
            SuspendMode::Sync => {
                assert_eq!(
                    snap.durable_snapshot.as_ref().unwrap().state,
                    Completed as i32
                );
                assert!(snap.locality.is_empty());
                assert!(s.status.as_ref().unwrap().assigned_node.is_empty());
            }
            SuspendMode::Async => {
                assert_eq!(
                    snap.durable_snapshot.as_ref().unwrap().state,
                    Pending as i32
                );
                assert_eq!(snap.locality, NODE);
                assert_eq!(s.status.as_ref().unwrap().assigned_node, NODE);
            }
        }
    }
}

#[tokio::test]
async fn suspend_actor_marked_suspending_state_matrix() {
    use pb::ActorState as S;
    for mode in MODES {
        for st in [
            S::Running,
            S::Suspending,
            S::Suspended,
            S::Resuming,
            S::Crashed,
            S::Deleting,
            S::Reverting,
        ] {
            let h = Harness::new().await;
            let a = h.seed_on_worker("a1", S::Running, vec![]).await;
            let uri = h.actor_uri(&a, "s1");
            let seeded = h
                .set_actor_status("a1", move |s| {
                    s.state = st as i32;
                    if st == S::Suspending {
                        s.snapshots = vec![actor_snapshot(1, "s1", &uri, Pending, "")];
                        s.last_assigned_generation = 1;
                    }
                })
                .await;
            let r = suspend(&h, "a1", mode).await;
            match st {
                S::Running | S::Suspending => {
                    assert_eq!(state(&r.unwrap()), S::Suspended, "{st:?}")
                }
                S::Suspended => assert_eq!(r.unwrap(), seeded, "returned as it is"),
                _ => {
                    assert_eq!(r.unwrap_err().code(), Code::FailedPrecondition, "{st:?}");
                    assert_eq!(h.get_actor("a1").await, seeded, "{st:?} left untouched");
                    assert_eq!(h.ks().checkpoint_calls, 0);
                }
            }
        }
    }
}

#[tokio::test]
async fn suspend_actor_crashes_when_suspending_actor_missing_worker_pod() {
    for mode in MODES {
        let h = Harness::new().await;
        let a = h
            .seed_on_worker("a1", pb::ActorState::Running, vec![])
            .await;
        let uri = h.actor_uri(&a, "s1");
        h.set_actor_status("a1", move |s| {
            s.state = pb::ActorState::Suspending as i32;
            s.worker_assignment = None;
            s.snapshots = vec![actor_snapshot(1, "s1", &uri, Pending, "")];
            s.last_assigned_generation = 1;
        })
        .await;
        assert_eq!(
            suspend(&h, "a1", mode).await.unwrap_err().code(),
            Code::FailedPrecondition
        );
        let a = h.get_actor("a1").await;
        assert_eq!(state(&a), pb::ActorState::Crashed);
        assert!(a
            .status
            .unwrap()
            .crash
            .unwrap()
            .message
            .contains("no worker assignment"));
        assert_eq!(
            h.ks().checkpoint_calls,
            0,
            "crashed before the keeper is called"
        );
    }
}

#[tokio::test]
async fn suspend_actor_async_crashes_when_pausing_actor_missing_worker_pod() {
    let h = Harness::new().await;
    h.seed_on_worker("a1", pb::ActorState::Running, vec![])
        .await;
    h.set_actor_status("a1", |s| {
        s.state = pb::ActorState::Suspending as i32;
        s.worker_assignment = None;
    })
    .await;
    assert_eq!(
        suspend(&h, "a1", SuspendMode::Async)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Crashed);
}

async fn dial_failure_leaves_retryable(mode: SuspendMode) {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.dialer.remove(NODE);
    assert_eq!(
        suspend(&h, "a1", mode).await.unwrap_err().code(),
        Code::Unavailable
    );
    let a = h.get_actor("a1").await;
    assert_eq!(state(&a), pb::ActorState::Suspending);
    assert!(a.status.unwrap().worker_assignment.is_some());
    h.dialer.set(NODE, h.keeper_addr.clone());
    assert_eq!(
        state(&suspend(&h, "a1", mode).await.unwrap()),
        pb::ActorState::Suspended
    );
}

#[tokio::test]
async fn suspend_actor_keeper_suspended_dial_failure_leaves_actor_retryable() {
    dial_failure_leaves_retryable(SuspendMode::Sync).await;
}

#[tokio::test]
async fn suspend_actor_async_keeper_paused_dial_failure_leaves_actor_retryable() {
    dial_failure_leaves_retryable(SuspendMode::Async).await;
}

#[tokio::test]
async fn suspend_actor_finalized_pause_releases_replaced_snapshot() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    let first = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    let replaced = uri_of(latest_durable(&first).unwrap());
    h.wait_worker_free("w1").await;
    h.resume("a1").await.unwrap();
    let paused = h
        .suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    let local = latest_local(&paused).unwrap().clone();
    // The keeper uploads it in the background and reports it.
    h.objects.write_snapshot(&uri_of(&local), &SNAPSHOT_OBJECTS);
    let report = pb::SnapshotReport {
        uuid: local.uuid.clone(),
        generation: local.generation,
        fidelity: local.fidelity,
        object: Some(pb::ObjectSnapshot {
            snapshot_uri: uri_of(&local),
        }),
        ..Default::default()
    };
    h.ws()
        .report_actor_snapshot_state(h.keeper_req(
            NODE,
            pb::ReportActorSnapshotStateRequest {
                actor: Some(oref(HIVE, "a1")),
                actor_uid: uid,
                state: Some(pb::ActorSnapshotState {
                    latest_local: Some(report.clone()),
                    latest_durable: Some(report),
                    ..Default::default()
                }),
            },
        ))
        .await
        .unwrap();
    h.wait_worker_free("w1").await;
    h.resume("a1").await.unwrap();
    let again = h
        .suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    assert!(
        h.objects.list(&replaced).is_empty(),
        "the snapshot the upload replaced is collected"
    );
    assert!(
        !h.objects.list(&uri_of(&local)).is_empty(),
        "the upload is kept"
    );
    assert_eq!(uri_of(latest_durable(&again).unwrap()), uri_of(&local));
}

#[tokio::test]
async fn suspend_and_pause_characteristic_matrix() {
    for mode in MODES {
        // RUNNING: one checkpoint, worker released.
        let h = Harness::new().await;
        h.running_actor("a1").await;
        let s = suspend(&h, "a1", mode).await.unwrap();
        assert_eq!(h.ks().checkpoint_calls, 1);
        let st = s.status.as_ref().unwrap();
        assert_eq!(st.last_assigned_generation, 1);
        assert!(st.worker_assignment.is_none());
        let snap = &st.snapshots[0];
        match mode {
            SuspendMode::Sync => assert!(
                (
                    snap.locality.is_empty(),
                    snap.durable_snapshot.as_ref().unwrap().state
                ) == (true, Completed as i32)
            ),
            SuspendMode::Async => assert!(
                (
                    snap.locality.as_str(),
                    snap.durable_snapshot.as_ref().unwrap().state
                ) == (NODE, Pending as i32)
            ),
        }
        h.wait_worker_free("w1").await;

        // RUNNING resumed from a pause: a new generation, the old local copy is dropped.
        h.resume("a1").await.unwrap();
        let s2 = suspend(&h, "a1", mode).await.unwrap();
        let st = s2.status.as_ref().unwrap();
        assert_eq!(st.last_assigned_generation, 2);
        assert!(st
            .snapshots
            .iter()
            .all(|x| x.generation == 2 || x.locality.is_empty()));

        // SUSPENDED: nothing is sent.
        let calls = h.ks().checkpoint_calls;
        suspend(&h, "a1", mode).await.unwrap();
        assert_eq!(h.ks().checkpoint_calls, calls);

        // CRASHED: rejected.
        h.set_actor_status("a1", |s| s.state = pb::ActorState::Crashed as i32)
            .await;
        assert_eq!(
            suspend(&h, "a1", mode).await.unwrap_err().code(),
            Code::FailedPrecondition
        );
    }
}

#[tokio::test]
async fn suspend_and_pause_finalize_without_assignment() {
    for mode in MODES {
        let h = Harness::new().await;
        h.running_actor("a1").await;
        crash_during_checkpoint(&h, "a1", "delete failed: original crash reason", false);
        let e = suspend(&h, "a1", mode).await.unwrap_err();
        assert_eq!(e.code(), Code::FailedPrecondition);
        let a = h.get_actor("a1").await;
        assert_eq!(state(&a), pb::ActorState::Crashed);
        assert_eq!(
            a.status.unwrap().crash.unwrap().message,
            "delete failed: original crash reason"
        );
    }
}

#[tokio::test]
async fn suspend_actor_async_finalized_already_crashed() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    crash_during_checkpoint(&h, "a1", "suspend failed: original crash reason", true);
    assert_eq!(
        suspend(&h, "a1", SuspendMode::Async)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let a = h.get_actor("a1").await;
    assert_eq!(
        a.status.as_ref().unwrap().crash.as_ref().unwrap().message,
        "suspend failed: original crash reason"
    );
    assert_eq!(
        a.status.as_ref().unwrap().snapshots.len(),
        1,
        "the in-progress snapshot is kept"
    );
    assert_eq!(h.crash_count(), 0.0, "no duplicate crash record");
}

#[tokio::test]
async fn suspend_and_pause_invalid_storage_location_rejected() {
    for mode in MODES {
        let h = Harness::with(Options {
            gates: hivekeep_core::preview::PreviewGates::all(),
            preview_client: true,
            ..Default::default()
        })
        .await;
        h.seed_on_worker("a1", pb::ActorState::Running, vec![])
            .await;
        let t = h
            .store
            .get_actor_template(&hivekeep_core::resource::ActorTemplateRef::new(
                HIVE, "tmpl1",
            ))
            .await
            .unwrap();
        h.memory
            .update_actor_template(
                &hivekeep_core::resource::ActorTemplateRef::new(HIVE, "tmpl1"),
                hivekeep_store::Precondition::of(&t),
                &mut |t| {
                    t.snapshot_config.as_mut().unwrap().storage_location = "not-a-uri".into();
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert!(suspend(&h, "a1", mode).await.is_err());
        let a = h.get_actor("a1").await;
        assert_eq!(state(&a), pb::ActorState::Running);
        assert_eq!(a.status.as_ref().unwrap().last_assigned_generation, 0);
        assert!(a.status.unwrap().snapshots.is_empty());
    }
}

#[tokio::test]
async fn suspend_actor_async_finalized_worker_gone() {
    let h = Harness::new().await;
    let a = h
        .seed_on_worker("a1", pb::ActorState::Running, vec![])
        .await;
    let uri = h.actor_uri(&a, "local-snap-1");
    h.set_actor_status("a1", move |s| {
        s.state = pb::ActorState::Suspending as i32;
        s.assigned_node.clear();
        s.snapshots = vec![actor_snapshot(1, "local-snap-1", &uri, Pending, "")];
        s.last_assigned_generation = 1;
    })
    .await;
    h.memory
        .delete_worker("w1", Default::default())
        .await
        .unwrap();
    assert_eq!(
        suspend(&h, "a1", SuspendMode::Async)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(h.ks().checkpoint_calls, 1);
    let a = h.get_actor("a1").await;
    let st = a.status.unwrap();
    assert_eq!(st.state, pb::ActorState::Crashed as i32);
    assert_eq!(
        st.crash.unwrap().message,
        "suspend failed: node holding the actor's local snapshot is unknown"
    );
    assert!(st.assigned_node.is_empty());
    assert_eq!(st.snapshots[0].uuid, "local-snap-1");
    assert!(st.worker_assignment.is_none());
    use hivekeep_store::WorkerStore;
}

#[tokio::test]
async fn suspend_actor_async_finalized_records_fidelity() {
    for (preferred, want) in [
        (pb::SnapshotFidelity::Volumes, pb::SnapshotFidelity::Volumes),
        (pb::SnapshotFidelity::Rootfs, pb::SnapshotFidelity::Rootfs),
        (pb::SnapshotFidelity::Memory, pb::SnapshotFidelity::Memory),
        (
            pb::SnapshotFidelity::Unspecified,
            pb::SnapshotFidelity::Memory,
        ),
    ] {
        let h = Harness::new().await;
        h.c()
            .create_actor_template(h.req(pb::CreateActorTemplateRequest {
                actor_template: Some(pb::ActorTemplate {
                    metadata: Some(md(HIVE, "tmpl1")),
                    containers: vec![container()],
                    worker_selector: Some(pb::Selector {
                        match_labels: [("pool".to_string(), "pool1".to_string())].into(),
                    }),
                    resources: Some(pb::ResourceRequirements {
                        limits: Some(pb::Resources {
                            cpu_millicores: 1000,
                            memory_bytes: 512 << 20,
                        }),
                    }),
                    snapshot_config: Some(pb::SnapshotConfig {
                        preferred_fidelity: preferred as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();
        let a = h.running_actor("a1").await;
        let runtime = a
            .status
            .as_ref()
            .unwrap()
            .worker_assignment
            .as_ref()
            .unwrap()
            .sandbox_runtime
            .clone();
        let s = suspend(&h, "a1", SuspendMode::Async).await.unwrap();
        let local = latest_local(&s).unwrap();
        assert_eq!(local.fidelity, want as i32, "{preferred:?}");
        assert_eq!(
            local.durable_snapshot.as_ref().unwrap().state,
            Pending as i32
        );
        assert_eq!(local.sandbox_runtime, runtime);
        assert_eq!(
            h.ks()
                .checkpoint
                .clone()
                .unwrap()
                .snapshot
                .unwrap()
                .fidelity,
            hivekeep_api::convert::fidelity_to_keeper(want as i32) as i32
        );
    }
}
