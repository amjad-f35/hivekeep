//! Worker RPCs and Worker workflows.

mod common;

use common::*;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{DeletePreconditions, Precondition};
use tonic::{Code, Status};

fn new_worker(name: &str) -> pb::Worker {
    pb::Worker {
        metadata: Some(pb::ResourceMetadata {
            name: name.into(),
            ..Default::default()
        }),
        worker_namespace: "workers".into(),
        worker_pool: "pool1".into(),
        worker_pod: name.into(),
        worker_pod_uid: uuid::Uuid::new_v4().to_string(),
        node_name: NODE.into(),
        labels: [("pool".to_string(), "pool1".to_string())].into(),
        ..Default::default()
    }
}

async fn create(h: &Harness, w: pb::Worker) -> Result<pb::Worker, Status> {
    h.c()
        .create_worker(h.req(pb::CreateWorkerRequest { worker: Some(w) }))
        .await
        .map(|r| r.into_inner())
}

#[tokio::test]
async fn create_and_get_worker() {
    let h = Harness::new().await;
    let w = create(&h, new_worker("w1")).await.unwrap();
    let st = w.status.as_ref().unwrap();
    assert_eq!(st.state, pb::WorkerState::Unavailable as i32);
    assert!(st.capacity.is_none(), "holds no capacity until reported");
    let got = h
        .c()
        .get_worker(h.req(pb::GetWorkerRequest {
            worker: Some(wref("w1")),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got, w);
    assert_code(
        h.c()
            .get_worker(h.req(pb::GetWorkerRequest {
                worker: Some(wref("nope")),
            }))
            .await,
        Code::NotFound,
        "Worker nope not found",
    );
}

#[tokio::test]
async fn create_worker_ignores_request_status_and_server_metadata() {
    let h = Harness::new().await;
    let mut w = new_worker("w1");
    w.status = Some(pb::WorkerStatus {
        state: pb::WorkerState::Active as i32,
        issued_epoch: 9,
        ..Default::default()
    });
    w.metadata.as_mut().unwrap().version = 7;
    w.metadata.as_mut().unwrap().uid = uuid::Uuid::new_v4().to_string();
    let got = create(&h, w.clone()).await.unwrap();
    assert_eq!(
        got.status.as_ref().unwrap().state,
        pb::WorkerState::Unavailable as i32
    );
    assert_eq!(got.status.as_ref().unwrap().issued_epoch, 0);
    assert_eq!(got.metadata.as_ref().unwrap().version, 1);
    assert_ne!(got.metadata.unwrap().uid, w.metadata.unwrap().uid);
}

#[tokio::test]
async fn create_worker_already_exists_and_invalid() {
    let h = Harness::new().await;
    create(&h, new_worker("w1")).await.unwrap();
    assert_code(
        create(&h, new_worker("w1")).await,
        Code::AlreadyExists,
        "already exists",
    );
    let mut bad = new_worker("w2");
    bad.worker_pod_uid = "not-a-uuid".into();
    assert_code(create(&h, bad).await, Code::InvalidArgument, "");
}

#[tokio::test]
async fn list_workers() {
    let h = Harness::new().await;
    for n in ["w1", "w2", "w3"] {
        create(&h, new_worker(n)).await.unwrap();
    }
    let page = h
        .c()
        .list_workers(h.req(pb::ListWorkersRequest {
            options: Some(pb::ListOptions {
                page_size: 2,
                page_token: String::new(),
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.workers.len(), 2);
    let rest = h
        .c()
        .list_workers(h.req(pb::ListWorkersRequest {
            options: Some(pb::ListOptions {
                page_size: 2,
                page_token: page.next_page_token,
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rest.workers.len(), 1);
}

#[tokio::test]
async fn update_worker_labels_and_immutables() {
    let h = Harness::new().await;
    let w = create(&h, new_worker("w1")).await.unwrap();
    let mut u = w.clone();
    u.labels.insert("extra".into(), "yes".into());
    let got = h
        .c()
        .update_worker(h.req(pb::UpdateWorkerRequest {
            worker: Some(u.clone()),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.labels.get("extra").map(String::as_str), Some("yes"));
    assert_eq!(got.status, w.status, "status is left alone");

    // Stale precondition.
    assert_code(
        h.c()
            .update_worker(h.req(pb::UpdateWorkerRequest { worker: Some(u) }))
            .await,
        Code::Aborted,
        "",
    );

    // An immutable field, including by omission.
    let mut imm = got.clone();
    imm.node_name = "other-node".into();
    assert_code(
        h.c()
            .update_worker(h.req(pb::UpdateWorkerRequest { worker: Some(imm) }))
            .await,
        Code::InvalidArgument,
        "",
    );
    let mut omitted = got.clone();
    omitted.worker_pool.clear();
    assert!(h
        .c()
        .update_worker(h.req(pb::UpdateWorkerRequest {
            worker: Some(omitted)
        }))
        .await
        .is_err());

    // An omitted mutable field is cleared.
    let mut cleared = got.clone();
    cleared.labels.clear();
    let c = h
        .c()
        .update_worker(h.req(pb::UpdateWorkerRequest {
            worker: Some(cleared),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(c.labels.is_empty());

    let mut ghost = c.clone();
    ghost.metadata.as_mut().unwrap().name = "ghost".into();
    assert_code(
        h.c()
            .update_worker(h.req(pb::UpdateWorkerRequest {
                worker: Some(ghost),
            }))
            .await,
        Code::NotFound,
        "",
    );
}

#[tokio::test]
async fn update_worker_cannot_change_capacity() {
    let h = Harness::new().await;
    let w = h.create_worker("w1", NODE, "pool1", 2).await;
    let mut u = w.clone();
    u.status.as_mut().unwrap().capacity.as_mut().unwrap().actors = 99;
    let got = h
        .c()
        .update_worker(h.req(pb::UpdateWorkerRequest { worker: Some(u) }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.status.unwrap().capacity.unwrap().actors, 2);
}

#[tokio::test]
async fn drain_and_availability() {
    let h = Harness::new().await;
    let w = create(&h, new_worker("w1")).await.unwrap();
    let a = h
        .c()
        .mark_worker_available(h.req(pb::MarkWorkerAvailableRequest {
            worker: Some(wref("w1")),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        a.worker.as_ref().unwrap().status.as_ref().unwrap().state,
        pb::WorkerState::Active as i32
    );
    // Idempotent: no version bump.
    let again = h
        .c()
        .mark_worker_available(h.req(pb::MarkWorkerAvailableRequest {
            worker: Some(wref("w1")),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        again.worker.unwrap().metadata.unwrap().version,
        a.worker
            .as_ref()
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .version
    );
    let u = h
        .c()
        .mark_worker_unavailable(h.req(pb::MarkWorkerUnavailableRequest {
            worker: Some(wref("w1")),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        u.worker.unwrap().status.unwrap().state,
        pb::WorkerState::Unavailable as i32
    );
    let d = h
        .c()
        .drain_worker(h.req(pb::DrainWorkerRequest {
            worker: Some(wref("w1")),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        d.worker.as_ref().unwrap().status.as_ref().unwrap().state,
        pb::WorkerState::Draining as i32
    );
    let d2 = h
        .c()
        .drain_worker(h.req(pb::DrainWorkerRequest {
            worker: Some(wref("w1")),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        d2.worker.unwrap().metadata.unwrap().version,
        d.worker.unwrap().metadata.unwrap().version
    );
    // DRAINING is final.
    assert_code(
        h.c()
            .mark_worker_available(h.req(pb::MarkWorkerAvailableRequest {
                worker: Some(wref("w1")),
            }))
            .await,
        Code::FailedPrecondition,
        "draining",
    );
    assert_code(
        h.c()
            .mark_worker_unavailable(h.req(pb::MarkWorkerUnavailableRequest {
                worker: Some(wref("w1")),
            }))
            .await,
        Code::FailedPrecondition,
        "draining",
    );
    // Labels stay mutable while draining.
    let mut cur = h.store.get_worker("w1").await.unwrap();
    cur.labels.insert("k".into(), "v".into());
    h.c()
        .update_worker(h.req(pb::UpdateWorkerRequest { worker: Some(cur) }))
        .await
        .unwrap();
    assert_code(
        h.c()
            .drain_worker(h.req(pb::DrainWorkerRequest {
                worker: Some(wref("nope")),
            }))
            .await,
        Code::NotFound,
        "",
    );
    let _ = w;
}

#[tokio::test]
async fn drain_and_unavailable_keep_assignment() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.c()
        .mark_worker_unavailable(h.req(pb::MarkWorkerUnavailableRequest {
            worker: Some(wref("w1")),
        }))
        .await
        .unwrap();
    assert_eq!(
        h.store
            .list_worker_assignments("w1", &Default::default())
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    h.c()
        .drain_worker(h.req(pb::DrainWorkerRequest {
            worker: Some(wref("w1")),
        }))
        .await
        .unwrap();
    assert_eq!(
        h.store
            .list_worker_assignments("w1", &Default::default())
            .await
            .unwrap()
            .items
            .len(),
        1
    );
}

#[tokio::test]
async fn list_worker_actor_assignments() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let resp = h
        .c()
        .list_worker_actor_assignments(h.req(pb::ListWorkerActorAssignmentsRequest {
            worker: Some(wref("w1")),
            options: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.actor_assignments.len(), 1);
    assert_eq!(resp.actor_assignments[0].actor_uid, a.metadata.unwrap().uid);
    let r = resp.actor_assignments[0].resources.unwrap();
    assert_eq!((r.cpu_millicores, r.memory_bytes), (1000, 512 << 20));
    assert_code(
        h.c()
            .list_worker_actor_assignments(h.req(pb::ListWorkerActorAssignmentsRequest {
                worker: Some(wref("nope")),
                options: None,
            }))
            .await,
        Code::NotFound,
        "",
    );
}

#[tokio::test]
async fn delete_worker_crashes_running_actor() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.reset_keeper();
    let d = h
        .c()
        .delete_worker(h.req(pb::DeleteWorkerRequest {
            worker: Some(wref("w1")),
            options: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(d.metadata.unwrap().delete_time.is_some());
    let a = h.get_actor("a1").await;
    assert_eq!(state(&a), pb::ActorState::Crashed);
    assert!(a.status.as_ref().unwrap().worker_assignment.is_none());
    assert_eq!(
        a.status.as_ref().unwrap().assigned_node,
        NODE,
        "a crash keeps the node"
    );
    assert_eq!(
        a.status.unwrap().crash.unwrap().message,
        "worker pod went away while hosting the actor"
    );
    assert!(h.ks().terminate.is_some(), "node state is reclaimed");
    assert!(h.store.get_worker("w1").await.is_err());
    assert_code(
        h.c()
            .delete_worker(h.req(pb::DeleteWorkerRequest {
                worker: Some(wref("w1")),
                options: None,
            }))
            .await,
        Code::NotFound,
        "",
    );
}

#[tokio::test]
async fn delete_worker_leaves_suspended_and_superseded() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    // A suspended actor stays resumable.
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    h.c()
        .delete_worker(h.req(pb::DeleteWorkerRequest {
            worker: Some(wref("w1")),
            options: None,
        }))
        .await
        .unwrap();
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Suspended);
}

#[tokio::test]
async fn delete_worker_assigned_to_absent_actor_deletes_anyway() {
    let h = Harness::new().await;
    h.create_worker("w1", NODE, "pool1", 2).await;
    h.store
        .bind_actor_to_worker(
            "w1",
            &pb::ActorAssignment {
                actor: Some(oref(HIVE, "ghost")),
                actor_uid: uuid::Uuid::new_v4().to_string(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    h.c()
        .delete_worker(h.req(pb::DeleteWorkerRequest {
            worker: Some(wref("w1")),
            options: None,
        }))
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_worker_busy_actor_is_aborted_and_retried() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let lease = h
        .store
        .acquire_lease(&format!("lease:actor:{HIVE}:a1"))
        .await
        .unwrap();
    assert_code(
        h.c()
            .delete_worker(h.req(pb::DeleteWorkerRequest {
                worker: Some(wref("w1")),
                options: None,
            }))
            .await,
        Code::Aborted,
        "operation in progress",
    );
    // Drained first, so nothing new lands.
    assert_eq!(
        h.store
            .get_worker("w1")
            .await
            .unwrap()
            .status
            .unwrap()
            .state,
        pb::WorkerState::Draining as i32
    );
    lease.close().await;
    h.c()
        .delete_worker(h.req(pb::DeleteWorkerRequest {
            worker: Some(wref("w1")),
            options: None,
        }))
        .await
        .unwrap();
    let _ = a;
}

#[tokio::test]
async fn delete_worker_preconditions() {
    let h = Harness::new().await;
    create(&h, new_worker("w1")).await.unwrap();
    let r = h
        .c()
        .delete_worker(h.req(pb::DeleteWorkerRequest {
            worker: Some(wref("w1")),
            options: Some(pb::DeleteOptions {
                uid: uuid::Uuid::new_v4().to_string(),
                version: 0,
            }),
        }))
        .await;
    assert_code(r, Code::Aborted, "does not have uid");
}

#[tokio::test]
async fn delete_worker_ignores_stale_incarnation_and_moved_actor() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    // The actor now points at another worker.
    h.create_worker("w2", NODE, "pool1", 2).await;
    h.set_actor_status("a1", |s| {
        s.worker_assignment
            .as_mut()
            .unwrap()
            .worker
            .as_mut()
            .unwrap()
            .name = "w2".into()
    })
    .await;
    h.c()
        .delete_worker(h.req(pb::DeleteWorkerRequest {
            worker: Some(wref("w1")),
            options: None,
        }))
        .await
        .unwrap();
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Running);
    let _ = a;
}

#[tokio::test]
async fn resume_crashes_if_assigned_worker_is_draining() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.ks().fail_run = Some(Status::unavailable("x"));
    let _ = h.resume("a1").await;
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Resuming);
    h.c()
        .drain_worker(h.req(pb::DrainWorkerRequest {
            worker: Some(wref("w1")),
        }))
        .await
        .unwrap();
    h.ks().fail_run = None;
    assert_code(h.resume("a1").await, Code::Aborted, "crashed");
    let a = h.get_actor("a1").await;
    assert_eq!(
        a.status.unwrap().crash.unwrap().message,
        "resume failed: assigned worker is draining"
    );
}

#[tokio::test]
async fn resume_dangling_worker_crashes() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.ks().fail_run = Some(Status::unavailable("x"));
    let _ = h.resume("a1").await;
    h.store
        .delete_worker("w1", DeletePreconditions::none())
        .await
        .unwrap();
    assert_code(h.resume("a1").await, Code::Aborted, "crashed");
    assert!(h
        .get_actor("a1")
        .await
        .status
        .unwrap()
        .crash
        .unwrap()
        .message
        .contains("assigned worker no longer exists"));
}

#[tokio::test]
async fn resume_unavailable_worker_is_unavailable() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.ks().fail_run = Some(Status::unavailable("x"));
    let _ = h.resume("a1").await;
    h.c()
        .mark_worker_unavailable(h.req(pb::MarkWorkerUnavailableRequest {
            worker: Some(wref("w1")),
        }))
        .await
        .unwrap();
    assert_code(h.resume("a1").await, Code::Unavailable, "is unavailable");
}

#[tokio::test]
async fn resume_releases_stale_worker_when_pool_becomes_ineligible() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    let _a = h.create_actor("a1", "tmpl1").await;
    h.ks().fail_run = Some(Status::unavailable("x"));
    let _ = h.resume("a1").await;
    let mut a = h.get_actor("a1").await;
    a.worker_selector = Some(pb::Selector {
        match_labels: [("pool".to_string(), "pool2".to_string())].into(),
    });
    h.c()
        .update_actor(h.req(pb::UpdateActorRequest {
            actor: Some(a.clone()),
        }))
        .await
        .unwrap();
    assert_code(h.resume("a1").await, Code::Aborted, "crashed");
    let uid = a.metadata.unwrap().uid;
    assert!(
        h.store.find_worker_hosting_actor(&uid).await.is_err(),
        "the worker is released"
    );
}

#[tokio::test]
async fn resume_concurrent_onto_one_worker() {
    let h = std::sync::Arc::new(Harness::new().await);
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    for n in ["a1", "a2", "a3"] {
        h.create_actor(n, "tmpl1").await;
    }
    let mut tasks = Vec::new();
    for n in ["a1", "a2", "a3"] {
        let h = h.clone();
        tasks.push(tokio::spawn(async move { h.resume(n).await }));
    }
    let mut ok = 0;
    for t in tasks {
        match t.await.unwrap() {
            Ok(_) => ok += 1,
            Err(s) => assert!(
                matches!(s.code(), Code::ResourceExhausted | Code::Aborted),
                "{s:?}"
            ),
        }
    }
    assert_eq!(ok, 1, "one slot admits exactly one actor");
    assert_eq!(
        h.store
            .list_worker_assignments("w1", &Default::default())
            .await
            .unwrap()
            .items
            .len(),
        1
    );
}

#[tokio::test]
async fn resume_retried_after_worker_restart_during_restore() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    h.wait_worker_free("w1").await;
    // The restore lands on epoch 1, but a new process registers epoch 2.
    h.register_worker("w1", NODE, 4).await;
    h.wait_for_cache("w1", |w| {
        w.status
            .as_ref()
            .is_some_and(|s| s.registered_epoch == 2 && !s.sandbox_classes.is_empty())
    })
    .await;
    h.ks().restore_epoch = Some(1);
    assert_code(
        h.resume("a1").await,
        Code::Unavailable,
        "restarted while actor",
    );
    h.ks().restore_epoch = Some(2);
    let r = h.resume("a1").await.unwrap();
    assert_eq!(
        r.actor
            .unwrap()
            .status
            .unwrap()
            .worker_assignment
            .unwrap()
            .worker_epoch,
        2
    );
}

#[tokio::test]
async fn reconcile_assignments_crashes_earlier_actors() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    let w = h.store.get_worker("w1").await.unwrap();
    assert!(
        !hivekeep_api::worker_workflow::needs_assignment_reconcile(&w)
            || w.status.as_ref().unwrap().observed_epoch == 0
    );
    // Observe epoch 1 first, so only the next registration counts.
    h.worker_workflow.reconcile_assignments("w1").await.unwrap();
    assert_eq!(
        state(&h.get_actor("a1").await),
        pb::ActorState::Running,
        "an actor stamped with the current epoch is kept"
    );
    h.register_worker("w1", NODE, 4).await;
    h.worker_workflow.reconcile_assignments("w1").await.unwrap();
    let a = h.get_actor("a1").await;
    assert_eq!(state(&a), pb::ActorState::Crashed);
    assert_eq!(
        a.status.unwrap().crash.unwrap().message,
        "worker restarted while hosting the actor"
    );
    let w = h.store.get_worker("w1").await.unwrap();
    assert_eq!(w.status.as_ref().unwrap().observed_epoch, 2);
    assert!(h
        .store
        .list_worker_assignments("w1", &Default::default())
        .await
        .unwrap()
        .items
        .is_empty());
    // Already observed: writes nothing.
    h.worker_workflow.reconcile_assignments("w1").await.unwrap();
    assert_eq!(
        h.store
            .get_worker("w1")
            .await
            .unwrap()
            .metadata
            .unwrap()
            .version,
        w.metadata.unwrap().version
    );
    // Gone: success.
    h.worker_workflow
        .reconcile_assignments("nope")
        .await
        .unwrap();
}

#[tokio::test]
async fn reconcile_assignments_waits_for_busy_actor_and_releases_orphans() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.store
        .bind_actor_to_worker(
            "w1",
            &pb::ActorAssignment {
                actor: Some(oref(HIVE, "ghost")),
                actor_uid: uuid::Uuid::new_v4().to_string(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    h.register_worker("w1", NODE, 4).await;
    let lease = h
        .store
        .acquire_lease(&format!("lease:actor:{HIVE}:a1"))
        .await
        .unwrap();
    assert!(h.worker_workflow.reconcile_assignments("w1").await.is_err());
    assert_eq!(
        h.store
            .get_worker("w1")
            .await
            .unwrap()
            .status
            .unwrap()
            .observed_epoch,
        0,
        "a failure keeps the observed epoch"
    );
    lease.close().await;
    h.worker_workflow.reconcile_assignments("w1").await.unwrap();
    assert!(h
        .store
        .list_worker_assignments("w1", &Default::default())
        .await
        .unwrap()
        .items
        .is_empty());
}

#[tokio::test]
async fn reconcile_assignments_keeps_suspended_and_resuming() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 4).await;
    h.create_actor("a1", "tmpl1").await;
    h.ks().fail_run = Some(Status::unavailable("x"));
    let _ = h.resume("a1").await;
    h.register_worker("w1", NODE, 4).await;
    h.worker_workflow.reconcile_assignments("w1").await.unwrap();
    assert_eq!(
        state(&h.get_actor("a1").await),
        pb::ActorState::Resuming,
        "a resuming actor is left to its resume"
    );
}

#[tokio::test]
async fn assignment_reconciler_releases_restarted_worker() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    let stop = tokio_util::sync::CancellationToken::new();
    let rec = hivekeep_api::assignment_reconciler::AssignmentReconciler::new(
        h.store.clone(),
        h.worker_workflow.clone(),
    );
    rec.start(&h.cache, stop.clone());
    h.register_worker("w1", NODE, 4).await;
    for _ in 0..400 {
        if state(&h.get_actor("a1").await) == pb::ActorState::Crashed {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Crashed);
    // A held sweep lease is retried rather than skipped.
    let l = h
        .store
        .acquire_lease("lease:workerassignment:w1")
        .await
        .unwrap();
    assert!(rec.reconcile_one("w1").await.is_err());
    l.close().await;
    rec.reconcile_one("w1").await.unwrap();
    stop.cancel();
}

#[tokio::test]
async fn crash_actor_reported_by_worker() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    let wf = h.control.workflow();
    let r = hivekeep_core::resource::ActorRef::new(HIVE, "a1");
    assert_eq!(
        wf.crash_actor_reported_by_worker(&r, &uid, "other", "x")
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let crashed = wf
        .crash_actor_reported_by_worker(&r, &uid, "w1", "sandbox died")
        .await
        .unwrap();
    assert_eq!(state(&crashed), pb::ActorState::Crashed);
    assert_eq!(
        crashed.status.unwrap().crash.unwrap().message,
        "sandbox died"
    );
    assert_eq!(
        wf.crash_actor_reported_by_worker(&r, &uid, "w1", "x")
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let _ = h.store.get_actor(&r).await.unwrap();
    let _ = Precondition::default();
}
