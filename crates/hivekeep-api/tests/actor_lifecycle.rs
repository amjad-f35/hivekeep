//! Functional tests of the Actor lifecycle through the Control API.

mod common;

use common::*;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::keeper::v1 as kp;
use tonic::{Code, Status};

#[tokio::test]
async fn create_actor_success() {
    let h = Harness::new().await;
    h.create_template().await;
    let a = h.create_actor("a1", "tmpl1").await;
    assert_eq!(state(&a), pb::ActorState::Suspended);
    let md = a.metadata.as_ref().unwrap();
    assert!(!md.uid.is_empty());
    assert_eq!(md.version, 1);
    assert!(a.status.as_ref().unwrap().snapshots.is_empty());
}

#[tokio::test]
async fn create_actor_template_not_found() {
    let h = Harness::new().await;
    let r = h
        .c()
        .create_actor(h.req(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(md(HIVE, "a1")),
                actor_template: Some(oref(HIVE, "nope")),
                ..Default::default()
            }),
        }))
        .await;
    assert_code(r, Code::FailedPrecondition, "actor template not found");
}

#[tokio::test]
async fn create_actor_duplicate() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_actor("a1", "tmpl1").await;
    let r = h
        .c()
        .create_actor(h.req(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(md(HIVE, "a1")),
                actor_template: Some(oref(HIVE, "tmpl1")),
                ..Default::default()
            }),
        }))
        .await;
    assert_code(r, Code::AlreadyExists, "already exists");
}

#[tokio::test]
async fn create_actor_hive_not_found() {
    let h = Harness::new().await;
    h.create_template().await;
    let r = h
        .c()
        .create_actor(h.req(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(md("other", "a1")),
                actor_template: Some(oref(HIVE, "tmpl1")),
                ..Default::default()
            }),
        }))
        .await;
    assert_code(r, Code::FailedPrecondition, "Hive other not found");
}

#[tokio::test]
async fn create_actor_borrows_golden_snapshot() {
    let h = Harness::new().await;
    let t = h.create_template_with_golden().await;
    let a = h.create_actor("a1", "tmpl1").await;
    let st = a.status.unwrap();
    assert_eq!(st.last_assigned_generation, 1);
    assert_eq!(st.snapshots.len(), 1);
    let s = &st.snapshots[0];
    assert_eq!(s.owner, pb::SnapshotOwner::Tag as i32);
    assert_eq!(s.actor_template_uid, t.metadata.unwrap().uid);
}

#[tokio::test]
async fn create_actor_pending_tag_refused() {
    let h = Harness::new().await;
    let t = h.create_template().await;
    let uid = t.metadata.as_ref().unwrap().uid.clone();
    h.store
        .create_tag(&pb::Tag {
            metadata: Some(md(HIVE, "pending")),
            source_actor: Some(oref(HIVE, "x")),
            scope: pb::TagScope::Hive as i32,
            status: Some(pb::TagStatus {
                snapshot: None,
                actor_template_uid: uid,
            }),
        })
        .await
        .unwrap();
    let r = h
        .c()
        .create_actor(h.req(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(md(HIVE, "a1")),
                actor_template: Some(oref(HIVE, "tmpl1")),
                source_tag: Some(oref(HIVE, "pending")),
                ..Default::default()
            }),
        }))
        .await;
    assert_code(r, Code::FailedPrecondition, "still being created");
}

#[tokio::test]
async fn get_and_list_actors() {
    let h = Harness::new().await;
    h.create_template().await;
    for n in ["a1", "a2", "a3"] {
        h.create_actor(n, "tmpl1").await;
    }
    let got = h
        .c()
        .get_actor(h.req(pb::GetActorRequest {
            actor: Some(oref(HIVE, "a2")),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.metadata.unwrap().name, "a2");
    assert_code(
        h.c()
            .get_actor(h.req(pb::GetActorRequest {
                actor: Some(oref(HIVE, "zz")),
            }))
            .await,
        Code::NotFound,
        "not found",
    );

    let page = h
        .c()
        .list_actors(h.req(pb::ListActorsRequest {
            hive: HIVE.into(),
            options: Some(pb::ListOptions {
                page_size: 2,
                page_token: String::new(),
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.actors.len(), 2);
    assert!(!page.next_page_token.is_empty());
    let rest = h
        .c()
        .list_actors(h.req(pb::ListActorsRequest {
            hive: HIVE.into(),
            options: Some(pb::ListOptions {
                page_size: 2,
                page_token: page.next_page_token,
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rest.actors.len(), 1);
    let all = h
        .c()
        .list_actors(h.req(pb::ListActorsRequest {
            hive: String::new(),
            options: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(all.actors.len(), 3);
    let bad = h
        .c()
        .list_actors(h.req(pb::ListActorsRequest {
            hive: HIVE.into(),
            options: Some(pb::ListOptions {
                page_size: 1,
                page_token: "garbage".into(),
            }),
        }))
        .await;
    assert_code(bad, Code::InvalidArgument, "page_token");
}

#[tokio::test]
async fn resume_cold_boots_then_is_idempotent() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    assert_eq!(state(&a), pb::ActorState::Running);
    let wa = a
        .status
        .as_ref()
        .unwrap()
        .worker_assignment
        .as_ref()
        .unwrap();
    assert_eq!(wa.worker.as_ref().unwrap().name, "w1");
    assert_eq!(wa.worker_epoch, REPORTED_EPOCH);
    assert_eq!(wa.node_name, NODE);
    assert_eq!(wa.sandbox_runtime.as_ref().unwrap().sandbox_class, "gvisor");
    let run = h.ks().run.clone().unwrap();
    assert_eq!(run.actor_name, "a1");
    assert_eq!(run.spec.unwrap().containers[0].image, IMAGE);
    assert_eq!(run.resources.unwrap().limits.unwrap().cpu_millicores, 1000);
    // Running already: resumed=false, no keeper call.
    h.reset_keeper();
    let again = h.resume("a1").await.unwrap();
    assert!(!again.resumed);
    assert!(h.ks().run.is_none());
}

#[tokio::test]
async fn resume_passes_literal_env() {
    let h = Harness::new().await;
    let mut c = container();
    c.env = vec![pb::EnvVar {
        name: "A".into(),
        value: "$(B)".into(),
    }];
    h.create_template_with("tmpl1", "pool1", vec![c], vec![])
        .await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.resume("a1").await.unwrap();
    assert_eq!(
        h.ks().run.clone().unwrap().spec.unwrap().containers[0].env[0].value,
        "$(B)"
    );
}

#[tokio::test]
async fn resume_no_workers_is_resource_exhausted() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_actor("a1", "tmpl1").await;
    assert_code(
        h.resume("a1").await,
        Code::ResourceExhausted,
        "no worker has room",
    );
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Suspended);
}

#[tokio::test]
async fn resume_requires_both_selectors() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.c()
        .create_actor(h.req(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(md(HIVE, "a1")),
                actor_template: Some(oref(HIVE, "tmpl1")),
                worker_selector: Some(pb::Selector {
                    match_labels: [("pool".to_string(), "pool2".to_string())].into(),
                }),
                ..Default::default()
            }),
        }))
        .await
        .unwrap();
    assert_code(h.resume("a1").await, Code::ResourceExhausted, "");
}

#[tokio::test]
async fn resume_keeper_failure_crashes_actor() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.ks().fail_run = Some(Status::internal("sandbox exploded"));
    assert!(h.resume("a1").await.is_err());
    let a = h.get_actor("a1").await;
    assert_eq!(state(&a), pb::ActorState::Crashed);
    let crash = a.status.as_ref().unwrap().crash.as_ref().unwrap();
    assert_eq!(crash.message, "resume failed: keeper Run: sandbox exploded");
    assert!(a.status.unwrap().worker_assignment.is_none());
    h.wait_worker_free("w1").await;
}

#[tokio::test]
async fn resume_transport_error_leaves_actor_resuming() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.ks().fail_run = Some(Status::unavailable("connection refused"));
    assert_code(h.resume("a1").await, Code::Unavailable, "keeper Run");
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Resuming);
    // The retry fast-forwards past assignment and succeeds.
    h.ks().fail_run = None;
    let r = h.resume("a1").await.unwrap();
    assert!(r.resumed);
    assert_eq!(state(r.actor.as_ref().unwrap()), pb::ActorState::Running);
}

#[tokio::test]
async fn suspend_sync_then_resume_restores() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    let s = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    assert_eq!(state(&s), pb::ActorState::Suspended);
    let st = s.status.as_ref().unwrap();
    assert!(st.worker_assignment.is_none());
    assert!(st.assigned_node.is_empty());
    assert_eq!(st.last_assigned_generation, 1);
    let snap = latest_durable(&s).unwrap();
    assert_eq!(snap.owner, pb::SnapshotOwner::Actor as i32);
    assert!(uri_of(snap).contains(&uid));
    assert!(!h.objects.list(&uri_of(snap)).is_empty());
    let cp = h.ks().checkpoint.clone().unwrap();
    assert_eq!(
        cp.upload_config.unwrap().mode,
        kp::UploadMode::Immediate as i32
    );
    assert_eq!(
        cp.snapshot.unwrap().fidelity,
        kp::SnapshotFidelity::Memory as i32
    );
    h.wait_worker_free("w1").await;

    // Suspending again is a no-op.
    let again = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    assert_eq!(
        again.metadata.unwrap().version,
        s.metadata.as_ref().unwrap().version
    );

    let r = h.resume("a1").await.unwrap();
    assert!(r.resumed);
    let restore = h.ks().restore.clone().unwrap();
    let rs = restore.snapshot.unwrap();
    assert_eq!(rs.generation, 1);
    assert_eq!(
        rs.object.unwrap().sharing,
        kp::SnapshotSharing::Private as i32
    );
}

#[tokio::test]
async fn suspend_async_pins_to_node_until_reported() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    let s = h
        .suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    let st = s.status.as_ref().unwrap();
    assert_eq!(st.assigned_node, NODE);
    let local = latest_local(&s).unwrap().clone();
    assert_eq!(
        local.durable_snapshot.as_ref().unwrap().state,
        pb::SnapshotStorageState::Pending as i32
    );
    assert_eq!(
        h.ks()
            .checkpoint
            .clone()
            .unwrap()
            .upload_config
            .unwrap()
            .mode,
        kp::UploadMode::Deferred as i32
    );

    // The keeper reports the upload, then the detach.
    let report = pb::SnapshotReport {
        uuid: local.uuid.clone(),
        generation: local.generation,
        fidelity: pb::SnapshotFidelity::Memory as i32,
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
                actor_uid: uid.clone(),
                state: Some(pb::ActorSnapshotState {
                    latest_local: Some(report.clone()),
                    latest_durable: Some(report.clone()),
                    ..Default::default()
                }),
            },
        ))
        .await
        .unwrap();
    let a = h.get_actor("a1").await;
    assert!(latest_durable(&a).is_some());
    assert_eq!(a.status.as_ref().unwrap().assigned_node, NODE);
    h.ws()
        .report_actor_snapshot_state(h.keeper_req(
            NODE,
            pb::ReportActorSnapshotStateRequest {
                actor: Some(oref(HIVE, "a1")),
                actor_uid: uid,
                state: Some(pb::ActorSnapshotState {
                    latest_durable: Some(report),
                    ..Default::default()
                }),
            },
        ))
        .await
        .unwrap();
    let a = h.get_actor("a1").await;
    assert!(a.status.as_ref().unwrap().assigned_node.is_empty());
    assert!(latest_local(&a).is_none());
}

#[tokio::test]
async fn suspend_keeps_local_copy_and_resumes_locally() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.ks().keep_checkpoint_local = true;
    let s = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    assert_eq!(s.status.as_ref().unwrap().assigned_node, NODE);
    let local = latest_local(&s).unwrap();
    assert_eq!(local.locality, NODE);
    assert!(latest_durable(&s).is_some());
    h.wait_worker_free("w1").await;
    h.resume("a1").await.unwrap();
    assert!(h.ks().restore.is_some());
}

#[tokio::test]
async fn checkpoint_failure_crashes_and_snapshot_conflict_preserves() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.ks().fail_checkpoint = Some(Status::aborted("snapshot conflict"));
    assert_code(
        h.suspend("a1", pb::DurabilityMode::Synchronous).await,
        Code::Aborted,
        "Checkpoint",
    );
    let a = h.get_actor("a1").await;
    assert_eq!(state(&a), pb::ActorState::Suspending);
    assert!(a.status.as_ref().unwrap().worker_assignment.is_some());

    h.ks().fail_checkpoint = Some(Status::internal("disk full"));
    assert!(h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .is_err());
    let a = h.get_actor("a1").await;
    assert_eq!(state(&a), pb::ActorState::Crashed);
    assert!(a
        .status
        .unwrap()
        .crash
        .unwrap()
        .message
        .contains("keeper Checkpoint: disk full"));
}

#[tokio::test]
async fn revert_running_actor() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    let r = h.revert("a1").await.unwrap();
    assert_eq!(state(&r), pb::ActorState::Suspended);
    assert!(r.status.as_ref().unwrap().worker_assignment.is_none());
    assert!(h.ks().terminate.is_some());
    h.wait_worker_free("w1").await;
}

#[tokio::test]
async fn revert_crashed_actor_clears_crash() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.ks().fail_run = Some(Status::internal("boom"));
    let _ = h.resume("a1").await;
    let r = h.revert("a1").await.unwrap();
    assert_eq!(state(&r), pb::ActorState::Suspended);
    assert!(r.status.unwrap().crash.is_none());
}

#[tokio::test]
async fn revert_rejects_suspended_and_missing() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_actor("a1", "tmpl1").await;
    assert_code(
        h.revert("a1").await,
        Code::FailedPrecondition,
        "not in a revertable state",
    );
    assert_code(h.revert("zz").await, Code::NotFound, "not found");
}

#[tokio::test]
async fn revert_terminate_failure_leaves_reverting() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.ks().fail_terminate = Some(Status::internal("stuck"));
    assert!(h.revert("a1").await.is_err());
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Reverting);
    h.ks().fail_terminate = None;
    assert_eq!(
        state(&h.revert("a1").await.unwrap()),
        pb::ActorState::Suspended
    );
}

#[tokio::test]
async fn delete_suspended_and_running_actors() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_actor("a1", "tmpl1").await;
    let d = h.delete("a1").await.unwrap();
    assert!(d.metadata.unwrap().delete_time.is_some());
    assert_code(h.delete("a1").await, Code::NotFound, "not found");

    h.running_actor("a2").await;
    h.suspend("a2", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    let snap_uri = uri_of(latest_durable(&h.get_actor("a2").await).unwrap());
    h.resume("a2").await.unwrap();
    h.reset_keeper();
    h.delete("a2").await.unwrap();
    assert!(h.ks().terminate.is_some());
    assert!(
        h.objects.list(&snap_uri).is_empty(),
        "the actor's snapshots are collected"
    );
    h.wait_worker_free("w1").await;
}

#[tokio::test]
async fn delete_terminate_failure_leaves_deleting() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.ks().fail_terminate = Some(Status::internal("stuck"));
    assert!(h.delete("a1").await.is_err());
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Deleting);
    h.ks().fail_terminate = None;
    h.delete("a1").await.unwrap();
}

#[tokio::test]
async fn delete_preconditions() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_actor("a1", "tmpl1").await;
    let r = h
        .c()
        .delete_actor(h.req(pb::DeleteActorRequest {
            actor: Some(oref(HIVE, "a1")),
            options: Some(pb::DeleteOptions {
                version: 0,
                uid: uuid::Uuid::new_v4().to_string(),
            }),
        }))
        .await;
    assert_code(r, Code::Aborted, "does not have uid");
    let r = h
        .c()
        .delete_actor(h.req(pb::DeleteActorRequest {
            actor: Some(oref(HIVE, "a1")),
            options: Some(pb::DeleteOptions {
                version: 99,
                uid: String::new(),
            }),
        }))
        .await;
    assert_code(r, Code::Aborted, "");
}

#[tokio::test]
async fn update_actor_and_preconditions() {
    let h = Harness::new().await;
    h.create_template().await;
    let a = h.create_actor("a1", "tmpl1").await;
    let mut changed = a.clone();
    changed.worker_selector = Some(pb::Selector {
        match_labels: [("pool".to_string(), "pool1".to_string())].into(),
    });
    let u = h
        .c()
        .update_actor(h.req(pb::UpdateActorRequest {
            actor: Some(changed.clone()),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(u.metadata.as_ref().unwrap().version, 2);
    assert!(u.worker_selector.is_some());
    // Stale version.
    assert_code(
        h.c()
            .update_actor(h.req(pb::UpdateActorRequest {
                actor: Some(changed),
            }))
            .await,
        Code::Aborted,
        "conflict",
    );
    // Missing actor.
    let mut ghost = u.clone();
    ghost.metadata.as_mut().unwrap().name = "ghost".into();
    assert_code(
        h.c()
            .update_actor(h.req(pb::UpdateActorRequest { actor: Some(ghost) }))
            .await,
        Code::NotFound,
        "not found",
    );
}

#[tokio::test]
async fn update_actor_repoint_template() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_template_with("tmpl2", "pool1", vec![container()], vec![])
        .await;
    let a = h.create_actor("a1", "tmpl1").await;
    let mut re = a.clone();
    re.actor_template = Some(oref(HIVE, "tmpl2"));
    let u = h
        .c()
        .update_actor(h.req(pb::UpdateActorRequest { actor: Some(re) }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(u.actor_template.as_ref().unwrap().name, "tmpl2");

    // A missing template is refused.
    let mut bad = u.clone();
    bad.actor_template = Some(oref(HIVE, "nope"));
    assert_code(
        h.c()
            .update_actor(h.req(pb::UpdateActorRequest { actor: Some(bad) }))
            .await,
        Code::FailedPrecondition,
        "actor template not found",
    );

    // A template with other volumes is refused.
    h.create_template_with(
        "tmpl3",
        "pool1",
        vec![container()],
        vec![pb::Volume {
            name: "d".into(),
            durable_dir: Some(pb::DurableDirVolumeSource {}),
            ..Default::default()
        }],
    )
    .await;
    let mut vol = u.clone();
    vol.actor_template = Some(oref(HIVE, "tmpl3"));
    assert_code(
        h.c()
            .update_actor(h.req(pb::UpdateActorRequest { actor: Some(vol) }))
            .await,
        Code::FailedPrecondition,
        "volumes differ",
    );

    // Only while SUSPENDED.
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.resume("a1").await.unwrap();
    let mut running = h.get_actor("a1").await;
    running.actor_template = Some(oref(HIVE, "tmpl1"));
    assert_code(
        h.c()
            .update_actor(h.req(pb::UpdateActorRequest {
                actor: Some(running),
            }))
            .await,
        Code::FailedPrecondition,
        "to change its actor template",
    );
}

#[tokio::test]
async fn mint_actor_jwt() {
    let h = Harness::new().await;
    h.create_template().await;
    let a = h.create_actor("a1", "tmpl1").await;
    let resp = h
        .c()
        .mint_actor_jwt(h.req(pb::MintActorJwtRequest {
            actor: Some(oref(HIVE, "a1")),
            audiences: vec!["aud".into()],
            expiration_seconds: 600,
        }))
        .await
        .unwrap()
        .into_inner();
    let verifier = hivekeep_identity::jwt::verifier::Verifier::new(
        ISSUER,
        vec!["aud".into()],
        std::sync::Arc::new(hivekeep_identity::jwt::verifier::SignerKeySource(
            h.jwt.clone(),
        )),
    )
    .with_actor_claims();
    let t = verifier
        .verify(&resp.actor_jwt, std::time::SystemTime::now())
        .unwrap();
    let (_, claims) = t.actor.unwrap();
    assert_eq!(claims.actor_uid, a.metadata.unwrap().uid);
    let exp = std::time::SystemTime::try_from(resp.expire_time.unwrap()).unwrap();
    assert_eq!(exp, t.expiration);
    assert_eq!(
        t.expiration.duration_since(t.issued_at).unwrap().as_secs(),
        600
    );
    assert_code(
        h.c()
            .mint_actor_jwt(h.req(pb::MintActorJwtRequest {
                actor: Some(oref(HIVE, "zz")),
                audiences: vec!["aud".into()],
                expiration_seconds: 600,
            }))
            .await,
        Code::NotFound,
        "actor not found",
    );
}

#[tokio::test]
async fn mint_actor_certificate_requires_client_cert() {
    let h = Harness::new().await;
    h.create_template().await;
    let a = h.create_actor("a1", "tmpl1").await;
    let key = rcgen::KeyPair::generate().unwrap();
    let csr = rcgen::CertificateParams::new(vec![])
        .unwrap()
        .serialize_request(&key)
        .unwrap()
        .der()
        .to_vec();
    let req = pb::MintActorCertificateRequest {
        actor: Some(oref(HIVE, "a1")),
        actor_uid: a.metadata.as_ref().unwrap().uid.clone(),
        certificate_signing_request: csr,
    };
    assert_code(
        h.c().mint_actor_certificate(h.req(req.clone())).await,
        Code::Unauthenticated,
        "peer certificate",
    );
    let chain = hivekeep_api::keeper_auth::testing::pod_cert(&h.pod_ca, "ns", "gate", NODE);
    let r = hivekeep_api::context::request_as(
        req.clone(),
        Some(hivekeep_api::context::Principal::mtls("spiffe://x")),
        Some(chain.clone()),
    );
    let certs = h
        .c()
        .mint_actor_certificate(r)
        .await
        .unwrap()
        .into_inner()
        .actor_certificates;
    let sans = hivekeep_api::peer_cert::uri_sans(&certs[0]);
    assert_eq!(
        sans,
        vec![format!("spiffe://hivekeep-actor.local/actor/{HIVE}/a1")]
    );
    let mut stale = req;
    stale.actor_uid = uuid::Uuid::new_v4().to_string();
    let r = hivekeep_api::context::request_as(
        stale,
        Some(hivekeep_api::context::Principal::mtls("spiffe://x")),
        Some(chain),
    );
    assert_code(
        h.c().mint_actor_certificate(r).await,
        Code::Aborted,
        "recreated",
    );
}
