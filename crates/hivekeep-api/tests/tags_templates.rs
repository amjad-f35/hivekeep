//! Tags, ActorTemplates and the golden snapshot reconciler.

mod common;

use common::*;
use hivekeep_api::template_reconciler::TemplateReconciler;
use hivekeep_core::resource::{ActorRef, ActorTemplateRef, TagRef};
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::Precondition;
use tonic::{Code, Status};

fn tag_req(name: &str, actor: &str, scope: pb::TagScope) -> pb::CreateTagRequest {
    pb::CreateTagRequest {
        tag: Some(pb::Tag {
            metadata: Some(md(HIVE, name)),
            source_actor: Some(oref(HIVE, actor)),
            scope: scope as i32,
            status: None,
        }),
    }
}

async fn suspended_actor(h: &Harness, name: &str) -> pb::Actor {
    h.running_actor(name).await;
    h.suspend(name, pb::DurabilityMode::Synchronous)
        .await
        .unwrap()
}

#[tokio::test]
async fn tag_actor_snapshot() {
    let h = Harness::new().await;
    let a = suspended_actor(&h, "a1").await;
    let src = uri_of(latest_durable(&a).unwrap());
    let tag = h
        .c()
        .create_tag(h.req(tag_req("t1", "a1", pb::TagScope::Hive)))
        .await
        .unwrap()
        .into_inner();
    let snap = tag.status.as_ref().unwrap().snapshot.as_ref().unwrap();
    assert_eq!(snap.owner, pb::SnapshotOwner::Tag as i32);
    assert_eq!(
        snap.durable_snapshot.as_ref().unwrap().state,
        pb::SnapshotStorageState::Completed as i32
    );
    let dst = uri_of(snap);
    assert!(dst.contains(&tag.metadata.as_ref().unwrap().uid));
    assert_eq!(h.objects.list(&dst), h.objects.list(&src));
    // Deleting the actor leaves the Tag's copy.
    h.delete("a1").await.unwrap();
    assert!(h.objects.list(&src).is_empty());
    assert!(!h.objects.list(&dst).is_empty());
    // A new actor can start from it.
    let a2 = h
        .c()
        .create_actor(h.req(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(md(HIVE, "a2")),
                actor_template: Some(oref(HIVE, "tmpl1")),
                source_tag: Some(oref(HIVE, "t1")),
                ..Default::default()
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(uri_of(&a2.status.as_ref().unwrap().snapshots[0]), dst);
    h.resume("a2").await.unwrap();
    assert_eq!(
        h.ks()
            .restore
            .clone()
            .unwrap()
            .snapshot
            .unwrap()
            .object
            .unwrap()
            .sharing,
        hivekeep_proto::keeper::v1::SnapshotSharing::Shared as i32
    );
}

#[tokio::test]
async fn tag_actor_snapshot_preconditions() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    assert_code(
        h.c()
            .create_tag(h.req(tag_req("t1", "a1", pb::TagScope::Hive)))
            .await,
        Code::FailedPrecondition,
        "must be ACTOR_STATE_SUSPENDED",
    );
    assert_code(
        h.c()
            .create_tag(h.req(tag_req("t1", "zz", pb::TagScope::Hive)))
            .await,
        Code::FailedPrecondition,
        "not found",
    );
    h.create_actor("fresh", "tmpl1").await;
    assert_code(
        h.c()
            .create_tag(h.req(tag_req("t1", "fresh", pb::TagScope::Hive)))
            .await,
        Code::FailedPrecondition,
        "holds no external snapshot",
    );
}

#[tokio::test]
async fn create_tag_reused_name_and_taken_by_another_actor() {
    let h = Harness::new().await;
    suspended_actor(&h, "a1").await;
    h.c()
        .create_tag(h.req(tag_req("t1", "a1", pb::TagScope::Hive)))
        .await
        .unwrap();
    assert_code(
        h.c()
            .create_tag(h.req(tag_req("t1", "a1", pb::TagScope::Hive)))
            .await,
        Code::AlreadyExists,
        "already exists",
    );
    h.resume("a1").await.unwrap();
    h.create_actor("a2", "tmpl1").await;
    h.resume("a2").await.unwrap();
    h.suspend("a2", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    assert_code(
        h.c()
            .create_tag(h.req(tag_req("t1", "a2", pb::TagScope::Hive)))
            .await,
        Code::AlreadyExists,
        "",
    );
}

#[tokio::test]
async fn create_tag_from_paused_actor_promotes_local_snapshot() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    let paused = h
        .suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    assert!(latest_durable(&paused).is_none());
    let tag = h
        .c()
        .create_tag(h.req(tag_req("t1", "a1", pb::TagScope::Hive)))
        .await
        .unwrap()
        .into_inner();
    assert!(h.ks().promote.is_some());
    let a = h.get_actor("a1").await;
    assert!(
        latest_durable(&a).is_some(),
        "the promoted upload is recorded"
    );
    assert!(latest_local(&a).is_some(), "the local copy is kept");
    assert!(!h
        .objects
        .list(&uri_of(
            tag.status.as_ref().unwrap().snapshot.as_ref().unwrap()
        ))
        .is_empty());
    // A failed promote fails the Tag but leaves the actor.
    h.running_actor("a2").await;
    h.suspend("a2", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    h.ks().fail_promote = Some(Status::unavailable("node down"));
    assert_code(
        h.c()
            .create_tag(h.req(tag_req("t2", "a2", pb::TagScope::Hive)))
            .await,
        Code::Unavailable,
        "PromoteSnapshot",
    );
    assert_eq!(state(&h.get_actor("a2").await), pb::ActorState::Suspended);
    assert!(
        h.store.get_tag(&TagRef::new(HIVE, "t2")).await.is_err(),
        "no pending tag is left"
    );
}

#[tokio::test]
async fn tag_recreate_after_copy_failure() {
    let h = Harness::new().await;
    suspended_actor(&h, "a1").await;
    // A copy with no source objects fails after the name is reserved.
    let a = h.get_actor("a1").await;
    let src = uri_of(latest_durable(&a).unwrap());
    let objs = h.objects.list(&src);
    use hivekeep_api::object_store::ObjectStore;
    h.objects.cleanup_snapshot(&src).await.unwrap();
    assert!(h
        .c()
        .create_tag(h.req(tag_req("t1", "a1", pb::TagScope::Hive)))
        .await
        .is_err());
    let pending = h.store.get_tag(&TagRef::new(HIVE, "t1")).await.unwrap();
    assert_eq!(
        pending
            .status
            .unwrap()
            .snapshot
            .unwrap()
            .durable_snapshot
            .unwrap()
            .state,
        pb::SnapshotStorageState::Pending as i32
    );
    assert_code(
        h.c()
            .update_tag(h.req(pb::UpdateTagRequest {
                tag: Some(h.store.get_tag(&TagRef::new(HIVE, "t1")).await.unwrap()),
            }))
            .await,
        Code::FailedPrecondition,
        "still being created",
    );
    // Delete, restore the source, and create again.
    h.c()
        .delete_tag(h.req(pb::DeleteTagRequest {
            tag: Some(oref(HIVE, "t1")),
            options: None,
        }))
        .await
        .unwrap();
    for o in objs {
        h.objects.put(format!("{src}/{o}"), "x");
    }
    h.c()
        .create_tag(h.req(tag_req("t1", "a1", pb::TagScope::Hive)))
        .await
        .unwrap();
}

#[tokio::test]
async fn list_get_update_tags() {
    let h = Harness::new().await;
    suspended_actor(&h, "a1").await;
    let t = h
        .c()
        .create_tag(h.req(tag_req("t1", "a1", pb::TagScope::Hive)))
        .await
        .unwrap()
        .into_inner();
    let got = h
        .c()
        .get_tag(h.req(pb::GetTagRequest {
            tag: Some(oref(HIVE, "t1")),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got, t);
    assert_code(
        h.c()
            .get_tag(h.req(pb::GetTagRequest {
                tag: Some(oref(HIVE, "nope")),
            }))
            .await,
        Code::NotFound,
        "",
    );
    let list = h
        .c()
        .list_tags(h.req(pb::ListTagsRequest {
            hive: HIVE.into(),
            options: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.tags.len(), 1);

    let mut pubd = t.clone();
    pubd.scope = pb::TagScope::Published as i32;
    let u = h
        .c()
        .update_tag(h.req(pb::UpdateTagRequest {
            tag: Some(pubd.clone()),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(u.scope, pb::TagScope::Published as i32);
    assert_eq!(u.status, t.status, "the snapshot never changes");
    // Stale.
    assert_code(
        h.c()
            .update_tag(h.req(pb::UpdateTagRequest { tag: Some(pubd) }))
            .await,
        Code::Aborted,
        "",
    );
    // Blind write.
    let mut blind = u.clone();
    blind.metadata.as_mut().unwrap().version = 0;
    blind.metadata.as_mut().unwrap().uid.clear();
    assert!(h
        .c()
        .update_tag(h.req(pb::UpdateTagRequest { tag: Some(blind) }))
        .await
        .is_err());
    // Not found.
    let mut ghost = u.clone();
    ghost.metadata.as_mut().unwrap().name = "ghost".into();
    assert_code(
        h.c()
            .update_tag(h.req(pb::UpdateTagRequest { tag: Some(ghost) }))
            .await,
        Code::NotFound,
        "",
    );
    // Moving the source is refused.
    let mut moved = u.clone();
    moved.source_actor = Some(oref(HIVE, "other"));
    assert_code(
        h.c()
            .update_tag(h.req(pb::UpdateTagRequest { tag: Some(moved) }))
            .await,
        Code::InvalidArgument,
        "",
    );
}

#[tokio::test]
async fn delete_tag_releases_snapshot_and_preconditions() {
    let h = Harness::new().await;
    suspended_actor(&h, "a1").await;
    let t = h
        .c()
        .create_tag(h.req(tag_req("t1", "a1", pb::TagScope::Hive)))
        .await
        .unwrap()
        .into_inner();
    let uri = uri_of(t.status.as_ref().unwrap().snapshot.as_ref().unwrap());
    let r = h
        .c()
        .delete_tag(h.req(pb::DeleteTagRequest {
            tag: Some(oref(HIVE, "t1")),
            options: Some(pb::DeleteOptions {
                uid: uuid::Uuid::new_v4().to_string(),
                version: 0,
            }),
        }))
        .await;
    assert_code(r, Code::Aborted, "does not have uid");
    h.c()
        .delete_tag(h.req(pb::DeleteTagRequest {
            tag: Some(oref(HIVE, "t1")),
            options: None,
        }))
        .await
        .unwrap();
    assert!(h.objects.list(&uri).is_empty());
    assert_code(
        h.c()
            .delete_tag(h.req(pb::DeleteTagRequest {
                tag: Some(oref(HIVE, "t1")),
                options: None,
            }))
            .await,
        Code::NotFound,
        "",
    );
}

#[tokio::test]
async fn update_tag_deleting_is_refused() {
    let h = Harness::new().await;
    suspended_actor(&h, "a1").await;
    let t = h
        .c()
        .create_tag(h.req(tag_req("t1", "a1", pb::TagScope::Hive)))
        .await
        .unwrap()
        .into_inner();
    let marked = h
        .store
        .mark_tag_for_deletion(&TagRef::new(HIVE, "t1"), Default::default(), None)
        .await
        .unwrap();
    let mut u = marked.clone();
    u.scope = pb::TagScope::Published as i32;
    assert_code(
        h.c()
            .update_tag(h.req(pb::UpdateTagRequest { tag: Some(u) }))
            .await,
        Code::FailedPrecondition,
        "being deleted",
    );
    let _ = t;
}

#[tokio::test]
async fn tag_scope_across_hives() {
    let h = Harness::new().await;
    suspended_actor(&h, "a1").await;
    h.c()
        .create_tag(h.req(tag_req("t1", "a1", pb::TagScope::Hive)))
        .await
        .unwrap();
    h.c().create_hive(h.req(hive("other"))).await.unwrap();
    let r = h
        .c()
        .create_actor(h.req(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(md("other", "x")),
                actor_template: Some(oref(HIVE, "tmpl1")),
                source_tag: Some(oref(HIVE, "t1")),
                ..Default::default()
            }),
        }))
        .await;
    assert_code(
        r,
        Code::FailedPrecondition,
        "not published outside its Hive",
    );
}

#[tokio::test]
async fn create_actor_rejects_different_template_for_tag() {
    let h = Harness::new().await;
    suspended_actor(&h, "a1").await;
    h.c()
        .create_tag(h.req(tag_req("t1", "a1", pb::TagScope::Published)))
        .await
        .unwrap();
    h.create_template_with("tmpl2", "pool1", vec![container()], vec![])
        .await;
    let r = h
        .c()
        .create_actor(h.req(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(md(HIVE, "x")),
                actor_template: Some(oref(HIVE, "tmpl2")),
                source_tag: Some(oref(HIVE, "t1")),
                ..Default::default()
            }),
        }))
        .await;
    assert_code(
        r,
        Code::FailedPrecondition,
        "must be taken from an actor with ActorTemplate uid",
    );
}

#[tokio::test]
async fn actor_template_crud() {
    let h = Harness::new().await;
    let t = h.create_template().await;
    let md_ = t.metadata.as_ref().unwrap();
    assert!(!md_.uid.is_empty());
    let sc = t.snapshot_config.as_ref().unwrap();
    assert_eq!(
        sc.preferred_fidelity,
        pb::SnapshotFidelity::Memory as i32,
        "defaults are applied"
    );
    assert_eq!(
        sc.golden_snapshot_config.as_ref().unwrap().mode,
        pb::GoldenSnapshotMode::Enabled as i32
    );
    assert!(t.status.is_some());
    let got = h
        .c()
        .get_actor_template(h.req(pb::GetActorTemplateRequest {
            actor_template: Some(oref(HIVE, "tmpl1")),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got, t);
    let l = h
        .c()
        .list_actor_templates(h.req(pb::ListActorTemplatesRequest {
            hive: HIVE.into(),
            options: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(l.actor_templates.len(), 1);
    let bad = h
        .c()
        .list_actor_templates(h.req(pb::ListActorTemplatesRequest {
            hive: HIVE.into(),
            options: Some(pb::ListOptions {
                page_size: 1,
                page_token: "bad".into(),
            }),
        }))
        .await;
    assert_code(bad, Code::InvalidArgument, "page_token");
    let dup = h
        .c()
        .create_actor_template(h.req(pb::CreateActorTemplateRequest {
            actor_template: Some(pb::ActorTemplate {
                metadata: Some(md(HIVE, "tmpl1")),
                containers: vec![container()],
                resources: Some(pb::ResourceRequirements {
                    limits: Some(pb::Resources {
                        cpu_millicores: 1000,
                        memory_bytes: 512 << 20,
                    }),
                }),
                ..Default::default()
            }),
        }))
        .await;
    assert_code(dup, Code::AlreadyExists, "");
    let r = h
        .c()
        .delete_actor_template(h.req(pb::DeleteActorTemplateRequest {
            actor_template: Some(oref(HIVE, "tmpl1")),
            options: Some(pb::DeleteOptions {
                uid: uuid::Uuid::new_v4().to_string(),
                version: 0,
            }),
        }))
        .await;
    assert_code(r, Code::Aborted, "does not have uid");
    h.c()
        .delete_actor_template(h.req(pb::DeleteActorTemplateRequest {
            actor_template: Some(oref(HIVE, "tmpl1")),
            options: None,
        }))
        .await
        .unwrap();
    assert_code(
        h.c()
            .get_actor_template(h.req(pb::GetActorTemplateRequest {
                actor_template: Some(oref(HIVE, "tmpl1")),
            }))
            .await,
        Code::NotFound,
        "",
    );
}

#[tokio::test]
async fn create_actor_template_ignores_server_owned_fields() {
    let h = Harness::new().await;
    let t = h
        .c()
        .create_actor_template(h.req(pb::CreateActorTemplateRequest {
            actor_template: Some(pb::ActorTemplate {
                metadata: Some(pb::ResourceMetadata {
                    hive: HIVE.into(),
                    name: "t".into(),
                    version: 9,
                    ..Default::default()
                }),
                containers: vec![container()],
                resources: Some(pb::ResourceRequirements {
                    limits: Some(pb::Resources {
                        cpu_millicores: 1000,
                        memory_bytes: 512 << 20,
                    }),
                }),
                status: Some(pb::ActorTemplateStatus {
                    golden_snapshot_status: Some(pb::GoldenSnapshotStatus {
                        error_message: "x".into(),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(t.metadata.unwrap().version, 1);
    assert_eq!(t.status, Some(pb::ActorTemplateStatus::default()));
}

#[tokio::test]
async fn delete_actor_template_removes_golden_actor_and_tag() {
    let h = Harness::new().await;
    let t = h.create_template_with_golden().await;
    let uid = t.metadata.as_ref().unwrap().uid.clone();
    h.create_actor(&uid, "tmpl1").await;
    h.c()
        .delete_actor_template(h.req(pb::DeleteActorTemplateRequest {
            actor_template: Some(oref(HIVE, "tmpl1")),
            options: None,
        }))
        .await
        .unwrap();
    assert!(h
        .store
        .get_actor(&ActorRef::new(HIVE, uid.clone()))
        .await
        .is_err());
    assert!(h
        .store
        .get_tag(&TagRef::new(HIVE, uid.clone()))
        .await
        .is_err());
    assert!(h.objects.list(&golden_uri(&uid)).is_empty());
}

#[tokio::test]
async fn data_snapshot_ignores_golden() {
    let h = Harness::new().await;
    h.create_template_with_golden().await;
    h.create_worker("w1", NODE, "pool1", 2).await;
    h.create_actor("a1", "tmpl1").await;
    h.resume("a1").await.unwrap();
    // The first resume restores the golden snapshot.
    let r = h.ks().restore.clone().unwrap();
    assert_eq!(
        r.snapshot.unwrap().object.unwrap().sharing,
        hivekeep_proto::keeper::v1::SnapshotSharing::Shared as i32
    );
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    h.reset_keeper();
    h.resume("a1").await.unwrap();
    let r = h.ks().restore.clone().unwrap();
    assert_eq!(
        r.snapshot.unwrap().object.unwrap().sharing,
        hivekeep_proto::keeper::v1::SnapshotSharing::Private as i32
    );
}

async fn run_reconciler_until_done(
    h: &Harness,
    rec: &TemplateReconciler,
    r: &ActorTemplateRef,
) -> pb::ActorTemplate {
    for _ in 0..20 {
        rec.reconcile_one(r).await.unwrap();
        let t = h.store.get_actor_template(r).await.unwrap();
        if hivekeep_api::template_reconciler::golden_done(
            t.status
                .as_ref()
                .and_then(|s| s.golden_snapshot_status.as_ref()),
        ) {
            return t;
        }
    }
    panic!("the golden flow never finished");
}

fn probed() -> pb::Container {
    let mut c = container();
    c.wakeup_probe = Some(pb::WakeupProbe {
        http_get: Some(pb::HttpGetAction {
            path: "/".into(),
            port: 8080,
        }),
        timeout_seconds: 30,
    });
    c
}

#[tokio::test]
async fn reconcile_one_builds_golden_tag() {
    let h = Harness::new().await;
    let t = h
        .create_template_with("tmpl1", "pool1", vec![probed()], vec![])
        .await;
    h.create_worker("w1", NODE, "pool1", 2).await;
    let rec = TemplateReconciler::new(
        h.store.clone(),
        h.control.clone(),
        std::time::Duration::from_secs(1),
    );
    let r = ActorTemplateRef::new(HIVE, "tmpl1");
    let done = run_reconciler_until_done(&h, &rec, &r).await;
    let gs = done.status.unwrap().golden_snapshot_status.unwrap();
    let uid = t.metadata.as_ref().unwrap().uid.clone();
    assert_eq!(gs.golden_tag, Some(oref(HIVE, &uid)));
    assert!(gs.error_message.is_empty());
    let tag = h
        .store
        .get_tag(&TagRef::new(HIVE, uid.clone()))
        .await
        .unwrap();
    assert_eq!(tag.scope, pb::TagScope::Published as i32);
    assert!(
        h.store.get_actor(&ActorRef::new(HIVE, uid)).await.is_err(),
        "the golden actor is deleted"
    );
    // New actors start from it.
    let a = h.create_actor("a1", "tmpl1").await;
    assert_eq!(a.status.unwrap().snapshots.len(), 1);
    // Terminal: nothing more happens.
    assert!(rec.reconcile_one(&r).await.unwrap().is_none());
}

#[tokio::test]
async fn reconcile_one_waits_for_warmup_without_probes() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 2).await;
    let rec = TemplateReconciler::new(
        h.store.clone(),
        h.control.clone(),
        std::time::Duration::from_secs(1),
    );
    let r = ActorTemplateRef::new(HIVE, "tmpl1");
    let after = rec.reconcile_one(&r).await.unwrap();
    let wait = after.expect("a warmup deadline");
    assert!(wait > std::time::Duration::from_secs(10));
    let t = h.store.get_actor_template(&r).await.unwrap();
    assert!(t
        .status
        .unwrap()
        .golden_snapshot_status
        .unwrap()
        .snapshot_time
        .is_some());
}

#[tokio::test]
async fn reconcile_one_records_crash_and_golden_tag_conflict() {
    let h = Harness::new().await;
    h.create_template_with("tmpl1", "pool1", vec![probed()], vec![])
        .await;
    h.create_worker("w1", NODE, "pool1", 2).await;
    h.ks().fail_run = Some(Status::internal("boom"));
    let rec = TemplateReconciler::new(
        h.store.clone(),
        h.control.clone(),
        std::time::Duration::from_secs(1),
    );
    let r = ActorTemplateRef::new(HIVE, "tmpl1");
    let _ = rec.reconcile_one(&r).await;
    let t = run_reconciler_until_done(&h, &rec, &r).await;
    assert!(t
        .status
        .unwrap()
        .golden_snapshot_status
        .unwrap()
        .error_message
        .starts_with("GoldenActorCrashed: "));

    // A Tag of another template under the golden name is a conflict.
    let t2 = h
        .create_template_with("tmpl2", "pool1", vec![probed()], vec![])
        .await;
    let uid = t2.metadata.as_ref().unwrap().uid.clone();
    h.store
        .create_tag(&pb::Tag {
            metadata: Some(md(HIVE, &uid)),
            source_actor: Some(oref(HIVE, "someone-else")),
            scope: pb::TagScope::Hive as i32,
            status: Some(pb::TagStatus {
                snapshot: None,
                actor_template_uid: uuid::Uuid::new_v4().to_string(),
            }),
        })
        .await
        .unwrap();
    let r2 = ActorTemplateRef::new(HIVE, "tmpl2");
    rec.reconcile_one(&r2).await.unwrap();
    let t2 = h.store.get_actor_template(&r2).await.unwrap();
    assert!(t2
        .status
        .unwrap()
        .golden_snapshot_status
        .unwrap()
        .error_message
        .starts_with("GoldenTagConflict: "));
}

#[tokio::test]
async fn reconcile_one_golden_egress_policy() {
    let h = Harness::with(Options {
        gates: hivekeep_core::preview::PreviewGates::all(),
        preview_client: true,
        ..Default::default()
    })
    .await;
    let rules = vec![pb::EgressRule {
        https: Some(pb::HttpsRule {
            hostnames: vec!["example.com".into()],
            ports: None,
            effects: None,
        }),
        ..Default::default()
    }];
    let t = h
        .c()
        .create_actor_template(h.req(pb::CreateActorTemplateRequest {
            actor_template: Some(pb::ActorTemplate {
                metadata: Some(md(HIVE, "tmpl1")),
                containers: vec![probed()],
                worker_selector: Some(pb::Selector {
                    match_labels: [("pool".to_string(), "pool1".to_string())].into(),
                }),
                golden_egress_policy: Some(pb::EgressPolicyTemplate {
                    rules: rules.clone(),
                }),
                resources: Some(pb::ResourceRequirements {
                    limits: Some(pb::Resources {
                        cpu_millicores: 1000,
                        memory_bytes: 512 << 20,
                    }),
                }),
                ..Default::default()
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    h.create_worker("w1", NODE, "pool1", 2).await;
    let uid = t.metadata.as_ref().unwrap().uid.clone();
    // A pre-existing, different policy is a conflict.
    h.create_actor(&uid, "tmpl1").await;
    h.c()
        .create_actor_egress_policy(h.req(pb::CreateActorEgressPolicyRequest {
            actor: Some(oref(HIVE, &uid)),
            egress_policy: Some(pb::EgressPolicy {
                metadata: Some(md(HIVE, "default")),
                rules: vec![],
            }),
        }))
        .await
        .unwrap();
    let rec = TemplateReconciler::new(
        h.store.clone(),
        h.control.clone(),
        std::time::Duration::from_secs(1),
    );
    let r = ActorTemplateRef::new(HIVE, "tmpl1");
    rec.reconcile_one(&r).await.unwrap();
    let got = h.store.get_actor_template(&r).await.unwrap();
    assert!(got
        .status
        .unwrap()
        .golden_snapshot_status
        .unwrap()
        .error_message
        .starts_with("GoldenEgressPolicyConflict: "));
}

#[tokio::test]
async fn checkpoint_terminal_state_and_disabled_mode() {
    let h = Harness::new().await;
    let mut t = h.create_template().await;
    let r = ActorTemplateRef::new(HIVE, "tmpl1");
    t = h
        .store
        .update_actor_template(&r, Precondition::of(&t), &mut |t| {
            t.snapshot_config
                .as_mut()
                .unwrap()
                .golden_snapshot_config
                .as_mut()
                .unwrap()
                .mode = pb::GoldenSnapshotMode::Disabled as i32;
            Ok(())
        })
        .await
        .unwrap();
    let rec = TemplateReconciler::new(
        h.store.clone(),
        h.control.clone(),
        std::time::Duration::from_secs(1),
    );
    assert!(rec.reconcile_one(&r).await.unwrap().is_none());
    assert_eq!(
        h.store.get_actor_template(&r).await.unwrap(),
        t,
        "a disabled template is untouched"
    );
}
