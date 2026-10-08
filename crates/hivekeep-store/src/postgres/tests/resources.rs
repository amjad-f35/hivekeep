//! Optimistic concurrency and foreign keys of the hive-scoped resources.

use std::sync::Arc;

use hivekeep_core::resource::{ActorRef, ActorTemplateRef, TagRef};
use hivekeep_proto::api::v1 as pb;

use super::*;
use crate::api::*;
use crate::{DeletePreconditions, ListOptions, Precondition, Resource, StoreError};

fn tag(hive: &str, name: &str) -> pb::Tag {
    pb::Tag {
        metadata: md(hive, name),
        scope: pb::TagScope::Hive as i32,
        status: Some(pb::TagStatus {
            snapshot: Some(pb::Snapshot {
                generation: 1,
                owner: pb::SnapshotOwner::Tag as i32,
                fidelity: pb::SnapshotFidelity::Memory as i32,
                durable_snapshot: Some(pb::SnapshotStorage {
                    state: pb::SnapshotStorageState::Completed as i32,
                    object: Some(pb::ObjectSnapshot {
                        snapshot_uri: format!("gs://bucket/hives/{hive}/tags/{name}"),
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn paid() -> Option<pb::Selector> {
    Some(pb::Selector {
        match_labels: [("tier".to_string(), "paid".to_string())].into(),
    })
}

/// A write that lands between an update's read and its write makes the
/// update fail with a version conflict, without retrying the mutation, and
/// the concurrent write survives.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn update_actor_concurrent_write_returns_conflict() {
    let db = fresh("actor_race").await;
    let s = &db.store;
    mk_hive(s, "team-a").await;
    let mut input = suspended("team-a", "actor-a");
    input.actor_template = Some(pb::ObjectRef {
        hive: "default".into(),
        name: "template-a".into(),
    });
    let created = s.create_actor(&input).await.unwrap();
    let r = ActorRef::of(&created);

    let mut mutations = 0;
    let err = s
        .update_actor(&r, Precondition::of(&created), &mut |a| {
            mutations += 1;
            block_on(s.update_actor(&r, Precondition::of(&created), &mut |c| {
                c.worker_selector = paid();
                Ok(())
            }))
            .map_err(StoreError::rejected)?;
            a.status.get_or_insert_with(Default::default).state = pb::ActorState::Running as i32;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::VersionConflict), "{err}");
    assert_eq!(mutations, 1);

    let stored = s.get_actor(&r).await.unwrap();
    assert_eq!(
        stored.status.as_ref().unwrap().state,
        pb::ActorState::Suspended as i32,
        "the losing update was persisted"
    );
    assert_eq!(stored.worker_selector, paid());
    assert_eq!(stored.version(), created.version() + 1);
    db.finish().await;
}

/// The foreign key, not a separate existence check, refuses an Actor whose
/// Hive is missing, which also covers a Hive deleted concurrently.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn create_actor_missing_hive_failed_precondition() {
    let db = fresh("actor_fk").await;
    let mut a = suspended("no-such-hive", "id1");
    a.actor_template = Some(pb::ObjectRef {
        hive: "ns1".into(),
        name: "tmpl1".into(),
    });
    assert!(matches!(
        db.store.create_actor(&a).await,
        Err(StoreError::FailedPrecondition)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn list_actors_invalid_page_token() {
    let db = fresh("actor_token").await;
    assert!(db
        .store
        .list_actors("", &ListOptions::new(10, "not-valid-base64!!"))
        .await
        .is_err());
    db.finish().await;
}

/// A page token only continues the listing it came from.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn list_actors_cross_scope_page_token() {
    let db = fresh("actor_scope").await;
    let s = &db.store;
    mk_hive(s, "team-a").await;
    mk_hive(s, "team-b").await;
    for name in ["a1", "a2"] {
        s.create_actor(&suspended("team-a", name)).await.unwrap();
    }
    let page = s
        .list_actors("team-a", &ListOptions::new(1, ""))
        .await
        .unwrap();
    assert!(page.has_next_page());
    let token = page.next_page_token;
    for hive in ["team-b", ""] {
        assert!(
            matches!(
                s.list_actors(hive, &ListOptions::new(1, token.clone()))
                    .await,
                Err(StoreError::InvalidPageToken(_))
            ),
            "team-a's token accepted by list_actors({hive:?})"
        );
    }

    // A Worker listing's token is no Hive listing's.
    for n in ["w1", "w2"] {
        s.create_worker(&worker(n, n)).await.unwrap();
    }
    let workers = s.list_workers(&ListOptions::new(1, "")).await.unwrap();
    assert!(workers.has_next_page());
    assert!(matches!(
        s.list_hives(&ListOptions::new(1, workers.next_page_token))
            .await,
        Err(StoreError::InvalidPageToken(_))
    ));
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn update_actor_template_concurrent_write_returns_conflict() {
    let db = fresh("template_race").await;
    let s = &db.store;
    mk_hive(s, "team-a").await;
    let r = ActorTemplateRef::new("team-a", "template-a");
    s.create_actor_template(&pb::ActorTemplate {
        metadata: md("team-a", "template-a"),
        ..Default::default()
    })
    .await
    .unwrap();
    let created = s.get_actor_template(&r).await.unwrap();

    let mut mutations = 0;
    let err = s
        .update_actor_template(&r, Precondition::of(&created), &mut |t| {
            mutations += 1;
            block_on(
                s.update_actor_template(&r, Precondition::of(&created), &mut |c| {
                    c.worker_selector = paid();
                    Ok(())
                }),
            )
            .map_err(StoreError::rejected)?;
            t.status = Some(pb::ActorTemplateStatus {
                golden_snapshot_status: Some(pb::GoldenSnapshotStatus {
                    error_message: "LosingUpdate".into(),
                    ..Default::default()
                }),
            });
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::VersionConflict), "{err}");
    assert_eq!(mutations, 1);
    let stored = s.get_actor_template(&r).await.unwrap();
    assert_eq!(stored.worker_selector, paid());
    let message = stored
        .status
        .and_then(|st| st.golden_snapshot_status)
        .map(|g| g.error_message)
        .unwrap_or_default();
    assert_eq!(message, "", "the losing update was persisted");
    db.finish().await;
}

/// Reads fill in defaults a stored template predates, both singly and in a
/// listing.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn get_actor_template_backfills_defaults() {
    let db = fresh("template_defaults").await;
    let s = &db.store;
    mk_hive(s, "team-a").await;
    s.create_actor_template(&pb::ActorTemplate {
        metadata: md("team-a", "template-a"),
        snapshot_config: Some(pb::SnapshotConfig::default()),
        ..Default::default()
    })
    .await
    .unwrap();
    let want = Some(pb::SnapshotConfig {
        preferred_fidelity: pb::SnapshotFidelity::Memory as i32,
        golden_snapshot_config: Some(pb::GoldenSnapshotConfig {
            mode: pb::GoldenSnapshotMode::Enabled as i32,
        }),
        ..Default::default()
    });
    // Write the record as an older build would have, without defaults.
    let raw = pb::ActorTemplate {
        snapshot_config: Some(pb::SnapshotConfig::default()),
        ..s.get_actor_template(&ActorTemplateRef::new("team-a", "template-a"))
            .await
            .unwrap()
    };
    sqlx::query("UPDATE actor_templates SET proto = $1")
        .bind(prost::Message::encode_to_vec(&raw))
        .execute(db.pool())
        .await
        .unwrap();

    let got = s
        .get_actor_template(&ActorTemplateRef::new("team-a", "template-a"))
        .await
        .unwrap();
    assert_eq!(got.snapshot_config, want);
    let page = s
        .list_actor_templates("team-a", &ListOptions::new(10, ""))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].snapshot_config, want);
    db.finish().await;
}

/// A Tag deleted and re-created between an update's read and its write is a
/// new incarnation: the update fails and the new Tag is untouched.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn update_tag_guard_prevents_delete_recreate_aba() {
    let db = fresh("tag_aba").await;
    let s = &db.store;
    mk_hive(s, "team-a").await;
    let original = s.create_tag(&tag("team-a", "tag-a")).await.unwrap();
    let r = TagRef::new("team-a", "tag-a");

    let mut mutations = 0;
    let mut recreated = None;
    let err = s
        .update_tag(&r, Precondition::of(&original), &mut |t| {
            mutations += 1;
            block_on(s.delete_tag(&r, DeletePreconditions::none()))
                .map_err(StoreError::rejected)?;
            recreated = Some(
                block_on(s.create_tag(&tag("team-a", "tag-a"))).map_err(StoreError::rejected)?,
            );
            t.scope = pb::TagScope::Published as i32;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::VersionConflict), "{err}");
    assert_eq!(mutations, 1);
    assert_eq!(s.get_tag(&r).await.unwrap(), recreated.unwrap());
    db.finish().await;
}

#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn create_tag_foreign_key_errors() {
    let db = fresh("tag_fk").await;
    mk_hive(&db.store, "team-a").await;
    let t = pb::Tag {
        metadata: md("gone", "latest"),
        status: Some(pb::TagStatus::default()),
        ..Default::default()
    };
    assert!(matches!(
        db.store.create_tag(&t).await,
        Err(StoreError::FailedPrecondition)
    ));
    db.finish().await;
}

/// Racing updates from one revision: exactly one wins, and the row is at
/// the next version with the winner's bindings.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn global_access_policy_concurrent_updates_have_one_winner() {
    let db = fresh("policy_race").await;
    let s = Arc::new(db.store.clone());
    let created = s
        .create_global_access_policy(&pb::AccessPolicy {
            metadata: md("", "default"),
            bindings: vec![pb::Binding {
                role: "owner".into(),
                members: vec!["user:alice".into()],
            }],
        })
        .await
        .unwrap();
    let pre = Precondition::of(&created);

    let tasks: Vec<_> = ["user:bob", "user:carol"]
        .into_iter()
        .map(|member| {
            let s = s.clone();
            let pre = pre.clone();
            tokio::spawn(async move {
                s.update_global_access_policy(pre, &mut |p| {
                    p.bindings = vec![pb::Binding {
                        role: "owner".into(),
                        members: vec![member.into()],
                    }];
                    Ok(())
                })
                .await
            })
        })
        .collect();
    let mut winners = 0;
    for t in tasks {
        match t.await.unwrap() {
            Ok(_) => winners += 1,
            Err(StoreError::VersionConflict) => {}
            Err(e) => panic!("update failed with {e}"),
        }
    }
    assert_eq!(winners, 1);
    let got = s.get_global_access_policy().await.unwrap();
    assert_eq!(got.version(), created.version() + 1);
    assert_ne!(got.bindings[0].members, vec!["user:alice".to_string()]);
    db.finish().await;
}
