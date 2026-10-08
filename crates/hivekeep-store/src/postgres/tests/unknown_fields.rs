//! Records written by a newer build can carry fields this build does not
//! know. Reads drop them, and nothing this build writes brings them back.

use hivekeep_core::resource::{ActorRef, ActorTemplateRef, TagRef};
use hivekeep_proto::api::v1 as pb;
use prost::Message;

use super::super::codec::decode_checked;
use super::*;
use crate::api::*;
use crate::{DeletePreconditions, ListOptions, Precondition, Resource};

/// Field 9999 (declared by no message here), varint 42.
const UNKNOWN: [u8; 4] = [0xf8, 0xf0, 0x04, 42];

/// `bytes` with an unknown field at the top level and another inside the
/// metadata (field 1): a repeated embedded message merges into the first.
fn with_unknown(bytes: &[u8]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    out.extend_from_slice(&UNKNOWN);
    out.push(0x0a);
    out.push(UNKNOWN.len() as u8);
    out.extend_from_slice(&UNKNOWN);
    out
}

fn decodes_clean<T: Resource + std::fmt::Debug>(msg: T) {
    let md = msg.metadata().cloned().unwrap_or_default();
    let got: T = decode_checked(&md.uid, md.version, &with_unknown(&msg.encode_to_vec())).unwrap();
    // Reads also fill defaults; that is not what is under test.
    let mut want = msg;
    want.backfill();
    assert_eq!(got.encode_to_vec(), want.encode_to_vec(), "{}", T::KIND);
}

/// Decoding drops unknown fields at every level, for every stored kind.
#[test]
fn decode_drops_unknown_fields_of_every_kind() {
    let meta = || {
        Some(pb::ResourceMetadata {
            hive: "h".into(),
            name: "n".into(),
            uid: "u".into(),
            version: 2,
            ..Default::default()
        })
    };
    decodes_clean(pb::Hive { metadata: meta() });
    decodes_clean(pb::Actor {
        metadata: meta(),
        ..Default::default()
    });
    decodes_clean(pb::EgressPolicy {
        metadata: meta(),
        ..Default::default()
    });
    decodes_clean(pb::Tag {
        metadata: meta(),
        ..Default::default()
    });
    decodes_clean(pb::ActorTemplate {
        metadata: meta(),
        ..Default::default()
    });
    decodes_clean(pb::Worker {
        metadata: meta(),
        ..Default::default()
    });
    decodes_clean(pb::AccessPolicy {
        metadata: meta(),
        ..Default::default()
    });
}

/// Rewrites the stored record of `uid` in `table` as a newer build would.
async fn taint(pool: &PgPool, table: &str, uid: &str) {
    let stored: Vec<u8> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT proto FROM {table} WHERE uid = $1"
    )))
    .bind(uid)
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {table} SET proto = $1 WHERE uid = $2"
    )))
    .bind(with_unknown(&stored))
    .bind(uid)
    .execute(pool)
    .await
    .unwrap();
}

/// The stored bytes of `uid` in `table`.
async fn raw(pool: &PgPool, table: &str, uid: &str) -> Vec<u8> {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT proto FROM {table} WHERE uid = $1"
    )))
    .bind(uid)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// `msg` without its server-owned revision fields, for comparing a record
/// before and after a no-op update or a delete.
fn content<T: Resource>(msg: &T) -> T {
    let mut out = msg.clone();
    let m = out.metadata_mut();
    m.version = 0;
    m.update_time = None;
    m.delete_time = None;
    out
}

#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn unknown_fields_actor() {
    let db = fresh("unknown_actor").await;
    let s = &db.store;
    mk_hive(s, "team-a").await;
    let created = s
        .create_actor(&suspended("team-a", "actor-1"))
        .await
        .unwrap();
    taint(db.pool(), "actors", created.uid()).await;
    let r = ActorRef::of(&created);
    assert_eq!(s.get_actor(&r).await.unwrap(), created);
    let listed = s
        .list_actors("team-a", &ListOptions::default())
        .await
        .unwrap();
    assert_eq!(listed.items, vec![created.clone()]);
    let updated = s
        .update_actor(&r, Precondition::of(&created), &mut |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(content(&updated), content(&created));
    assert_eq!(
        raw(db.pool(), "actors", created.uid()).await,
        updated.encode_to_vec()
    );
    taint(db.pool(), "actors", created.uid()).await;
    let deleted = s
        .delete_actor(&r, DeletePreconditions::none())
        .await
        .unwrap();
    assert_eq!(deleted.encode_to_vec(), updated.encode_to_vec());
    db.finish().await;
}

#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn unknown_fields_egress_policy() {
    let db = fresh("unknown_egress").await;
    let s = &db.store;
    mk_hive(s, "team-a").await;
    let actor = s
        .create_actor(&suspended("team-a", "actor-1"))
        .await
        .unwrap();
    let r = ActorRef::of(&actor);
    let created = s
        .create_egress_policy(
            &r,
            &pb::EgressPolicy {
                metadata: md("team-a", "default"),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    taint(db.pool(), "actor_egress_policies", created.uid()).await;
    assert_eq!(s.get_egress_policy(&r).await.unwrap(), created);
    let updated = s
        .update_egress_policy(&r, Precondition::of(&created), &mut |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(content(&updated), content(&created));
    assert_eq!(
        raw(db.pool(), "actor_egress_policies", created.uid()).await,
        updated.encode_to_vec()
    );
    taint(db.pool(), "actor_egress_policies", created.uid()).await;
    let deleted = s
        .delete_egress_policy(&r, DeletePreconditions::none())
        .await
        .unwrap();
    assert_eq!(content(&deleted), content(&updated));
    db.finish().await;
}

#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn unknown_fields_tag() {
    let db = fresh("unknown_tag").await;
    let s = &db.store;
    mk_hive(s, "team-a").await;
    let created = s
        .create_tag(&pb::Tag {
            metadata: md("team-a", "tag-1"),
            status: Some(pb::TagStatus::default()),
            ..Default::default()
        })
        .await
        .unwrap();
    taint(db.pool(), "tags", created.uid()).await;
    let r = TagRef::new("team-a", "tag-1");
    assert_eq!(s.get_tag(&r).await.unwrap(), created);
    let listed = s
        .list_tags("team-a", &ListOptions::default())
        .await
        .unwrap();
    assert_eq!(listed.items, vec![created.clone()]);
    let updated = s
        .update_tag(&r, Precondition::of(&created), &mut |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(content(&updated), content(&created));
    assert_eq!(
        raw(db.pool(), "tags", created.uid()).await,
        updated.encode_to_vec()
    );
    taint(db.pool(), "tags", created.uid()).await;
    let deleted = s.delete_tag(&r, DeletePreconditions::none()).await.unwrap();
    assert_eq!(content(&deleted), content(&updated));
    db.finish().await;
}

#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn unknown_fields_hive() {
    let db = fresh("unknown_hive").await;
    let s = &db.store;
    mk_hive(s, "team-a").await;
    let created = s.get_hive("team-a").await.unwrap();
    taint(db.pool(), "hives", created.uid()).await;
    assert_eq!(s.get_hive("team-a").await.unwrap(), created);
    let listed = s.list_hives(&ListOptions::default()).await.unwrap();
    assert_eq!(listed.items, vec![created.clone()]);
    let deleted = s
        .delete_hive("team-a", DeletePreconditions::none())
        .await
        .unwrap();
    assert_eq!(content(&deleted), content(&created));
    db.finish().await;
}

#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn unknown_fields_actor_template() {
    let db = fresh("unknown_template").await;
    let s = &db.store;
    mk_hive(s, "team-a").await;
    let r = ActorTemplateRef::new("team-a", "tmpl-1");
    s.create_actor_template(&pb::ActorTemplate {
        metadata: md("team-a", "tmpl-1"),
        ..Default::default()
    })
    .await
    .unwrap();
    let created = s.get_actor_template(&r).await.unwrap();
    taint(db.pool(), "actor_templates", created.uid()).await;
    assert_eq!(s.get_actor_template(&r).await.unwrap(), created);
    let listed = s
        .list_actor_templates("team-a", &ListOptions::default())
        .await
        .unwrap();
    assert_eq!(listed.items, vec![created.clone()]);
    let updated = s
        .update_actor_template(&r, Precondition::of(&created), &mut |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(content(&updated), content(&created));
    taint(db.pool(), "actor_templates", created.uid()).await;
    let deleted = s
        .delete_actor_template(&r, DeletePreconditions::none())
        .await
        .unwrap();
    assert_eq!(content(&deleted), content(&updated));
    db.finish().await;
}

#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn unknown_fields_worker() {
    let db = fresh("unknown_worker").await;
    let s = &db.store;
    let created = s.create_worker(&worker("w1", "pod")).await.unwrap();
    taint(db.pool(), "workers", created.uid()).await;
    assert_eq!(s.get_worker("w1").await.unwrap(), created);
    let listed = s.list_workers(&ListOptions::default()).await.unwrap();
    assert_eq!(listed.items, vec![created.clone()]);
    let mut watch = s.watch_workers().await.unwrap();
    let updated = s
        .update_worker("w1", Precondition::of(&created), &mut |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(content(&updated), content(&created));
    assert_eq!(
        raw(db.pool(), "workers", created.uid()).await,
        updated.encode_to_vec()
    );
    let event = tokio::time::timeout(PATIENCE, watch.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.worker.encode_to_vec(), updated.encode_to_vec());
    taint(db.pool(), "workers", created.uid()).await;
    let deleted = s
        .delete_worker("w1", DeletePreconditions::none())
        .await
        .unwrap();
    assert_eq!(content(&deleted), content(&updated));
    db.finish().await;
}
