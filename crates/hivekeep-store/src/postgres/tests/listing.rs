//! Listings over damaged rows.

use hivekeep_proto::api::v1 as pb;

use super::*;
use crate::api::*;
use crate::{ListOptions, Result};

/// One undecodable row fails a listing as a whole, so the error names it.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn list_names_an_undecodable_row() {
    let db = fresh("bad_row").await;
    let s = &db.store;
    mk_hive(s, "team-a").await;
    s.create_actor_template(&pb::ActorTemplate {
        metadata: md("team-a", "t1"),
        ..Default::default()
    })
    .await
    .unwrap();
    s.create_tag(&pb::Tag {
        metadata: md("team-a", "v1"),
        status: Some(pb::TagStatus::default()),
        ..Default::default()
    })
    .await
    .unwrap();
    s.create_actor(&suspended("team-a", "a1")).await.unwrap();
    s.create_worker(&worker("w1", "pod")).await.unwrap();

    // Not a valid encoding of any message.
    let bad = vec![0xffu8];
    for table in ["hives", "actors", "actor_templates", "tags", "workers"] {
        sqlx::query(AssertSqlSafe(format!("UPDATE {table} SET proto = $1")))
            .bind(&bad)
            .execute(db.pool())
            .await
            .unwrap();
    }
    sqlx::query(
        "INSERT INTO worker_assignments (actor_uid, worker_name, proto) VALUES ('uid-1', 'w1', $1)",
    )
    .bind(&bad)
    .execute(db.pool())
    .await
    .unwrap();

    let o = ListOptions::new(10, "");
    let o = &o;
    fn err<T>(r: Result<T>) -> String {
        match r {
            Ok(_) => "no error".into(),
            Err(e) => e.to_string(),
        }
    }
    let cases = [
        ("hives", err(s.list_hives(o).await), "hive team-a"),
        (
            "actors",
            err(s.list_actors("team-a", o).await),
            "actor team-a/a1",
        ),
        (
            "actors, all hives",
            err(s.list_actors("", o).await),
            "actor team-a/a1",
        ),
        (
            "actor templates",
            err(s.list_actor_templates("team-a", o).await),
            "actor template team-a/t1",
        ),
        (
            "actor templates, all hives",
            err(s.list_actor_templates("", o).await),
            "actor template team-a/t1",
        ),
        ("tags", err(s.list_tags("team-a", o).await), "tag team-a/v1"),
        (
            "tags, all hives",
            err(s.list_tags("", o).await),
            "tag team-a/v1",
        ),
        ("workers", err(s.list_workers(o).await), "worker w1"),
        (
            "worker assignments",
            err(s.list_worker_assignments("w1", o).await),
            "actor assignment uid-1",
        ),
    ];
    for (name, got, want) in cases {
        assert!(
            got.contains(want),
            "{name}: error {got:?} does not name {want:?}"
        );
    }
    db.finish().await;
}
