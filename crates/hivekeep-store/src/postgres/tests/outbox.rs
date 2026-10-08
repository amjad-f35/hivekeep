//! The worker event outbox: transactional append, watch delivery and its
//! safety fences, retention and maintenance.

use std::time::Duration;

use hivekeep_proto::api::v1 as pb;

use super::super::outbox::{decode_event, encode_event, POLL_BATCH, POLL_SQL};
use super::super::WATCH_POOL_MAX;
use super::*;
use crate::api::WorkerStore;
use crate::{DeletePreconditions, Precondition, WorkerEvent, WorkerEventType, WorkerWatch};

/// What a watch yields within `wait`: `None` if nothing, `Some(None)` if it
/// ended, `Some(Some(event))` for an event.
async fn next(watch: &mut WorkerWatch, wait: Duration) -> Option<Option<WorkerEvent>> {
    tokio::time::timeout(wait, watch.recv()).await.ok()
}

async fn expect_event(watch: &mut WorkerWatch) -> WorkerEvent {
    match next(watch, PATIENCE).await {
        Some(Some(e)) => e,
        Some(None) => panic!("watch ended"),
        None => panic!("timed out waiting for a worker event"),
    }
}

async fn expect_quiet(watch: &mut WorkerWatch, wait: Duration) {
    if let Some(got) = next(watch, wait).await {
        panic!("expected nothing from the watch, got {got:?}");
    }
}

async fn expect_end(watch: &mut WorkerWatch) {
    match next(watch, PATIENCE).await {
        Some(None) => {}
        Some(Some(e)) => panic!("watch delivered {e:?}; expected it to end for resync"),
        None => panic!("watch stayed open"),
    }
}

fn payload(kind: WorkerEventType, pod: &str) -> Vec<u8> {
    encode_event(kind, &worker(pod, pod))
}

async fn insert_event(conn: &mut sqlx::PgConnection, payload: &[u8]) {
    sqlx::query("INSERT INTO worker_events (payload) VALUES ($1)")
        .bind(payload)
        .execute(conn)
        .await
        .unwrap();
}

/// `connect` gives watches a pool of their own, and events flow through it.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn connect_dedicated_watch_pool() {
    let db = fresh("watch_pool").await;
    let s = &db.store;
    assert_eq!(s.watch_pool.options().get_max_connections(), WATCH_POOL_MAX);
    assert_ne!(
        s.pool.options().get_max_connections(),
        WATCH_POOL_MAX,
        "requests share the watch pool"
    );
    let mut watch = s.watch_workers().await.unwrap();
    s.create_worker(&worker("watchpool-worker", "watchpool-pod"))
        .await
        .unwrap();
    let e = expect_event(&mut watch).await;
    assert_eq!(e.kind, WorkerEventType::Created);
    assert_eq!(e.worker.worker_pod, "watchpool-pod");
    db.finish().await;
}

/// An outbox row shares its write's transaction: a rolled-back write never
/// produces an event, a committed one always does.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn worker_event_only_after_commit() {
    let db = fresh("only_after_commit").await;
    let s = &db.store;
    let mut watch = s.watch_workers().await.unwrap();
    const NAME: &str = "6e4d2f81-b3a9-4c05-8e72-1f9d4a0c7b63";
    let mut w = worker(NAME, "pod");
    w.worker_pod_uid = NAME.into();

    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query(
        "INSERT INTO workers (name, uid, version, proto) VALUES ($1, 'rolled-back-uid', 1, $2)",
    )
    .bind(NAME)
    .bind(prost::Message::encode_to_vec(&w))
    .execute(&mut *tx)
    .await
    .unwrap();
    insert_event(&mut tx, b"rolled-back-payload").await;
    tx.rollback().await.unwrap();
    expect_quiet(&mut watch, Duration::from_millis(500)).await;

    s.create_worker(&w).await.unwrap();
    let e = expect_event(&mut watch).await;
    assert_eq!(e.kind, WorkerEventType::Created);
    let mut want = w.clone();
    let got_md = e.worker.metadata.clone().unwrap();
    let want_md = want.metadata.as_mut().unwrap();
    want_md.version = 1;
    want_md.uid = got_md.uid.clone();
    want_md.create_time = got_md.create_time;
    want_md.update_time = got_md.update_time;
    assert_eq!(e.worker, want);
    db.finish().await;
}

/// Ids are taken at a transaction's first write but rows appear at commit,
/// so a lower id can commit after a higher one. The fence holds the later
/// event back until the earlier transaction ends, then delivers both in id
/// order.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn watch_out_of_order_commit_not_skipped() {
    let db = fresh("out_of_order").await;
    let mut watch = db.store.watch_workers().await.unwrap();

    let mut tx1 = db.pool().begin().await.unwrap();
    insert_event(
        &mut tx1,
        &payload(WorkerEventType::Created, "first-xid-late-commit"),
    )
    .await;
    let mut tx2 = db.pool().begin().await.unwrap();
    insert_event(
        &mut tx2,
        &payload(WorkerEventType::Created, "second-xid-early-commit"),
    )
    .await;
    tx2.commit().await.unwrap();

    expect_quiet(&mut watch, Duration::from_millis(500)).await;
    tx1.commit().await.unwrap();

    let first = expect_event(&mut watch).await;
    let second = expect_event(&mut watch).await;
    assert_eq!(
        [first.worker.worker_pod, second.worker.worker_pod],
        ["first-xid-late-commit", "second-xid-early-commit"]
    );
    db.finish().await;
}

/// Retention discards events past their age, keeps fresh ones, and records
/// the highest discarded id so lagging watches notice the gap.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn outbox_retention_trims_aged_events() {
    let db = fresh("retention").await;
    let aged: String = sqlx::query_scalar(
        "INSERT INTO worker_events (payload, created_at) VALUES ('old', now() - interval '2 hours') RETURNING xid::text",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    exec(
        db.pool(),
        "INSERT INTO worker_events (payload, created_at) VALUES ('older', now() - interval '1 day')",
    )
    .await;
    exec(
        db.pool(),
        "INSERT INTO worker_events (payload) VALUES ('fresh')",
    )
    .await;

    let report = db.store.run_maintenance().await.unwrap();
    assert_eq!(report.expired_events, 2);
    let left: Vec<Vec<u8>> = sqlx::query_scalar("SELECT payload FROM worker_events")
        .fetch_all(db.pool())
        .await
        .unwrap();
    assert_eq!(left, vec![b"fresh".to_vec()]);
    let covered: bool =
        sqlx::query_scalar("SELECT (SELECT xid FROM worker_events_trim) >= $1::text::xid8")
            .bind(&aged)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert!(covered, "trim mark does not cover discarded id {aged}");
    db.finish().await;
}

/// Replicas racing a maintenance pass neither fail nor leave aged events.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn outbox_maintenance_concurrent_passes_are_harmless() {
    let db = fresh("maintenance_race").await;
    let replica = PgStore::from_pool(db.pool().clone()).await.unwrap();
    exec(
        db.pool(),
        "INSERT INTO worker_events (payload, created_at) \
         SELECT 'old', now() - interval '2 hours' FROM generate_series(1, 500)",
    )
    .await;
    let (a, b) = tokio::join!(db.store.run_maintenance(), replica.run_maintenance());
    let removed = a.unwrap().expired_events + b.unwrap().expired_events;
    assert_eq!(removed, 500);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM worker_events")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(left, 0);
    // Shares the store's pool, so only the store closes it.
    replica.shutdown.cancel();
    db.finish().await;
}

/// Maintenance passes interleaved with a stream of Worker writes neither
/// deadlock nor fail the writers.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn outbox_maintenance_no_deadlock_with_concurrent_writers() {
    let db = fresh("maintenance_writers").await;
    exec(
        db.pool(),
        "INSERT INTO worker_events (payload, created_at) \
         SELECT 'old', now() - interval '2 hours' FROM generate_series(1, 200)",
    )
    .await;
    let stop = tokio_util::sync::CancellationToken::new();
    let writers: Vec<_> = (0..4)
        .map(|g| {
            let s = db.store.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut i = 0;
                while !stop.is_cancelled() {
                    let name = format!("ddl-worker-{g}-{i}");
                    s.create_worker(&worker(&name, &name))
                        .await
                        .unwrap_or_else(|e| panic!("writer {g} iteration {i}: {e}"));
                    i += 1;
                }
            })
        })
        .collect();
    for i in 0..4 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        // Age everything written so far, so each pass has work under load.
        exec(
            db.pool(),
            "UPDATE worker_events SET created_at = now() - interval '2 hours'",
        )
        .await;
        tokio::time::timeout(PATIENCE, db.store.run_maintenance())
            .await
            .unwrap_or_else(|_| panic!("maintenance pass {i} hung"))
            .unwrap_or_else(|e| panic!("maintenance pass {i}: {e}"));
    }
    stop.cancel();
    for w in writers {
        w.await.unwrap();
    }
    db.finish().await;
}

/// Each Worker write appends exactly one outbox row, so ids are distinct
/// and a poll batch never splits a transaction.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn worker_events_one_row_per_transaction() {
    let db = fresh("one_row").await;
    let s = &db.store;
    s.create_worker(&worker("one-row-worker", "pod"))
        .await
        .unwrap();
    for _ in 0..10 {
        let stored = s.get_worker("one-row-worker").await.unwrap();
        s.update_worker("one-row-worker", Precondition::of(&stored), &mut |_| Ok(()))
            .await
            .unwrap();
    }
    s.delete_worker("one-row-worker", DeletePreconditions::none())
        .await
        .unwrap();
    let (total, distinct): (i64, i64) =
        sqlx::query_as("SELECT count(*), count(DISTINCT xid) FROM worker_events")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(total, 12);
    assert_eq!(total, distinct);
    db.finish().await;
}

/// The fence's real bound: any older transaction holds back delivery of
/// everything committed after it, for as long as it lives.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn watch_delivery_fenced_by_oldest_transaction() {
    let db = fresh("fenced").await;
    let mut watch = db.store.watch_workers().await.unwrap();
    let mut blocker = db.pool().begin().await.unwrap();
    sqlx::query("SELECT pg_current_xact_id()")
        .execute(&mut *blocker)
        .await
        .unwrap();
    db.store
        .create_worker(&worker("fenced-worker", "fenced"))
        .await
        .unwrap();
    expect_quiet(&mut watch, Duration::from_millis(600)).await;
    blocker.rollback().await.unwrap();
    assert_eq!(expect_event(&mut watch).await.worker.worker_pod, "fenced");
    db.finish().await;
}

/// `close` returns only once the maintenance task has stopped.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn close_stops_maintenance() {
    let db = fresh("close").await;
    tokio::time::timeout(PATIENCE, db.store.close())
        .await
        .expect("close did not stop maintenance");
    assert!(db.store.maintenance.lock().await.is_none());
    drop_schema(&db.schema).await;
}

/// The poll walks the id index in `xid8` order. Sorting by the text
/// rendering instead would diverge from the cursor predicate across digit
/// boundaries and force a full sort.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn poll_query_plan_stays_on_index() {
    let db = fresh("poll_plan").await;
    exec(
        db.pool(),
        "INSERT INTO worker_events (payload) SELECT 'x'::bytea FROM generate_series(1, 3000); ANALYZE worker_events",
    )
    .await;
    let plan: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!("EXPLAIN {POLL_SQL}")))
        .bind("100")
        .bind(POLL_BATCH)
        .fetch_all(db.pool())
        .await
        .unwrap();
    let plan = plan.join("\n");
    assert!(
        !plan.contains("Sort Key"),
        "poll plan sorts instead of walking the index:\n{plan}"
    );
    assert!(
        plan.contains("worker_events_xid_idx"),
        "poll plan does not use the id index:\n{plan}"
    );
    db.finish().await;
}

/// Consumes transaction ids until the next one is a few short of a power
/// of ten, so the following writes straddle a change in digit count. Each
/// subtransaction that writes takes an id of its own. Gives up (quietly)
/// when the boundary is millions of ids away.
async fn burn_ids_to_digit_boundary(pool: &PgPool, short_by: i64) {
    let current: i64 = sqlx::query_scalar("SELECT pg_current_xact_id()::text::bigint")
        .fetch_one(pool)
        .await
        .unwrap();
    let mut boundary = 10i64;
    while boundary <= current + short_by {
        boundary *= 10;
    }
    let burn = boundary - current - short_by;
    if burn > 2_000_000 {
        // Too far on this database; the ordering checks still run, just
        // without a digit change to trip over.
        eprintln!("not burning {burn} ids to reach {boundary}");
        return;
    }
    exec(
        pool,
        &format!(
            "CREATE TEMP TABLE IF NOT EXISTS burn (i int); \
             DO $$ BEGIN FOR i IN 1..{burn} LOOP \
                 BEGIN INSERT INTO burn VALUES (i); EXCEPTION WHEN others THEN NULL; END; \
             END LOOP; END $$"
        ),
    )
    .await;
}

/// Ids that differ in digit count sort differently as text; the watch must
/// still deliver in id order, exactly once each.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn watch_orders_by_id_not_text() {
    let db = fresh("poll_order").await;
    let mut watch = db.store.watch_workers().await.unwrap();
    burn_ids_to_digit_boundary(db.pool(), 6).await;
    for i in 0..12 {
        let mut tx = db.pool().begin().await.unwrap();
        insert_event(
            &mut tx,
            &payload(WorkerEventType::Updated, &format!("w{i:02}")),
        )
        .await;
        tx.commit().await.unwrap();
    }
    for i in 0..12 {
        assert_eq!(
            expect_event(&mut watch).await.worker.worker_pod,
            format!("w{i:02}")
        );
    }
    expect_quiet(&mut watch, Duration::from_millis(300)).await;
    db.finish().await;
}

/// Events discarded before a watch read them end the watch, the signal to
/// re-list, rather than being skipped silently.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn watch_ends_when_trimmed_past_cursor() {
    let db = fresh("trimmed").await;
    let mut watch = db.store.watch_workers().await.unwrap();
    db.store
        .create_worker(&worker("trim-worker", "pod"))
        .await
        .unwrap();
    expect_event(&mut watch).await;

    let mut tx = db.pool().begin().await.unwrap();
    let p = payload(WorkerEventType::Updated, "trim-worker");
    for _ in 0..3 {
        insert_event(&mut tx, &p).await;
    }
    sqlx::query(
        "WITH doomed AS ( \
             DELETE FROM worker_events \
             WHERE xid > COALESCE((SELECT xid FROM worker_events_trim), '0'::xid8) RETURNING xid) \
         INSERT INTO worker_events_trim (id, xid) \
         SELECT true, xid FROM doomed ORDER BY xid DESC LIMIT 1 \
         ON CONFLICT (id) DO UPDATE SET xid = EXCLUDED.xid WHERE EXCLUDED.xid > worker_events_trim.xid",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    expect_end(&mut watch).await;
    db.finish().await;
}

/// Polling that keeps failing ends the watch within the configured window,
/// so callers stop serving a frozen view.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn watch_ends_after_persistent_poll_failure() {
    let mut db = fresh("poll_failure").await;
    db.store.poll_failure_close_after = Duration::from_millis(300);
    let mut watch = db.store.watch_workers().await.unwrap();
    db.store.watch_pool.close().await;
    expect_end(&mut watch).await;
    db.finish().await;
}

/// A transaction that took its id before a watch began but commits after is
/// owed to the watch. An already-committed later id must not raise the
/// watch's baseline above it, or trimming the owed event would go unnoticed.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn watch_baseline_does_not_mask_owed_trims() {
    let db = fresh("baseline").await;
    // L pins the fence.
    let mut l = db.pool().begin().await.unwrap();
    sqlx::query("SELECT pg_current_xact_id()")
        .execute(&mut *l)
        .await
        .unwrap();
    // W takes the next id now and commits after the watch starts.
    let mut w = db.pool().begin().await.unwrap();
    let xid_w: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
        .fetch_one(&mut *w)
        .await
        .unwrap();
    // C: a later id, committed before the watch starts.
    db.store
        .create_worker(&worker("baseline-poison", "baseline-pod"))
        .await
        .unwrap();

    let mut watch = db.store.watch_workers().await.unwrap();
    insert_event(&mut w, &[WorkerEventType::Updated as u8]).await;
    w.commit().await.unwrap();
    // Retention takes W's event while L still holds the fence.
    sqlx::query(
        "INSERT INTO worker_events_trim (id, xid) VALUES (true, $1::text::xid8) \
         ON CONFLICT (id) DO UPDATE SET xid = EXCLUDED.xid WHERE EXCLUDED.xid > worker_events_trim.xid",
    )
    .bind(&xid_w)
    .execute(db.pool())
    .await
    .unwrap();
    expect_end(&mut watch).await;
    l.rollback().await.unwrap();
    db.finish().await;
}

/// The read side checks what the write side promises: a known event type
/// and a Worker with a name.
#[test]
fn decode_event_boundary_assertions() {
    let valid = encode_event(WorkerEventType::Updated, &worker("w1", "pod"));
    let mut garbage = vec![WorkerEventType::Created as u8];
    garbage.extend_from_slice(&[0xde, 0xad, 0xbe]);
    let cases: [(&str, Vec<u8>, bool); 6] = [
        ("valid", valid, true),
        ("empty", vec![], false),
        ("unknown type byte", vec![0xff, 0x00], false),
        (
            "type byte only",
            vec![WorkerEventType::Created as u8],
            false,
        ),
        ("garbage proto", garbage, false),
        (
            "nameless worker",
            encode_event(WorkerEventType::Deleted, &pb::Worker::default()),
            false,
        ),
    ];
    for (name, payload, ok) in cases {
        assert_eq!(decode_event(&payload).is_ok(), ok, "{name}");
    }
}

/// A payload failing those checks ends the watch instead of being skipped.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn watch_ends_on_corrupt_payload() {
    let db = fresh("corrupt").await;
    let mut watch = db.store.watch_workers().await.unwrap();
    exec(
        db.pool(),
        "INSERT INTO worker_events (payload) VALUES ('\\xffdead'::bytea)",
    )
    .await;
    expect_end(&mut watch).await;
    db.finish().await;
}
