//! Transactional outbox behind [`WorkerStore::watch_workers`].
//!
//! A Worker write appends one `worker_events` row in its own transaction,
//! stamped with that transaction's id. A watch polls for rows above its
//! cursor and below the oldest transaction still running
//! (`pg_snapshot_xmin`), so a transaction that commits late can never be
//! skipped: until it finishes, nothing at or above its id is read. Events
//! reach every replica's watches, not only the writer's.
//!
//! Rows that had already committed when the watch began are skipped even if
//! an older transaction was still running then, so a watch reports exactly
//! the changes committed after it was created.
//!
//! A watch ends, asking its owner to resynchronize, if retention discarded
//! rows it had not read yet, if the database restarted, or if polling keeps
//! failing.
//!
//! [`WorkerStore::watch_workers`]: crate::WorkerStore::watch_workers

use std::time::{Duration, Instant};

use hivekeep_proto::api::v1 as pb;
use prost::Message;
use sqlx::{PgConnection, Row};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::codec::{db, decode};
use super::PgStore;
use crate::watch::WATCH_BUFFER;
use crate::{Result, StoreError, WorkerEvent, WorkerEventType, WorkerWatch};

/// How often watches poll.
pub(super) const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// Rows read per poll.
pub(super) const POLL_BATCH: i64 = 1024;
/// How long a watch tolerates failing polls before it ends.
pub(super) const POLL_FAILURE_CLOSE_AFTER: Duration = Duration::from_secs(30);
/// How long events are retained.
pub(super) const RETENTION: Duration = Duration::from_secs(15 * 60);

/// Encodes an event as its type byte followed by the Worker message.
pub(super) fn encode_event(kind: WorkerEventType, worker: &pb::Worker) -> Vec<u8> {
    let mut out = vec![kind as u8];
    worker.encode(&mut out).expect("Vec has unbounded capacity");
    out
}

pub(super) fn decode_event(payload: &[u8]) -> Result<WorkerEvent> {
    let (&b, rest) = payload
        .split_first()
        .ok_or_else(|| StoreError::Corrupt("empty worker event".into()))?;
    let kind = WorkerEventType::from_u8(b)
        .ok_or_else(|| StoreError::Corrupt(format!("unknown worker event type {b}")))?;
    let worker: pb::Worker = decode(rest)?;
    if worker.metadata.as_ref().is_none_or(|m| m.name.is_empty()) {
        return Err(StoreError::Corrupt("worker event names no worker".into()));
    }
    Ok(WorkerEvent { kind, worker })
}

/// Appends the event of a Worker write; `conn` must be in the write's
/// transaction.
pub(super) async fn append_event(
    conn: &mut PgConnection,
    kind: WorkerEventType,
    worker: &pb::Worker,
) -> Result<()> {
    sqlx::query("INSERT INTO worker_events (payload) VALUES ($1)")
        .bind(encode_event(kind, worker))
        .execute(&mut *conn)
        .await
        .map_err(db)?;
    Ok(())
}

/// Discards events older than the retention period, recording the highest
/// discarded id. Returns how many rows went.
pub(super) async fn trim(conn: &mut PgConnection) -> Result<u64> {
    let secs = RETENTION.as_secs_f64();
    let row = sqlx::query(
        "WITH gone AS ( \
             DELETE FROM worker_events WHERE created_at < clock_timestamp() - make_interval(secs => $1) \
             RETURNING xid), \
         mark AS ( \
             INSERT INTO worker_events_trim (id, xid) \
             SELECT true, xid FROM gone ORDER BY xid DESC LIMIT 1 \
             ON CONFLICT (id) DO UPDATE SET xid = EXCLUDED.xid WHERE EXCLUDED.xid > worker_events_trim.xid) \
         SELECT count(*) FROM gone",
    )
    .bind(secs)
    .fetch_one(&mut *conn)
    .await
    .map_err(db)?;
    let n: i64 = row.try_get(0).map_err(db)?;
    Ok(n as u64)
}

struct Cursor {
    /// Events at or below this transaction id have been handled.
    xid: String,
    /// Events above `xid` that had already committed when the watch began,
    /// and so are not news to it.
    seen: std::collections::HashSet<String>,
    baseline: String,
    started: String,
}

async fn start_cursor(conn: &mut PgConnection) -> Result<Cursor> {
    let row = sqlx::query(
        "SELECT (pg_snapshot_xmin(pg_current_snapshot())::text::numeric - 1)::text, \
                GREATEST( \
                    COALESCE((SELECT max(xid::text::numeric) FROM worker_events \
                              WHERE xid < pg_snapshot_xmin(pg_current_snapshot())), 0), \
                    COALESCE((SELECT xid::text::numeric FROM worker_events_trim), 0))::text, \
                pg_postmaster_start_time()::text, \
                ARRAY(SELECT xid::text FROM worker_events \
                      WHERE xid >= pg_snapshot_xmin(pg_current_snapshot()))",
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(db)?;
    let seen: Vec<String> = row.try_get(3).map_err(db)?;
    Ok(Cursor {
        xid: row.try_get(0).map_err(db)?,
        seen: seen.into_iter().collect(),
        baseline: row.try_get(1).map_err(db)?,
        started: row.try_get(2).map_err(db)?,
    })
}

enum Poll {
    Events(Vec<(String, Vec<u8>)>),
    Resync(&'static str),
}

/// Reads the next batch above a cursor. The text rendering of the id gets
/// its own name: a bare `ORDER BY xid` would bind to an output column named
/// `xid` and sort the ids as text, which diverges from the `xid8` order of
/// the cursor predicate (`"10" < "9"`) and defeats the index.
pub(super) const POLL_SQL: &str = "SELECT xid::text AS xid_text, payload FROM worker_events \
     WHERE xid > $1::text::xid8 AND xid < pg_snapshot_xmin(pg_current_snapshot()) \
     ORDER BY worker_events.xid LIMIT $2";

async fn poll(conn: &mut PgConnection, cursor: &Cursor) -> Result<Poll> {
    let rows = sqlx::query(POLL_SQL)
        .bind(&cursor.xid)
        .bind(POLL_BATCH)
        .fetch_all(&mut *conn)
        .await
        .map_err(db)?;
    let safety = sqlx::query(
        "SELECT EXISTS(SELECT 1 FROM worker_events_trim \
                       WHERE xid > $1::text::xid8 AND xid > $2::text::xid8), \
                pg_postmaster_start_time()::text",
    )
    .bind(&cursor.xid)
    .bind(&cursor.baseline)
    .fetch_one(&mut *conn)
    .await
    .map_err(db)?;
    let fell_behind: bool = safety.try_get(0).map_err(db)?;
    let started: String = safety.try_get(1).map_err(db)?;
    if started != cursor.started {
        return Ok(Poll::Resync("the database restarted"));
    }
    if fell_behind {
        return Ok(Poll::Resync("retention discarded unread events"));
    }
    rows.into_iter()
        .map(|r| Ok((r.try_get(0).map_err(db)?, r.try_get(1).map_err(db)?)))
        .collect::<Result<Vec<_>>>()
        .map(Poll::Events)
}

/// Starts a watch whose poller runs until the watch is closed, the store is
/// shut down, or resynchronization is needed.
pub(super) async fn watch(store: &PgStore) -> Result<WorkerWatch> {
    let mut cursor = {
        let mut conn = store.watch_pool.acquire().await.map_err(db)?;
        start_cursor(&mut conn).await?
    };
    let (tx, rx) = mpsc::channel(WATCH_BUFFER);
    let stop = CancellationToken::new();
    let pool = store.watch_pool.clone();
    let shutdown = store.shutdown.clone();
    let close_after = store.poll_failure_close_after;
    let done = stop.clone();
    tokio::spawn(async move {
        let mut failing_since: Option<Instant> = None;
        loop {
            tokio::select! {
                _ = done.cancelled() => return,
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(POLL_INTERVAL) => {}
            }
            loop {
                let result = match pool.acquire().await {
                    Ok(mut conn) => poll(&mut conn, &cursor).await,
                    Err(e) => Err(db(e)),
                };
                let batch = match result {
                    Ok(Poll::Events(batch)) => {
                        failing_since = None;
                        batch
                    }
                    Ok(Poll::Resync(why)) => {
                        tracing::warn!(reason = why, "ending worker watch for resync");
                        return;
                    }
                    Err(e) => {
                        let since = *failing_since.get_or_insert_with(Instant::now);
                        if since.elapsed() > close_after {
                            tracing::warn!(error = %e, "worker event polling keeps failing; ending watch");
                            return;
                        }
                        tracing::warn!(error = %e, "worker event poll failed");
                        break;
                    }
                };
                let full = batch.len() as i64 == POLL_BATCH;
                for (xid, payload) in batch {
                    if cursor.seen.remove(&xid) {
                        cursor.xid = xid;
                        continue;
                    }
                    let event = match decode_event(&payload) {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::error!(error = %e, xid, "undecodable worker event; ending watch for resync");
                            return;
                        }
                    };
                    tokio::select! {
                        _ = done.cancelled() => return,
                        sent = tx.send(event) => if sent.is_err() { return },
                    }
                    cursor.xid = xid;
                }
                if !full {
                    break;
                }
            }
        }
    });
    Ok(WorkerWatch::new(rx, stop))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_round_trip() {
        let w = pb::Worker {
            metadata: Some(pb::ResourceMetadata {
                name: "w".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let e = decode_event(&encode_event(WorkerEventType::Deleted, &w)).unwrap();
        assert_eq!(e.kind, WorkerEventType::Deleted);
        assert_eq!(e.worker, w);
        assert!(decode_event(&[]).is_err());
        assert!(decode_event(&[9]).is_err());
        assert!(decode_event(&encode_event(
            WorkerEventType::Created,
            &pb::Worker::default()
        ))
        .is_err());
    }
}
