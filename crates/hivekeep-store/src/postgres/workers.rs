//! Workers and their assignments. Every Worker write runs in a transaction
//! that also appends its event to the outbox.

use async_trait::async_trait;
use hivekeep_core::compute::add_to_allocated;
use hivekeep_proto::api::v1 as pb;
use sqlx::{PgConnection, Row};

use super::codec::{db, decode};
use super::outbox::append_event;
use super::table::{self, WORKERS};
use super::PgStore;
use crate::api::*;
use crate::page_token::{self, ListKind};
use crate::resource::{prepare_create, stamp_update, Resource};
use crate::{
    DeletePreconditions, ListOptions, ListResponse, Precondition, Result, StoreError,
    WorkerEventType, WorkerWatch,
};

fn allocated(worker: &pb::Worker) -> Option<&pb::WorkerResources> {
    worker.status.as_ref().and_then(|s| s.allocated.as_ref())
}

fn set_allocated(worker: &mut pb::Worker, total: Option<pb::WorkerResources>) {
    worker.status.get_or_insert_with(Default::default).allocated = total;
}

/// Writes an allocation change to a locked Worker, advancing its revision.
pub(super) async fn save_worker(conn: &mut PgConnection, worker: &mut pb::Worker) -> Result<()> {
    let read = worker.metadata().cloned().unwrap_or_default();
    stamp_update(worker, &read, false);
    table::write(
        conn,
        WORKERS,
        &[&read.name],
        worker,
        Some((&read.uid, read.version)),
    )
    .await
}

async fn get_assignment(
    conn: &mut PgConnection,
    actor_uid: &str,
) -> Result<Option<(String, pb::ActorAssignment)>> {
    let row = sqlx::query("SELECT worker_name, proto FROM worker_assignments WHERE actor_uid = $1")
        .bind(actor_uid)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let worker: String = row.try_get(0).map_err(db)?;
    let proto: Vec<u8> = row.try_get(1).map_err(db)?;
    Ok(Some((worker, decode(&proto)?)))
}

#[async_trait]
impl WorkerStore for PgStore {
    async fn create_worker(&self, worker: &pb::Worker) -> Result<pb::Worker> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let created = table::insert(&mut tx, WORKERS, &[worker.name()], worker).await?;
        append_event(&mut tx, WorkerEventType::Created, &created).await?;
        tx.commit().await.map_err(db)?;
        Ok(created)
    }

    async fn get_worker(&self, name: &str) -> Result<pb::Worker> {
        table::get(&mut *self.conn().await?, WORKERS, &[name], false).await
    }

    async fn list_workers(&self, opts: &ListOptions) -> Result<ListResponse<pb::Worker>> {
        table::list(
            &mut *self.conn().await?,
            WORKERS,
            "",
            opts,
            ListKind::Worker,
        )
        .await
    }

    async fn update_worker(
        &self,
        name: &str,
        precondition: Precondition,
        mutate: Mutate<'_, pb::Worker>,
    ) -> Result<pb::Worker> {
        precondition.validate()?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        let updated = table::update_locked(
            &mut tx,
            WORKERS,
            &[name],
            &precondition.into(),
            Some(mutate),
            false,
        )
        .await?;
        append_event(&mut tx, WorkerEventType::Updated, &updated).await?;
        tx.commit().await.map_err(db)?;
        Ok(updated)
    }

    async fn mark_worker_for_deletion(
        &self,
        name: &str,
        precondition: DeletePreconditions,
        mutate: Option<Mutate<'_, pb::Worker>>,
    ) -> Result<pb::Worker> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let marked =
            table::update_locked(&mut tx, WORKERS, &[name], &precondition, mutate, true).await?;
        append_event(&mut tx, WorkerEventType::Updated, &marked).await?;
        tx.commit().await.map_err(db)?;
        Ok(marked)
    }

    async fn delete_worker(
        &self,
        name: &str,
        precondition: DeletePreconditions,
    ) -> Result<pb::Worker> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let stored: pb::Worker = table::get(&mut tx, WORKERS, &[name], true).await?;
        precondition.check(stored.metadata())?;
        // Assignments go with the row (ON DELETE CASCADE).
        let removed: pb::Worker =
            table::delete(&mut tx, WORKERS, &[name], &DeletePreconditions::none()).await?;
        append_event(&mut tx, WorkerEventType::Deleted, &removed).await?;
        tx.commit().await.map_err(db)?;
        Ok(removed)
    }

    async fn watch_workers(&self) -> Result<WorkerWatch> {
        super::outbox::watch(self).await
    }
}

#[async_trait]
impl WorkerAssignmentStore for PgStore {
    async fn bind_actor_to_worker(
        &self,
        worker: &str,
        assignment: &pb::ActorAssignment,
        admit: Option<Admit<'_>>,
    ) -> Result<pb::ActorAssignment> {
        let uid = assignment.actor_uid.clone();
        if uid.is_empty() {
            return Err(StoreError::InvalidArgument(format!(
                "binding an assignment with no actor_uid to worker {worker}"
            )));
        }
        let mut tx = self.pool.begin().await.map_err(db)?;
        let mut w: pb::Worker = table::get(&mut tx, WORKERS, &[worker], true).await?;

        let mut stored = assignment.clone();
        stored.metadata = Some(pb::ResourceMetadata {
            name: uid.clone(),
            ..Default::default()
        });
        let fresh = prepare_create(&stored)?;
        // Claim the Actor first: a concurrent claim of the same Actor by
        // another Worker blocks on the unique key here and then sees ours.
        let inserted = sqlx::query(
            "INSERT INTO worker_assignments (actor_uid, worker_name, proto) VALUES ($1, $2, $3) \
             ON CONFLICT (actor_uid) DO NOTHING",
        )
        .bind(&uid)
        .bind(worker)
        .bind(super::codec::encode(&fresh))
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            == 1;

        if inserted {
            stored = fresh;
        } else {
            let Some((holder, previous)) = get_assignment(&mut tx, &uid).await? else {
                return Err(StoreError::VersionConflict);
            };
            if holder != worker {
                return Err(StoreError::FailedPrecondition);
            }
            let total = add_to_allocated(allocated(&w), &previous, -1);
            set_allocated(&mut w, total);
            let prev_md = previous.metadata.clone().unwrap_or_default();
            stamp_update(&mut stored, &prev_md, false);
            let n = sqlx::query("UPDATE worker_assignments SET proto = $3 WHERE actor_uid = $1 AND worker_name = $2")
                .bind(&uid)
                .bind(worker)
                .bind(super::codec::encode(&stored))
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected();
            if n != 1 {
                return Err(StoreError::VersionConflict);
            }
        }
        if let Some(admit) = admit {
            admit(&w)?;
        }
        let total = add_to_allocated(allocated(&w), &stored, 1);
        set_allocated(&mut w, total);
        save_worker(&mut tx, &mut w).await?;
        append_event(&mut tx, WorkerEventType::Updated, &w).await?;
        tx.commit().await.map_err(db)?;
        Ok(stored)
    }

    async fn release_actor_from_worker(
        &self,
        worker: &str,
        actor_uid: &str,
    ) -> Result<Option<pb::Worker>> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let mut w: pb::Worker = table::get(&mut tx, WORKERS, &[worker], true).await?;
        let row = sqlx::query("DELETE FROM worker_assignments WHERE actor_uid = $1 AND worker_name = $2 RETURNING proto")
            .bind(actor_uid)
            .bind(worker)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        let Some(row) = row else {
            // Nothing to release, and so nothing to announce.
            return Ok(None);
        };
        let proto: Vec<u8> = row.try_get(0).map_err(db)?;
        let released: pb::ActorAssignment = decode(&proto)?;
        let total = add_to_allocated(allocated(&w), &released, -1);
        set_allocated(&mut w, total);
        save_worker(&mut tx, &mut w).await?;
        append_event(&mut tx, WorkerEventType::Updated, &w).await?;
        tx.commit().await.map_err(db)?;
        Ok(Some(w))
    }

    async fn get_worker_assignment(
        &self,
        worker: &str,
        actor_uid: &str,
    ) -> Result<pb::ActorAssignment> {
        match get_assignment(&mut *self.conn().await?, actor_uid).await? {
            Some((holder, a)) if holder == worker => Ok(a),
            _ => Err(StoreError::NotFound),
        }
    }

    async fn list_worker_assignments(
        &self,
        worker: &str,
        opts: &ListOptions,
    ) -> Result<ListResponse<pb::ActorAssignment>> {
        let opts = opts.normalize()?;
        let after = page_token::decode(&opts.page_token, ListKind::WorkerAssignment, worker, 1)?;
        let rows = sqlx::query(
            "SELECT actor_uid, proto FROM worker_assignments \
             WHERE worker_name = $1 AND ($2::text IS NULL OR actor_uid > $2) ORDER BY actor_uid LIMIT $3",
        )
        .bind(worker)
        .bind(after.map(|a| a[0].clone()))
        .bind(i64::from(opts.page_size) + 1)
        .fetch_all(&mut *self.conn().await?)
        .await
        .map_err(db)?;
        table::page(
            rows,
            1,
            opts.page_size as usize,
            ListKind::WorkerAssignment,
            worker,
            "",
        )
    }

    async fn find_worker_hosting_actor(&self, actor_uid: &str) -> Result<String> {
        let row = sqlx::query("SELECT worker_name FROM worker_assignments WHERE actor_uid = $1")
            .bind(actor_uid)
            .fetch_optional(&mut *self.conn().await?)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        row.try_get(0).map_err(db)
    }
}
