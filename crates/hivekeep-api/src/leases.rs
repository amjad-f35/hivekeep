//! The distributed leases that serialize operations on one resource, and the
//! [`Ctx`] a leased operation runs under.

use std::future::Future;

use hivekeep_core::resource::{ActorRef, ActorTemplateRef, TagRef};
use hivekeep_store::{Lease, Store, StoreError};
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::error::{status, Error, Result};

/// Lease key of an Actor.
pub fn actor_key(r: &ActorRef) -> String {
    format!("lease:actor:{}:{}", r.hive, r.name)
}

/// Lease key of a Tag.
pub fn tag_key(r: &TagRef) -> String {
    format!("lease:tag:{}:{}", r.hive, r.name)
}

/// Lease key of an ActorTemplate.
pub fn template_key(r: &ActorTemplateRef) -> String {
    format!("lease:actortemplate:{}:{}", r.hive, r.name)
}

/// Lease key of a Worker's assignment sweep.
pub fn worker_assignment_key(worker: &str) -> String {
    format!("lease:workerassignment:{worker}")
}

/// Takes the lease `key`; a conflict is `ABORTED`, naming `subject`.
pub async fn acquire(store: &dyn Store, key: &str, subject: &str) -> Result<Lease> {
    match store.acquire_lease(key).await {
        Ok(l) => Ok(l),
        Err(StoreError::LeaseConflict) => Err(status::aborted(format!(
            "another operation is in progress for this {subject}"
        ))),
        Err(e) => Err(Error::from(e).context("while acquiring lease")),
    }
}

/// The context of a workflow step: done once the caller's leases are lost.
#[derive(Debug, Clone, Default)]
pub struct Ctx {
    lost: Vec<CancellationToken>,
}

impl Ctx {
    /// A context that never ends.
    pub fn background() -> Self {
        Ctx::default()
    }

    /// This context, also ended by losing `lease`.
    pub fn with_lease(&self, lease: &Lease) -> Ctx {
        let mut lost = self.lost.clone();
        lost.push(lease.token());
        Ctx { lost }
    }

    /// Whether a lease this context depends on was lost.
    pub fn is_done(&self) -> bool {
        self.lost.iter().any(|t| t.is_cancelled())
    }

    /// Runs `fut`, abandoning it with `CANCELLED` if the context ends first.
    pub async fn run<T>(
        &self,
        fut: impl Future<Output = std::result::Result<T, Status>>,
    ) -> std::result::Result<T, Status> {
        if self.lost.is_empty() {
            return fut.await;
        }
        let tokens = self.lost.clone();
        let lost = async move {
            futures::future::select_all(tokens.iter().map(|t| Box::pin(t.cancelled()))).await;
        };
        tokio::select! {
            r = fut => r,
            _ = lost => Err(Status::cancelled("lease lost")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_store::MemoryStore;

    #[tokio::test]
    async fn conflicts_and_context() {
        let store = MemoryStore::new();
        let l = acquire(&store, "k", "actor").await.unwrap();
        let e = acquire(&store, "k", "actor").await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::Aborted);
        let ctx = Ctx::background().with_lease(&l);
        assert!(!ctx.is_done());
        assert_eq!(ctx.run(async { Ok::<_, Status>(1) }).await.unwrap(), 1);
        l.close().await;
        assert!(ctx.is_done());
        let r = ctx
            .run(std::future::pending::<std::result::Result<(), Status>>())
            .await;
        assert_eq!(r.unwrap_err().code(), tonic::Code::Cancelled);
        acquire(&store, "k", "actor").await.unwrap().close().await;
        assert_eq!(actor_key(&ActorRef::new("h", "a")), "lease:actor:h:a");
    }
}
