//! An actor's operation in progress: a background upload, or a detach.

use std::sync::Arc;

use hivekeep_proto::keeper::v1 as pb;
use parking_lot::Mutex;
use tokio::sync::watch;
use tonic::Status;

use super::snapshot_ref::SnapshotRef;
use crate::context::{Ctx, CtxError};

/// What an operation does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpKind {
    /// Makes a snapshot durable in the background.
    Upload = 0,
    /// Makes the latest local snapshot durable, then deletes the actor's
    /// local snapshots: a background detach, or a Detach call.
    Detach = 1,
}

/// How many kinds of operation there are.
pub const NUM_OP_KINDS: usize = 2;

impl OpKind {
    /// Both kinds, in queue order.
    pub const ALL: [OpKind; NUM_OP_KINDS] = [OpKind::Upload, OpKind::Detach];

    /// The kind's name, for logs.
    pub fn name(self) -> &'static str {
        match self {
            OpKind::Upload => "upload",
            OpKind::Detach => "detach",
        }
    }
}

/// The result of a detach.
pub type OpResult = Result<pb::ActorSnapshotState, Status>;

/// An operation in progress. An actor has at most one, of either kind. It
/// holds the actor's lock only while it begins and ends.
#[derive(Debug, Clone)]
pub struct ActorOp {
    /// What it does.
    pub kind: OpKind,
    /// The snapshot it acts on.
    pub target: SnapshotRef,
    /// The context it runs under; cancelling it stops the operation.
    pub ctx: Ctx,
    done: watch::Receiver<bool>,
    result: Arc<Mutex<Option<OpResult>>>,
}

/// The running side of an [`ActorOp`]: marks it done, with a result.
pub struct OpDone {
    tx: watch::Sender<bool>,
    result: Arc<Mutex<Option<OpResult>>>,
}

impl ActorOp {
    /// A new operation under `ctx` and the handle that ends it.
    pub fn new(kind: OpKind, target: SnapshotRef, ctx: Ctx) -> (ActorOp, OpDone) {
        let (tx, rx) = watch::channel(false);
        let result = Arc::new(Mutex::new(None));
        (
            ActorOp {
                kind,
                target,
                ctx,
                done: rx,
                result: result.clone(),
            },
            OpDone { tx, result },
        )
    }

    /// Stops the operation.
    pub fn cancel(&self) {
        self.ctx.cancel();
    }

    /// Whether this is the same operation as `other`.
    pub fn same(&self, other: &ActorOp) -> bool {
        Arc::ptr_eq(&self.result, &other.result)
    }

    /// Waits until the operation has ended.
    pub async fn wait(&self) {
        let mut rx = self.done.clone();
        let _ = rx.wait_for(|d| *d).await;
    }

    /// Waits until the operation has ended or `ctx` does.
    pub async fn wait_ctx(&self, ctx: &Ctx) -> Result<(), CtxError> {
        ctx.run(self.wait()).await
    }

    /// The detach's result, once it has ended.
    pub fn result(&self) -> Option<OpResult> {
        self.result.lock().clone()
    }
}

impl OpDone {
    /// Records the result and marks the operation done.
    pub fn finish(self, result: Option<OpResult>) {
        *self.result.lock() = result;
        let _ = self.tx.send(true);
    }
}

impl Drop for OpDone {
    fn drop(&mut self) {
        // An operation that ends without finish still ends.
        let _ = self.tx.send(true);
    }
}
