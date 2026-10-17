//! Moving Actors to CRASHED, and what a failed keeper call does to one.

use std::time::SystemTime;

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{Precondition, Store, StoreError};
use tonic::{Code, Status};

use crate::error::{status, Error, Result};
use crate::events;
use crate::fields;
use crate::leases::Ctx;
use crate::metrics::{op, Metrics};

/// Crash messages for failures that are not a keeper's own error.
pub mod msg {
    /// See the constant.
    pub const WORKER_ASSIGNMENT_MISSING: &str =
        "actor has no worker assignment in a state that requires one";
    /// See the constant.
    pub const LOCAL_SNAPSHOT_NODE_UNKNOWN: &str =
        "node holding the actor's local snapshot is unknown";
    /// See the constant.
    pub const LOCAL_SNAPSHOT_NODE_GONE: &str =
        "node holding the actor's latest snapshot no longer exists";
    /// See the constant.
    pub const LOCAL_SNAPSHOT_MISSING: &str = "actor has no local snapshot";
    /// See the constant.
    pub const DURABLE_SNAPSHOT_MISSING: &str = "actor has no durable snapshot";
    /// See the constant.
    pub const SNAPSHOT_STATE_CONFLICT: &str =
        "keeper's snapshot state conflicts with the actor's record";
    /// See the constant.
    pub const WORKER_GONE: &str = "assigned worker no longer exists";
    /// See the constant.
    pub const WORKER_DRAINING: &str = "assigned worker is draining";
    /// See the constant.
    pub const WORKER_REASSIGNED: &str = "assigned worker no longer hosts the actor";
    /// See the constant.
    pub const WORKER_INELIGIBLE: &str =
        "assigned worker no longer satisfies the actor's placement constraints";
    /// See the constant.
    pub const WORKER_POD_GONE: &str = "worker pod went away while hosting the actor";
    /// See the constant.
    pub const WORKER_RESTARTED: &str = "worker restarted while hosting the actor";
}

/// Bound of `ActorCrash.message`.
pub const MAX_CRASH_MESSAGE_BYTES: usize = 4096;

/// Cuts `s` to at most `n` bytes on a character boundary.
pub fn truncate_utf8(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut n = n;
    while n > 0 && !s.is_char_boundary(n) {
        n -= 1;
    }
    s[..n].to_string()
}

/// Describes a failed keeper call with the keeper's own message.
pub fn keeper_crash_message(rpc: &str, s: &Status) -> String {
    format!("keeper {rpc}: {}", s.message())
}

/// A crash record for a crash happening now, prefixed with the failed
/// operation when known.
pub fn new_crash(operation: &str, message: &str) -> pb::ActorCrash {
    let message = if operation.is_empty() || operation == op::UNKNOWN {
        message.to_string()
    } else {
        format!("{operation} failed: {message}")
    };
    pb::ActorCrash {
        message: truncate_utf8(&message, MAX_CRASH_MESSAGE_BYTES),
        crash_time: Some(SystemTime::now().into()),
    }
}

fn keeper_error_crashes(ctx: &Ctx, is_terminate: bool, s: &Status) -> bool {
    // A failed terminate must keep the assignment its retry needs.
    if is_terminate || ctx.is_done() {
        return false;
    }
    !matches!(
        s.code(),
        Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded
    )
}

/// What a failed keeper call means for the operation: a keeper-side
/// failure crashes the Actor; transport errors and an ended context leave
/// it as it is for a retry.
#[allow(clippy::too_many_arguments)]
pub async fn handle_keeper_error(
    ctx: &Ctx,
    store: &dyn Store,
    metrics: &Metrics,
    actor: &ActorRef,
    operation: &str,
    rpc: &str,
    is_terminate: bool,
    s: Status,
) -> Error {
    if keeper_error_crashes(ctx, is_terminate, &s) {
        tracing::error!(actor = %actor, rpc, error = %s.message(), "crashing actor after keeper error");
        if let Err(e) = crash_actor(
            store,
            metrics,
            actor,
            operation,
            &keeper_crash_message(rpc, &s),
        )
        .await
        {
            return e;
        }
        // The keeper's own code does not reach the caller: the Actor is
        // CRASHED and only a revert recovers it.
        return status::internal(format!("actor {actor} crashed: {}", s.message()));
    }
    if ctx.is_done() {
        return status::aborted(format!(
            "while calling keeper {rpc}: {}: lease lost",
            s.message()
        ));
    }
    if s.code() == Code::Unavailable {
        return status::unavailable(format!("while calling keeper {rpc}: {}", s.message()));
    }
    status::internal(format!("while calling keeper {rpc}: {}", s.message()))
}

/// Crashes the Actor for a step that found a record no retry can fix.
/// FAILED_PRECONDITION once the crash commits; the crash's own error
/// otherwise, so the caller retries.
pub async fn crash_and_fail(
    store: &dyn Store,
    metrics: &Metrics,
    actor: &ActorRef,
    operation: &str,
    message: &str,
) -> Error {
    if let Err(e) = crash_actor(store, metrics, actor, operation, message).await {
        return e;
    }
    status::failed_precondition(format!("actor {actor} is CRASHED: {message}"))
}

/// Moves the Actor to CRASHED and frees its worker. The worker is released
/// first: if that fails the Actor keeps its assignment, so a retry can
/// release it again. An Actor crashed already keeps its first crash record.
pub async fn crash_actor(
    store: &dyn Store,
    metrics: &Metrics,
    r: &ActorRef,
    operation: &str,
    message: &str,
) -> Result<()> {
    let actor = store
        .get_actor(r)
        .await
        .map_err(|e| Error::from(e).context("while loading actor to crash"))?;
    let was_crashed = fields::state(&actor) == pb::ActorState::Crashed;
    release_worker(store, &actor)
        .await
        .map_err(|e| e.context("while releasing worker to crash actor"))?;
    let crash = new_crash(operation, message);
    store
        .update_actor(r, Precondition::of(&actor), &mut |a| {
            let st = fields::status_mut(a);
            st.state = pb::ActorState::Crashed as i32;
            if !was_crashed {
                st.crash = Some(crash.clone());
            }
            // In-progress snapshot entries stay: they are the only pointer
            // to objects a later delete or revert must collect.
            st.worker_assignment = None;
            Ok(())
        })
        .await
        .map_err(|e| Error::from(e).store_context("while marking actor crashed"))?;
    if !was_crashed {
        events::crashed(&actor, operation);
        metrics.record_crash(&actor, operation);
    }
    Ok(())
}

/// Releases the Actor's worker if that worker still hosts it. A missing
/// worker or assignment is not an error. Returns the worker as released.
pub async fn release_worker(store: &dyn Store, actor: &pb::Actor) -> Result<Option<pb::Worker>> {
    let name = fields::assigned_worker(actor);
    if fields::assignment(actor).is_none() {
        return Ok(None);
    }
    match store.get_worker(name).await {
        Ok(_) => {}
        Err(StoreError::NotFound) => {
            tracing::warn!(worker = name, "worker already gone, skipping release");
            return Ok(None);
        }
        Err(e) => return Err(Error::from(e).context("while getting worker to release")),
    }
    let released = store
        .release_actor_from_worker(name, fields::actor_uid(actor))
        .await
        .map_err(|e| Error::from(e).context("while releasing worker"))?;
    if released.is_none() {
        tracing::warn!(
            worker = name,
            "worker is not hosting this actor, skipping release"
        );
    }
    Ok(released)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crash_records() {
        assert_eq!(truncate_utf8("héllo", 2), "h");
        assert_eq!(truncate_utf8("ok", 10), "ok");
        let c = new_crash(op::RESUME, "boom");
        assert_eq!(c.message, "resume failed: boom");
        assert_eq!(new_crash(op::UNKNOWN, "x").message, "x");
        assert_eq!(
            new_crash("", &"a".repeat(5000)).message.len(),
            MAX_CRASH_MESSAGE_BYTES
        );
    }

    #[test]
    fn which_errors_crash() {
        let ctx = Ctx::background();
        assert!(keeper_error_crashes(&ctx, false, &Status::internal("x")));
        assert!(keeper_error_crashes(&ctx, false, &Status::unknown("x")));
        assert!(!keeper_error_crashes(
            &ctx,
            false,
            &Status::unavailable("x")
        ));
        assert!(!keeper_error_crashes(
            &ctx,
            false,
            &Status::deadline_exceeded("x")
        ));
        assert!(!keeper_error_crashes(&ctx, true, &Status::internal("x")));
    }
}
