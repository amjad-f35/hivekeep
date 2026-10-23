//! Actor lifecycle steps shared by the suites: references, reads, state
//! waits, and create-and-resume with cleanup.
//!
//! Actor records outlive the fixture namespaces, so creation first clears a
//! record an interrupted run left behind (delete needs SUSPENDED or
//! CRASHED, hence the suspend before it), and every created Actor is
//! suspended and deleted when its test ends.

use std::time::{Duration, Instant};

use hivekeep_proto::api::v1 as pb;
use tonic::Code;

use crate::clients::Clients;
use crate::harness::T;
use crate::retry::resume_actor_await_capacity;

/// How long [`wait_for_actor_state`] waits.
pub const DEFAULT_STATE_TIMEOUT: Duration = Duration::from_secs(60);

/// The reference to `hive/name`.
pub fn oref(hive: &str, name: &str) -> pb::ObjectRef {
    pb::ObjectRef {
        hive: hive.into(),
        name: name.into(),
    }
}

/// Reads the Actor, failing the test on error.
pub async fn get_actor(t: &T, clients: &Clients, hive: &str, name: &str) -> pb::Actor {
    clients
        .api()
        .get_actor(pb::GetActorRequest {
            actor: Some(oref(hive, name)),
        })
        .await
        .unwrap_or_else(|e| t.fatal(format!("GetActor {hive}/{name}: {e}")))
        .into_inner()
}

/// Polls until the Actor reaches `want`, for up to
/// [`DEFAULT_STATE_TIMEOUT`], and returns it.
pub async fn wait_for_actor_state(
    t: &T,
    clients: &Clients,
    hive: &str,
    name: &str,
    want: pb::ActorState,
) -> pb::Actor {
    wait_for_actor_state_within(t, clients, hive, name, want, DEFAULT_STATE_TIMEOUT).await
}

/// [`wait_for_actor_state`] with an explicit timeout. Read errors are
/// retried until the deadline.
pub async fn wait_for_actor_state_within(
    t: &T,
    clients: &Clients,
    hive: &str,
    name: &str,
    want: pb::ActorState,
    timeout: Duration,
) -> pb::Actor {
    let deadline = Instant::now() + timeout;
    let mut last = None;
    while Instant::now() < deadline {
        if let Ok(resp) = clients
            .api()
            .get_actor(pb::GetActorRequest {
                actor: Some(oref(hive, name)),
            })
            .await
        {
            let actor = resp.into_inner();
            let state = actor.status.as_ref().map(|s| s.state).unwrap_or_default();
            if state == want as i32 {
                return actor;
            }
            last = pb::ActorState::try_from(state).ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    t.fatal(format!("timed out after {timeout:?} waiting for actor {hive}/{name} to reach state {want:?} (last seen {last:?})"))
}

/// Suspends and deletes a record a previous run left. NotFound is the
/// normal case; other errors are logged in case the create then fails.
pub async fn clear_leftover_actor(t: &T, clients: &Clients, r: &pb::ObjectRef) {
    if let Err(e) = clients
        .api()
        .suspend_actor(pb::SuspendActorRequest {
            actor: Some(r.clone()),
            ..Default::default()
        })
        .await
    {
        if e.code() != Code::NotFound {
            t.log(format!(
                "removing leftover actor {}/{}: SuspendActor: {e}",
                r.hive, r.name
            ));
        }
    }
    if let Err(e) = clients
        .api()
        .delete_actor(pb::DeleteActorRequest {
            actor: Some(r.clone()),
            ..Default::default()
        })
        .await
    {
        if e.code() != Code::NotFound {
            t.log(format!(
                "removing leftover actor {}/{}: DeleteActor: {e}",
                r.hive, r.name
            ));
        }
    }
}

/// Suspends and deletes `r` when the test ends, within two minutes. A
/// failure is only logged: the next run's [`clear_leftover_actor`] removes
/// what is left.
pub fn delete_actor_on_cleanup(t: &T, clients: &Clients, r: pb::ObjectRef) {
    let mut api = clients.api();
    t.cleanup(move |t| async move {
        let work = async {
            let _ = api.suspend_actor(pb::SuspendActorRequest { actor: Some(r.clone()), ..Default::default() }).await;
            api.delete_actor(pb::DeleteActorRequest { actor: Some(r.clone()), ..Default::default() }).await
        };
        match tokio::time::timeout(Duration::from_secs(120), work).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => t.log(format!("cleanup: DeleteActor {}/{} failed, actor leaked (remove with: hivectl delete actor {}/{}): {e}", r.hive, r.name, r.hive, r.name)),
            Err(_) => t.log(format!("cleanup: DeleteActor {}/{} timed out, actor leaked", r.hive, r.name)),
        }
    });
}

/// Clears any leftover record, creates `hive/name` from `template` (in the
/// same hive) and registers its cleanup.
pub async fn create_actor(t: &T, clients: &Clients, hive: &str, name: &str, template: &str) {
    let r = oref(hive, name);
    clear_leftover_actor(t, clients, &r).await;
    let actor = pb::Actor {
        metadata: Some(pb::ResourceMetadata {
            hive: hive.into(),
            name: name.into(),
            ..Default::default()
        }),
        actor_template: Some(oref(hive, template)),
        ..Default::default()
    };
    if let Err(e) = clients
        .api()
        .create_actor(pb::CreateActorRequest { actor: Some(actor) })
        .await
    {
        t.fatal(format!("CreateActor {hive}/{name}: {e}"));
    }
    delete_actor_on_cleanup(t, clients, r);
}

/// Resumes `hive/name`, retrying while the pool has no capacity.
pub async fn resume_actor(t: &T, clients: &Clients, hive: &str, name: &str) {
    if let Err(e) = resume_actor_await_capacity(
        t,
        clients,
        pb::ResumeActorRequest {
            actor: Some(oref(hive, name)),
        },
    )
    .await
    {
        t.fatal(format!("ResumeActor {hive}/{name}: {e}"));
    }
}

/// [`create_actor`] then [`resume_actor`], without waiting for RUNNING.
pub async fn create_and_resume_actor(
    t: &T,
    clients: &Clients,
    hive: &str,
    name: &str,
    template: &str,
) {
    create_actor(t, clients, hive, name, template).await;
    resume_actor(t, clients, hive, name).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oref_names_hive_and_actor() {
        let r = oref("h", "a");
        assert_eq!((r.hive.as_str(), r.name.as_str()), ("h", "a"));
    }
}
