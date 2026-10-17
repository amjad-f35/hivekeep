//! Which node holds an Actor, for refusing credentials to nodes that do not.

use hivekeep_core::actor_state::ActorState;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{Store, StoreError};

use crate::error::{Error, Result};
use crate::fields;
use crate::workflow::resume::worker_hosts_actor;

/// The node holding the Actor, or empty when none does.
///
/// * SUSPENDED with no worker: `assigned_node`, set only while a local
///   snapshot is there.
/// * RESUMING, RUNNING, SUSPENDING with a worker assignment: that worker's
///   node, but only while the store's assignments still bind the worker to
///   this Actor's uid.
/// * Anything else: empty.
pub async fn hosting_node(store: &dyn Store, actor: &pb::Actor) -> Result<String> {
    let Some(state) = ActorState::of(actor) else {
        return Ok(String::new());
    };
    if !state.is_transfer_state() {
        return Ok(String::new());
    }
    if fields::assignment(actor).is_none() {
        if state != ActorState::Suspended {
            return Ok(String::new());
        }
        return Ok(fields::assigned_node(actor).to_string());
    }
    if !matches!(
        state,
        ActorState::Resuming | ActorState::Running | ActorState::Suspending
    ) {
        return Ok(String::new());
    }
    let worker = fields::assigned_worker(actor);
    if !worker_hosts_actor(store, worker, fields::actor_uid(actor)).await? {
        return Ok(String::new());
    }
    match store.get_worker(worker).await {
        Ok(w) => Ok(w.node_name),
        Err(StoreError::NotFound) => Ok(String::new()),
        Err(e) => Err(Error::from(e).context(format!("while retrieving worker {worker}"))),
    }
}
