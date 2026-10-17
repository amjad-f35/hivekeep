//! The Actor event log: one `hk.actor.state_changed` or `hk.actor.crashed`
//! record (see [`hivekeep_core::actor_event`]) per committed state change,
//! carrying the identity that metric labels may not.
//!
//! The last record for an Actor uid names the state it is in now.

use hivekeep_core::actor_event::{self, Attr, Event};
use hivekeep_core::attr;
use hivekeep_core::resource::ActorAttribution;
use hivekeep_proto::api::v1 as pb;

use crate::fields;

pub use hivekeep_core::actor_event::{CRASHED, STATE_CHANGED};

/// The attributes of a lifecycle record: the Actor's identity, the
/// (bounded) operation and the state reached.
pub fn lifecycle_attrs(actor: &pb::Actor, op: &str, state: &str) -> Vec<Attr> {
    let mut attrs = actor_event::identity_attrs(&ActorAttribution::of(actor));
    attrs.push(Attr::str(
        attr::ACTOR_OPERATION_NAME_KEY,
        attr::normalize_operation_name(op),
    ));
    attrs.push(Attr::str(attr::ACTOR_STATE_KEY, state));
    attrs
}

fn log(ev: &Event, actor: &pb::Actor, op: &str, state: &str) {
    crate::metrics::count_state_change(state);
    actor_event::log(None, ev, &lifecycle_attrs(actor, op, state));
}

/// Records the state of a committed Actor, read off the record itself.
pub fn state_changed(actor: &pb::Actor, op: &str) {
    log(
        &STATE_CHANGED,
        actor,
        op,
        attr::actor_state_value(fields::state(actor) as i32),
    );
}

/// Records the removal of an Actor. The row is gone, so this is the one
/// state named by hand.
pub fn deleted(actor: &pb::Actor, op: &str) {
    log(&STATE_CHANGED, actor, op, attr::actor_state::DELETED);
}

/// Records a crash. It names the state too: a crash is the one transition
/// a worker never observes, so a consumer following an Actor's last state
/// needs this record to see it crash.
pub fn crashed(actor: &pb::Actor, op: &str) {
    log(&CRASHED, actor, op, attr::actor_state::CRASHED);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_attrs_name_identity_operation_and_state() {
        let actor = pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: "h".into(),
                name: "a".into(),
                uid: "u".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let attrs = lifecycle_attrs(&actor, "not-an-operation", attr::actor_state::RUNNING);
        let get = |k: &str| {
            attrs
                .iter()
                .find(|a| a.key == k)
                .map(|a| a.value.as_display())
        };
        assert_eq!(get(attr::ACTOR_NAME_KEY).as_deref(), Some("a"));
        assert_eq!(get(attr::ACTOR_UID_KEY).as_deref(), Some("u"));
        assert_eq!(
            get(attr::ACTOR_OPERATION_NAME_KEY).as_deref(),
            Some(attr::operation::UNKNOWN)
        );
        assert_eq!(get(attr::ACTOR_STATE_KEY).as_deref(), Some("running"));
        // Every key the vocabulary promises is present.
        for k in STATE_CHANGED.keys {
            assert!(get(k).is_some(), "missing {k}");
        }
    }
}
