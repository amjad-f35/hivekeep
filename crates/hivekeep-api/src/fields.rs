//! Shorthand accessors for the optional fields of stored messages.

use hivekeep_core::resource::{ActorRef, ActorTemplateRef, TagRef};
use hivekeep_proto::api::v1 as pb;

/// The Actor's state, UNSPECIFIED when unset.
pub fn state(a: &pb::Actor) -> pb::ActorState {
    a.status
        .as_ref()
        .and_then(|s| pb::ActorState::try_from(s.state).ok())
        .unwrap_or(pb::ActorState::Unspecified)
}

/// The wire name of a state.
pub fn state_name(s: pb::ActorState) -> &'static str {
    s.as_str_name()
}

/// The Actor's status, created if unset.
pub fn status_mut(a: &mut pb::Actor) -> &mut pb::ActorStatus {
    a.status.get_or_insert_with(Default::default)
}

/// The Actor's worker assignment.
pub fn assignment(a: &pb::Actor) -> Option<&pb::WorkerAssignment> {
    a.status.as_ref().and_then(|s| s.worker_assignment.as_ref())
}

/// The name of the Worker the Actor's status names, or empty.
pub fn assigned_worker(a: &pb::Actor) -> &str {
    assignment(a)
        .and_then(|w| w.worker.as_ref())
        .map(|w| w.name.as_str())
        .unwrap_or("")
}

/// The Actor's assigned node, or empty.
pub fn assigned_node(a: &pb::Actor) -> &str {
    a.status
        .as_ref()
        .map(|s| s.assigned_node.as_str())
        .unwrap_or("")
}

/// The metadata uid, or empty.
pub fn uid(md: Option<&pb::ResourceMetadata>) -> &str {
    md.map(|m| m.uid.as_str()).unwrap_or("")
}

/// The Actor's uid.
pub fn actor_uid(a: &pb::Actor) -> &str {
    uid(a.metadata.as_ref())
}

/// The Actor's hive.
pub fn actor_hive(a: &pb::Actor) -> &str {
    a.metadata.as_ref().map(|m| m.hive.as_str()).unwrap_or("")
}

/// The Actor's name.
pub fn actor_name(a: &pb::Actor) -> &str {
    a.metadata.as_ref().map(|m| m.name.as_str()).unwrap_or("")
}

/// The reference of a stored Actor.
pub fn actor_ref(a: &pb::Actor) -> ActorRef {
    ActorRef::new(actor_hive(a), actor_name(a))
}

/// The reference of the Actor's template.
pub fn template_ref_of(a: &pb::Actor) -> ActorTemplateRef {
    a.actor_template
        .as_ref()
        .map(ActorTemplateRef::from_object_ref)
        .unwrap_or_else(|| ActorTemplateRef::new("", ""))
}

/// The reference of a stored template.
pub fn template_ref(t: &pb::ActorTemplate) -> ActorTemplateRef {
    let md = t.metadata.clone().unwrap_or_default();
    ActorTemplateRef::new(md.hive, md.name)
}

/// The reference of a stored Tag.
pub fn tag_ref(t: &pb::Tag) -> TagRef {
    let md = t.metadata.clone().unwrap_or_default();
    TagRef::new(md.hive, md.name)
}

/// The template's uid.
pub fn template_uid(t: &pb::ActorTemplate) -> &str {
    uid(t.metadata.as_ref())
}

/// A Worker's name.
pub fn worker_name(w: &pb::Worker) -> &str {
    w.metadata.as_ref().map(|m| m.name.as_str()).unwrap_or("")
}

/// A Worker's state.
pub fn worker_state(w: &pb::Worker) -> pb::WorkerState {
    w.status
        .as_ref()
        .and_then(|s| pb::WorkerState::try_from(s.state).ok())
        .unwrap_or(pb::WorkerState::Unspecified)
}

/// A template's preferred fidelity.
pub fn preferred_fidelity(t: &pb::ActorTemplate) -> i32 {
    t.snapshot_config
        .as_ref()
        .map(|c| c.preferred_fidelity)
        .unwrap_or(0)
}

/// A `ref` as `hive/name` for messages.
pub fn object_ref_string(r: Option<&pb::ObjectRef>) -> String {
    r.map(|r| format!("{}/{}", r.hive, r.name))
        .unwrap_or_default()
}
