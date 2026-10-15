//! Who a worker request is about, for logs, usage records and crash reports.

use hivekeep_core::resource::{ActorAttribution, ActorRef};
use hivekeep_proto::worker::v1 as pb;

/// A request that names an Actor.
pub trait ActorRequest {
    /// The Actor the request is about.
    fn attribution(&self) -> ActorAttribution;
}

macro_rules! impl_actor_request {
    ($($t:ty),*) => {$(
        impl ActorRequest for $t {
            fn attribution(&self) -> ActorAttribution {
                ActorAttribution {
                    actor: ActorRef::new(self.hive.clone(), self.actor_name.clone()),
                    uid: self.actor_uid.clone(),
                    template_hive: self.actor_template_hive.clone(),
                    template_name: self.actor_template_name.clone(),
                }
            }
        }
    )*};
}

impl_actor_request!(
    pb::RunWorkloadRequest,
    pb::RestoreWorkloadRequest,
    pb::CheckpointWorkloadRequest,
    pb::TerminateWorkloadRequest
);

/// An attribution that names only a uid, for tests and internal bookkeeping.
pub fn uid_only(uid: &str) -> ActorAttribution {
    ActorAttribution {
        actor: ActorRef::new("", ""),
        uid: uid.into(),
        template_hive: String::new(),
        template_name: String::new(),
    }
}
