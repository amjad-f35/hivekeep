use super::*;

/// Everything the control plane needs from persistence. Implemented for
/// every type that implements all the resource traits.
pub trait Store:
    HiveStore
    + ActorStore
    + EgressPolicyStore
    + AccessPolicyStore
    + ActorTemplateStore
    + TagStore
    + WorkerStore
    + WorkerAssignmentStore
    + LeaseStore
    + HealthCheck
{
}

impl<T> Store for T where
    T: HiveStore
        + ActorStore
        + EgressPolicyStore
        + AccessPolicyStore
        + ActorTemplateStore
        + TagStore
        + WorkerStore
        + WorkerAssignmentStore
        + LeaseStore
        + HealthCheck
{
}
