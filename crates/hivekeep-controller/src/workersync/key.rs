//! The identity of a worker pod incarnation.

/// The pod incarnation a queued event concerns. Namespace and name locate
/// the pod in the cache; the UID belongs in the key because the queue
/// dedupes by key: a pod and its same-named replacement are different
/// Workers, and without the UID the delete of `worker-1(uid-A)` and the add
/// of `worker-1(uid-B)` would collapse into one item, leaving uid-A's record
/// orphaned.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkerKey {
    /// Pod namespace.
    pub namespace: String,
    /// Pod name.
    pub name: String,
    /// Pod UID.
    pub uid: String,
}

impl WorkerKey {
    /// A key.
    pub fn new(
        namespace: impl Into<String>,
        name: impl Into<String>,
        uid: impl Into<String>,
    ) -> Self {
        WorkerKey {
            namespace: namespace.into(),
            name: name.into(),
            uid: uid.into(),
        }
    }

    /// The name of the Worker this key identifies. The syncer mints Workers
    /// from pods and names them after the pod UID; this and worker creation
    /// are the only places that choice is expressed. Everywhere else a
    /// Worker name is opaque.
    pub fn worker_name(&self) -> &str {
        &self.uid
    }

    /// `namespace/name` of the pod, for logs.
    pub fn pod(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_is_named_after_the_pod_uid() {
        let k = WorkerKey::new("ns", "pod", "uid-1");
        assert_eq!(k.worker_name(), "uid-1");
        assert_eq!(k.pod(), "ns/pod");
        assert_ne!(k, WorkerKey::new("ns", "pod", "uid-2"));
    }
}
