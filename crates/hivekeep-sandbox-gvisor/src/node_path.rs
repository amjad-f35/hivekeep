//! Well-known node paths the worker shares with the keeper.

use std::path::{Path, PathBuf};

/// Root of all Hivekeep state on a node.
pub const BASE_PATH: &str = "/var/lib/hivekeep";

/// Where the keeper keeps per-Actor directories.
pub fn actors_dir() -> PathBuf {
    Path::new(BASE_PATH).join("actors")
}

/// The directory the keeper stages an Actor's runtime binaries into, under
/// `actors_dir`.
pub fn actor_sandbox_assets_dir(actors_dir: &Path, actor_uid: &str) -> PathBuf {
    actors_dir.join(actor_uid).join("sandbox-assets")
}

/// The keeper's worker-support socket.
pub fn worker_support_socket() -> PathBuf {
    Path::new(BASE_PATH).join("worker-support.sock")
}

/// Directory holding per-worker-pod state.
pub fn workers_dir() -> PathBuf {
    Path::new(BASE_PATH).join("workers")
}

/// The directory of one worker pod.
pub fn worker_path(pod_uid: &str) -> PathBuf {
    workers_dir().join(pod_uid)
}

/// The socket a worker pod serves the `Worker` API on.
pub fn worker_socket_path(pod_uid: &str) -> PathBuf {
    worker_path(pod_uid).join("worker.sock")
}

/// The file the keeper writes the sandbox runtimes a worker may run into.
pub fn worker_sandbox_runtimes_path(pod_uid: &str) -> PathBuf {
    worker_path(pod_uid).join("sandbox-runtimes.json")
}

/// The name of an Actor's network namespace.
pub fn actor_netns_name(actor_uid: &str) -> String {
    format!("worker-actor:{actor_uid}")
}

/// The bind-mounted network namespace of an Actor.
pub fn actor_netns_path(actor_uid: &str) -> PathBuf {
    Path::new("/run/netns").join(actor_netns_name(actor_uid))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths() {
        assert_eq!(
            actor_sandbox_assets_dir(&actors_dir(), "uid-a"),
            PathBuf::from("/var/lib/hivekeep/actors/uid-a/sandbox-assets")
        );
        assert_eq!(
            worker_socket_path("pod"),
            PathBuf::from("/var/lib/hivekeep/workers/pod/worker.sock")
        );
        assert_eq!(
            actor_netns_path("uid-a"),
            PathBuf::from("/run/netns/worker-actor:uid-a")
        );
    }
}
