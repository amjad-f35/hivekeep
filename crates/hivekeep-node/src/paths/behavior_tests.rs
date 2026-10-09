//! Worker paths, socket path limits, and the base-directory containment
//! check.

use super::*;

const POD_UID: &str = "123e4567-e89b-12d3-a456-426614174000";
/// A Unix socket path holds at most 107 bytes plus its NUL.
const MAX_UNIX_SOCKET_LEN: usize = 107;

#[test]
fn worker_path() {
    let p = NodePaths::default();
    let path = p.worker_dir(POD_UID);
    assert!(
        path.to_string_lossy()
            .ends_with(&format!("/workers/{POD_UID}")),
        "{path:?}"
    );
}

#[test]
fn worker_socket_path_limits() {
    let p = NodePaths::default();
    let sock = p.worker_socket(POD_UID);
    let len = sock.as_os_str().len();
    assert!(
        len <= MAX_UNIX_SOCKET_LEN,
        "socket path {sock:?} is {len} bytes"
    );
    assert_eq!(sock, p.worker_socket(POD_UID), "not deterministic");
}

#[test]
fn worker_support_socket_is_node_scoped() {
    // The node-scoped socket every worker reaches the keeper on: under the
    // base dir (mounted at the same path in workers), short enough to bind,
    // and distinct from any per-pod socket.
    let p = NodePaths::default();
    let sock = p.worker_support_socket();
    assert!(sock.as_os_str().len() <= MAX_UNIX_SOCKET_LEN);
    assert!(p.under_base(&sock), "{sock:?}");
    assert_ne!(sock, p.worker_socket(POD_UID));
}

#[test]
fn worker_path_uniqueness() {
    let p = NodePaths::default();
    assert_ne!(
        p.worker_dir(POD_UID),
        p.worker_dir("987f6543-e21b-32d1-b654-246614174111")
    );
}

#[test]
fn under_base_path() {
    let p = NodePaths::default();
    let base = DEFAULT_BASE_DIR;
    for (name, dir, want) in [
        ("inside", format!("{base}/image-cache"), true),
        (
            "inside with doubled separator",
            format!("{base}//image-cache"),
            true,
        ),
        (
            "inside via dot-dot",
            format!("{base}/x/../image-cache"),
            true,
        ),
        ("base path itself is not inside", base.to_string(), false),
        (
            "sibling with the base path as name prefix",
            format!("{base}-other/image-cache"),
            false,
        ),
        (
            "outside",
            "/var/lib/elsewhere/image-cache".to_string(),
            false,
        ),
        (
            "dot-dot escaping the base path",
            format!("{base}/../elsewhere/image-cache"),
            false,
        ),
        (
            "relative resolves against the cwd, not the base path",
            "image-cache".to_string(),
            false,
        ),
    ] {
        assert_eq!(p.under_base(Path::new(&dir)), want, "{name}: {dir}");
    }
}
