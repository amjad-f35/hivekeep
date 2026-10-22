//! Gating tests that need root (mounts, mknod, trusted.* xattrs, ...).

use crate::harness::T;

/// Whether the process runs as root.
pub fn is_root() -> bool {
    // SAFETY: geteuid has no preconditions and cannot fail.
    unsafe { libc::geteuid() == 0 }
}

/// Skips `t` unless it runs as root; `reason` names the privileged
/// operation the test needs.
pub fn require_root(t: &T, reason: &str) {
    if !is_root() {
        t.skip(format!(
            "needs root: {reason} (rerun the test binary under sudo)"
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::run_test;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn require_gates_on_euid() {
        let reached = Arc::new(AtomicBool::new(false));
        let r = reached.clone();
        run_test("Require", |t| async move {
            t.run("gate", |t| async move {
                require_root(&t, "gate probe");
                r.store(true, Ordering::SeqCst);
            })
            .await;
        })
        .await;
        assert_eq!(reached.load(Ordering::SeqCst), is_root());
    }
}
