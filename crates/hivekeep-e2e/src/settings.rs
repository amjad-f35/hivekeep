//! Run settings, read from the environment.
//!
//! The suites run under `cargo test`, which owns the command line, so what a
//! flag would select elsewhere is an environment variable here.

use std::path::PathBuf;

/// Turns the cluster suites on when set to `1`. Without it every cluster
/// test prints a skip and passes, so `cargo test -- --ignored` stays safe.
pub const ENABLE_ENV: &str = "HK_E2E";
/// Path of the kubeconfig for the cluster under test; `KUBECONFIG` when
/// unset.
pub const KUBECONFIG_ENV: &str = "HK_E2E_KUBECONFIG";
/// Kubernetes context to use, if not the kubeconfig's current one.
pub const KUBE_CONTEXT_ENV: &str = "HK_E2E_KUBE_CONTEXT";
/// StorageClass the external-volume tests provision from.
pub const STORAGE_CLASS_ENV: &str = "HK_E2E_STORAGE_CLASS";
/// Disables colored output when set.
pub const NO_COLOR_ENV: &str = "HK_E2E_NO_COLOR";
/// Preview gates the install under test enabled; see [`crate::preview`].
pub const PREVIEW_ENV: &str = "HK_E2E_PREVIEW";

/// StorageClass used when [`STORAGE_CLASS_ENV`] is unset.
pub const DEFAULT_STORAGE_CLASS: &str = "csi-nfs-sc";

fn non_empty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// Whether the cluster suites are switched on (`HK_E2E=1`).
pub fn e2e_enabled() -> bool {
    std::env::var(ENABLE_ENV).as_deref() == Ok("1")
}

/// The kubeconfig path: `HK_E2E_KUBECONFIG`, else `KUBECONFIG`.
pub fn kubeconfig() -> Option<PathBuf> {
    non_empty(KUBECONFIG_ENV)
        .or_else(|| non_empty("KUBECONFIG"))
        .map(PathBuf::from)
}

/// The Kubernetes context to use, if one was named.
pub fn kube_context() -> Option<String> {
    non_empty(KUBE_CONTEXT_ENV)
}

/// The StorageClass for external-volume tests.
pub fn storage_class() -> String {
    non_empty(STORAGE_CLASS_ENV).unwrap_or_else(|| DEFAULT_STORAGE_CLASS.to_string())
}

/// `--context=<ctx>` for kubectl invocations, when a context was named.
pub fn kubectl_context_args() -> Vec<String> {
    let mut args = Vec::new();
    if let Some(path) = kubeconfig() {
        args.push(format!("--kubeconfig={}", path.display()));
    }
    if let Some(ctx) = kube_context() {
        args.push(format!("--context={ctx}"));
    }
    args
}
