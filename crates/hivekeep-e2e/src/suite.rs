//! Running cluster tests from `cargo test`.
//!
//! Every cluster test is a plain `#[test] #[ignore]` function whose body is
//! [`e2e_test`]. The first one to run sets the suite up: it installs the
//! rustls provider, reads the preview gates, connects the shared
//! [`Clients`] and runs the preflight checks; a failure there fails every
//! test of the binary with the same message. Tests run on one shared tokio
//! runtime, so the shared connections outlive any single test.
//!
//! At process exit the suite's own cleanups run, newest first, and the
//! namespaces still registered are deleted, unless a test failed: then they
//! are kept for diagnosis and listed (see [`crate::namespace`]).

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Once, OnceLock};

use parking_lot::Mutex;
use tokio::runtime::Runtime;

use crate::clients::Clients;
use crate::harness::{self, Fatal, Skip, T};
use crate::pretty::colorf;

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("hk-e2e")
        .build()
        .expect("building the e2e runtime")
});

static CLIENTS: OnceLock<Arc<Clients>> = OnceLock::new();
static SETUP: tokio::sync::OnceCell<Result<(), String>> = tokio::sync::OnceCell::const_new();
static ANY_FAILED: AtomicBool = AtomicBool::new(false);
static SUITE_CLEANUPS: Mutex<Vec<Box<dyn FnOnce() + Send>>> = Mutex::new(Vec::new());
static HOOKS: Once = Once::new();

/// The shared clients. Panics outside a running [`e2e_test`].
pub fn clients() -> Arc<Clients> {
    CLIENTS.get().cloned().expect(
        "e2e clients are not initialized; run the test through hivekeep_e2e::suite::e2e_test",
    )
}

/// The shared runtime the suites run on, for helpers that need a handle.
pub fn runtime() -> &'static Runtime {
    &RUNTIME
}

/// Registers `f` to run once at the end of the test binary, newest first.
pub fn register_suite_cleanup(f: impl FnOnce() + Send + 'static) {
    SUITE_CLEANUPS.lock().push(Box::new(f));
}

/// Whether a cluster run is configured: `HK_E2E=1` and a kubeconfig
/// (`HK_E2E_KUBECONFIG`, else `KUBECONFIG`).
pub fn enabled() -> bool {
    crate::settings::e2e_enabled() && crate::settings::kubeconfig().is_some()
}

fn install_hooks() {
    HOOKS.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
        // Fatal and Skip unwind on purpose; the harness has already logged
        // them, so the default hook's backtrace banner is noise.
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let payload = info.payload();
            if payload.downcast_ref::<Fatal>().is_some() || payload.downcast_ref::<Skip>().is_some()
            {
                return;
            }
            default(info);
        }));
        // SAFETY: registering a plain extern "C" function with no captured
        // state; it runs once at process exit.
        unsafe {
            libc::atexit(finish);
        }
    });
}

extern "C" fn finish() {
    let cleanups = std::mem::take(&mut *SUITE_CLEANUPS.lock());
    for f in cleanups.into_iter().rev() {
        f();
    }
    if CLIENTS.get().is_none() {
        return;
    }
    if ANY_FAILED.load(Ordering::SeqCst) {
        crate::namespace::retain_namespaces();
    } else {
        RUNTIME.block_on(crate::namespace::cleanup_namespaces());
    }
}

async fn setup() -> Result<(), String> {
    crate::preview::init_preview()?;
    let clients = Clients::connect()
        .await
        .map_err(|e| format!("Failed to initialize E2E clients: {e}"))?;
    crate::preflight::preflight_checks(&clients)
        .await
        .map_err(|e| format!("Preflight checks FAILED: {e}"))?;
    let _ = CLIENTS.set(Arc::new(clients));
    Ok(())
}

/// Runs `body` as the cluster test `name`. Unless [`enabled`] the test is
/// reported as skipped and returns; with it, the suite is set up
/// (once per binary) and the body runs, failing the surrounding `#[test]`
/// if it fails.
pub fn e2e_test<F, Fut>(name: &str, body: F)
where
    F: FnOnce(T) -> Fut,
    Fut: Future<Output = ()>,
{
    if !enabled() {
        eprintln!(
            "{}",
            colorf(&format!(
                "<yellow>{name}: skipped; this is an e2e test and needs a cluster. Set {}=1 and {} (or KUBECONFIG).</yellow>",
                crate::settings::ENABLE_ENV,
                crate::settings::KUBECONFIG_ENV
            ))
        );
        return;
    }
    install_hooks();
    RUNTIME.block_on(async {
        if let Err(e) = SETUP.get_or_init(setup).await {
            ANY_FAILED.store(true, Ordering::SeqCst);
            panic!("{}", colorf(&format!("<red>{e}</red>")));
        }
        let outcome = harness::run_outcome(name, body).await;
        if let harness::Outcome::Failed(msg) = outcome {
            ANY_FAILED.store(true, Ordering::SeqCst);
            panic!(
                "{name} failed: {}",
                msg.unwrap_or_else(|| "see the log above".into())
            );
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_without_a_cluster() {
        if enabled() {
            return;
        }
        let mut ran = false;
        e2e_test("NoCluster", |_| {
            ran = true;
            async {}
        });
        assert!(!ran);
    }
}
