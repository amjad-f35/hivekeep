//! A small test context in the style the suites are written in.
//!
//! [`T`] carries a test's name, its failure state, cleanups that run in
//! reverse registration order once the body returns (or panics), and
//! per-test temp directories. [`T::fatal`] and [`T::skip`] end the test by
//! unwinding with a typed payload, which [`run_test`] catches, so helpers can
//! stop a test from any depth without threading `Result`s through every
//! call. Subtests ([`T::run`]) get their own context; their failure marks the
//! parent failed without stopping it.

use std::any::Any;
use std::fmt::Display;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::FutureExt;
use parking_lot::Mutex;

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type Cleanup = Box<dyn FnOnce(T) -> BoxFuture + Send + 'static>;

/// Unwind payload of [`T::fatal`]: the test failed and stops here.
#[derive(Debug)]
pub struct Fatal(pub String);

/// Unwind payload of [`T::skip`]: the test does not apply and stops here.
#[derive(Debug)]
pub struct Skip(pub String);

/// How a test (or subtest) ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The body returned and nothing marked the test failed.
    Passed,
    /// The test called [`T::skip`].
    Skipped(String),
    /// The test failed; the message of the fatal error or panic, if any.
    Failed(Option<String>),
}

struct Inner {
    name: String,
    failed: AtomicBool,
    cleanups: Mutex<Vec<Cleanup>>,
    temp_dirs: Mutex<Vec<tempfile::TempDir>>,
    parent: Option<T>,
}

/// The context handed to a test body. Cheap to clone; clones share state.
#[derive(Clone)]
pub struct T {
    inner: Arc<Inner>,
}

impl T {
    /// A fresh top-level context named `name`.
    pub fn new(name: impl Into<String>) -> Self {
        T::with_parent(name.into(), None)
    }

    fn with_parent(name: String, parent: Option<T>) -> Self {
        T {
            inner: Arc::new(Inner {
                name,
                failed: AtomicBool::new(false),
                cleanups: Mutex::new(Vec::new()),
                temp_dirs: Mutex::new(Vec::new()),
                parent,
            }),
        }
    }

    /// The test's full name; subtests are `parent/child`.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Logs a line attributed to this test.
    pub fn log(&self, msg: impl Display) {
        eprintln!("    {}: {}", self.inner.name, msg);
    }

    /// Records a failure and keeps going.
    pub fn error(&self, msg: impl Display) {
        self.log(format!("ERROR: {msg}"));
        self.mark_failed();
    }

    /// Records a failure and stops the test.
    pub fn fatal(&self, msg: impl Display) -> ! {
        let msg = msg.to_string();
        self.log(format!("FATAL: {msg}"));
        self.mark_failed();
        std::panic::panic_any(Fatal(msg))
    }

    /// Stops the test without failing it.
    pub fn skip(&self, msg: impl Display) -> ! {
        let msg = msg.to_string();
        self.log(format!("SKIP: {msg}"));
        std::panic::panic_any(Skip(msg))
    }

    /// Whether this test (or any of its subtests) has failed.
    pub fn failed(&self) -> bool {
        self.inner.failed.load(Ordering::SeqCst)
    }

    fn mark_failed(&self) {
        self.inner.failed.store(true, Ordering::SeqCst);
        if let Some(parent) = &self.inner.parent {
            parent.mark_failed();
        }
    }

    /// Registers `f` to run after the test body, newest first. It receives
    /// this context, so it can check [`T::failed`] or log.
    pub fn cleanup<F, Fut>(&self, f: F)
    where
        F: FnOnce(T) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.inner
            .cleanups
            .lock()
            .push(Box::new(move |t| Box::pin(f(t))));
    }

    /// A new, empty directory removed when the test ends.
    pub fn temp_dir(&self) -> PathBuf {
        let dir = tempfile::Builder::new()
            .prefix("hk-e2e-")
            .tempdir()
            .unwrap_or_else(|e| self.fatal(format!("creating temp dir: {e}")));
        let path = dir.path().to_path_buf();
        self.inner.temp_dirs.lock().push(dir);
        path
    }

    /// Runs `body` as the subtest `name` and reports whether it passed (a
    /// skip counts as passing). A failing subtest marks this test failed.
    pub async fn run<F, Fut>(&self, name: &str, body: F) -> bool
    where
        F: FnOnce(T) -> Fut,
        Fut: Future<Output = ()>,
    {
        let child = T::with_parent(format!("{}/{}", self.inner.name, name), Some(self.clone()));
        let outcome = drive(child.clone(), body).await;
        report(&child, &outcome);
        !matches!(outcome, Outcome::Failed(_))
    }

    async fn run_cleanups(&self) {
        loop {
            let next = self.inner.cleanups.lock().pop();
            let Some(cleanup) = next else { break };
            let t = self.clone();
            if let Err(payload) = AssertUnwindSafe(cleanup(t)).catch_unwind().await {
                if !is_skip(&*payload) {
                    self.mark_failed();
                    self.log(format!("cleanup panicked: {}", panic_message(&*payload)));
                }
            }
        }
        self.inner.temp_dirs.lock().clear();
    }
}

fn is_skip(payload: &(dyn Any + Send)) -> bool {
    payload.downcast_ref::<Skip>().is_some()
}

/// Renders an unwind payload as text.
pub fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(Fatal(m)) = payload.downcast_ref::<Fatal>() {
        m.clone()
    } else if let Some(Skip(m)) = payload.downcast_ref::<Skip>() {
        m.clone()
    } else if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic".to_string()
    }
}

async fn drive<F, Fut>(t: T, body: F) -> Outcome
where
    F: FnOnce(T) -> Fut,
    Fut: Future<Output = ()>,
{
    let result = AssertUnwindSafe(async { body(t.clone()).await })
        .catch_unwind()
        .await;
    let outcome = match result {
        Ok(()) => None,
        Err(payload) => {
            if let Some(Skip(m)) = payload.downcast_ref::<Skip>() {
                Some(Outcome::Skipped(m.clone()))
            } else {
                if payload.downcast_ref::<Fatal>().is_none() {
                    t.log(format!("panicked: {}", panic_message(&*payload)));
                }
                t.mark_failed();
                Some(Outcome::Failed(Some(panic_message(&*payload))))
            }
        }
    };
    t.run_cleanups().await;
    match outcome {
        Some(Outcome::Skipped(m)) if !t.failed() => Outcome::Skipped(m),
        Some(Outcome::Failed(m)) => Outcome::Failed(m),
        _ if t.failed() => Outcome::Failed(None),
        _ => Outcome::Passed,
    }
}

fn report(t: &T, outcome: &Outcome) {
    match outcome {
        Outcome::Passed => eprintln!("--- PASS: {}", t.name()),
        Outcome::Skipped(m) => eprintln!("--- SKIP: {} ({m})", t.name()),
        Outcome::Failed(_) => eprintln!("--- FAIL: {}", t.name()),
    }
}

/// Runs `body` as the top-level test `name`: the body, then its cleanups,
/// then the outcome. Unlike [`run_test`] it never panics.
pub async fn run_outcome<F, Fut>(name: &str, body: F) -> Outcome
where
    F: FnOnce(T) -> Fut,
    Fut: Future<Output = ()>,
{
    let t = T::new(name);
    let outcome = drive(t.clone(), body).await;
    report(&t, &outcome);
    outcome
}

/// Runs `body` as the top-level test `name` and panics if it failed, so the
/// surrounding `#[test]` fails too. A skipped test returns normally.
pub async fn run_test<F, Fut>(name: &str, body: F)
where
    F: FnOnce(T) -> Fut,
    Fut: Future<Output = ()>,
{
    if let Outcome::Failed(msg) = run_outcome(name, body).await {
        panic!(
            "{name} failed: {}",
            msg.unwrap_or_else(|| "see the log above".into())
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn cleanups_run_newest_first_after_a_pass() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let o = order.clone();
        let outcome = run_outcome("Ordered", |t| async move {
            for i in 0..3 {
                let o = o.clone();
                t.cleanup(move |_| async move { o.lock().push(i) });
            }
        })
        .await;
        assert_eq!(outcome, Outcome::Passed);
        assert_eq!(*order.lock(), vec![2, 1, 0]);
    }

    #[tokio::test]
    async fn fatal_fails_and_still_cleans_up() {
        let ran = Arc::new(AtomicBool::new(false));
        let seen_failed = Arc::new(AtomicBool::new(false));
        let (r, s) = (ran.clone(), seen_failed.clone());
        let outcome = run_outcome("Fatal", |t| async move {
            t.cleanup(move |t| async move {
                r.store(true, Ordering::SeqCst);
                s.store(t.failed(), Ordering::SeqCst);
            });
            t.fatal("boom");
        })
        .await;
        assert_eq!(outcome, Outcome::Failed(Some("boom".into())));
        assert!(ran.load(Ordering::SeqCst));
        assert!(seen_failed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn skip_is_not_a_failure() {
        let outcome = run_outcome("Skip", |t| async move { t.skip("not here") }).await;
        assert_eq!(outcome, Outcome::Skipped("not here".into()));
    }

    #[tokio::test]
    async fn error_fails_without_stopping() {
        let reached = Arc::new(AtomicBool::new(false));
        let r = reached.clone();
        let outcome = run_outcome("Error", |t| async move {
            t.error("first");
            r.store(true, Ordering::SeqCst);
        })
        .await;
        assert_eq!(outcome, Outcome::Failed(None));
        assert!(reached.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn plain_panics_fail_the_test() {
        let outcome = run_outcome("Panic", |_| async move { panic!("oops") }).await;
        assert_eq!(outcome, Outcome::Failed(Some("oops".into())));
    }

    #[tokio::test]
    async fn subtests_propagate_failure_but_not_control_flow() {
        let after = Arc::new(AtomicUsize::new(0));
        let a = after.clone();
        let outcome = run_outcome("Parent", |t| async move {
            assert!(!t.run("bad", |t| async move { t.fatal("child") }).await);
            assert!(t.run("skipped", |t| async move { t.skip("n/a") }).await);
            assert!(
                t.run(
                    "good",
                    |t| async move { assert_eq!(t.name(), "Parent/good") }
                )
                .await
            );
            a.fetch_add(1, Ordering::SeqCst);
        })
        .await;
        assert_eq!(after.load(Ordering::SeqCst), 1);
        assert_eq!(outcome, Outcome::Failed(None));
    }

    #[tokio::test]
    async fn temp_dirs_are_removed_at_the_end() {
        let kept = Arc::new(Mutex::new(PathBuf::new()));
        let k = kept.clone();
        run_test("Temp", |t| async move {
            let dir = t.temp_dir();
            assert!(dir.is_dir());
            *k.lock() = dir;
        })
        .await;
        assert!(!kept.lock().exists());
    }

    #[tokio::test]
    #[should_panic(expected = "Boom failed: bang")]
    async fn run_test_panics_on_failure() {
        run_test("Boom", |t| async move { t.fatal("bang") }).await;
    }
}
