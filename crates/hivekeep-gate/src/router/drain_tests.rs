use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::*;

/// A drainer running a closure.
struct FnDrainer<F>(F);

#[async_trait]
impl<F> DataplaneDrainer for FnDrainer<F>
where
    F: Fn() -> Result<(), String> + Send + Sync,
{
    async fn drain(&self, _window: Duration) -> Result<(), String> {
        (self.0)()
    }
}

/// A stopper whose graceful stop blocks until `release` fires, like an
/// in-flight (parked) ext_proc stream.
#[derive(Default)]
struct FakeStopper {
    release: CancellationToken,
    graceful_called: AtomicBool,
    stop_called: AtomicBool,
}

#[async_trait]
impl GrpcStopper for FakeStopper {
    async fn graceful_stop(&self) {
        self.graceful_called.store(true, Ordering::SeqCst);
        self.release.cancelled().await;
    }
    fn stop(&self) {
        self.stop_called.store(true, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct OrderRecorder(Mutex<Vec<&'static str>>);

impl OrderRecorder {
    fn add(&self, step: &'static str) {
        self.0.lock().push(step);
    }
    fn get(&self) -> Vec<&'static str> {
        self.0.lock().clone()
    }
}

// The full sequence runs in the forced order, and an in-flight stream
// releasing promptly lets the drain finish without a force stop.
#[tokio::test]
async fn drain_on_shutdown_ordering() {
    let rec = Arc::new(OrderRecorder::default());
    let stopper = Arc::new(FakeStopper::default());
    let readiness = Arc::new(Readiness::default());
    readiness.mark_ready();

    tokio::spawn({
        let (stopper, rec) = (stopper.clone(), rec.clone());
        async move {
            while !stopper.graceful_called.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            rec.add("extproc-drain-started");
            tokio::time::sleep(Duration::from_millis(20)).await;
            stopper.release.cancel();
        }
    });

    let shutdown = CancellationToken::new();
    let (r, rec_dp) = (readiness.clone(), rec.clone());
    let (st, rec_rest) = (stopper.clone(), rec.clone());
    let done = drain_on_shutdown(
        shutdown.clone(),
        DrainParams {
            readiness: readiness.clone(),
            delay: Duration::ZERO,
            dataplane: Some(Arc::new(FnDrainer(move || {
                assert!(!r.ready(), "dataplane drain ran before the readiness flip");
                rec_dp.add("dataplane");
                Ok(())
            }))),
            dataplane_window: Duration::from_secs(1),
            extproc: stopper.clone(),
            timeout: Duration::from_secs(5),
            stop_rest: Box::new(move || {
                assert!(
                    st.graceful_called.load(Ordering::SeqCst),
                    "stop_rest ran before the ext_proc drain"
                );
                rec_rest.add("stopRest");
            }),
        },
    );
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), done)
        .await
        .expect("drain did not complete")
        .unwrap();

    assert_eq!(
        rec.get(),
        ["dataplane", "extproc-drain-started", "stopRest"]
    );
    assert!(
        !stopper.stop_called.load(Ordering::SeqCst),
        "force stop fired although the graceful drain completed in time"
    );
    assert!(
        !readiness.ready(),
        "readiness should be not-ready after drain"
    );
}

// A stream that never finishes is force-stopped at the timeout and the
// sequence still completes.
#[tokio::test]
async fn drain_on_shutdown_force_stops_after_timeout() {
    let stopper = Arc::new(FakeStopper::default());
    let stop_rest = Arc::new(AtomicBool::new(false));
    let shutdown = CancellationToken::new();
    let start = Instant::now();
    let sr = stop_rest.clone();
    let done = drain_on_shutdown(
        shutdown.clone(),
        DrainParams {
            readiness: Arc::new(Readiness::default()),
            delay: Duration::ZERO,
            dataplane: None, // the dataplane offers no drain hook
            dataplane_window: Duration::from_secs(1),
            extproc: stopper.clone(),
            timeout: Duration::from_millis(100),
            stop_rest: Box::new(move || sr.store(true, Ordering::SeqCst)),
        },
    );
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), done)
        .await
        .expect("drain did not force-stop within deadline")
        .unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "force stop took {:?}",
        start.elapsed()
    );
    assert!(
        stopper.stop_called.load(Ordering::SeqCst),
        "force stop was not called for the wedged stream"
    );
    assert!(
        stop_rest.load(Ordering::SeqCst),
        "stop_rest did not run after the force stop"
    );
}

// An incomplete dataplane drain does not wedge the sequence.
#[tokio::test]
async fn drain_on_shutdown_dataplane_failure_continues() {
    let stopper = Arc::new(FakeStopper::default());
    stopper.release.cancel(); // ext_proc idle
    let shutdown = CancellationToken::new();
    let done = drain_on_shutdown(
        shutdown.clone(),
        DrainParams {
            readiness: Arc::new(Readiness::default()),
            delay: Duration::ZERO,
            dataplane: Some(Arc::new(FnDrainer(|| {
                Err("context deadline exceeded".to_string())
            }))),
            dataplane_window: Duration::from_millis(50),
            extproc: stopper.clone(),
            timeout: Duration::from_secs(1),
            stop_rest: Box::new(|| {}),
        },
    );
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), done)
        .await
        .expect("drain wedged on a failed dataplane drain")
        .unwrap();
    assert!(
        stopper.graceful_called.load(Ordering::SeqCst),
        "ext_proc drain skipped after a dataplane drain failure"
    );
}

// The preStop handshake marker: removed at startup, created on drain
// completion, and an empty path disables both.
#[test]
fn drain_marker_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("nested/drain-complete");
    let marker = marker.to_str().unwrap();

    remove_stale_drain_marker("");
    write_drain_marker("");

    remove_stale_drain_marker(marker); // absent: quiet

    write_drain_marker(marker);
    let body = std::fs::read_to_string(marker).expect("marker not created");
    assert!(
        body.ends_with('\n') && chrono::DateTime::parse_from_rfc3339(body.trim()).is_ok(),
        "marker = {body:?}"
    );

    remove_stale_drain_marker(marker);
    assert!(
        !std::path::Path::new(marker).exists(),
        "stale marker not removed"
    );
}

// A served task stops gracefully once its in-flight work finishes, and a
// wedged one is aborted by a force stop.
#[tokio::test]
async fn serve_handle_graceful_and_forced_stop() {
    let h = ServeHandle::spawn(|shutdown| async move {
        shutdown.await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok::<_, String>(())
    });
    assert!(!h.is_finished());
    tokio::time::timeout(Duration::from_secs(1), h.graceful_stop())
        .await
        .expect("graceful stop did not finish");
    assert!(h.is_finished());

    let wedged = ServeHandle::spawn(|_shutdown| async move {
        futures::future::pending::<()>().await;
        Ok::<_, String>(())
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), wedged.graceful_stop())
            .await
            .is_err()
    );
    wedged.stop();
    // Aborted: the graceful wait now resolves too.
    tokio::time::timeout(Duration::from_secs(1), wedged.graceful_stop())
        .await
        .expect("aborted server still reported running");
}
