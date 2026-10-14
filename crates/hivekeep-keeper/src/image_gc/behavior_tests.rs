//! Targets, flag validation, pass gating and shortfall log backoff of the
//! image cache GC loop.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use super::*;
use crate::node_svc_test_support::LogCapture;
use async_trait::async_trait;

const GIB: i64 = 1 << 30;

#[test]
fn image_cache_gc_target_cases() {
    let g = GIB as u64;
    // (name, capacity, available, cache_size, max_bytes, high, low, want)
    type Case = (&'static str, u64, u64, i64, i64, u32, u32, i64);
    let cases: [Case; 9] = [
        (
            "below high watermark: no target",
            100 * g,
            30 * g,
            0,
            0,
            85,
            80,
            0,
        ),
        (
            "at high watermark: free down to low",
            100 * g,
            10 * g,
            50 * GIB,
            0,
            85,
            80,
            10 * GIB,
        ),
        (
            "exactly high watermark triggers",
            100 * g,
            15 * g,
            50 * GIB,
            0,
            85,
            80,
            5 * GIB,
        ),
        // The kubelet's formula assumes it owns the volume; the cache does
        // not, so the target never exceeds what the cache holds.
        (
            "watermark target capped at what the cache holds",
            105 * g,
            2 * g,
            11 << 20,
            0,
            85,
            80,
            11 << 20,
        ),
        (
            "empty cache under volume pressure: nothing to free",
            100 * g,
            g,
            0,
            0,
            85,
            80,
            0,
        ),
        (
            "max-bytes cap independent of watermarks",
            100 * g,
            90 * g,
            8 * GIB,
            5 * GIB,
            85,
            80,
            3 * GIB,
        ),
        (
            "both: larger target wins",
            100 * g,
            10 * g,
            60 * GIB,
            40 * GIB,
            85,
            80,
            20 * GIB,
        ),
        (
            "max-bytes zero means no cap",
            100 * g,
            90 * g,
            500 * GIB,
            0,
            85,
            80,
            0,
        ),
        (
            "zero capacity: watermark half disabled",
            0,
            0,
            2 * GIB,
            GIB,
            85,
            80,
            GIB,
        ),
    ];
    for (name, cap, avail, size, max, high, low, want) in cases {
        assert_eq!(
            image_cache_gc_target(cap, avail, size, max, high, low),
            want,
            "{name}"
        );
    }
}

#[test]
fn validate_image_cache_gc_flags() {
    // Negative periods, low percents and min ages are unrepresentable here
    // (unsigned types, rejected by flag parsing), so only the rest remain.
    let five_min = Duration::from_secs(300);
    let cases = [
        ("defaults", five_min, 85, 80, false),
        ("boundary high=100 low=0", five_min, 100, 0, false),
        (
            "zero period disables the periodic pass",
            Duration::ZERO,
            85,
            80,
            false,
        ),
        ("high over 100", five_min, 101, 80, true),
        ("high zero", five_min, 0, 0, true),
        ("low equals high", five_min, 85, 85, true),
        ("low above high", five_min, 85, 90, true),
    ];
    for (name, period, high, low, want_err) in cases {
        let c = ImageGcConfig {
            period,
            high_pct: high,
            low_pct: low,
            ..Default::default()
        };
        assert_eq!(c.validate().is_err(), want_err, "{name}");
    }
}

#[derive(Default)]
struct FakeStore {
    size: i64,
    size_err: bool,
    size_calls: AtomicUsize,
    evict_calls: AtomicUsize,
    got_target: AtomicI64,
    got_dry_run: AtomicBool,
    freed: i64,
    panic_on_evict: bool,
}

#[async_trait]
impl ImageGcStore for FakeStore {
    async fn cache_size(&self) -> Result<i64, String> {
        self.size_calls.fetch_add(1, Ordering::SeqCst);
        if self.size_err {
            return Err("unreadable size file".into());
        }
        Ok(self.size)
    }
    async fn evict_unused(
        &self,
        target: i64,
        dry_run: bool,
    ) -> (ImageEvictStats, Option<ImageEvictError>) {
        self.evict_calls.fetch_add(1, Ordering::SeqCst);
        self.got_target.store(target, Ordering::SeqCst);
        self.got_dry_run.store(dry_run, Ordering::SeqCst);
        if self.panic_on_evict {
            panic!("boom");
        }
        (
            ImageEvictStats {
                freed_bytes: self.freed,
                ..Default::default()
            },
            None,
        )
    }
}

fn gc(store: Arc<FakeStore>, dir: PathBuf, config: ImageGcConfig) -> ImageCacheGc {
    ImageCacheGc::new(store, dir, config)
}

fn sidelined() -> ImageGcConfig {
    // high=100 keeps the watermark out of the way on any volume with free
    // space.
    ImageGcConfig {
        high_pct: 100,
        low_pct: 0,
        ..Default::default()
    }
}

#[tokio::test]
async fn run_pass_skips_on_statfs_failure() {
    let d = tempfile::tempdir().unwrap();
    let store = Arc::new(FakeStore::default());
    let mut g = gc(
        store.clone(),
        d.path().join("missing"),
        ImageGcConfig::default(),
    );
    assert_eq!(g.run_pass().await, ImagePassOutcome::Skipped);
    assert_eq!(store.size_calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.evict_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn run_pass_skips_on_cache_size_failure() {
    let d = tempfile::tempdir().unwrap();
    let store = Arc::new(FakeStore {
        size_err: true,
        ..Default::default()
    });
    let mut g = gc(store.clone(), d.path().into(), ImageGcConfig::default());
    g.run_pass().await;
    assert_eq!(store.evict_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn run_pass_evicts_and_passes_dry_run() {
    let d = tempfile::tempdir().unwrap();
    let store = Arc::new(FakeStore {
        size: 100,
        freed: 100,
        ..Default::default()
    });
    let mut g = gc(
        store.clone(),
        d.path().into(),
        ImageGcConfig {
            max_bytes: 1,
            dry_run: true,
            ..sidelined()
        },
    );
    g.shortfalls = 5; // a met target resets it
    g.run_pass().await;
    assert_eq!(store.evict_calls.load(Ordering::SeqCst), 1);
    // The cap overage is 99; a near-full volume may lift it to the size.
    assert!(store.got_target.load(Ordering::SeqCst) >= 99);
    assert!(store.got_dry_run.load(Ordering::SeqCst));
    assert_eq!(g.shortfalls, 0);
}

#[tokio::test]
async fn run_first_pass_is_immediate() {
    // A cancelled context and an hour-long period: the one pass can only
    // be the immediate first one.
    let d = tempfile::tempdir().unwrap();
    let store = Arc::new(FakeStore::default());
    let g = gc(
        store.clone(),
        d.path().into(),
        ImageGcConfig {
            period: Duration::from_secs(3600),
            ..sidelined()
        },
    );
    let ctx = Ctx::background();
    ctx.cancel();
    g.run(ctx).await;
    assert_eq!(store.evict_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn run_ticks() {
    let d = tempfile::tempdir().unwrap();
    let store = Arc::new(FakeStore::default());
    let g = gc(
        store.clone(),
        d.path().into(),
        ImageGcConfig {
            period: Duration::from_millis(10),
            ..sidelined()
        },
    );
    g.run(Ctx::background().with_timeout(Duration::from_millis(150)))
        .await;
    assert!(store.evict_calls.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn run_pass_recovers_panic() {
    let d = tempfile::tempdir().unwrap();
    let store = Arc::new(FakeStore {
        panic_on_evict: true,
        ..Default::default()
    });
    let mut g = gc(store, d.path().into(), sidelined());
    assert_eq!(g.run_pass().await, ImagePassOutcome::Skipped);
}

#[test]
fn note_outcome_shortfall_backoff() {
    let logs = LogCapture::start();
    let d = PassDetails::default();
    let mut g = gc(
        Arc::new(FakeStore::default()),
        PathBuf::new(),
        ImageGcConfig::default(),
    );

    for _ in 0..SHORTFALL_WARN_LIMIT {
        g.note_outcome(ImagePassOutcome::Shortfall, None, &d);
    }
    assert_eq!(
        logs.count("could not reach target"),
        SHORTFALL_WARN_LIMIT as usize
    );

    logs.reset();
    while g.shortfalls < 2 * SHORTFALL_REMINDER_EVERY {
        g.note_outcome(ImagePassOutcome::Shortfall, None, &d);
    }
    assert_eq!(logs.count("still short of target"), 2);
    assert_eq!(logs.count("could not reach target"), 0);

    let streak = g.shortfalls;
    logs.reset();
    let gated = ImageEvictError::IncompleteEnumeration("gated".into());
    g.note_outcome(ImagePassOutcome::Skipped, Some(&gated), &d);
    assert_eq!(g.shortfalls, streak, "a gated pass keeps the streak");
    assert_eq!(logs.count("Image cache GC pass skipped"), 1);

    logs.reset();
    let partial = ImageEvictError::Partial("one dir failed".into());
    g.note_outcome(ImagePassOutcome::Complete, Some(&partial), &d);
    assert_eq!(g.shortfalls, 0);
    assert_eq!(logs.count("pass complete"), 1);
    assert_eq!(logs.count("finished with errors"), 1);

    logs.reset();
    g.note_outcome(ImagePassOutcome::Shortfall, None, &d);
    assert_eq!(logs.count("could not reach target"), 1, "re-armed");

    g.shortfalls = SHORTFALL_WARN_LIMIT + 1;
    logs.reset();
    g.note_outcome(ImagePassOutcome::Quiet, None, &d);
    assert_eq!(g.shortfalls, 0);
    assert_eq!(logs.text(), "", "a quiet pass is silent");
}

#[test]
fn classify_gc_pass_cases() {
    let gated = ImageEvictError::IncompleteEnumeration("pass gated".into());
    let per_item =
        ImageEvictError::Partial("while removing retired layer: permission denied".into());
    let cases = [
        (
            "gated pass",
            Some(&gated),
            100,
            0,
            ImagePassOutcome::Skipped,
        ),
        (
            "gated wins even with zero target",
            Some(&gated),
            0,
            0,
            ImagePassOutcome::Skipped,
        ),
        (
            "per-item errors are not a skip",
            Some(&per_item),
            100,
            100,
            ImagePassOutcome::Complete,
        ),
        (
            "per-item errors with shortfall",
            Some(&per_item),
            100,
            40,
            ImagePassOutcome::Shortfall,
        ),
        ("shortfall", None, 100, 40, ImagePassOutcome::Shortfall),
        ("target met", None, 100, 100, ImagePassOutcome::Complete),
        ("no target", None, 0, 0, ImagePassOutcome::Quiet),
    ];
    for (name, err, target, freed, want) in cases {
        assert_eq!(classify_pass(err, target, freed), want, "{name}");
    }
}
