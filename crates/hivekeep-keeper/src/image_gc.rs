//! The image cache's eviction loop, driven by the cache volume's usage
//! watermarks and an optional absolute cap.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::FutureExt;

use crate::context::Ctx;

/// Consecutive shortfalls that warn before backing off to reminders.
const SHORTFALL_WARN_LIMIT: u32 = 3;
/// How often a persistent shortfall is repeated.
const SHORTFALL_REMINDER_EVERY: u32 = 12;

pub use hivekeep_node::image_gc::{statfs, ImageEvictError, ImageEvictStats, ImageGcStore};

/// Image cache eviction settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageGcConfig {
    /// How often a pass runs; zero disables the loop.
    pub period: Duration,
    /// Volume usage percentage above which eviction starts.
    pub high_pct: u32,
    /// Volume usage percentage eviction frees down to.
    pub low_pct: u32,
    /// Absolute cap on the cache's size; zero means none.
    pub max_bytes: i64,
    /// Log decisions without deleting.
    pub dry_run: bool,
}

impl Default for ImageGcConfig {
    fn default() -> Self {
        ImageGcConfig {
            period: Duration::from_secs(300),
            high_pct: 85,
            low_pct: 80,
            max_bytes: 0,
            dry_run: false,
        }
    }
}

impl ImageGcConfig {
    /// Rejects values the loop could not act on.
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=100).contains(&self.high_pct) {
            return Err(format!(
                "--image-cache-high-percent {} out of range [1,100]",
                self.high_pct
            ));
        }
        if self.low_pct >= self.high_pct {
            return Err(format!(
                "--image-cache-low-percent {} must be in [0,{})",
                self.low_pct, self.high_pct
            ));
        }
        if self.max_bytes < 0 {
            return Err(format!(
                "--image-cache-max-bytes {} must be >= 0",
                self.max_bytes
            ));
        }
        Ok(())
    }
}

/// The bytes a pass should free: enough to bring the volume from above
/// `high_pct` down to `low_pct`, or the cache under `max_bytes`, whichever
/// is more, capped at the cache's size.
pub fn image_cache_gc_target(
    capacity: u64,
    available: u64,
    cache_size: i64,
    max_bytes: i64,
    high_pct: u32,
    low_pct: u32,
) -> i64 {
    let mut target: i64 = 0;
    if let Some(free_pct) = available.saturating_mul(100).checked_div(capacity) {
        let used_pct = 100 - free_pct as i64;
        if used_pct >= high_pct as i64 {
            target =
                (capacity as i64).saturating_mul(100 - low_pct as i64) / 100 - available as i64;
        }
    }
    if max_bytes > 0 && cache_size > max_bytes {
        target = target.max(cache_size - max_bytes);
    }
    target.min(cache_size).max(0)
}

/// How a pass ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImagePassOutcome {
    /// Nothing attempted.
    Skipped,
    /// Ran, short of target.
    Shortfall,
    /// Ran, target met.
    Complete,
    /// No target.
    Quiet,
}

/// Classifies a pass.
pub fn classify_pass(err: Option<&ImageEvictError>, target: i64, freed: i64) -> ImagePassOutcome {
    match err {
        Some(ImageEvictError::IncompleteEnumeration(_)) => ImagePassOutcome::Skipped,
        _ if target > 0 && freed < target => ImagePassOutcome::Shortfall,
        _ if target > 0 => ImagePassOutcome::Complete,
        _ => ImagePassOutcome::Quiet,
    }
}

/// What one pass did, for its log record.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct PassDetails {
    target: i64,
    stats: ImageEvictStats,
    cache_size: i64,
    dry_run: bool,
    took: Duration,
}

/// The image cache's eviction loop.
pub struct ImageCacheGc {
    store: Arc<dyn ImageGcStore>,
    dir: PathBuf,
    config: ImageGcConfig,
    shortfalls: u32,
}

impl ImageCacheGc {
    /// A loop over `store`, whose volume holds `dir`.
    pub fn new(store: Arc<dyn ImageGcStore>, dir: PathBuf, config: ImageGcConfig) -> Self {
        ImageCacheGc {
            store,
            dir,
            config,
            shortfalls: 0,
        }
    }

    /// Runs passes every period until `ctx` ends, the first at once.
    pub async fn run(mut self, ctx: Ctx) {
        loop {
            self.run_pass().await;
            if ctx.sleep(self.config.period).await.is_err() {
                return;
            }
        }
    }

    /// One pass; a panic is logged and the pass skipped.
    pub async fn run_pass(&mut self) -> ImagePassOutcome {
        match std::panic::AssertUnwindSafe(self.pass())
            .catch_unwind()
            .await
        {
            Ok(o) => o,
            Err(_) => {
                tracing::error!("Image cache GC pass panicked; skipping this pass");
                ImagePassOutcome::Skipped
            }
        }
    }

    async fn pass(&mut self) -> ImagePassOutcome {
        let (capacity, available) = match statfs(&self.dir) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(dir = %self.dir.display(), %err, "Image cache GC: statfs failed");
                return ImagePassOutcome::Skipped;
            }
        };
        let cache_size = match self.store.cache_size().await {
            Ok(s) => s,
            Err(err) => {
                tracing::warn!(%err, "Image cache GC: sizing the pool failed; skipping this pass");
                return ImagePassOutcome::Skipped;
            }
        };
        let c = self.config;
        let target = image_cache_gc_target(
            capacity,
            available,
            cache_size,
            c.max_bytes,
            c.high_pct,
            c.low_pct,
        );
        let started = Instant::now();
        let (stats, err) = self.store.evict_unused(target, c.dry_run).await;
        let outcome = classify_pass(err.as_ref(), target, stats.freed_bytes);
        let details = PassDetails {
            target,
            stats,
            cache_size,
            dry_run: c.dry_run,
            took: started.elapsed(),
        };
        self.note_outcome(outcome, err.as_ref(), &details);
        outcome
    }

    /// Logs a finished pass and tracks the shortfall streak: a shortfall
    /// warns for the first few passes, then only as a periodic reminder,
    /// since on a volume under someone else's pressure it is the steady
    /// state. A skipped pass keeps the streak; any other outcome resets it.
    pub(crate) fn note_outcome(
        &mut self,
        outcome: ImagePassOutcome,
        err: Option<&ImageEvictError>,
        d: &PassDetails,
    ) {
        let st = &d.stats;
        let took_ms = d.took.as_millis() as u64;
        if outcome == ImagePassOutcome::Skipped {
            tracing::error!(target_bytes = d.target, err = ?err, "Image cache GC pass skipped");
            return;
        }
        if let Some(e) = err {
            // Per-item failures; each retries next pass.
            tracing::warn!(target_bytes = d.target, err = ?e, "Image cache GC pass finished with errors");
        }
        match outcome {
            ImagePassOutcome::Shortfall => {
                self.shortfalls += 1;
                let n = self.shortfalls;
                if n <= SHORTFALL_WARN_LIMIT {
                    tracing::warn!(
                        target_bytes = d.target,
                        freed_bytes = st.freed_bytes,
                        evicted_images = st.evicted_images,
                        evicted_layers = st.evicted_layers,
                        candidates = st.candidates,
                        rooted_images = st.rooted_images,
                        orphan_layers = st.orphan_layers,
                        skipped_rooted = st.skipped_rooted,
                        skipped_fresh = st.skipped_fresh,
                        cache_size_bytes = d.cache_size,
                        dry_run = d.dry_run,
                        took_ms,
                        consecutive = n,
                        "Image cache GC could not reach target"
                    );
                } else if n.is_multiple_of(SHORTFALL_REMINDER_EVERY) {
                    tracing::warn!(
                        target_bytes = d.target,
                        freed_bytes = st.freed_bytes,
                        cache_size_bytes = d.cache_size,
                        consecutive = n,
                        "Image cache GC still short of target; the remaining pressure is not the image cache's to free"
                    );
                }
            }
            ImagePassOutcome::Complete => {
                self.shortfalls = 0;
                tracing::info!(
                    target_bytes = d.target,
                    freed_bytes = st.freed_bytes,
                    evicted_images = st.evicted_images,
                    evicted_layers = st.evicted_layers,
                    dry_run = d.dry_run,
                    took_ms,
                    "Image cache GC pass complete"
                );
            }
            _ => self.shortfalls = 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    #[test]
    fn targets() {
        // 90% used, low water 80%: free 10% of capacity.
        assert_eq!(image_cache_gc_target(1000, 100, 500, 0, 85, 80), 100);
        // Below the high watermark: nothing.
        assert_eq!(image_cache_gc_target(1000, 500, 500, 0, 85, 80), 0);
        // The absolute cap wins when larger.
        assert_eq!(image_cache_gc_target(1000, 500, 500, 200, 85, 80), 300);
        // Never more than the cache holds.
        assert_eq!(image_cache_gc_target(1000, 0, 50, 0, 85, 80), 50);
        assert_eq!(image_cache_gc_target(0, 0, 0, 0, 85, 80), 0);
    }

    #[test]
    fn outcomes() {
        assert_eq!(
            classify_pass(
                Some(&ImageEvictError::IncompleteEnumeration("x".into())),
                10,
                0
            ),
            ImagePassOutcome::Skipped
        );
        assert_eq!(classify_pass(None, 10, 5), ImagePassOutcome::Shortfall);
        assert_eq!(
            classify_pass(Some(&ImageEvictError::Partial("x".into())), 10, 10),
            ImagePassOutcome::Complete
        );
        assert_eq!(classify_pass(None, 0, 0), ImagePassOutcome::Quiet);
    }

    #[test]
    fn config_validation() {
        assert!(ImageGcConfig::default().validate().is_ok());
        assert!(ImageGcConfig {
            high_pct: 0,
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(ImageGcConfig {
            low_pct: 90,
            ..Default::default()
        }
        .validate()
        .is_err());
    }

    #[test]
    fn statfs_reads_the_temp_volume() {
        let d = tempfile::tempdir().unwrap();
        let (cap, avail) = statfs(d.path()).unwrap();
        assert!(cap > 0 && avail <= cap);
    }

    struct Fake;
    #[async_trait]
    impl ImageGcStore for Fake {
        async fn cache_size(&self) -> Result<i64, String> {
            Ok(10)
        }
        async fn evict_unused(
            &self,
            _: i64,
            _: bool,
        ) -> (ImageEvictStats, Option<ImageEvictError>) {
            (ImageEvictStats::default(), None)
        }
    }

    #[tokio::test]
    async fn a_pass_with_a_cap() {
        let d = tempfile::tempdir().unwrap();
        let mut gc = ImageCacheGc::new(
            Arc::new(Fake),
            d.path().to_path_buf(),
            ImageGcConfig {
                max_bytes: 1,
                high_pct: 100,
                low_pct: 99,
                ..Default::default()
            },
        );
        assert_eq!(gc.run_pass().await, ImagePassOutcome::Shortfall);
    }
}

#[cfg(test)]
mod behavior_tests;
