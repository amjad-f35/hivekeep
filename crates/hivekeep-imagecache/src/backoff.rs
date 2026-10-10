//! Per-registry retry backoff for pulls.
//!
//! Every request of a pull (the `/v2/` ping, token fetches, manifests,
//! blobs) retries transient failures with a backoff chosen by registry.
//! Registries where the deployment has its own quota retry fast: pulls sit
//! on the Run/Restore critical path, and nobody else competes for the
//! limit. Communal registries with anonymous, shared rate limits retry
//! patiently with full jitter, so a throttled fleet spreads out instead of
//! retrying in lockstep. The caller's context still bounds the total.

use std::time::Duration;

/// Exponential backoff parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Backoff {
    /// The first wait.
    pub duration: Duration,
    /// Each wait's multiplier over the previous one.
    pub factor: f64,
    /// Random spread, as a fraction of the wait (1.0 = full jitter).
    pub jitter: f64,
    /// Attempts in total, the first included.
    pub steps: u32,
    /// The longest single wait; zero means uncapped.
    pub cap: Duration,
}

impl Backoff {
    /// The wait before retry number `attempt` (0-based), jitter applied.
    pub fn delay(&self, attempt: u32) -> Duration {
        let mut d = self.duration.as_secs_f64() * self.factor.powi(attempt as i32);
        if !self.cap.is_zero() {
            d = d.min(self.cap.as_secs_f64());
        }
        if self.jitter > 0.0 {
            d += rand::random::<f64>() * self.jitter * d;
        }
        Duration::from_secs_f64(d)
    }
}

/// Backoff for registries the deployment has its own quota with.
pub const DEDICATED_REGISTRY_BACKOFF: Backoff = Backoff {
    duration: Duration::from_millis(200),
    factor: 2.0,
    jitter: 1.0,
    steps: 4,
    cap: Duration::from_secs(2),
};

/// Backoff for communal registries with shared anonymous rate limits.
pub const SHARED_REGISTRY_BACKOFF: Backoff = Backoff {
    duration: Duration::from_secs(1),
    factor: 2.0,
    jitter: 1.0,
    steps: 4,
    cap: Duration::from_secs(10),
};

/// The well-known communal registries. Everything else is assumed
/// dedicated: fleet-scale pulls realistically hit the deployment's own
/// registry.
pub const SHARED_REGISTRIES: &[&str] = &[
    "registry.k8s.io",
    // References normalize docker.io to index.docker.io; both are listed so
    // either spelling classifies the same.
    "docker.io",
    "index.docker.io",
    "registry-1.docker.io",
    "quay.io",
    "ghcr.io",
    "public.ecr.aws",
    "mcr.microsoft.com",
    "registry.gitlab.com",
    "cgr.dev",
];

/// The retry backoff for pulls from `registry` (host and optional port).
pub fn retry_backoff_for(registry: &str) -> Backoff {
    if SHARED_REGISTRIES.contains(&registry) {
        SHARED_REGISTRY_BACKOFF
    } else {
        DEDICATED_REGISTRY_BACKOFF
    }
}

/// Whether a registry response status is worth retrying.
pub fn retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_for_splits_registries() {
        for (reg, want) in [
            ("gcr.io", DEDICATED_REGISTRY_BACKOFF),
            ("us.gcr.io", DEDICATED_REGISTRY_BACKOFF),
            ("us-docker.pkg.dev", DEDICATED_REGISTRY_BACKOFF),
            (
                "123.dkr.ecr.us-east-1.amazonaws.com",
                DEDICATED_REGISTRY_BACKOFF,
            ),
            ("harbor.example.com", DEDICATED_REGISTRY_BACKOFF),
            ("127.0.0.1:5000", DEDICATED_REGISTRY_BACKOFF),
            ("registry.k8s.io", SHARED_REGISTRY_BACKOFF),
            ("docker.io", SHARED_REGISTRY_BACKOFF),
            ("index.docker.io", SHARED_REGISTRY_BACKOFF),
            ("quay.io", SHARED_REGISTRY_BACKOFF),
            ("ghcr.io", SHARED_REGISTRY_BACKOFF),
        ] {
            assert_eq!(retry_backoff_for(reg), want, "{reg}");
        }
    }

    #[test]
    fn delay_grows_and_caps() {
        let b = Backoff {
            jitter: 0.0,
            ..DEDICATED_REGISTRY_BACKOFF
        };
        assert_eq!(b.delay(0), Duration::from_millis(200));
        assert_eq!(b.delay(1), Duration::from_millis(400));
        assert_eq!(b.delay(10), Duration::from_secs(2));
        let j = DEDICATED_REGISTRY_BACKOFF.delay(0);
        assert!(j >= Duration::from_millis(200) && j <= Duration::from_millis(400));
    }
}
