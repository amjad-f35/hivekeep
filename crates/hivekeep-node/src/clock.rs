//! Wall-clock time for due times that are saved to disk.
//!
//! Background snapshot work is scheduled by wall-clock times persisted in
//! each actor's state file, so the schedule survives a restart. Waiting is
//! done with tokio timers on the durations those times imply, so a test
//! that pauses tokio time drives both through [`MockClock`].

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};

/// A source of the current wall-clock time.
pub trait Clock: Send + Sync + 'static {
    /// The current time.
    fn now(&self) -> DateTime<Utc>;
}

/// A shared clock.
pub type SharedClock = Arc<dyn Clock>;

/// The system's real-time clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A clock that follows tokio's time: `now` is a fixed start plus the
/// tokio time elapsed since the clock was made. Under a paused tokio
/// runtime it advances only with `tokio::time::advance`, together with
/// every timer.
#[derive(Debug, Clone)]
pub struct MockClock {
    start: DateTime<Utc>,
    origin: tokio::time::Instant,
}

impl MockClock {
    /// A clock reading `start` now.
    pub fn new(start: DateTime<Utc>) -> Self {
        MockClock {
            start,
            origin: tokio::time::Instant::now(),
        }
    }
}

impl Clock for MockClock {
    fn now(&self) -> DateTime<Utc> {
        let elapsed = tokio::time::Instant::now().saturating_duration_since(self.origin);
        self.start + chrono::Duration::from_std(elapsed).unwrap_or(chrono::Duration::MAX)
    }
}

/// The time from `now` until `at`, or zero once `at` has passed.
pub fn until(now: DateTime<Utc>, at: DateTime<Utc>) -> Duration {
    (at - now).to_std().unwrap_or(Duration::ZERO)
}

/// `t` plus `d`, saturating at the largest representable time.
pub fn add(t: DateTime<Utc>, d: Duration) -> DateTime<Utc> {
    chrono::Duration::from_std(d)
        .ok()
        .and_then(|d| t.checked_add_signed(d))
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn mock_clock_follows_tokio_time() {
        let start = DateTime::from_timestamp(1_000_000, 0).unwrap();
        let clock = MockClock::new(start);
        assert_eq!(clock.now(), start);
        tokio::time::advance(Duration::from_secs(90)).await;
        assert_eq!(clock.now(), start + chrono::Duration::seconds(90));
    }

    #[test]
    fn until_never_negative() {
        let t = DateTime::from_timestamp(100, 0).unwrap();
        assert_eq!(until(t, t - chrono::Duration::seconds(5)), Duration::ZERO);
        assert_eq!(
            until(t, add(t, Duration::from_secs(3))),
            Duration::from_secs(3)
        );
    }
}
