//! Paces calls to a target rate with a burst of one: each caller gets the
//! next evenly spaced slot instead of a second's budget at once.

use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;

/// An evenly spacing rate limiter whose rate can change at any time.
#[derive(Debug)]
pub struct Pacer {
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    /// Calls per second; `None` is unlimited.
    rate: Option<f64>,
    /// The earliest instant the next slot may start.
    next: Instant,
}

impl Default for Pacer {
    fn default() -> Self {
        Pacer {
            state: Mutex::new(State {
                rate: None,
                next: Instant::now(),
            }),
        }
    }
}

impl Pacer {
    /// Sets the rate; `None` (or a non-positive rate) is unlimited.
    pub fn set_rate(&self, rate: Option<f64>) {
        let rate = rate.filter(|r| *r > 0.0);
        let mut s = self.state.lock();
        if s.rate != rate {
            s.rate = rate;
        }
    }

    /// Reserves the next slot and returns when it starts.
    pub fn reserve(&self) -> Instant {
        let now = Instant::now();
        let mut s = self.state.lock();
        let Some(rate) = s.rate else {
            s.next = now;
            return now;
        };
        let slot = s.next.max(now);
        s.next = slot + Duration::from_secs_f64(1.0 / rate);
        slot
    }

    /// Waits for the next slot.
    pub async fn wait(&self) {
        tokio::time::sleep_until(self.reserve()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn spaces_slots_evenly() {
        let p = Pacer::default();
        let start = Instant::now();
        p.wait().await;
        assert_eq!(start.elapsed(), Duration::ZERO, "unlimited goes at once");
        p.set_rate(Some(10.0));
        for _ in 0..3 {
            p.wait().await;
        }
        assert_eq!(
            start.elapsed(),
            Duration::from_millis(200),
            "first slot at once, then 100ms each"
        );
        p.set_rate(None);
        let t = Instant::now();
        p.wait().await;
        assert_eq!(t.elapsed(), Duration::ZERO);
    }
}
