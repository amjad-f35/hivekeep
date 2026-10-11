//! Exponential backoff with jitter for transient storage errors.

use std::future::Future;
use std::time::Duration;

use rand::Rng;

use crate::error::{Error, ErrorKind};

/// How long to wait between attempts: a random pause up to the current
/// ceiling, which grows by `multiplier` per attempt up to `max`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Backoff {
    /// The first ceiling.
    pub initial: Duration,
    /// The largest ceiling.
    pub max: Duration,
    /// Growth per attempt.
    pub multiplier: f64,
}

impl Backoff {
    /// A backoff starting at `initial`, doubling up to `max`.
    pub const fn new(initial: Duration, max: Duration) -> Self {
        Backoff {
            initial,
            max,
            multiplier: 2.0,
        }
    }

    /// The pauses this backoff takes, in order.
    pub fn pauses(&self) -> Pauses {
        Pauses {
            cur: self.initial,
            b: *self,
        }
    }
}

/// An iterator of backoff pauses.
#[derive(Debug, Clone)]
pub struct Pauses {
    cur: Duration,
    b: Backoff,
}

impl Pauses {
    /// The next pause.
    pub fn next_pause(&mut self) -> Duration {
        let ceiling = self.cur.as_nanos().max(1) as u64;
        let d = Duration::from_nanos(1 + rand::rng().random_range(0..ceiling));
        let next = self.cur.as_secs_f64() * self.b.multiplier;
        self.cur = Duration::from_secs_f64(next.min(self.b.max.as_secs_f64()));
        d
    }
}

/// When to retry.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    /// The pauses between attempts.
    pub backoff: Backoff,
    /// The most attempts, `None` for no cap: the caller then bounds the
    /// operation by dropping it.
    pub max_attempts: Option<u32>,
}

/// Whether an error is worth another attempt: 408, 429 and 5xx answers, and
/// requests that got no answer.
pub fn is_transient(err: &Error) -> bool {
    match err.kind() {
        ErrorKind::Service(r) => r.status == 408 || r.status == 429 || r.status >= 500,
        ErrorKind::Transport => true,
        _ => false,
    }
}

/// Runs `op` until it succeeds, fails permanently, or runs out of attempts.
pub async fn retry<T, F, Fut>(policy: RetryPolicy, mut op: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut pauses = policy.backoff.pauses();
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let more = policy.max_attempts.is_none_or(|m| attempt < m);
                if !more || !is_transient(&e) {
                    return Err(e);
                }
                tokio::time::sleep(pauses.next_pause()).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ServiceDetail, ServiceResponse};
    use std::sync::atomic::{AtomicU32, Ordering};

    fn status(code: u16) -> Error {
        Error::service(
            ServiceResponse {
                status: code,
                detail: ServiceDetail::Other,
            },
            format!("HTTP {code}"),
        )
    }

    #[tokio::test]
    async fn retries_transient_up_to_the_cap() {
        let policy = RetryPolicy {
            backoff: Backoff::new(Duration::from_millis(1), Duration::from_millis(1)),
            max_attempts: Some(3),
        };
        let n = AtomicU32::new(0);
        let r: Result<(), _> = retry(policy, || async {
            n.fetch_add(1, Ordering::SeqCst);
            Err(status(429))
        })
        .await;
        assert_eq!(r.unwrap_err().http_status(), Some(429));
        assert_eq!(n.load(Ordering::SeqCst), 3);
        n.store(0, Ordering::SeqCst);
        let r: Result<(), _> = retry(policy, || async {
            n.fetch_add(1, Ordering::SeqCst);
            Err(status(403))
        })
        .await;
        assert!(r.is_err());
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn pauses_stay_under_the_ceiling() {
        let mut p = Backoff::new(Duration::from_millis(250), Duration::from_secs(5)).pauses();
        for _ in 0..20 {
            assert!(p.next_pause() <= Duration::from_secs(5));
        }
    }
}
