//! An open-loop pacer: units of work issued at a fixed rate whatever each
//! one takes. Each unit carries the instant it was due and latency is
//! measured from then, so a generator falling behind shows it as latency
//! and lag instead of quietly offering less load.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::report::Percentiles;

/// One scheduled piece of work.
#[derive(Debug, Clone, Copy)]
pub struct Unit {
    pub due: Instant,
    /// False during warmup.
    pub measured: bool,
}

/// Collected durations. Runs are short enough to keep every sample, so the
/// percentiles are exact.
#[derive(Debug, Default)]
pub struct Samples(Mutex<Vec<Duration>>);

impl Samples {
    pub fn record(&self, d: Duration) {
        self.0.lock().push(d);
    }

    /// The distribution so far, in milliseconds; drains the samples.
    pub fn percentiles(&self) -> Percentiles {
        let mut d = std::mem::take(&mut *self.0.lock());
        if d.is_empty() {
            return Percentiles::default();
        }
        d.sort_unstable();
        let sum: Duration = d.iter().sum();
        let at = |q: f64| ms(d[(d.len() - 1).min((q * d.len() as f64) as usize)]);
        Percentiles {
            p50: at(0.50),
            p90: at(0.90),
            p99: at(0.99),
            max: ms(d[d.len() - 1]),
            mean: ms(sum / d.len() as u32),
        }
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

/// Issues units at `rate` per second for `duration`.
#[derive(Debug)]
pub struct Pacer {
    pub rate: f64,
    pub duration: Duration,
    pub warmup: Duration,
    pub workers: usize,
    /// Bounds units queued for a free worker; beyond it they are skipped
    /// and counted.
    pub max_pending: usize,
    pub scheduled: AtomicU64,
    pub skipped: AtomicU64,
    pub lag: Samples,
}

impl Pacer {
    pub fn new(
        rate: f64,
        duration: Duration,
        warmup: Duration,
        workers: usize,
        max_pending: usize,
    ) -> Arc<Pacer> {
        Arc::new(Pacer {
            rate,
            duration,
            warmup,
            workers,
            max_pending,
            scheduled: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            lag: Samples::default(),
        })
    }

    /// Calls `work` for every unit until the duration is over or `cancel`,
    /// with at most `workers` in flight, and waits for them to finish.
    pub async fn run<F, Fut>(self: &Arc<Self>, cancel: &CancellationToken, work: F)
    where
        F: Fn(Unit) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if self.rate <= 0.0 {
            return;
        }
        let (tx, rx) = tokio::sync::mpsc::channel::<Unit>(self.max_pending.max(1));
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let work = Arc::new(work);
        let mut workers = Vec::with_capacity(self.workers);
        for _ in 0..self.workers.max(1) {
            let (rx, work, me, cancel) = (rx.clone(), work.clone(), self.clone(), cancel.clone());
            workers.push(tokio::spawn(async move {
                loop {
                    let next = rx.lock().await.recv().await;
                    let Some(u) = next else { return };
                    if u.measured {
                        me.lag.record(u.due.elapsed());
                    }
                    tokio::select! {
                        _ = work(u) => {}
                        _ = cancel.cancelled() => {}
                    }
                }
            }));
        }

        let start = Instant::now();
        let interval = Duration::from_secs_f64(1.0 / self.rate);
        for i in 0u32.. {
            let offset = interval * i;
            if offset >= self.duration {
                break;
            }
            let due = start + offset;
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep_until(due) => {}
            }
            self.scheduled.fetch_add(1, Ordering::Relaxed);
            if tx
                .try_send(Unit {
                    due,
                    measured: offset >= self.warmup,
                })
                .is_err()
            {
                self.skipped.fetch_add(1, Ordering::Relaxed);
            }
        }
        drop(tx);
        for w in workers {
            let _ = w.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn offers_the_rate_whatever_the_work() {
        let p = Pacer::new(
            200.0,
            Duration::from_secs(1),
            Duration::from_millis(200),
            4,
            1000,
        );
        let measured = Arc::new(AtomicU64::new(0));
        let m = measured.clone();
        p.run(&CancellationToken::new(), move |u| {
            let m = m.clone();
            async move {
                if u.measured {
                    m.fetch_add(1, Ordering::Relaxed);
                }
                // Slower than the interval for one worker; four keep up.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert_eq!(p.scheduled.load(Ordering::Relaxed), 200);
        assert_eq!(measured.load(Ordering::Relaxed), 160, "after a 20% warmup");
        assert_eq!(
            p.skipped.load(Ordering::Relaxed),
            0,
            "skipped units with room to queue"
        );
    }

    #[tokio::test]
    async fn skips_what_cannot_queue() {
        let p = Pacer::new(1000.0, Duration::from_millis(200), Duration::ZERO, 1, 1);
        p.run(&CancellationToken::new(), |_| {
            tokio::time::sleep(Duration::from_millis(50))
        })
        .await;
        assert!(
            p.skipped.load(Ordering::Relaxed) > 0,
            "nothing skipped though one worker cannot keep up and the queue holds one"
        );
    }

    #[test]
    fn percentiles() {
        let s = Samples::default();
        for i in 1..=100 {
            s.record(Duration::from_millis(i));
        }
        let got = s.percentiles();
        let want = Percentiles {
            p50: 51.0,
            p90: 91.0,
            p99: 100.0,
            max: 100.0,
            mean: 50.5,
        };
        assert_eq!(got, want);
        assert_eq!(s.percentiles(), Percentiles::default(), "after draining");
    }
}
