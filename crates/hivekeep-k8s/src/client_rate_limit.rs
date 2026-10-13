//! Client-side rate limiting of Kubernetes API requests: a token bucket
//! allowing `qps` requests per second on average, in bursts of up to
//! `burst`, applied as a tower layer under the kube client so every
//! request it sends waits its turn.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::{Instant, Sleep};
use tower::{Layer, Service};

/// A token bucket. Reservations are taken in arrival order and may run the
/// balance negative; each one waits until its token has accrued.
#[derive(Debug)]
pub struct TokenBucket {
    qps: f64,
    burst: f64,
    state: Mutex<(f64, Instant)>,
}

impl TokenBucket {
    /// A full bucket of `burst` tokens refilled at `qps` per second. Both
    /// must be positive.
    pub fn new(qps: f32, burst: u32) -> Result<Self, String> {
        if !(qps.is_finite() && qps > 0.0) || burst == 0 {
            return Err(format!(
                "rate limit needs a positive qps and burst (got qps {qps}, burst {burst})"
            ));
        }
        Ok(TokenBucket {
            qps: f64::from(qps),
            burst: f64::from(burst),
            state: Mutex::new((f64::from(burst), Instant::now())),
        })
    }

    /// Reserves one token and returns how long to wait before using it.
    pub fn reserve(&self) -> Duration {
        let mut st = self.state.lock();
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(st.1).as_secs_f64();
        st.0 = (st.0 + elapsed * self.qps).min(self.burst);
        st.1 = now;
        st.0 -= 1.0;
        if st.0 >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-st.0 / self.qps)
        }
    }
}

/// Rate-limits the requests of the wrapped service with a shared bucket.
#[derive(Debug, Clone)]
pub struct RateLimitLayer {
    bucket: Arc<TokenBucket>,
}

impl RateLimitLayer {
    /// A layer drawing on `bucket`.
    pub fn new(bucket: Arc<TokenBucket>) -> Self {
        RateLimitLayer { bucket }
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimit<S>;
    fn layer(&self, inner: S) -> RateLimit<S> {
        RateLimit {
            inner,
            bucket: self.bucket.clone(),
            wait: None,
            reserved: false,
        }
    }
}

/// See [`RateLimitLayer`]. Readiness waits for a token, so callers that
/// drive `poll_ready` (as the kube client's buffer does) queue up.
pub struct RateLimit<S> {
    inner: S,
    bucket: Arc<TokenBucket>,
    wait: Option<Pin<Box<Sleep>>>,
    reserved: bool,
}

impl<S, Req> Service<Req> for RateLimit<S>
where
    S: Service<Req>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if !self.reserved {
            if self.wait.is_none() {
                let wait = self.bucket.reserve();
                if wait.is_zero() {
                    self.reserved = true;
                } else {
                    self.wait = Some(Box::pin(tokio::time::sleep(wait)));
                }
            }
            if let Some(w) = self.wait.as_mut() {
                if w.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                self.wait = None;
                self.reserved = true;
            }
        }
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        self.reserved = false;
        self.inner.call(req)
    }
}

/// A kube client for `config` whose requests are limited to `qps` per
/// second with bursts of `burst`.
pub fn rate_limited_client(
    config: kube::Config,
    qps: f32,
    burst: u32,
) -> Result<kube::Client, String> {
    let bucket = Arc::new(TokenBucket::new(qps, burst)?);
    let builder = kube::client::ClientBuilder::try_from(config).map_err(|e| e.to_string())?;
    Ok(builder.with_layer(&RateLimitLayer::new(bucket)).build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn rejects_non_positive_limits() {
        assert!(TokenBucket::new(0.0, 1).is_err());
        assert!(TokenBucket::new(-1.0, 1).is_err());
        assert!(TokenBucket::new(f32::NAN, 1).is_err());
        assert!(TokenBucket::new(1.0, 0).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn bursts_then_paces_at_qps() {
        let b = TokenBucket::new(10.0, 3).unwrap();
        for _ in 0..3 {
            assert_eq!(b.reserve(), Duration::ZERO);
        }
        // The next reservations wait 100ms, 200ms, ... for their tokens.
        let w1 = b.reserve();
        let w2 = b.reserve();
        assert!((w1.as_secs_f64() - 0.1).abs() < 1e-6, "{w1:?}");
        assert!((w2.as_secs_f64() - 0.2).abs() < 1e-6, "{w2:?}");
        // After a long idle the bucket is full again, but no fuller.
        tokio::time::advance(Duration::from_secs(60)).await;
        for _ in 0..3 {
            assert_eq!(b.reserve(), Duration::ZERO);
        }
        assert!(b.reserve() > Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn layer_delays_requests_beyond_the_burst() {
        let served = Arc::new(AtomicUsize::new(0));
        let s = served.clone();
        let inner = tower::service_fn(move |_: ()| {
            s.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, Infallible>(()) }
        });
        let layer = RateLimitLayer::new(Arc::new(TokenBucket::new(2.0, 2).unwrap()));
        let mut svc = tower::ServiceBuilder::new().layer(layer).service(inner);
        let start = Instant::now();
        for _ in 0..4 {
            use tower::ServiceExt;
            svc.ready().await.unwrap().call(()).await.unwrap();
        }
        assert_eq!(served.load(Ordering::SeqCst), 4);
        // Two from the burst, then one every 500ms.
        let took = start.elapsed();
        assert!(
            took >= Duration::from_millis(1000) && took < Duration::from_millis(1100),
            "{took:?}"
        );
    }

    #[tokio::test]
    async fn rate_limited_client_builds() {
        // Feature unification can link more than one rustls provider.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = kube::Config::new("http://127.0.0.1:1".parse().unwrap());
        assert!(rate_limited_client(config.clone(), 5.0, 10).is_ok());
        assert!(rate_limited_client(config, 0.0, 10).is_err());
    }
}
