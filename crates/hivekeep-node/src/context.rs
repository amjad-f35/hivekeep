//! Request contexts: a cancellation signal plus an optional deadline.
//!
//! Lifecycle RPCs keep running after their caller goes away when that is
//! safer than stopping halfway (a checkpoint that already stopped the
//! workload must commit its snapshot), so work is not tied to the lifetime
//! of a future. Instead every operation carries a [`Ctx`] it can observe,
//! derive from, or detach from.

use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Why a context ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CtxError {
    /// The context, or one it derives from, was cancelled.
    #[error("context canceled")]
    Canceled,
    /// The context's deadline passed.
    #[error("context deadline exceeded")]
    DeadlineExceeded,
}

impl From<CtxError> for tonic::Status {
    fn from(e: CtxError) -> Self {
        match e {
            CtxError::Canceled => tonic::Status::cancelled(e.to_string()),
            CtxError::DeadlineExceeded => tonic::Status::deadline_exceeded(e.to_string()),
        }
    }
}

/// A cancellation signal with an optional deadline. Cloning shares the
/// signal; [`child`](Ctx::child) derives one that ends with its parent.
#[derive(Debug, Clone)]
pub struct Ctx {
    token: CancellationToken,
    deadline: Option<Instant>,
}

impl Default for Ctx {
    fn default() -> Self {
        Self::background()
    }
}

impl Ctx {
    /// A context that never ends on its own.
    pub fn background() -> Self {
        Ctx {
            token: CancellationToken::new(),
            deadline: None,
        }
    }

    /// A context ending when `token` is cancelled.
    pub fn from_token(token: CancellationToken) -> Self {
        Ctx {
            token,
            deadline: None,
        }
    }

    /// A context that ends with this one, and can also be cancelled on its
    /// own without affecting this one.
    pub fn child(&self) -> Self {
        Ctx {
            token: self.token.child_token(),
            deadline: self.deadline,
        }
    }

    /// A child context that also ends after `d`.
    pub fn with_timeout(&self, d: Duration) -> Self {
        match Instant::now().checked_add(d) {
            Some(at) => self.with_deadline(at),
            // Too far out to represent: no deadline of its own.
            None => self.child(),
        }
    }

    /// A child context that also ends at `at`, or at this context's
    /// deadline if that is earlier.
    pub fn with_deadline(&self, at: Instant) -> Self {
        let deadline = match self.deadline {
            Some(d) if d < at => d,
            _ => at,
        };
        Ctx {
            token: self.token.child_token(),
            deadline: Some(deadline),
        }
    }

    /// A context that keeps none of this one's cancellation or deadline.
    pub fn without_cancel(&self) -> Self {
        Self::background()
    }

    /// The deadline, if any.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Cancels this context and every context derived from it.
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// The token that signals this context's cancellation. It does not
    /// fire on the deadline.
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Why the context has ended, or `None` while it is live.
    pub fn err(&self) -> Option<CtxError> {
        if self.token.is_cancelled() {
            return Some(CtxError::Canceled);
        }
        match self.deadline {
            Some(d) if Instant::now() >= d => Some(CtxError::DeadlineExceeded),
            _ => None,
        }
    }

    /// Whether the context has ended.
    pub fn is_done(&self) -> bool {
        self.err().is_some()
    }

    /// Resolves once the context ends, with the reason.
    pub async fn done(&self) -> CtxError {
        match self.deadline {
            Some(d) => tokio::select! {
                _ = self.token.cancelled() => CtxError::Canceled,
                _ = tokio::time::sleep_until(d) => CtxError::DeadlineExceeded,
            },
            None => {
                self.token.cancelled().await;
                CtxError::Canceled
            }
        }
    }

    /// Resolves once the context ends, owning the context: for APIs that
    /// take a `'static` shutdown signal.
    pub async fn done_owned(self) {
        self.done().await;
    }

    /// Runs `f` until it completes or the context ends, whichever is first.
    pub async fn run<F: Future>(&self, f: F) -> Result<F::Output, CtxError> {
        if let Some(e) = self.err() {
            return Err(e);
        }
        tokio::select! {
            biased;
            out = f => Ok(out),
            e = self.done() => Err(e),
        }
    }

    /// Sleeps for `d`, or returns the reason the context ended first.
    pub async fn sleep(&self, d: Duration) -> Result<(), CtxError> {
        self.run(tokio::time::sleep(d)).await
    }
}

/// Cancels a context when dropped, unless disarmed. A gRPC handler holds
/// one for its caller's context, so a caller that goes away cancels the
/// work it started.
#[derive(Debug)]
pub struct CancelOnDrop(Option<Ctx>);

impl CancelOnDrop {
    /// Guards `ctx`.
    pub fn new(ctx: Ctx) -> Self {
        CancelOnDrop(Some(ctx))
    }

    /// Leaves the context running when dropped.
    pub fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(ctx) = self.0.take() {
            ctx.cancel();
        }
    }
}

/// The caller's context for a gRPC request, bounded by the `grpc-timeout`
/// the caller sent. Pair it with a [`CancelOnDrop`] held by the handler so
/// a caller that goes away cancels it.
pub fn request_ctx<T>(req: &tonic::Request<T>) -> Ctx {
    let ctx = Ctx::background();
    match req
        .metadata()
        .get("grpc-timeout")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_grpc_timeout)
    {
        Some(d) => ctx.with_timeout(d),
        None => ctx,
    }
}

/// Parses a `grpc-timeout` header value: up to 8 digits and a unit.
pub fn parse_grpc_timeout(v: &str) -> Option<Duration> {
    if v.len() < 2 || v.len() > 9 {
        return None;
    }
    let (digits, unit) = v.split_at(v.len() - 1);
    let n: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(n * 3600),
        "M" => Duration::from_secs(n * 60),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn child_ends_with_parent() {
        let parent = Ctx::background();
        let child = parent.child();
        assert!(child.err().is_none());
        parent.cancel();
        assert_eq!(child.err(), Some(CtxError::Canceled));
    }

    #[tokio::test]
    async fn child_cancel_leaves_parent() {
        let parent = Ctx::background();
        let child = parent.child();
        child.cancel();
        assert!(parent.err().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_keeps_the_earlier() {
        let a = Ctx::background().with_timeout(Duration::from_secs(5));
        let b = a.with_timeout(Duration::from_secs(60));
        assert_eq!(a.deadline(), b.deadline());
        tokio::time::advance(Duration::from_secs(6)).await;
        assert_eq!(b.err(), Some(CtxError::DeadlineExceeded));
    }

    #[tokio::test(start_paused = true)]
    async fn run_stops_at_deadline() {
        let ctx = Ctx::background().with_timeout(Duration::from_secs(1));
        let r = ctx.run(tokio::time::sleep(Duration::from_secs(10))).await;
        assert_eq!(r, Err(CtxError::DeadlineExceeded));
    }

    #[test]
    fn grpc_timeout_header() {
        assert_eq!(parse_grpc_timeout("10S"), Some(Duration::from_secs(10)));
        assert_eq!(parse_grpc_timeout("250m"), Some(Duration::from_millis(250)));
        assert_eq!(parse_grpc_timeout("5x"), None);
        assert_eq!(parse_grpc_timeout("S"), None);
    }
}
