//! Request lifetime: cancellation and deadlines shared by every part of a
//! request, including the OS threads a CPU burn runs on.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tonic::Status;

/// A request's lifetime. Cloning shares it; [`Ctx::child`] derives one that
/// ends with its parent and can also be ended on its own.
#[derive(Clone, Debug, Default)]
pub struct Ctx {
    token: CancellationToken,
    deadline_hit: Arc<AtomicBool>,
}

impl Ctx {
    /// A lifetime that ends only when cancelled.
    pub fn background() -> Ctx {
        Ctx::default()
    }

    /// A lifetime that ends after `timeout`. Must be called on a tokio
    /// runtime, which watches the deadline.
    pub fn with_timeout(timeout: Duration) -> Ctx {
        let ctx = Ctx::default();
        let watched = ctx.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = watched.token.cancelled() => {}
                _ = tokio::time::sleep(timeout) => {
                    watched.deadline_hit.store(true, Ordering::SeqCst);
                    watched.token.cancel();
                }
            }
        });
        ctx
    }

    /// A lifetime that ends with this one, or earlier when cancelled itself.
    pub fn child(&self) -> Ctx {
        Ctx {
            token: self.token.child_token(),
            deadline_hit: self.deadline_hit.clone(),
        }
    }

    /// Ends this lifetime (and its children).
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// A guard that ends this lifetime when dropped, so a request whose
    /// caller goes away stops its background work.
    pub fn cancel_on_drop(&self) -> tokio_util::sync::DropGuard {
        self.token.clone().drop_guard()
    }

    /// Whether the lifetime has ended.
    pub fn is_done(&self) -> bool {
        self.token.is_cancelled()
    }

    /// The status a request reports once its lifetime has ended, if it has.
    pub fn err(&self) -> Option<Status> {
        if !self.token.is_cancelled() {
            return None;
        }
        if self.deadline_hit.load(Ordering::SeqCst) {
            Some(Status::deadline_exceeded("context deadline exceeded"))
        } else {
            Some(Status::cancelled("context canceled"))
        }
    }

    /// Resolves when the lifetime ends.
    pub async fn done(&self) {
        self.token.cancelled().await
    }
}

/// Parses a gRPC `grpc-timeout` header value (`<digits><unit>`).
pub fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    if value.len() < 2 {
        return None;
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    let n: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(n.checked_mul(3600)?),
        "M" => Duration::from_secs(n.checked_mul(60)?),
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

    #[test]
    fn grpc_timeouts() {
        assert_eq!(parse_grpc_timeout("5S"), Some(Duration::from_secs(5)));
        assert_eq!(parse_grpc_timeout("20m"), Some(Duration::from_millis(20)));
        assert_eq!(parse_grpc_timeout("1H"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_grpc_timeout("x"), None);
        assert_eq!(parse_grpc_timeout("10q"), None);
    }

    #[tokio::test]
    async fn deadline_and_cancel_report_distinct_codes() {
        let ctx = Ctx::with_timeout(Duration::from_millis(5));
        assert!(ctx.err().is_none());
        ctx.done().await;
        assert_eq!(ctx.err().unwrap().code(), tonic::Code::DeadlineExceeded);
        let parent = Ctx::background();
        let child = parent.child();
        child.cancel();
        assert!(parent.err().is_none());
        assert_eq!(child.err().unwrap().code(), tonic::Code::Cancelled);
    }
}
