//! What a user class does about a failed Actor lifecycle call, and the
//! retry loop for transient update conflicts.

use std::future::Future;
use std::time::Duration;

use tonic::{Code, Status};

/// Whether a failed lifecycle call means this Actor is wedged or the
/// cluster is busy. Only a wedged Actor is worth a delete and re-create.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureAction {
    /// A replacement would hit the same error, so keep the Actor.
    RetryLater,
    /// This Actor can never make progress again.
    ReplaceNow,
    /// Counts toward the caller's consecutive-failure threshold.
    ReplaceIfPersistent,
}

/// The message hivekeep-api returns with `ABORTED` when two callers race an
/// Actor update. It is transient: the loser retries and one of them wins.
pub const CONCURRENT_UPDATE_MSG: &str = "concurrent update conflict, please retry";

/// How many attempts [`retry_on_conflict`] makes.
pub const CONFLICT_RETRY_ATTEMPTS: usize = 5;

/// The gap before the second and later retries; the first is immediate,
/// since a conflict often clears the instant the racing writer commits.
pub const CONFLICT_RETRY_BACKOFF: Duration = Duration::from_millis(50);

/// Uniform jitter added to the backoff so the losers of one race do not
/// retry in lockstep and lose the next one the same way.
const CONFLICT_RETRY_JITTER: Duration = Duration::from_millis(5);

/// Maps an error from ResumeActor or SuspendActor onto what to do about it.
/// Codes not named here are recoverable until proven otherwise: a wrapped
/// keeper error arrives as `INTERNAL`.
pub fn classify_lifecycle_failure(err: &Status) -> FailureAction {
    match err.code() {
        // The Actor, or the snapshot it would resume from, is gone.
        Code::NotFound | Code::DataLoss => FailureAction::ReplaceNow,
        // A state this operation has no edge out of (left in SUSPENDING, or
        // CRASHED). Nothing a client can call moves it on.
        Code::FailedPrecondition => FailureAction::ReplaceNow,
        Code::Aborted => {
            if is_crashed(err) {
                FailureAction::ReplaceNow
            } else {
                // An update conflict: the retry budget is spent, but losing
                // every race in one burst is still a race.
                FailureAction::ReplaceIfPersistent
            }
        }
        // Cluster-wide and load-dependent: every user sees these at once,
        // and a replacement needs the capacity the original was denied.
        Code::ResourceExhausted | Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled => {
            FailureAction::RetryLater
        }
        // A misconfigured run: the replacement fails the same way.
        Code::InvalidArgument
        | Code::PermissionDenied
        | Code::Unauthenticated
        | Code::Unimplemented => FailureAction::RetryLater,
        _ => FailureAction::ReplaceIfPersistent,
    }
}

/// [`classify_lifecycle_failure`] for an error that may not be a status at
/// all; anything else counts toward the threshold.
pub fn classify_any(err: Option<&Status>) -> FailureAction {
    match err {
        Some(s) => classify_lifecycle_failure(s),
        None => FailureAction::ReplaceIfPersistent,
    }
}

/// Whether `err` is hivekeep-api's verdict on a CRASHED Actor: `ABORTED`
/// with "crashed" in the message. Such an Actor never recovers.
pub fn is_crashed(err: &Status) -> bool {
    err.code() == Code::Aborted && err.message().contains("crashed")
}

/// Whether `err` is the transient update conflict. Kept apart from
/// [`is_crashed`]: both are `ABORTED` and mean opposite things.
pub fn is_concurrent_update_conflict(err: &Status) -> bool {
    err.code() == Code::Aborted && err.message().contains(CONCURRENT_UPDATE_MSG)
}

/// Runs `call` up to [`CONFLICT_RETRY_ATTEMPTS`] times while it keeps
/// failing with an update conflict, and returns any other outcome as is.
/// Dropping the returned future (a caller's timeout) ends the loop.
pub async fn retry_on_conflict<T, F, Fut>(mut call: F) -> Result<T, Status>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Status>>,
{
    let mut backoff = Duration::ZERO;
    let mut last = None;
    for attempt in 0..CONFLICT_RETRY_ATTEMPTS {
        match call().await {
            Err(e) if is_concurrent_update_conflict(&e) => last = Some(e),
            other => return other,
        }
        if attempt + 1 == CONFLICT_RETRY_ATTEMPTS {
            break;
        }
        if !backoff.is_zero() {
            let jitter = CONFLICT_RETRY_JITTER.mul_f64(rand::random::<f64>());
            tokio::time::sleep(backoff + jitter).await;
        }
        backoff = CONFLICT_RETRY_BACKOFF;
    }
    Err(last.expect("at least one attempt"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn conflict() -> Status {
        Status::aborted(CONCURRENT_UPDATE_MSG)
    }

    #[test]
    fn classify_lifecycle_failure_table() {
        use FailureAction::*;
        for (name, err, want) in [
            (
                "actor gone",
                Some(Status::not_found("Actor not found")),
                ReplaceNow,
            ),
            (
                "snapshot unreadable",
                Some(Status::data_loss("external snapshot")),
                ReplaceNow,
            ),
            (
                "stuck state",
                Some(Status::failed_precondition(
                    "MarkSuspending prerequisite not met",
                )),
                ReplaceNow,
            ),
            (
                "crashed",
                Some(Status::aborted("actor bench/sb-1 crashed")),
                ReplaceNow,
            ),
            (
                "no capacity",
                Some(Status::resource_exhausted(
                    "no worker has room for the actor",
                )),
                RetryLater,
            ),
            (
                "api server down",
                Some(Status::unavailable("connection refused")),
                RetryLater,
            ),
            (
                "timeout",
                Some(Status::deadline_exceeded("context deadline exceeded")),
                RetryLater,
            ),
            (
                "canceled",
                Some(Status::cancelled("context canceled")),
                RetryLater,
            ),
            (
                "misconfigured run",
                Some(Status::invalid_argument("bad template")),
                RetryLater,
            ),
            (
                "unauthorized",
                Some(Status::permission_denied("denied")),
                RetryLater,
            ),
            ("update conflict", Some(conflict()), ReplaceIfPersistent),
            (
                "wrapped keeper error",
                Some(Status::internal(
                    "internal server error: while checkpointing workload",
                )),
                ReplaceIfPersistent,
            ),
            (
                "internal",
                Some(Status::internal("boom")),
                ReplaceIfPersistent,
            ),
            ("not a status", None, ReplaceIfPersistent),
        ] {
            assert_eq!(classify_any(err.as_ref()), want, "{name}");
        }
    }

    #[test]
    fn is_crashed_vs_conflict() {
        let crashed = Status::aborted("actor bench/sb-1 crashed");
        assert!(is_crashed(&crashed) && !is_concurrent_update_conflict(&crashed));
        assert!(!is_crashed(&conflict()) && is_concurrent_update_conflict(&conflict()));
    }

    /// A call that pops `errs` in order and succeeds once drained, counting
    /// attempts.
    fn queue(
        errs: Vec<Status>,
    ) -> (
        impl FnMut() -> futures::future::Ready<Result<(), Status>>,
        Arc<AtomicUsize>,
    ) {
        let errs = Arc::new(Mutex::new(std::collections::VecDeque::from(errs)));
        let attempts = Arc::new(AtomicUsize::new(0));
        let a = attempts.clone();
        (
            move || {
                a.fetch_add(1, Ordering::SeqCst);
                futures::future::ready(match errs.lock().pop_front() {
                    Some(e) => Err(e),
                    None => Ok(()),
                })
            },
            attempts,
        )
    }

    #[tokio::test]
    async fn retry_on_conflict_retries_then_succeeds() {
        let (call, attempts) = queue(vec![conflict(), conflict()]);
        let start = std::time::Instant::now();
        retry_on_conflict(call).await.expect("conflicts clear");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "every conflict, then success"
        );
        assert!(
            start.elapsed() >= CONFLICT_RETRY_BACKOFF,
            "the second retry must back off"
        );
    }

    #[tokio::test]
    async fn retry_on_conflict_passes_other_errors_through() {
        let (call, attempts) = queue(vec![Status::unavailable("down")]);
        let err = retry_on_conflict(call).await.unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "no retry for non-conflict errors"
        );
    }

    #[tokio::test]
    async fn retry_on_conflict_gives_up() {
        let (call, attempts) = queue(vec![conflict(); CONFLICT_RETRY_ATTEMPTS + 1]);
        let err = retry_on_conflict(call).await.unwrap_err();
        assert!(is_concurrent_update_conflict(&err));
        assert_eq!(attempts.load(Ordering::SeqCst), CONFLICT_RETRY_ATTEMPTS);
    }

    /// Cancellation: the first retry is immediate, so a caller that gives up
    /// while the loop backs off sees exactly two attempts.
    #[tokio::test]
    async fn retry_on_conflict_honors_cancellation() {
        let (call, attempts) = queue(vec![conflict(), conflict(), conflict()]);
        let res = tokio::time::timeout(Duration::from_millis(10), retry_on_conflict(call)).await;
        assert!(res.is_err(), "the timeout must win over the backoff");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
