//! How a session's failures are told apart: a busy fleet, a gone Actor, or
//! a wedged one, whether the verdict arrives as a gRPC status from
//! hivekeep-api or as the gate router's HTTP mapping of one.

use std::fmt;
use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;
use tonic::{Code, Status};

use crate::loadgen::lifecycle::{classify_lifecycle_failure, FailureAction};

/// The first wait between wake retries; short enough that a slot freed by
/// another session's suspend is taken within a second.
pub const WAKE_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(50);
/// The longest wait between wake retries.
pub const WAKE_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);
/// Each wait is lengthened by up to this fraction of itself, so sessions
/// that hit a full fleet together drift apart instead of retrying in step.
pub const WAKE_RETRY_JITTER_FRACTION: f64 = 0.1;

/// A router reply with a status of 400 or above.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpError {
    /// The route requested.
    pub route: String,
    /// The HTTP status.
    pub status: u16,
    /// The trimmed response body.
    pub body: String,
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: HTTP {}: {}", self.route, self.status, self.body)
    }
}

/// A failed op or control call.
#[derive(Debug, Clone)]
pub enum OpError {
    /// hivekeep-api's verdict.
    Status(Status),
    /// The router's verdict.
    Http(HttpError),
    /// Anything else: transport failures, undecodable replies.
    Other(String),
}

impl fmt::Display for OpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpError::Status(s) => {
                write!(f, "rpc error: code = {:?} desc = {}", s.code(), s.message())
            }
            OpError::Http(h) => h.fmt(f),
            OpError::Other(m) => f.write_str(m),
        }
    }
}

impl From<Status> for OpError {
    fn from(s: Status) -> Self {
        OpError::Status(s)
    }
}

/// Maps a router status onto a failure action. 503, 504 and 429 are the
/// fleet or the control plane being busy, which a replacement would only
/// add to; 404 means the Actor record is gone; anything else counts toward
/// replacement.
pub fn classify_http(status: u16) -> FailureAction {
    match status {
        503 | 504 | 429 => FailureAction::RetryLater,
        404 => FailureAction::ReplaceNow,
        _ => FailureAction::ReplaceIfPersistent,
    }
}

/// What to do about `err`.
pub fn classify(err: &OpError) -> FailureAction {
    match err {
        OpError::Status(s) => classify_lifecycle_failure(s),
        OpError::Http(h) => classify_http(h.status),
        OpError::Other(_) => FailureAction::ReplaceIfPersistent,
    }
}

/// Whether `err` says the fleet or the control plane had no room just now:
/// `RESOURCE_EXHAUSTED` or `UNAVAILABLE`, or the router's 503, 504 and 429
/// faces of them. The Actor itself is fine and a later try may find a slot.
pub fn is_capacity_error(err: Option<&OpError>) -> bool {
    match err {
        Some(OpError::Http(h)) => classify_http(h.status) == FailureAction::RetryLater,
        Some(OpError::Status(s)) => matches!(s.code(), Code::ResourceExhausted | Code::Unavailable),
        _ => false,
    }
}

/// Runs `call` and, while it fails with a capacity error and the next try
/// would still start before `deadline`, waits with jittered exponential
/// backoff and runs it again. Any other outcome returns at once.
pub async fn retry_while_full<F, Fut>(deadline: Instant, mut call: F) -> Result<(), OpError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), OpError>>,
{
    let mut delay = WAKE_RETRY_INITIAL_BACKOFF;
    loop {
        let res = call().await;
        match &res {
            Err(e) if is_capacity_error(Some(e)) => {}
            _ => return res,
        }
        let wait = delay + delay.mul_f64(rand::random::<f64>() * WAKE_RETRY_JITTER_FRACTION);
        if Instant::now() + wait > deadline {
            return res;
        }
        tokio::time::sleep(wait).await;
        delay = (delay * 2).min(WAKE_RETRY_MAX_BACKOFF);
    }
}
