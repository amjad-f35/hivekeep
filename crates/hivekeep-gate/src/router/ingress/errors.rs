//! How resume failures reach the client: an HTTP status and a message
//! that never leaks control-plane internals beyond what helps the caller.

use hivekeep_core::resource::ActorRef;

use super::resumer::ResumeError;
use crate::attr::outcome;
use crate::router::extproc::{ErrorKind, RouteError};

/// 404 for an Actor that does not exist.
pub fn actor_not_found(actor: &ActorRef) -> RouteError {
    RouteError::req(404, format!("actor {actor} not found"))
}

/// The outcome label a shed request reports: capacity when the flight
/// was blocked on capacity, unavailable otherwise.
pub fn shed_outcome(retry: Option<&tonic::Status>) -> &'static str {
    match retry.map(|s| s.code()) {
        Some(tonic::Code::ResourceExhausted) => outcome::NO_CAPACITY,
        _ => outcome::UNAVAILABLE,
    }
}

/// 503 for a request that had to park but found the lot full. A capacity
/// cause is kept so the outcome stays attributable to capacity.
pub fn parking_full(actor_id: &str, retry: Option<&tonic::Status>) -> RouteError {
    let msg = format!("actor {actor_id:?} unavailable: router at capacity");
    match retry {
        Some(s) if shed_outcome(Some(s)) == outcome::NO_CAPACITY => {
            RouteError::wrap(503, &RouteError::from(s.clone()), msg)
        }
        _ => RouteError::req(503, msg),
    }
}

fn from_status(actor: &ActorRef, s: &tonic::Status) -> RouteError {
    let cause = RouteError::from(s.clone());
    let desc = s.message();
    let (status, msg) = match s.code() {
        tonic::Code::NotFound => (404, format!("actor {actor} not found")),
        tonic::Code::FailedPrecondition | tonic::Code::Aborted | tonic::Code::ResourceExhausted => {
            (503, format!("actor {actor} unavailable: {desc}"))
        }
        tonic::Code::Unavailable => (503, format!("actor {actor} unavailable")),
        tonic::Code::DeadlineExceeded => (504, format!("actor {actor} request timed out")),
        tonic::Code::PermissionDenied => (403, format!("actor {actor} access denied")),
        tonic::Code::Unauthenticated => (401, format!("actor {actor} authentication required")),
        _ => (500, format!("error resuming actor {actor}")),
    };
    RouteError::wrap(status, &cause, msg)
}

/// Maps a resume failure to the client's answer, keeping the original as
/// the cause for logs.
pub fn map_resume_error(actor: &ActorRef, err: &ResumeError) -> RouteError {
    match err {
        ResumeError::Route(e) => e.clone(),
        ResumeError::Canceled => RouteError {
            kind: ErrorKind::Canceled,
            cause: Some("context canceled".into()),
            ..RouteError::req(408, format!("request for actor {actor} canceled by client"))
        },
        ResumeError::Timeout => RouteError {
            kind: ErrorKind::DeadlineExceeded,
            cause: Some("context deadline exceeded".into()),
            ..RouteError::req(504, format!("actor {actor} request timed out"))
        },
        ResumeError::Status(s) | ResumeError::BudgetExhausted(s) => from_status(actor, s),
        ResumeError::Other(e) => RouteError {
            cause: Some(e.clone()),
            ..RouteError::req(500, format!("error resuming actor {actor}"))
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::{Code, Status};

    fn a() -> ActorRef {
        ActorRef::new("team-a", "ctr6")
    }

    #[test]
    fn actor_not_found_err() {
        let e = actor_not_found(&a());
        assert_eq!(e.http_status(), 404);
        assert_eq!(e.to_string(), "actor team-a/ctr6 not found");
    }

    #[test]
    fn parking_full_err() {
        for (name, retry, want) in [
            (
                "ResourceExhausted is kept",
                Some(Status::new(
                    Code::ResourceExhausted,
                    "no worker has room for the actor",
                )),
                Some(Code::ResourceExhausted),
            ),
            (
                "Aborted is dropped",
                Some(Status::new(Code::Aborted, "conflict")),
                None,
            ),
            (
                "FailedPrecondition is dropped",
                Some(Status::new(Code::FailedPrecondition, "suspending")),
                None,
            ),
            (
                "Unavailable is dropped",
                Some(Status::new(Code::Unavailable, "restarting")),
                None,
            ),
            ("nil", None, None),
        ] {
            let e = parking_full("team-a/ctr6", retry.as_ref());
            assert_eq!(e.http_status(), 503, "{name}");
            assert_eq!(
                e.to_string(),
                r#"actor "team-a/ctr6" unavailable: router at capacity"#,
                "{name}"
            );
            assert_eq!(e.code, want, "{name}");
        }
    }

    #[test]
    fn map_resume_error_cases() {
        let st = |c, m: &str| ResumeError::Status(Status::new(c, m));
        let cases: Vec<(&str, ResumeError, u16, &str)> = vec![
            (
                "NotFound maps to 404",
                st(Code::NotFound, "actor not found"),
                404,
                "actor team-a/ctr6 not found",
            ),
            (
                "FailedPrecondition maps to 503 and preserves desc",
                st(Code::FailedPrecondition, "no worker has room for the actor"),
                503,
                "actor team-a/ctr6 unavailable: no worker has room for the actor",
            ),
            (
                "Unavailable maps to 503",
                st(Code::Unavailable, "control-plane down"),
                503,
                "actor team-a/ctr6 unavailable",
            ),
            (
                "DeadlineExceeded maps to 504",
                st(Code::DeadlineExceeded, "context deadline exceeded"),
                504,
                "actor team-a/ctr6 request timed out",
            ),
            (
                "Aborted maps to 503 and preserves desc",
                st(
                    Code::Aborted,
                    "another operation is in progress for this actor",
                ),
                503,
                "actor team-a/ctr6 unavailable: another operation is in progress for this actor",
            ),
            (
                "budget exhausted on Aborted-only maps to 503, not 500",
                ResumeError::BudgetExhausted(Status::new(
                    Code::Aborted,
                    "concurrent update conflict, please retry",
                )),
                503,
                "actor team-a/ctr6 unavailable: concurrent update conflict, please retry",
            ),
            (
                "budget exhausted on capacity keeps a clean body",
                ResumeError::BudgetExhausted(Status::new(
                    Code::FailedPrecondition,
                    "no worker has room for the actor",
                )),
                503,
                "actor team-a/ctr6 unavailable: no worker has room for the actor",
            ),
            (
                "bare cancellation maps to 408",
                ResumeError::Canceled,
                408,
                "request for actor team-a/ctr6 canceled by client",
            ),
            (
                "bare deadline maps to 504",
                ResumeError::Timeout,
                504,
                "actor team-a/ctr6 request timed out",
            ),
            (
                "PermissionDenied maps to 403",
                st(Code::PermissionDenied, "denied"),
                403,
                "actor team-a/ctr6 access denied",
            ),
            (
                "Unauthenticated maps to 401",
                st(Code::Unauthenticated, "no creds"),
                401,
                "actor team-a/ctr6 authentication required",
            ),
            (
                "ResourceExhausted maps to 503 preserving the description",
                st(Code::ResourceExhausted, "no worker has room for the actor"),
                503,
                "actor team-a/ctr6 unavailable: no worker has room for the actor",
            ),
            (
                "unknown gRPC code maps to 500 without leaking desc",
                st(Code::Internal, "stack trace: foo bar"),
                500,
                "error resuming actor team-a/ctr6",
            ),
            (
                "non-gRPC error maps to 500 without leaking message",
                ResumeError::Other("raw error with secret".into()),
                500,
                "error resuming actor team-a/ctr6",
            ),
        ];
        for (name, err, status, body) in cases {
            let got = map_resume_error(&a(), &err);
            assert_eq!(got.http_status(), status, "{name}");
            assert_eq!(got.to_string(), body, "{name}");
            assert!(
                got.cause.is_some(),
                "{name}: the original must be kept for logs"
            );
        }
    }

    #[test]
    fn map_resume_error_keeps_route_errors() {
        let e = RouteError::req(503, "x");
        assert_eq!(map_resume_error(&a(), &ResumeError::Route(e.clone())), e);
    }
}
