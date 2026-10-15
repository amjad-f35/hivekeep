//! The router's route-duration histogram and outcome classification.

use std::time::Duration;

use prometheus::{HistogramOpts, HistogramVec, Registry};

use super::error::{ErrorKind, RouteError};
use crate::attr::{self, outcome};

/// The service name used for the router's telemetry.
pub const SERVICE_NAME: &str = "hivekeep-gate-router";
/// Name of the route-duration histogram.
pub const ROUTE_DURATION_METRIC: &str = "hivekeep_gate_router_route_duration_seconds";

/// Bucket boundaries of the route-duration histogram, in seconds.
pub const ROUTE_DURATION_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    15.0, 30.0,
];

/// Registers the histogram of time between receiving a request and
/// resolving its target worker (excluding the Actor's own work).
pub fn new_route_duration_histogram(registry: &Registry) -> prometheus::Result<HistogramVec> {
    let h = HistogramVec::new(
        HistogramOpts::new(
            ROUTE_DURATION_METRIC,
            "latency between the router receiving a request and resolving the target worker endpoint, excluding Actor execution and response",
        )
        .buckets(ROUTE_DURATION_BUCKETS.to_vec()),
        &[attr::TEMPLATE_HIVE, attr::TEMPLATE_NAME, attr::ROUTER_OUTCOME, attr::ROUTER_RESUME],
    )?;
    registry.register(Box::new(h.clone()))?;
    Ok(h)
}

/// Records one routing decision.
pub fn record_route_duration(
    h: &HistogramVec,
    d: Duration,
    template_hive: &str,
    template_name: &str,
    outcome: &str,
    resume: &str,
) {
    h.with_label_values(&[
        attr::normalize_template_dimension(template_hive),
        attr::normalize_template_dimension(template_name),
        outcome,
        resume,
    ])
    .observe(d.as_secs_f64());
}

/// The outcome label of a routing result.
pub fn classify_outcome(err: Option<&RouteError>) -> &'static str {
    let Some(err) = err else {
        return outcome::OK;
    };
    match err.kind {
        ErrorKind::Canceled => return outcome::CANCELLED,
        ErrorKind::DeadlineExceeded => return outcome::TIMEOUT,
        ErrorKind::Other => {}
    }
    match err.code {
        Some(tonic::Code::Cancelled) => return outcome::CANCELLED,
        Some(tonic::Code::DeadlineExceeded) => return outcome::TIMEOUT,
        Some(tonic::Code::ResourceExhausted) => return outcome::NO_CAPACITY,
        Some(tonic::Code::FailedPrecondition) => return outcome::FAILED_PRECONDITION,
        Some(tonic::Code::Aborted) => return outcome::LOCK_CONFLICT,
        Some(tonic::Code::NotFound) => return outcome::NOT_FOUND,
        Some(tonic::Code::Unavailable) => return outcome::UNAVAILABLE,
        Some(tonic::Code::PermissionDenied) => return outcome::DENIED,
        _ => {}
    }
    match err.status {
        Some(404) => outcome::NOT_FOUND,
        Some(503) => outcome::UNAVAILABLE,
        Some(504) => outcome::TIMEOUT,
        Some(429) => outcome::RATE_LIMITED,
        Some(403) => outcome::DENIED,
        _ => outcome::RESUME_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::{Code, Status};

    #[test]
    fn classify_outcome_cases() {
        let cases: Vec<(&str, Option<RouteError>, &str)> = vec![
            ("nil error maps to ok", None, "ok"),
            (
                "context Canceled maps to cancelled",
                Some(RouteError::canceled()),
                "cancelled",
            ),
            (
                "context DeadlineExceeded maps to timeout",
                Some(RouteError::deadline_exceeded()),
                "timeout",
            ),
            (
                "FailedPrecondition gRPC code",
                Some(Status::new(Code::FailedPrecondition, "x").into()),
                "failed_precondition",
            ),
            (
                "Aborted gRPC code",
                Some(Status::new(Code::Aborted, "lock conflict").into()),
                "lock_conflict",
            ),
            (
                "NotFound gRPC code",
                Some(Status::new(Code::NotFound, "missing").into()),
                "not_found",
            ),
            (
                "Unavailable gRPC code",
                Some(Status::new(Code::Unavailable, "down").into()),
                "unavailable",
            ),
            (
                "ResourceExhausted gRPC code",
                Some(Status::new(Code::ResourceExhausted, "no room").into()),
                "no_capacity",
            ),
            (
                "404 ReqError",
                Some(RouteError::req(404, "missing")),
                "not_found",
            ),
            (
                "bare 503 ReqError",
                Some(RouteError::req(503, "router at capacity")),
                "unavailable",
            ),
            (
                "503 with an uncoded cause",
                Some(RouteError::wrap(
                    503,
                    &RouteError::other("policy lookup failed"),
                    "egress unavailable",
                )),
                "unavailable",
            ),
            (
                "503 wrapping ResourceExhausted",
                Some(RouteError::wrap(
                    503,
                    &Status::new(Code::ResourceExhausted, "no room").into(),
                    "actor unavailable",
                )),
                "no_capacity",
            ),
            (
                "429 ReqError",
                Some(RouteError::req(429, "rate limited")),
                "rate_limited",
            ),
            (
                "PermissionDenied gRPC code",
                Some(Status::new(Code::PermissionDenied, "denied").into()),
                "denied",
            ),
            (
                "403 ReqError",
                Some(RouteError::req(403, "egress denied")),
                "denied",
            ),
            (
                "403 with an uncoded cause",
                Some(RouteError::wrap(
                    403,
                    &RouteError::other("invalid actor certificate"),
                    "egress denied",
                )),
                "denied",
            ),
            (
                "Unknown error",
                Some(RouteError::other("internal storage glitch")),
                "resume_error",
            ),
            (
                "Internal gRPC code",
                Some(Status::new(Code::Internal, "resume failed").into()),
                "resume_error",
            ),
            (
                "500 ReqError",
                Some(RouteError::req(500, "resume failed")),
                "resume_error",
            ),
        ];
        for (name, err, want) in cases {
            assert_eq!(classify_outcome(err.as_ref()), want, "{name}");
        }
    }

    fn labels(reg: &Registry) -> std::collections::HashMap<String, String> {
        let mf = reg.gather();
        let m = &mf[0].get_metric()[0];
        m.get_label()
            .iter()
            .map(|l| (l.name().to_string(), l.value().to_string()))
            .collect()
    }

    #[test]
    fn record_route_duration_attributes() {
        let reg = Registry::new();
        let h = new_route_duration_histogram(&reg).unwrap();
        record_route_duration(
            &h,
            Duration::from_millis(10),
            "team-a-ns",
            "tmpl-a",
            classify_outcome(None),
            attr::resume::TRIGGERED,
        );
        let got = labels(&reg);
        for (k, v) in [
            ("template_hive", "team-a-ns"),
            ("template_name", "tmpl-a"),
            ("outcome", "ok"),
            ("resume", "triggered"),
        ] {
            assert_eq!(got.get(k).map(String::as_str), Some(v), "{k}");
        }
    }

    #[test]
    fn record_route_duration_normalizes_empty_template_dimensions() {
        let reg = Registry::new();
        let h = new_route_duration_histogram(&reg).unwrap();
        let err = RouteError::other("fail");
        record_route_duration(
            &h,
            Duration::from_millis(5),
            "",
            "",
            classify_outcome(Some(&err)),
            attr::resume::UNKNOWN,
        );
        let got = labels(&reg);
        for (k, v) in [
            ("template_hive", "unknown"),
            ("template_name", "unknown"),
            ("outcome", "resume_error"),
            ("resume", "unknown"),
        ] {
            assert_eq!(got.get(k).map(String::as_str), Some(v), "{k}");
        }
    }
}
