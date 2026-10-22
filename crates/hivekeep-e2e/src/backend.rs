//! Gating suites on the install's snapshot backend.

use crate::harness::T;

/// Makes a suite that needs GCS fail, rather than skip, on an install that
/// stores snapshots elsewhere. A GCS lane sets it, so a lane misconfigured
/// as S3 cannot pass by running nothing.
pub const GCS_REQUIRED_ENV: &str = "HK_E2E_REQUIRE_GCS";

/// Whether the lane declared itself a GCS lane.
pub fn gcs_required() -> bool {
    std::env::var(GCS_REQUIRED_ENV).as_deref() == Ok("true")
}

/// What [`require_gcs_backend`] does for a backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GcsDecision {
    /// The backend is GCS: run.
    Run,
    /// Not GCS and not required: skip with this reason.
    Skip(String),
    /// Not GCS but required: fail with this reason.
    Fail(String),
}

/// The rule behind [`require_gcs_backend`], apart from a test context.
pub fn require_gcs_decision(backend: &str, required: bool) -> GcsDecision {
    if backend == "gcs" {
        GcsDecision::Run
    } else if required {
        GcsDecision::Fail(format!(
            "snapshot backend is {backend} and GCS is required ({GCS_REQUIRED_ENV} is set)"
        ))
    } else {
        GcsDecision::Skip(format!(
            "snapshot backend is {backend}, this suite needs GCS"
        ))
    }
}

/// Skips `t` unless `backend` (the install's snapshot backend as the
/// object-store plugin reports it) is GCS; fails it instead when the lane
/// requires GCS.
pub fn require_gcs_backend(t: &T, backend: &str) {
    match require_gcs_decision(backend, gcs_required()) {
        GcsDecision::Run => {}
        GcsDecision::Skip(m) => t.skip(m),
        GcsDecision::Fail(m) => t.fatal(m),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::{run_outcome, Outcome};

    #[tokio::test]
    async fn require_gcs_backend_cases() {
        let cases = [
            ("gcs runs", "gcs", false, false, false),
            ("gcs runs when required", "gcs", true, false, false),
            ("s3 skips", "s3", false, true, false),
            ("s3 fails when required", "s3", true, false, true),
        ];
        for (name, backend, required, skip, fail) in cases {
            let decision = require_gcs_decision(backend, required);
            assert_eq!(matches!(decision, GcsDecision::Skip(_)), skip, "{name}");
            assert_eq!(matches!(decision, GcsDecision::Fail(_)), fail, "{name}");
            // The same decision drives the test context.
            let outcome = run_outcome(name, |t| async move {
                match decision {
                    GcsDecision::Run => {}
                    GcsDecision::Skip(m) => t.skip(m),
                    GcsDecision::Fail(m) => t.fatal(m),
                }
            })
            .await;
            assert_eq!(matches!(outcome, Outcome::Skipped(_)), skip, "{name}");
            assert_eq!(matches!(outcome, Outcome::Failed(_)), fail, "{name}");
        }
    }
}
