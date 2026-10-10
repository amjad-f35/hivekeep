//! The image cache's request counter: every lookup by outcome.

use prometheus::{IntCounterVec, Opts, Registry};

use crate::error::{Error, ErrorKind};

/// The counter's name.
pub const REQUESTS_METRIC: &str = "hivekeep_imagecache_requests_total";

/// A complete cached image.
pub const OUTCOME_HIT: &str = "hit";
/// A lookup that had to pull.
pub const OUTCOME_MISS: &str = "miss";
/// A lookup that failed.
pub const OUTCOME_ERROR: &str = "error";
/// A caller that gave up.
pub const OUTCOME_CANCELLED: &str = "cancelled";
/// A caller that ran out of time.
pub const OUTCOME_TIMEOUT: &str = "timeout";

/// The `error_type` of a failure carrying no reportable status.
pub const ERR_TYPE_OTHER: &str = "_OTHER";

/// Registry statuses reported verbatim. Each has its own remedy:
/// credentials (401, 403), the reference (404), throttling (429), a
/// registry fault (5xx). Anything else could mint a series per status.
const REPORTED_STATUSES: [u16; 8] = [401, 403, 404, 429, 500, 502, 503, 504];

/// Counts lookups by `outcome` and, for errors only, `error_type`.
#[derive(Clone)]
pub struct RequestMetrics {
    requests: IntCounterVec,
}

impl RequestMetrics {
    /// Creates the counter and registers it with `registry`.
    pub fn register(registry: &Registry) -> prometheus::Result<Self> {
        let requests = IntCounterVec::new(
            Opts::new(
                REQUESTS_METRIC,
                "Number of image lookups in the node-local image cache, by outcome.",
            ),
            &["outcome", "error_type"],
        )?;
        registry.register(Box::new(requests.clone()))?;
        Ok(RequestMetrics { requests })
    }

    /// Counts one lookup. A failure replaces the hit-or-miss `outcome`.
    pub fn record(&self, outcome: &str, err: Option<&Error>) {
        let (outcome, error_type) = match err {
            None => (outcome, String::new()),
            Some(e) => {
                let o = failure_outcome(e);
                (
                    o,
                    if o == OUTCOME_ERROR {
                        error_type(e)
                    } else {
                        String::new()
                    },
                )
            }
        };
        self.requests
            .with_label_values(&[outcome, &error_type])
            .inc();
    }
}

/// Separates a failed lookup from a caller that gave up. Cancellation is
/// read first: an abandoned request also carries a transport error, which
/// would otherwise read as a registry outage.
pub fn failure_outcome(err: &Error) -> &'static str {
    if err.is(ErrorKind::Canceled) {
        OUTCOME_CANCELLED
    } else if err.is(ErrorKind::DeadlineExceeded) {
        OUTCOME_TIMEOUT
    } else {
        OUTCOME_ERROR
    }
}

/// The registry's own status for its rejection, when reportable.
pub fn error_type(err: &Error) -> String {
    match err.registry_status() {
        Some(s) if REPORTED_STATUSES.contains(&s) => s.to_string(),
        _ => ERR_TYPE_OTHER.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The bounded label set: each value comes from a class, never a
    // message.
    #[test]
    fn classify_failure() {
        let reg = |s| Error::new(ErrorKind::Registry(s), "status");
        for (name, err, want_outcome, want_type) in [
            (
                "caller cancelled",
                Error::new(ErrorKind::Canceled, "x").context("while resolving tag"),
                OUTCOME_CANCELLED,
                "",
            ),
            (
                "caller timed out",
                Error::new(ErrorKind::DeadlineExceeded, "x").context("while resolving tag"),
                OUTCOME_TIMEOUT,
                "",
            ),
            ("registry rejection", reg(404), OUTCOME_ERROR, "404"),
            (
                "credentials expired",
                reg(401).context("in remote.Image"),
                OUTCOME_ERROR,
                "401",
            ),
            ("throttled", reg(429), OUTCOME_ERROR, "429"),
            ("unlisted status", reg(418), OUTCOME_ERROR, ERR_TYPE_OTHER),
            (
                "disk full",
                Error::io("write /var/lib/x", std::io::Error::from_raw_os_error(28)),
                OUTCOME_ERROR,
                ERR_TYPE_OTHER,
            ),
            (
                "unclassified",
                Error::msg("layer dir vanished during pull"),
                OUTCOME_ERROR,
                ERR_TYPE_OTHER,
            ),
        ] {
            assert_eq!(failure_outcome(&err), want_outcome, "{name}");
            if !want_type.is_empty() {
                assert_eq!(error_type(&err), want_type, "{name}");
            }
        }
    }
}
