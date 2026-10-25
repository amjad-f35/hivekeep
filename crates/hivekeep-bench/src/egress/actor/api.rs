//! The actor's control contract: the routes a driver calls on port 80 (the
//! only inbound port of a sandbox), the `/start` body, and the protocol
//! header that tells a driver which contract an actor speaks.

use serde::{Deserialize, Serialize};

use super::endpoints::MAX_ENDPOINTS;

/// GET: 200 once the actor serves.
pub const READYZ_ROUTE: &str = "/readyz";
/// POST: starts a loop with a [`StartRequest`] body.
pub const START_ROUTE: &str = "/start";
/// GET: the running loop's counters without distributions, cheap enough to
/// poll; nothing is consumed by a read. `?endpoints=1` adds per-endpoint
/// counts.
pub const STATS_ROUTE: &str = "/stats";
/// POST: cuts a window, serving the counters plus the distributions recorded
/// since the previous cut (or `/start`). Each sample is served once; the
/// driver numbers cuts with [`WINDOW_PARAM`] so a cut it did not hear can be
/// asked for again. `?endpoints=1` as on `/stats`.
pub const COLLECT_ROUTE: &str = "/collect";
/// POST: ends the loop and serves its final counters and tail window. The
/// final answer is kept, so a repeated `/stop` answers it again until the
/// next `/start`, while `/stats` and `/collect` answer 404.
pub const STOP_ROUTE: &str = "/stop";

/// Query parameter of `POST /collect`: the window id, a positive integer.
/// The first cut is 1 and each cut is the previous id plus one. The last
/// cut's id is answered again from the kept answer, consuming nothing. A
/// missing or malformed value is 400; an id that is neither the last cut nor
/// the next is 409; either body names the next id.
pub const WINDOW_PARAM: &str = "window";

/// Body of the 404 that `/stats`, `/collect` and `/stop` answer while no loop
/// runs.
pub const NO_LOOP_MESSAGE: &str = "no loop is running";

/// Header on every answer, so a driver can refuse an actor whose image
/// speaks another contract instead of misreading it.
pub const PROTOCOL_HEADER: &str = "Benchmark-Egress-Protocol";
/// The contract version this actor speaks: window ids on `/collect`.
pub const PROTOCOL_VERSION: &str = "2";

/// Bound on one request when [`StartRequest::request_timeout_ms`] is zero.
pub const DEFAULT_REQUEST_TIMEOUT_MS: i64 = 5000;

/// Plain HTTP endpoints.
pub const SCHEME_HTTP: &str = "http";
/// HTTPS endpoints.
pub const SCHEME_HTTPS: &str = "https";

/// The JSON body of `POST /start`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartRequest {
    /// C: the loop requests endpoint 0 through C-1, each on its own task,
    /// until stopped.
    #[serde(default)]
    pub endpoints: i64,
    /// Bound on one request; zero means [`DEFAULT_REQUEST_TIMEOUT_MS`].
    #[serde(default, rename = "requestTimeoutMs")]
    pub request_timeout_ms: i64,
    /// Pace: endpoint i first starts `offset_ms + i * interval_ms` after the
    /// loop, then each endpoint is requested once per period of C intervals,
    /// so the actor sends one request per interval. Zero sends every
    /// endpoint's requests back to back.
    #[serde(default, rename = "intervalMs")]
    pub interval_ms: i64,
    /// Delays the whole schedule, so a fleet started at once does not send
    /// in lockstep.
    #[serde(default, rename = "offsetMs", skip_serializing_if = "is_zero")]
    pub offset_ms: i64,
    /// [`SCHEME_HTTP`] or [`SCHEME_HTTPS`]; empty means HTTP.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub scheme: String,
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

impl StartRequest {
    /// The scheme the loop requests endpoints over.
    pub fn url_scheme(&self) -> &str {
        if self.scheme.is_empty() {
            SCHEME_HTTP
        } else {
            &self.scheme
        }
    }

    /// Whether this request can start a loop.
    pub fn validate(&self) -> Result<(), String> {
        if self.endpoints < 1 || self.endpoints > MAX_ENDPOINTS {
            return Err(format!(
                "endpoints must be between 1 and {MAX_ENDPOINTS}, got {}",
                self.endpoints
            ));
        }
        if self.request_timeout_ms < 0 {
            return Err(format!(
                "requestTimeoutMs cannot be negative: {}",
                self.request_timeout_ms
            ));
        }
        if self.interval_ms < 0 {
            return Err(format!(
                "intervalMs cannot be negative: {}",
                self.interval_ms
            ));
        }
        if self.offset_ms < 0 {
            return Err(format!("offsetMs cannot be negative: {}", self.offset_ms));
        }
        let period = self.endpoints * self.interval_ms;
        if self.offset_ms > period {
            return Err(format!(
                "offsetMs cannot exceed one period, endpoints x intervalMs = {period}: {}",
                self.offset_ms
            ));
        }
        if !self.scheme.is_empty() && self.scheme != SCHEME_HTTP && self.scheme != SCHEME_HTTPS {
            return Err(format!(
                "scheme must be {SCHEME_HTTP} or {SCHEME_HTTPS}, got {:?}",
                self.scheme
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(endpoints: i64) -> StartRequest {
        StartRequest {
            endpoints,
            ..Default::default()
        }
    }

    #[test]
    fn start_request_validate() {
        let with = |f: &dyn Fn(&mut StartRequest), endpoints| {
            let mut r = req(endpoints);
            f(&mut r);
            r
        };
        let cases: Vec<(&str, StartRequest, bool)> = vec![
            ("ok", req(10), false),
            ("http", with(&|r| r.scheme = SCHEME_HTTP.into(), 10), false),
            (
                "https",
                with(&|r| r.scheme = SCHEME_HTTPS.into(), 10),
                false,
            ),
            ("bad scheme", with(&|r| r.scheme = "ftp".into(), 10), true),
            ("most endpoints", req(MAX_ENDPOINTS), false),
            ("no endpoints", req(0), true),
            ("too many endpoints", req(MAX_ENDPOINTS + 1), true),
            (
                "negative timeout",
                with(&|r| r.request_timeout_ms = -1, 1),
                true,
            ),
            ("negative interval", with(&|r| r.interval_ms = -1, 1), true),
            (
                "offset",
                with(
                    &|r| {
                        r.interval_ms = 100;
                        r.offset_ms = 50
                    },
                    3,
                ),
                false,
            ),
            (
                "offset of one period",
                with(
                    &|r| {
                        r.interval_ms = 100;
                        r.offset_ms = 300
                    },
                    3,
                ),
                false,
            ),
            (
                "offset over one period",
                with(
                    &|r| {
                        r.interval_ms = 100;
                        r.offset_ms = 301
                    },
                    3,
                ),
                true,
            ),
            (
                "negative offset",
                with(
                    &|r| {
                        r.interval_ms = 100;
                        r.offset_ms = -1
                    },
                    1,
                ),
                true,
            ),
            (
                "offset without interval",
                with(&|r| r.offset_ms = 1, 1),
                true,
            ),
        ];
        for (name, r, want_err) in cases {
            assert_eq!(
                r.validate().is_err(),
                want_err,
                "{name}: {:?}",
                r.validate()
            );
        }
    }

    #[test]
    fn url_scheme() {
        assert_eq!(StartRequest::default().url_scheme(), SCHEME_HTTP);
        let r = StartRequest {
            scheme: SCHEME_HTTPS.into(),
            ..Default::default()
        };
        assert_eq!(r.url_scheme(), SCHEME_HTTPS);
    }

    #[test]
    fn wire_names_are_camel_case() {
        let r: StartRequest = serde_json::from_str(
            r#"{"endpoints":2,"requestTimeoutMs":3,"intervalMs":4,"offsetMs":5,"scheme":"https"}"#,
        )
        .unwrap();
        assert_eq!(
            r,
            StartRequest {
                endpoints: 2,
                request_timeout_ms: 3,
                interval_ms: 4,
                offset_ms: 5,
                scheme: "https".into()
            }
        );
    }
}
