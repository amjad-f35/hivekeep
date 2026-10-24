//! `/mux`: many parallel uploads, for checking that the egress path
//! multiplexes them over a bounded number of HTTP/2 CONNECT tunnels.
//!
//! [`MUX_REQUEST_COUNT`] connections are opened and each sends its request
//! headers. Only once every one of them has done so do the bodies go out,
//! so all connections are open at the same moment. Every request must then
//! answer 200. A test can afterwards count the tunnels the gateway opened.

use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use tokio::time::{sleep_until, Instant};
use tokio_util::sync::CancellationToken;

use crate::address::validate_url;
use crate::dial::{self, DEADLINE_EXCEEDED};
use crate::http1::read_response;
use crate::limits::{MAX_RESPONSE_BODY, MUX_REQUEST_COUNT, REQUEST_TIMEOUT};
use crate::post::{upload_body, upload_head};
use crate::wire::{FetchRequest, FetchResponse};

/// Why one request did not produce a response.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Failure {
    /// Stopped because another request failed first.
    Canceled,
    Other(String),
}

type Outcome = Result<(u16, String), Failure>;

/// Counts requests still writing their headers.
#[derive(Clone)]
struct Countdown(Arc<watch::Sender<usize>>);

impl Countdown {
    fn new(n: usize) -> Self {
        Self(Arc::new(watch::channel(n).0))
    }
    fn done(&self) {
        self.0.send_modify(|n| *n = n.saturating_sub(1));
    }
    async fn wait(&self) {
        let mut rx = self.0.subscribe();
        let _ = rx.wait_for(|n| *n == 0).await;
    }
}

/// Until disarmed, dropping this counts the request as finished with its
/// headers and stops every other request: a request that fails before its
/// headers are out means the barrier can never be reached as intended.
struct HeaderGuard {
    countdown: Countdown,
    cancel: CancellationToken,
    armed: bool,
}

impl HeaderGuard {
    fn headers_sent(&mut self) {
        self.armed = false;
        self.countdown.done();
    }
}

impl Drop for HeaderGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cancel.cancel();
            self.countdown.done();
        }
    }
}

struct Target {
    address: String,
    host: String,
    request_uri: String,
}

async fn exchange(target: &Target, guard: &mut HeaderGuard) -> Outcome {
    let mut conn = dial::open(&target.address, None)
        .await
        .map_err(|e| Failure::Other(format!("dialing {}: {e}", target.address)))?;
    conn.write_all(upload_head(&target.request_uri, &target.host).as_bytes())
        .await
        .map_err(|e| Failure::Other(format!("writing request headers: {e}")))?;
    conn.flush()
        .await
        .map_err(|e| Failure::Other(format!("writing request headers: {e}")))?;
    guard.headers_sent();

    tokio::select! {
        _ = guard.countdown.wait() => {}
        _ = guard.cancel.cancelled() => return Err(Failure::Canceled),
    }
    if guard.cancel.is_cancelled() {
        return Err(Failure::Canceled);
    }
    conn.write_all(&upload_body())
        .await
        .map_err(|e| Failure::Other(format!("writing request body: {e}")))?;
    conn.flush()
        .await
        .map_err(|e| Failure::Other(format!("writing request body: {e}")))?;
    let resp = read_response(&mut conn, "POST", MAX_RESPONSE_BODY)
        .await
        .map_err(|e| Failure::Other(format!("reading response: {e}")))?;
    Ok((
        resp.status,
        String::from_utf8_lossy(&resp.body).into_owned(),
    ))
}

async fn one(
    target: Arc<Target>,
    countdown: Countdown,
    cancel: CancellationToken,
    deadline: Instant,
) -> Outcome {
    let mut guard = HeaderGuard {
        countdown,
        cancel: cancel.clone(),
        armed: true,
    };
    tokio::select! {
        biased;
        out = exchange(&target, &mut guard) => out,
        _ = cancel.cancelled() => Err(Failure::Canceled),
        _ = sleep_until(deadline) => Err(Failure::Other(DEADLINE_EXCEEDED.into())),
    }
}

/// Runs the instruction and returns the HTTP status and report.
pub async fn run(input: FetchRequest) -> (u16, FetchResponse) {
    let parts = match validate_url(&input.url) {
        Ok(p) => p,
        Err(e) => return (400, FetchResponse::error(e)),
    };
    if parts.scheme != "http" {
        return (400, FetchResponse::error("URL scheme must be http"));
    }
    let target = Arc::new(Target {
        address: parts.dial_address("80"),
        host: parts.host.clone(),
        request_uri: parts.request_uri.clone(),
    });

    let deadline = Instant::now() + REQUEST_TIMEOUT;
    let countdown = Countdown::new(MUX_REQUEST_COUNT);
    let cancel = CancellationToken::new();
    let outcomes: Vec<Outcome> = futures::future::join_all(
        (0..MUX_REQUEST_COUNT)
            .map(|_| one(target.clone(), countdown.clone(), cancel.clone(), deadline)),
    )
    .await;
    summarize(&outcomes)
}

/// The report for a set of outcomes: the first real failure or non-200
/// answer wins; requests stopped only because of another's failure are
/// reported last.
fn summarize(outcomes: &[Outcome]) -> (u16, FetchResponse) {
    for (i, out) in outcomes.iter().enumerate() {
        match out {
            Err(Failure::Other(e)) => {
                return (
                    502,
                    FetchResponse::error(format!("request {i} failed: {e}")),
                );
            }
            Ok((status, body)) if *status != 200 => {
                return (
                    *status,
                    FetchResponse {
                        status_code: *status,
                        body: body.clone(),
                        error: format!("request {i} returned status {status}, want 200"),
                        ..Default::default()
                    },
                );
            }
            _ => {}
        }
    }
    if let Some(i) = outcomes.iter().position(|o| o.is_err()) {
        return (
            502,
            FetchResponse::error(format!("request {i} failed: context canceled")),
        );
    }
    (
        200,
        FetchResponse {
            status_code: 200,
            ..Default::default()
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_prefers_real_failures_over_cancellations() {
        let outcomes = vec![
            Err(Failure::Canceled),
            Ok((200, String::new())),
            Err(Failure::Other("boom".into())),
        ];
        let (status, resp) = summarize(&outcomes);
        assert_eq!(status, 502);
        assert_eq!(resp.error, "request 2 failed: boom");

        let (status, resp) = summarize(&[Ok((200, String::new())), Ok((500, "x".into()))]);
        assert_eq!(
            (status, resp.status_code, resp.body.as_str()),
            (500, 500, "x")
        );

        let (status, resp) = summarize(&[Err(Failure::Canceled)]);
        assert_eq!(status, 502);
        assert!(resp.error.contains("canceled"));

        assert_eq!(summarize(&[Ok((200, String::new()))]).0, 200);
    }
}
