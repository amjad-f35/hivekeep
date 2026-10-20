//! `/healthz` and `/readyz`. Liveness always answers 200; readiness
//! answers 200 only between the gRPC listener opening and shutdown
//! starting, so load balancers stop routing before the drain.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::http::{not_found, response, HttpResponse};

/// Whether the process should receive traffic.
#[derive(Debug, Clone, Default)]
pub struct Readiness(Arc<AtomicBool>);

impl Readiness {
    /// Marks the process ready.
    pub fn mark_ready(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    /// Marks the process not ready, e.g. at the start of shutdown.
    pub fn mark_not_ready(&self) {
        self.0.store(false, Ordering::SeqCst);
    }

    /// Whether the process is ready.
    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    /// The health endpoints' handler.
    pub fn handle(&self, req: &http::request::Parts) -> HttpResponse {
        let text = "text/plain; charset=utf-8";
        match req.uri.path() {
            "/healthz" => response(http::StatusCode::OK, text, "ok\n"),
            "/readyz" if self.is_ready() => response(http::StatusCode::OK, text, "ok\n"),
            "/readyz" => response(http::StatusCode::SERVICE_UNAVAILABLE, text, "not ready\n"),
            _ => not_found(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get(r: &Readiness, path: &str) -> http::StatusCode {
        let (parts, ()) = http::Request::get(path).body(()).unwrap().into_parts();
        r.handle(&parts).status()
    }

    #[test]
    fn readiness_follows_the_flag() {
        let r = Readiness::default();
        assert_eq!(get(&r, "/healthz"), http::StatusCode::OK);
        assert_eq!(get(&r, "/readyz"), http::StatusCode::SERVICE_UNAVAILABLE);
        r.mark_ready();
        assert_eq!(get(&r, "/readyz"), http::StatusCode::OK);
        r.mark_not_ready();
        assert_eq!(get(&r, "/readyz"), http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(get(&r, "/other"), http::StatusCode::NOT_FOUND);
    }
}
