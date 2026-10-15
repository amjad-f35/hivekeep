//! Drains the Envoy sidecar through its admin interface (same-pod
//! loopback): fail health checks, gracefully drain listeners, then poll
//! active downstream connections until none remain.
//!
//! Every failure degrades instead of wedging: an unreachable admin
//! interface means Envoy has already exited, which is itself a completed
//! drain.

use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use super::drain::DataplaneDrainer;
use super::loopback::http_request;

/// How often active connections are re-read while draining.
pub const ENVOY_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Bound on each admin call.
const ADMIN_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// The Envoy [`DataplaneDrainer`].
#[derive(Debug, Clone)]
pub struct EnvoyDrainer {
    admin_addr: String,
    poll_interval: Duration,
}

impl EnvoyDrainer {
    /// A drainer for the admin interface at `admin_addr` (`host:port`;
    /// `localhost` tries both loopbacks).
    pub fn new(admin_addr: impl Into<String>) -> Self {
        EnvoyDrainer {
            admin_addr: admin_addr.into(),
            poll_interval: ENVOY_DRAIN_POLL_INTERVAL,
        }
    }

    /// Overrides the poll interval.
    pub fn with_poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// Drains, returning `Ok` once no downstream connection is active or
    /// when Envoy is already gone.
    pub async fn drain_until(&self, deadline: Instant) -> Result<(), String> {
        for path in ["/healthcheck/fail", "/drain_listeners?graceful&skip_exit"] {
            if !self.post(path, deadline).await {
                return admin_gone(deadline);
            }
        }
        loop {
            let Some(active) = self.active_connections(deadline).await else {
                return admin_gone(deadline);
            };
            if active == 0 {
                return Ok(());
            }
            let next = Instant::now() + self.poll_interval;
            if next >= deadline {
                tokio::time::sleep_until(deadline).await;
                return Err(format!(
                    "{active} downstream connections still active at the drain deadline: context deadline exceeded"
                ));
            }
            tokio::time::sleep_until(next).await;
        }
    }

    async fn request(
        &self,
        method: http::Method,
        path: &str,
        deadline: Instant,
    ) -> Result<(u16, String), String> {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err("context deadline exceeded".into());
        }
        http_request(
            &self.admin_addr,
            method,
            path,
            left.min(ADMIN_REQUEST_TIMEOUT),
        )
        .await
    }

    /// POSTs an admin call, reporting whether Envoy answered at all; a
    /// non-2xx answer is logged and the drain continues.
    async fn post(&self, path: &str, deadline: Instant) -> bool {
        match self.request(http::Method::POST, path, deadline).await {
            Err(e) => {
                tracing::info!(path, err = %e, "Envoy admin unreachable; treating the sidecar as already stopped");
                false
            }
            Ok((status, _)) => {
                if status / 100 != 2 {
                    tracing::warn!(path, status, "Envoy admin call failed");
                }
                true
            }
        }
    }

    async fn active_connections(&self, deadline: Instant) -> Option<u64> {
        let (_, body) = self
            .request(
                http::Method::GET,
                "/stats?filter=downstream_cx_active",
                deadline,
            )
            .await
            .ok()?;
        Some(sum_active_connections(&body))
    }
}

/// Sums the non-admin `downstream_cx_active` gauges in a stats dump. The
/// admin listener's own connections (this poll included) are excluded, or
/// the count could never reach zero.
pub fn sum_active_connections(stats: &str) -> u64 {
    stats
        .lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| {
            !name.contains("admin") && name.trim().ends_with("downstream_cx_active")
        })
        .filter_map(|(_, v)| v.trim().parse::<u64>().ok())
        .sum()
}

/// Tells a failed admin call apart: one failing because the window ran out
/// is an incomplete drain, not evidence that Envoy exited.
fn admin_gone(deadline: Instant) -> Result<(), String> {
    if Instant::now() >= deadline {
        return Err("drain window expired while querying the Envoy admin interface: context deadline exceeded".into());
    }
    Ok(())
}

#[async_trait]
impl DataplaneDrainer for EnvoyDrainer {
    async fn drain(&self, window: Duration) -> Result<(), String> {
        self.drain_until(Instant::now() + window).await
    }
}

#[cfg(test)]
#[path = "envoy_drain_tests.rs"]
mod tests;
