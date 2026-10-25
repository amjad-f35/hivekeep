//! The workload of the egress scale test. Once told to start, it opens every
//! endpoint, one per interval, then on one task per endpoint sends it an
//! HTTP GET once per period of C intervals, and counts what happened under
//! one lock.
//!
//! The loop starts on `POST /start` rather than at boot: every actor
//! restores from a golden snapshot taken after the template's warmup, so a
//! loop started at boot would run in the golden actor, which has no egress
//! policy, and be frozen into every actor's snapshot.
//!
//! The contract the test's programs share is in [`api`], [`endpoints`],
//! [`stats`] and [`hist`].

pub mod api;
pub mod endpoints;
pub mod hist;
pub mod stats;

pub mod classify;
pub mod client;
pub mod recorder;
pub mod run;
pub mod server;

#[cfg(test)]
mod tests;

use std::sync::Arc;

/// Serves the actor's routes on `listen` until SIGINT or SIGTERM.
pub async fn run(listen: &str) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(crate::boot::normalize_addr(listen))
        .await
        .map_err(|e| anyhow::anyhow!("serving: listening on {listen}: {e}"))?;
    tracing::info!(addr = listen, "egress-bench actor listening");
    server::serve(
        listener,
        Arc::new(server::ActorServer::new()),
        crate::boot::shutdown_signal(),
    )
    .await;
    Ok(())
}
