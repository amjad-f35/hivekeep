//! Spawning work that keeps the caller's tracing subscriber, so a pull's
//! log lines land wherever its caller's do.

use std::future::Future;

use tracing::instrument::WithSubscriber;

use crate::error::{Error, Result};

/// Runs `f` on the blocking pool.
pub async fn blocking<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let dispatch = tracing::dispatcher::get_default(|d| d.clone());
    tokio::task::spawn_blocking(move || tracing::dispatcher::with_default(&dispatch, f))
        .await
        .map_err(|e| Error::msg(format!("blocking task failed: {e}")))?
}

/// Spawns `fut` as a detached task.
pub fn task<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(fut.with_current_subscriber())
}
