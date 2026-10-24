//! HTTP surface: every path appends to the history and returns it.

use crate::history;
use crate::net_addr::current_ip;
use axum::http::StatusCode;
use axum::Router;
use std::path::PathBuf;
use std::sync::Arc;

/// A router appending to the history file at `path` on every request.
pub fn router(path: PathBuf) -> Router {
    let path = Arc::new(path);
    Router::new().fallback(move || {
        let path = path.clone();
        async move { handle(path).await }
    })
}

async fn handle(path: Arc<PathBuf>) -> (StatusCode, String) {
    let result = tokio::task::spawn_blocking(move || {
        let ip = current_ip();
        history::append(&path, &ip).map(|data| format!("pod: {ip}\n--- history ---\n{data}"))
    })
    .await;
    match result {
        Ok(Ok(body)) => {
            tracing::info!(response = %body, "Handled request");
            (StatusCode::OK, body)
        }
        Ok(Err((_, e))) => {
            tracing::error!(err = %e, "Failed to persist history");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to persist history\n".into(),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("handler failed: {e}\n"),
        ),
    }
}
