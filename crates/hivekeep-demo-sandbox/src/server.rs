//! The workload's HTTP surface: `POST /process`.

use crate::wire::ProcessRequest;
use crate::{duration, exec};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};

/// Path the workload serves.
pub const PROCESS_PATH: &str = "/process";

pub fn router() -> Router {
    Router::new().route(PROCESS_PATH, post(process))
}

async fn process(body: axum::body::Bytes) -> Response {
    let Ok(req) = serde_json::from_slice::<ProcessRequest>(&body) else {
        return (StatusCode::BAD_REQUEST, "Bad request\n").into_response();
    };
    if req.command.is_empty() {
        return (StatusCode::BAD_REQUEST, "Command required\n").into_response();
    }
    let timeout = if req.timeout.is_empty() {
        None
    } else {
        match duration::parse(&req.timeout) {
            Ok(d) => Some(d),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, format!("Invalid timeout: {e}\n")).into_response()
            }
        }
    };
    Json(exec::run(&req, timeout).await).into_response()
}
