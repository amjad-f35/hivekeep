//! The HTTP surface: routing, method checks, instruction decoding and JSON
//! replies. Every endpoint but `/readyz` takes a POSTed JSON instruction.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::address::validate_url;
use crate::fetch::{Fetch, HttpFetcher, Outbound};
use crate::limits::{MAX_REQUEST_BODY, POST_BODY_DELAY, SLOW_HEADER_DELAY};
use crate::wire::{
    ConnectRequest, FetchRequest, FetchResponse, GrpcRequest, GrpcResponse, WebSocketRequest,
    WebSocketResponse,
};
use crate::{connect, grpc, mux, post, slow_headers, websocket};

/// Pauses used by the deliberately slow endpoints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Delays {
    /// `/post`: between the headers and the body.
    pub post_body: Duration,
    /// `/slow-headers`: before the header block is finished.
    pub slow_header: Duration,
}

impl Default for Delays {
    fn default() -> Self {
        Self {
            post_body: POST_BODY_DELAY,
            slow_header: SLOW_HEADER_DELAY,
        }
    }
}

/// Shared server state.
#[derive(Clone)]
pub struct AppState {
    /// Client for `/` instructions that need nothing request-specific.
    pub fetcher: Arc<dyn Fetch>,
    pub delays: Delays,
}

impl AppState {
    pub fn new(fetcher: Arc<dyn Fetch>) -> Self {
        Self {
            fetcher,
            delays: Delays::default(),
        }
    }
}

/// The demo's routes; anything unmatched is a fetch.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/readyz", any(|| async { "ok\n" }))
        .route("/post", any(handle_post))
        .route("/slow-headers", any(handle_slow_headers))
        .route("/mux", any(handle_mux))
        .route("/websocket", any(handle_websocket))
        .route("/grpc", any(handle_grpc))
        .route("/connect", any(handle_connect))
        .fallback(handle_fetch)
        .with_state(state)
}

/// A JSON reply with a trailing newline.
fn json<T: Serialize>(status: u16, body: &T) -> Response {
    let mut text = serde_json::to_string(body).unwrap_or_else(|_| "{}".into());
    text.push('\n');
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    (status, [(header::CONTENT_TYPE, "application/json")], text).into_response()
}

fn method_not_allowed<T: Serialize>(body: &T) -> Response {
    let mut resp = json(405, body);
    resp.headers_mut()
        .insert(header::ALLOW, header::HeaderValue::from_static("POST"));
    resp
}

/// Reads and decodes the instruction: the first JSON value in a body of at
/// most [`MAX_REQUEST_BODY`] bytes.
async fn decode<T: DeserializeOwned>(body: Body) -> Result<T, String> {
    let bytes = to_bytes(body, MAX_REQUEST_BODY)
        .await
        .map_err(|_| "invalid JSON payload: request body too large".to_string())?;
    match serde_json::Deserializer::from_slice(&bytes)
        .into_iter::<T>()
        .next()
    {
        None => Err("invalid JSON payload: EOF".into()),
        Some(Err(e)) => Err(format!("invalid JSON payload: {e}")),
        Some(Ok(v)) => Ok(v),
    }
}

/// Method check plus decode, failing with `err` replies. The error is the
/// finished reply, returned straight to the client.
#[allow(clippy::result_large_err)]
async fn instruction<T: DeserializeOwned, R: Serialize>(
    method: &Method,
    body: Body,
    err: impl Fn(String) -> R,
) -> Result<T, Response> {
    if method != Method::POST {
        return Err(method_not_allowed(&err("method must be POST".into())));
    }
    decode(body).await.map_err(|e| json(400, &err(e)))
}

async fn handle_fetch(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let input: FetchRequest = match instruction(&method, body, FetchResponse::error).await {
        Ok(i) => i,
        Err(r) => return r,
    };
    if let Err(e) = validate_url(&input.url) {
        return json(400, &FetchResponse::error(e));
    }
    let scoped = match HttpFetcher::for_request(&input) {
        Ok(s) => s,
        Err(e) => {
            return json(
                400,
                &FetchResponse::error(format!("configuring request: {e}")),
            )
        }
    };
    let traceparent = headers
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(str::to_string);
    let outbound = Outbound {
        url: input.url.clone(),
        traceparent,
    };
    let result = match &scoped {
        Some(fetcher) => fetcher.get(outbound).await,
        None => state.fetcher.get(outbound).await,
    };
    match result {
        Err(e) => json(502, &FetchResponse::error(e)),
        Ok(f) => json(
            f.status,
            &FetchResponse {
                status_code: f.status,
                body: String::from_utf8_lossy(&f.body).into_owned(),
                server_cert: f.server_cert,
                protocol: f.protocol,
                tls: f.tls,
                error: String::new(),
            },
        ),
    }
}

async fn handle_post(State(state): State<AppState>, method: Method, body: Body) -> Response {
    match instruction(&method, body, FetchResponse::error).await {
        Ok(input) => {
            let (status, resp) = post::run(input, state.delays.post_body).await;
            json(status, &resp)
        }
        Err(r) => r,
    }
}

async fn handle_slow_headers(
    State(state): State<AppState>,
    method: Method,
    body: Body,
) -> Response {
    match instruction(&method, body, FetchResponse::error).await {
        Ok(input) => {
            let (status, resp) = slow_headers::run(input, state.delays.slow_header).await;
            json(status, &resp)
        }
        Err(r) => r,
    }
}

async fn handle_mux(method: Method, body: Body) -> Response {
    match instruction(&method, body, FetchResponse::error).await {
        Ok(input) => {
            let (status, resp) = mux::run(input).await;
            json(status, &resp)
        }
        Err(r) => r,
    }
}

async fn handle_websocket(method: Method, body: Body) -> Response {
    match instruction::<WebSocketRequest, _>(&method, body, WebSocketResponse::error).await {
        Ok(input) => {
            let (status, resp) = websocket::run(input).await;
            json(status, &resp)
        }
        Err(r) => r,
    }
}

async fn handle_grpc(method: Method, body: Body) -> Response {
    match instruction::<GrpcRequest, _>(&method, body, GrpcResponse::error).await {
        Ok(input) => {
            let (status, resp) = grpc::run(input).await;
            json(status, &resp)
        }
        Err(r) => r,
    }
}

async fn handle_connect(method: Method, body: Body) -> Response {
    match instruction::<ConnectRequest, _>(&method, body, crate::wire::ConnectResponse::error).await
    {
        Ok(input) => {
            let (status, resp) = connect::run(input).await;
            json(status, &resp)
        }
        Err(r) => r,
    }
}
