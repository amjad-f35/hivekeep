//! HTTP routes of the dashboard.
//!
//! | route                  | answers                                         |
//! |------------------------|-------------------------------------------------|
//! | `GET /`, `/index.html` | the page                                        |
//! | `GET /healthz`         | hive, API endpoint and whether logs are enabled |
//! | `GET /api/pods`        | `{"pods": [PodSummary]}`                        |
//! | `GET /api/actors`      | `{"actors": [ActorSummary]}`                    |
//! | `GET /api/logs/<pod>`  | `{"logs": "...", "stderr": ""}`                 |
//! | `GET /api/task-status` | `{"assignments": [Assignment]}`                 |
//! | `POST /api/give-task`  | the new `Assignment`                            |
//!
//! Every JSON answer carries `Cache-Control: no-store`; failures are
//! `{"error": "..."}`.

use crate::inventory::{Inventory, RPC_TIMEOUT};
use crate::logs::{parse_pod_ref, LogSource};
use crate::summaries::{actors_in_hive, agent_names, pods_from_workers};
use crate::tasks::{new_assignment, now, RandomChooser, TaskBoard};
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use serde::Serialize;
use serde_json::json;
use std::future::Future;
use std::sync::Arc;

/// The dashboard page.
pub const INDEX_HTML: &str = include_str!("../../../demos/claude-code-multiplex/ui/index.html");

/// Shared handler state.
pub struct AppState {
    /// Hive whose Actors the dashboard shows; also the namespace pod logs
    /// are read from.
    pub hive: String,
    /// The endpoint shown on `/healthz`.
    pub api_endpoint: String,
    /// Control-plane reads; `None` answers 503.
    pub inventory: Option<Arc<dyn Inventory>>,
    /// Pod logs; `None` answers 503.
    pub logs: Option<Arc<dyn LogSource>>,
    /// Simulated task assignments.
    pub board: TaskBoard,
}

type Shared = Arc<AppState>;

/// Builds the router.
pub fn router(state: Shared) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/index.html", get(index))
        .route("/healthz", get(healthz))
        .route("/api/pods", get(pods))
        .route("/api/actors", get(actors))
        .route("/api/logs/", get(bad_pod_ref))
        .route("/api/logs/{*pod}", get(logs))
        .route("/api/task-status", get(task_status))
        .route("/api/give-task", post(give_task))
        .with_state(state)
}

fn json_response<T: Serialize>(status: StatusCode, body: &T) -> Response {
    match serde_json::to_vec(body) {
        Ok(data) => (
            status,
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            data,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn error(status: StatusCode, msg: impl Into<String>) -> Response {
    json_response(status, &json!({ "error": msg.into() }))
}

async fn with_timeout<T, E: std::fmt::Display>(
    f: impl Future<Output = Result<T, E>>,
) -> Result<T, String> {
    match tokio::time::timeout(RPC_TIMEOUT, f).await {
        Ok(r) => r.map_err(|e| e.to_string()),
        Err(_) => Err("deadline exceeded".to_string()),
    }
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn healthz(State(s): State<Shared>) -> Response {
    json_response(
        StatusCode::OK,
        &json!({
            "ok": true,
            "namespace": s.hive,
            "api_endpoint": s.api_endpoint,
            "logs": s.logs.is_some(),
        }),
    )
}

async fn pods(State(s): State<Shared>) -> Response {
    let Some(inv) = &s.inventory else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "hivekeep-api client not initialized",
        );
    };
    match with_timeout(inv.list_workers()).await {
        Ok(workers) => json_response(
            StatusCode::OK,
            &json!({ "pods": pods_from_workers(&workers) }),
        ),
        Err(e) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("ListWorkers: {e}"),
        ),
    }
}

async fn actors(State(s): State<Shared>) -> Response {
    let Some(inv) = &s.inventory else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "hivekeep-api client not initialized",
        );
    };
    match with_timeout(inv.list_actors()).await {
        Ok(actors) => json_response(
            StatusCode::OK,
            &json!({ "actors": actors_in_hive(&actors, &s.hive) }),
        ),
        Err(e) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("ListActors: {e}"),
        ),
    }
}

async fn bad_pod_ref() -> Response {
    error(StatusCode::BAD_REQUEST, "bad pod ref")
}

async fn logs(State(s): State<Shared>, Path(raw): Path<String>) -> Response {
    let Some(pod) = parse_pod_ref(&raw) else {
        return bad_pod_ref().await;
    };
    let Some(src) = &s.logs else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Kubernetes client not initialized",
        );
    };
    match with_timeout(src.tail(pod)).await {
        Ok(logs) => json_response(StatusCode::OK, &json!({ "logs": logs, "stderr": "" })),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn task_status(State(s): State<Shared>) -> Response {
    let (secs, _) = now();
    json_response(
        StatusCode::OK,
        &json!({ "assignments": s.board.snapshot(secs) }),
    )
}

async fn give_task(State(s): State<Shared>) -> Response {
    // Agents come from the control plane; a failed listing is reported the
    // same way as an empty hive, as there is no one to give the task to.
    let agents = match &s.inventory {
        Some(inv) => match with_timeout(inv.list_actors()).await {
            Ok(actors) => agent_names(&actors, &s.hive),
            Err(e) => {
                tracing::warn!(error = %e, "ListActors failed while giving a task");
                Vec::new()
            }
        },
        None => Vec::new(),
    };
    let (secs, millis) = now();
    match new_assignment(&agents, &mut RandomChooser, secs, millis) {
        Ok(a) => {
            s.board.push(a.clone());
            json_response(StatusCode::OK, &a)
        }
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::summaries::fixtures::{actor, worker};
    use async_trait::async_trait;
    use axum::body::Body;
    use hivekeep_proto::api::v1 as pb;
    use http::Request;
    use tonic::Status;
    use tower::ServiceExt;

    struct FakeInventory {
        workers: Vec<pb::Worker>,
        actors: Vec<pb::Actor>,
        fail: bool,
    }

    #[async_trait]
    impl Inventory for FakeInventory {
        async fn list_workers(&self) -> Result<Vec<pb::Worker>, Status> {
            if self.fail {
                return Err(Status::unavailable("down"));
            }
            Ok(self.workers.clone())
        }
        async fn list_actors(&self) -> Result<Vec<pb::Actor>, Status> {
            if self.fail {
                return Err(Status::unavailable("down"));
            }
            Ok(self.actors.clone())
        }
    }

    struct FakeLogs;

    #[async_trait]
    impl LogSource for FakeLogs {
        async fn tail(&self, pod: &str) -> Result<String, String> {
            if pod == "missing" {
                Err("pods \"missing\" not found".into())
            } else {
                Ok(format!("logs of {pod}\n"))
            }
        }
    }

    fn state(inventory: Option<FakeInventory>, logs: bool) -> Shared {
        Arc::new(AppState {
            hive: "demo".into(),
            api_endpoint: "http://localhost:8443".into(),
            inventory: inventory.map(|i| Arc::new(i) as Arc<dyn Inventory>),
            logs: if logs { Some(Arc::new(FakeLogs)) } else { None },
            board: TaskBoard::default(),
        })
    }

    fn healthy() -> FakeInventory {
        FakeInventory {
            workers: vec![
                worker("w-2", "claude-workerpool", 0),
                worker("w-1", "claude-workerpool", 1),
            ],
            actors: vec![
                actor("agent-luna", "demo", "agent-luna", pb::ActorState::Running),
                actor("stranger", "other", "x", pb::ActorState::Running),
            ],
            fail: false,
        }
    }

    async fn call(
        s: Shared,
        method: &str,
        uri: &str,
    ) -> (StatusCode, Option<String>, serde_json::Value) {
        let resp = router(s)
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let cache = resp
            .headers()
            .get(header::CACHE_CONTROL)
            .map(|v| v.to_str().unwrap().to_string());
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::String(
            String::from_utf8_lossy(&bytes).into(),
        ));
        (status, cache, v)
    }

    #[tokio::test]
    async fn index_served() {
        for path in ["/", "/index.html"] {
            let (status, _, body) = call(state(None, false), "GET", path).await;
            assert_eq!(status, StatusCode::OK);
            assert!(body.as_str().unwrap().contains("/api/give-task"));
        }
        let (status, _, _) = call(state(None, false), "GET", "/nope").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn healthz_reports_config() {
        let (status, cache, body) = call(state(None, true), "GET", "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cache.as_deref(), Some("no-store"));
        assert_eq!(
            body,
            json!({"ok": true, "namespace": "demo", "api_endpoint": "http://localhost:8443", "logs": true})
        );
    }

    #[tokio::test]
    async fn pods_and_actors() {
        let s = state(Some(healthy()), false);
        let (status, _, body) = call(s.clone(), "GET", "/api/pods").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["pods"][0]["name"], "w-1");
        assert_eq!(body["pods"][0]["phase"], "Running");
        assert_eq!(body["pods"][1]["ready"], false);
        let (status, _, body) = call(s, "GET", "/api/actors").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["actors"].as_array().unwrap().len(), 1);
        assert_eq!(body["actors"][0]["kind"], "Actor");
    }

    #[tokio::test]
    async fn unavailable_and_failing_inventory() {
        for path in ["/api/pods", "/api/actors"] {
            let (status, _, body) = call(state(None, false), "GET", path).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            assert!(body["error"].is_string());
            let failing = FakeInventory {
                fail: true,
                ..healthy()
            };
            let (status, _, body) = call(state(Some(failing), false), "GET", path).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert!(
                body["error"].as_str().unwrap().starts_with("List"),
                "{body}"
            );
        }
    }

    #[tokio::test]
    async fn logs_routes() {
        let (status, _, body) = call(state(None, true), "GET", "/api/logs/w-1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"logs": "logs of w-1\n", "stderr": ""}));
        let (status, _, _) = call(state(None, true), "GET", "/api/logs/w-1/").await;
        assert_eq!(status, StatusCode::OK);
        for bad in ["/api/logs/", "/api/logs/a/b"] {
            let (status, _, body) = call(state(None, true), "GET", bad).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
            assert_eq!(body["error"], "bad pod ref");
        }
        let (status, _, _) = call(state(None, false), "GET", "/api/logs/w-1").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let (status, _, _) = call(state(None, true), "GET", "/api/logs/missing").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn give_task_then_status() {
        let s = state(Some(healthy()), false);
        let (status, _, body) = call(s.clone(), "POST", "/api/give-task").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["agent"], "agent-luna");
        assert_eq!(body["state"], "queued");
        let (status, _, body) = call(s, "GET", "/api/task-status").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["assignments"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn give_task_without_agents() {
        let (status, _, body) = call(state(None, false), "POST", "/api/give-task").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "no agents available in the demo hive");
        let (status, _, _) = call(state(None, false), "GET", "/api/give-task").await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    }
}
