//! HTTP handlers of the counter workload.

use crate::file_counter;
use crate::net_addr::current_ip;
use crate::state::State;
use axum::extract::{Query, State as Extract};
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use serde::Deserialize;
use std::sync::Arc;

/// The main listener: counters on every path, plus readiness and the SIGTERM
/// linger knob.
pub fn router(state: Arc<State>) -> Router {
    Router::new()
        .route("/readyz", get(readyz))
        .route(
            "/set-sigterm-sleep",
            get(set_sigterm_sleep).post(set_sigterm_sleep),
        )
        .fallback(count)
        .with_state(state)
}

/// The response body of a counter request.
pub fn count_response(state: &State) -> String {
    let p = &state.paths;
    let file = file_counter::increment(&p.file_counter_dir.join("a.txt"));
    let rootfs = file_counter::increment(&p.rootfs_counter_file);
    let memory = state.next_request();
    let ip = current_ip();

    let mut file_content = String::new();
    if let Some(path) = &p.validate_existing_file {
        match std::fs::read_to_string(path) {
            Ok(c) => file_content = format!(" | file content: {c}"),
            Err(e) => return format!("failed to read test file: {e}\n"),
        }
    }
    // A second directory shows two durable volumes persisting independently.
    let second = match &p.second_file_counter_dir {
        Some(dir) => format!(
            " | preserved second file counter: {}",
            file_counter::increment(&dir.join("a.txt"))
        ),
        None => String::new(),
    };
    format!(
        "hello from: {ip} | preserved memory count: {memory} | preserved file counter: {file} | preserved rootfs counter: {rootfs}{second}{file_content}\n"
    )
}

async fn count(Extract(state): Extract<Arc<State>>) -> (StatusCode, String) {
    let body = tokio::task::spawn_blocking(move || count_response(&state))
        .await
        .unwrap_or_else(|e| format!("handler failed: {e}\n"));
    tracing::info!(response = %body, "Handled request");
    (StatusCode::OK, body)
}

/// 200 only after start-up has finished. The flag is in memory, so a
/// restored Actor answers ready immediately.
async fn readyz(Extract(state): Extract<Arc<State>>) -> (StatusCode, &'static str) {
    if state.is_ready() {
        (StatusCode::OK, "ok\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    }
}

#[derive(Deserialize)]
struct SleepQuery {
    duration: Option<String>,
}

async fn set_sigterm_sleep(
    Extract(state): Extract<Arc<State>>,
    Query(q): Query<SleepQuery>,
) -> (StatusCode, String) {
    let Some(raw) = q.duration.filter(|d| !d.is_empty()) else {
        return (
            StatusCode::BAD_REQUEST,
            "missing duration parameter\n".into(),
        );
    };
    match raw.parse::<i64>() {
        Ok(d) if d >= 0 => {
            state.set_sigterm_sleep_secs(d);
            tracing::info!(duration_secs = d, "Updated SIGTERM sleep duration");
            (
                StatusCode::OK,
                format!("SIGTERM sleep duration set to {d} seconds\n"),
            )
        }
        _ => (
            StatusCode::BAD_REQUEST,
            "invalid duration parameter\n".into(),
        ),
    }
}

/// A listener on an additional port that names the port in its answer, so a
/// test can prove traffic reached it rather than the default port.
pub fn extra_port_router(port: u16) -> Router {
    Router::new().fallback(move || async move {
        let body = format!("hello from extra port {port} on pod {}\n", current_ip());
        tracing::info!(response = %body, "Handled extra-port request");
        (StatusCode::OK, body)
    })
}

/// Echoes every byte back on each accepted connection until the listener fails.
pub async fn tcp_echo(listener: tokio::net::TcpListener) {
    loop {
        match listener.accept().await {
            Ok((mut conn, _)) => {
                tokio::spawn(async move {
                    let (mut r, mut w) = conn.split();
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                });
            }
            Err(e) => {
                tracing::error!(err = %e, "TCP echo accept failed");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Paths;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn state(dir: &std::path::Path) -> Arc<State> {
        Arc::new(State::new(Paths {
            file_counter_dir: dir.to_path_buf(),
            second_file_counter_dir: None,
            rootfs_counter_file: dir.join("rootfs.txt"),
            validate_existing_file: None,
        }))
    }

    async fn call(app: Router, uri: &str) -> (StatusCode, String) {
        let resp = app
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn counters_advance_together() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let (s, first) = call(router(st.clone()), "/").await;
        assert_eq!(s, StatusCode::OK);
        assert!(first.contains("preserved memory count: 1 | preserved file counter: 1 | preserved rootfs counter: 1\n"), "{first}");
        let (_, second) = call(router(st), "/anything").await;
        assert!(second.contains("preserved memory count: 2 | preserved file counter: 2 | preserved rootfs counter: 2\n"), "{second}");
    }

    #[test]
    fn second_counter_and_file_content() {
        let dir = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let probe = dir.path().join("probe");
        std::fs::write(&probe, "hi").unwrap();
        let st = State::new(Paths {
            file_counter_dir: dir.path().to_path_buf(),
            second_file_counter_dir: Some(other.path().to_path_buf()),
            rootfs_counter_file: dir.path().join("rootfs.txt"),
            validate_existing_file: Some(probe),
        });
        let body = count_response(&st);
        assert!(
            body.ends_with(" | preserved second file counter: 1 | file content: hi\n"),
            "{body}"
        );
    }

    #[test]
    fn unreadable_validation_file() {
        let dir = tempfile::tempdir().unwrap();
        let st = State::new(Paths {
            file_counter_dir: dir.path().to_path_buf(),
            second_file_counter_dir: None,
            rootfs_counter_file: dir.path().join("rootfs.txt"),
            validate_existing_file: Some(dir.path().join("nope")),
        });
        assert!(count_response(&st).starts_with("failed to read test file: "));
    }

    #[tokio::test]
    async fn readyz_follows_flag() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        assert_eq!(
            call(router(st.clone()), "/readyz").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        st.mark_ready();
        assert_eq!(
            call(router(st), "/readyz").await,
            (StatusCode::OK, "ok\n".into())
        );
    }

    #[tokio::test]
    async fn sigterm_sleep_parameter() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        for (uri, want) in [
            ("/set-sigterm-sleep", StatusCode::BAD_REQUEST),
            ("/set-sigterm-sleep?duration=", StatusCode::BAD_REQUEST),
            ("/set-sigterm-sleep?duration=-1", StatusCode::BAD_REQUEST),
            ("/set-sigterm-sleep?duration=abc", StatusCode::BAD_REQUEST),
            ("/set-sigterm-sleep?duration=3", StatusCode::OK),
        ] {
            assert_eq!(call(router(st.clone()), uri).await.0, want, "{uri}");
        }
        assert_eq!(st.sigterm_sleep_secs(), 3);
    }

    #[tokio::test]
    async fn extra_port_names_itself() {
        let (s, body) = call(extra_port_router(9090), "/x").await;
        assert_eq!(s, StatusCode::OK);
        assert!(body.starts_with("hello from extra port 9090 on pod "));
    }

    #[tokio::test]
    async fn tcp_echo_round_trip() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(tcp_echo(l));
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        c.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        c.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
    }
}
