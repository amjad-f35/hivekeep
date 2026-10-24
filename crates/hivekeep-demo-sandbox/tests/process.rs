//! The workload and client against each other over loopback.

use hivekeep_demo_sandbox::client::{run_command, ActorRef, ClientError};
use hivekeep_demo_sandbox::server::router;
use std::net::SocketAddr;

async fn serve() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, router()).await });
    addr
}

fn actor() -> ActorRef {
    ActorRef {
        hive: "h".into(),
        name: "a".into(),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn client_runs_command_through_server() {
    let addr = serve().await;
    let out = run_command(
        &reqwest::Client::new(),
        &addr.to_string(),
        &actor(),
        "echo hi; exit 2",
    )
    .await
    .unwrap();
    assert_eq!(out.stdout, "hi\n");
    assert_eq!(out.exit_code, 2);
}

#[tokio::test]
async fn server_rejects_bad_bodies() {
    let addr = serve().await;
    let http = reqwest::Client::new();
    for (body, want) in [
        ("not json", "Bad request"),
        (r#"{"command":[]}"#, "Command required"),
        (
            r#"{"command":["true"],"timeout":"soon"}"#,
            "Invalid timeout",
        ),
    ] {
        let r = http
            .post(format!("http://{addr}/process"))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "{body}");
        assert!(r.text().await.unwrap().starts_with(want), "{body}");
    }
    let r = http
        .get(format!("http://{addr}/process"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 405);
}

#[tokio::test]
async fn client_reports_non_200() {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let app = axum::Router::new()
        .fallback(|| async { (axum::http::StatusCode::SERVICE_UNAVAILABLE, "no actor") });
    tokio::spawn(async move { axum::serve(l, app).await });
    let err = run_command(&reqwest::Client::new(), &addr.to_string(), &actor(), "true")
        .await
        .unwrap_err();
    match err {
        ClientError::Status { status, body } => {
            assert_eq!((status, body.as_str()), (503, "no actor"))
        }
        other => panic!("{other}"),
    }
}

#[tokio::test]
async fn wire_names_are_stable() {
    let resp = hivekeep_demo_sandbox::wire::ProcessResponse {
        exit_code: 1,
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_string(&resp).unwrap(),
        r#"{"stdout":"","stderr":"","exitCode":1}"#
    );
    let req: hivekeep_demo_sandbox::wire::ProcessRequest =
        serde_json::from_str(r#"{"command":["a"],"envvars":{"K":"V"},"cwd":"/","timeout":"1s"}"#)
            .unwrap();
    assert_eq!(req.env_vars["K"], "V");
}
