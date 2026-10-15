use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::*;
use crate::router::ingress::parking::ParkingLot;
use crate::router::ingress::{ParkedRequestConfig, DEFAULT_PARKED_REQUEST_MAX};
use crate::router::loopback::http_request;

fn source_with_one_query() -> StatusSource {
    let recorder = Arc::new(QueryRecorder::new(100));
    recorder.add(RecordedQuery {
        timestamp: chrono::Utc::now(),
        client: "127.0.0.1".into(),
        host: "example.com".into(),
        path: "/v1/test".into(),
        method: "GET".into(),
        action: "Matched test-actor".into(),
        target: "10.0.0.5".into(),
        duration: Duration::from_millis(10),
    });
    let lot = Arc::new(ParkingLot::new(ParkedRequestConfig::default(), None));
    StatusSource {
        namespace: "default".into(),
        mode: "ingress".into(),
        https_port: 8443,
        xds_port: 18000,
        extproc_port: 50051,
        queries: Some(recorder),
        parking: Some(Arc::new(move || lot.status())),
        ..Default::default()
    }
}

#[tokio::test]
async fn statusz_endpoint_serves_html_and_json() {
    let ln = listen_status("127.0.0.1", 0).await.unwrap();
    let addr = ln.local_addr().unwrap().to_string();
    let mut source = source_with_one_query();
    source.status_port = ln.local_addr().unwrap().port();
    let cancel = CancellationToken::new();
    let srv = tokio::spawn(serve_status(ln, Arc::new(source), cancel.clone()));

    let (status, html) = http_request(&addr, http::Method::GET, "/statusz", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(status, 200);
    assert!(
        html.contains("gate Router Status"),
        "status page missing its header"
    );
    assert!(
        html.contains("Matched test-actor"),
        "recorded query missing from the HTML"
    );
    assert!(
        html.contains("Request Parking"),
        "status page missing the Request Parking section"
    );
    assert!(html.contains("Offline Mode (No Cluster IP)"));

    let (status, json) = http_request(
        &addr,
        http::Method::GET,
        "/statusz?format=json",
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(status, 200);
    let d: serde_json::Value = serde_json::from_str(&json).unwrap();
    let queries = d["queries"].as_array().unwrap();
    assert_eq!(queries.len(), 1, "queries = {queries:?}");
    assert_eq!(queries[0]["target"], "10.0.0.5");
    assert_eq!(queries[0]["duration"], "10.00 ms");
    assert_eq!(
        d["parking"]["enabled"], true,
        "parking should be reported enabled"
    );
    assert_eq!(d["parking"]["max_parked"], DEFAULT_PARKED_REQUEST_MAX);
    assert_eq!(d["port_https"], 8443);
    assert_eq!(d["mode"], "ingress");
    assert!(d["health"]["hivekeep_api"].is_object());

    let (status, _) = http_request(&addr, http::Method::GET, "/other", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(status, 404);
    cancel.cancel();
    srv.await.unwrap().unwrap();
}

#[tokio::test]
async fn statusz_honors_accept_header() {
    let source = source_with_one_query();
    let resp = handle_statusz(&source, "application/json, text/plain", None).await;
    assert_eq!(
        resp.headers()[http::header::CONTENT_TYPE],
        "application/json"
    );
    let resp = handle_statusz(&source, "text/html", Some("format=html")).await;
    assert_eq!(
        resp.headers()[http::header::CONTENT_TYPE],
        "text/html; charset=utf-8"
    );
}

#[tokio::test]
async fn statusz_escapes_recorded_values_and_reports_missing_parking() {
    let recorder = Arc::new(QueryRecorder::new(10));
    recorder.add(RecordedQuery {
        timestamp: chrono::Utc::now(),
        client: "c".into(),
        host: "h".into(),
        path: "/<script>alert(1)</script>".into(),
        method: "GET".into(),
        action: "a".into(),
        target: "t".into(),
        duration: Duration::from_micros(1234),
    });
    let source = StatusSource {
        queries: Some(recorder),
        router_ip: Some(Arc::new(
            || -> futures::future::BoxFuture<'static, String> {
                Box::pin(async { "10.96.0.10".to_string() })
            },
        )),
        ..Default::default()
    };
    let d = source.dashboard().await;
    assert_eq!(d.router_cluster_ip, "10.96.0.10");
    assert!(!d.parking.enabled);
    assert_eq!(d.queries[0].duration, "1.23 ms");
    let html = render_html(&d);
    assert!(
        !html.contains("<script>alert"),
        "query path was not escaped"
    );
    assert!(html.contains("&lt;script&gt;"));
    assert!(html.contains("Disabled"));
}

// The status page must stay off the pod IP by default: binding the loopback
// default yields a loopback socket.
#[tokio::test]
async fn status_listener_binds_loopback() {
    let ln = listen_status("127.0.0.1", 0).await.unwrap();
    assert!(ln.local_addr().unwrap().ip().is_loopback());
    let err = listen_status("256.0.0.1", 0).await.unwrap_err();
    assert!(
        err.contains("failed binding Router HTTP status server address"),
        "{err}"
    );
}
