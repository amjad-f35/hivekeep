use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::router::status::{handle_statusz, StatusSource};

/// A dataplane probe answering from a closure.
struct FnProbe<F>(F);

impl<F> DataplaneProbe for FnProbe<F>
where
    F: Fn(String) -> BoxFuture<'static, Result<(u16, String), String>> + Send + Sync + 'static,
{
    fn get(&self, addr: &str, path: &str) -> BoxFuture<'static, Result<(u16, String), String>> {
        (self.0)(format!("http://{addr}{path}"))
    }
}

fn healthy_dataplane() -> Arc<dyn DataplaneProbe> {
    Arc::new(FnProbe(|_| -> BoxFuture<'static, _> {
        Box::pin(async { Ok((200, "LIVE".to_string())) })
    }))
}

/// A Kubernetes probe answering from a closure; `None` models a client
/// without a discovery endpoint.
struct FnK8s<F>(Option<F>);

impl<F> K8sProbe for FnK8s<F>
where
    F: Fn() -> BoxFuture<'static, Result<String, String>> + Send + Sync + 'static,
{
    fn version(&self) -> Option<BoxFuture<'static, Result<String, String>>> {
        self.0.as_ref().map(|f| f())
    }
}

struct FnApi<F>(F);

impl<F> ApiProbe for FnApi<F>
where
    F: Fn() -> BoxFuture<'static, Result<(), String>> + Send + Sync + 'static,
{
    fn list_actors(&self, req: pb::ListActorsRequest) -> BoxFuture<'static, Result<(), String>> {
        assert_eq!(req.options.map(|o| o.page_size), Some(1));
        (self.0)()
    }
}

fn ingress(dataplane: Dataplane, interval: Duration) -> HealthConfig {
    HealthConfig {
        serves_ingress: true,
        dataplane,
        interval,
    }
}

/// A Kubernetes probe that signals `started` and answers once `release`
/// fires (never, if it does not).
fn gated_k8s(
    started: mpsc::UnboundedSender<&'static str>,
    release: CancellationToken,
) -> Arc<dyn K8sProbe> {
    Arc::new(FnK8s(Some(
        move || -> BoxFuture<'static, Result<String, String>> {
            let (started, release) = (started.clone(), release.clone());
            Box::pin(async move {
                let _ = started.send("k8s");
                release.cancelled().await;
                Ok("v1.36.1".to_string())
            })
        },
    )))
}

#[tokio::test]
async fn check_dataplane_per_dataplane_kind() {
    let cases = [(
        "envoy",
        Dataplane::Envoy,
        "http://localhost:9901/ready",
        "LIVE",
        "LIVE",
    )];
    for (name, dp, want_url, response, want_message) in cases {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let s = seen.clone();
        let rh = RouterHealth::new(ingress(dp, Duration::from_secs(1)), None, None)
            .with_dataplane_probe(Arc::new(FnProbe(
                move |url: String| -> BoxFuture<'static, _> {
                    s.lock().push(url);
                    Box::pin(async move { Ok((200, response.to_string())) })
                },
            )));
        let (healthy, message) = rh.check_dataplane().await;
        assert_eq!(seen.lock().as_slice(), [want_url], "{name}: health URL");
        assert!(
            healthy && message == want_message,
            "{name}: check_dataplane() = ({healthy}, {message:?}), want (true, {want_message:?})"
        );
    }
}

// An egress-only router has no ingress dataplane beside it; probing one
// would report a dependency that is permanently down.
#[tokio::test]
async fn check_dataplane_skipped_in_egress_mode() {
    let cfg = HealthConfig {
        serves_ingress: false,
        dataplane: Dataplane::Envoy,
        interval: Duration::from_secs(1),
    };
    let rh = RouterHealth::new(cfg, None, None).with_dataplane_probe(Arc::new(FnProbe(
        |_| -> BoxFuture<'static, _> {
            panic!("check_dataplane dialed the ingress dataplane admin port in egress mode")
        },
    )));
    let (healthy, msg) = rh.check_dataplane().await;
    assert!(
        healthy,
        "check_dataplane healthy = false, want true (skipped)"
    );
    assert!(
        msg.contains("Skipped"),
        "message = {msg:?}, want it to report the check was skipped"
    );
}

#[tokio::test]
async fn check_k8s_times_out() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let rh = RouterHealth::new(
        ingress(Dataplane::Envoy, Duration::from_secs(1)),
        Some(gated_k8s(tx, CancellationToken::new())),
        None,
    );
    let started = Instant::now();
    let (healthy, msg) = rh.check_k8s().await;
    let elapsed = started.elapsed();
    assert!(!healthy, "check_k8s returned healthy for a stalled request");
    assert!(
        msg.contains("context deadline exceeded"),
        "message = {msg:?}"
    );
    assert!(
        elapsed <= 3 * DEPENDENCY_HEALTH_CHECK_TIMEOUT,
        "check_k8s took {elapsed:?}"
    );
}

#[tokio::test]
async fn check_k8s_without_rest_client() {
    type NoVersion = fn() -> BoxFuture<'static, Result<String, String>>;
    let k8s: Arc<dyn K8sProbe> = Arc::new(FnK8s::<NoVersion>(None));
    let rh = RouterHealth::new(
        ingress(Dataplane::Envoy, Duration::from_secs(1)),
        Some(k8s),
        None,
    );
    let (healthy, msg) = rh.check_k8s().await;
    assert!(
        !healthy,
        "check_k8s returned healthy without a discovery client"
    );
    assert_eq!(msg, "Kubernetes discovery REST client is unavailable");
}

#[tokio::test]
async fn health_check_does_not_block_report_or_statusz() {
    let (tx, mut started) = mpsc::unbounded_channel();
    let release = CancellationToken::new();
    let _guard = release.clone().drop_guard();
    let rh = Arc::new(
        RouterHealth::new(
            ingress(Dataplane::Envoy, Duration::from_secs(1)),
            Some(gated_k8s(tx, release.clone())),
            None,
        )
        .with_dataplane_probe(healthy_dataplane()),
    );
    let check = tokio::spawn({
        let rh = rh.clone();
        async move { rh.check().await }
    });
    tokio::time::timeout(Duration::from_secs(1), started.recv())
        .await
        .expect("Kubernetes health request did not start");

    let report = tokio::task::spawn_blocking({
        let rh = rh.clone();
        move || rh.report()
    });
    tokio::time::timeout(Duration::from_millis(100), report)
        .await
        .expect("report blocked while a dependency health request was in flight")
        .unwrap();

    let source = StatusSource {
        mode: "ingress".into(),
        health: Some(rh.clone()),
        ..Default::default()
    };
    let resp = tokio::time::timeout(
        Duration::from_millis(200),
        handle_statusz(&source, "", Some("format=json")),
    )
    .await
    .expect("/statusz blocked while a health check was in flight");
    assert_eq!(resp.status(), http::StatusCode::OK);

    release.cancel();
    tokio::time::timeout(Duration::from_secs(1), check)
        .await
        .expect("health check did not finish after the Kubernetes response was released")
        .unwrap();

    let r = rh.report();
    assert!(
        r.dataplane.healthy && r.dataplane.success_count == 1 && r.dataplane.last_success.is_some(),
        "dataplane = {:?}",
        r.dataplane
    );
    assert!(
        r.k8s_api.healthy && r.k8s_api.success_count == 1 && r.k8s_api.last_success.is_some(),
        "k8s = {:?}",
        r.k8s_api
    );
    assert_eq!(r.k8s_api.message, "Version: v1.36.1");
    assert!(
        !r.hivekeep_api.healthy
            && r.hivekeep_api.failure_count == 1
            && r.hivekeep_api.last_failure.is_some(),
        "Hivekeep API = {:?}",
        r.hivekeep_api
    );
}

#[tokio::test]
async fn health_checks_run_concurrently() {
    let (tx, mut started) = mpsc::unbounded_channel();
    let release = CancellationToken::new();
    let _guard = release.clone().drop_guard();

    let api_tx = tx.clone();
    let api_release = release.clone();
    let api: Arc<dyn ApiProbe> =
        Arc::new(FnApi(move || -> BoxFuture<'static, Result<(), String>> {
            let (tx, release) = (api_tx.clone(), api_release.clone());
            Box::pin(async move {
                let _ = tx.send("hivekeep-api");
                release.cancelled().await;
                Ok(())
            })
        }));
    let dp_tx = tx.clone();
    let dp_release = release.clone();
    let dataplane = Arc::new(FnProbe(move |_| -> BoxFuture<'static, _> {
        let (tx, release) = (dp_tx.clone(), dp_release.clone());
        Box::pin(async move {
            let _ = tx.send("dataplane");
            release.cancelled().await;
            Ok((200, "LIVE".to_string()))
        })
    }));
    let rh = Arc::new(
        RouterHealth::new(
            ingress(Dataplane::Envoy, Duration::from_secs(1)),
            Some(gated_k8s(tx, release.clone())),
            Some(api),
        )
        .with_dataplane_probe(dataplane),
    );
    let check = tokio::spawn({
        let rh = rh.clone();
        async move { rh.check().await }
    });

    let mut seen = std::collections::BTreeSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while seen.len() < 3 {
        match tokio::time::timeout_at(deadline, started.recv()).await {
            Ok(Some(dep)) => {
                seen.insert(dep);
            }
            _ => panic!("started health checks = {seen:?}, want all three before any finishes"),
        }
    }
    release.cancel();
    tokio::time::timeout(Duration::from_secs(1), check)
        .await
        .expect("health check did not finish after dependencies were released")
        .unwrap();
    let r = rh.report();
    assert!(
        r.dataplane.healthy && r.k8s_api.healthy && r.hivekeep_api.healthy,
        "report = {r:?}"
    );
}

#[tokio::test]
async fn health_run_stops_when_k8s_check_is_canceled() {
    let (tx, mut started) = mpsc::unbounded_channel();
    let rh = Arc::new(
        RouterHealth::new(
            ingress(Dataplane::Envoy, Duration::from_secs(3600)),
            Some(gated_k8s(tx, CancellationToken::new())),
            None,
        )
        .with_dataplane_probe(healthy_dataplane()),
    );
    let cancel = CancellationToken::new();
    let run = tokio::spawn({
        let (rh, cancel) = (rh.clone(), cancel.clone());
        async move { rh.run(cancel).await }
    });
    tokio::time::timeout(Duration::from_secs(1), started.recv())
        .await
        .expect("Kubernetes health request did not start");
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .expect("health loop did not stop after cancellation")
        .unwrap();
}

#[tokio::test]
async fn probe_server_reports_liveness_and_readiness() {
    let ln = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap().to_string();
    let readiness = Arc::new(Readiness::default());
    let cancel = CancellationToken::new();
    let srv = tokio::spawn(serve_probes(ln, readiness.clone(), cancel.clone()));
    let get = |path: &'static str| {
        let addr = addr.clone();
        async move {
            http_request(&addr, http::Method::GET, path, Duration::from_secs(2))
                .await
                .unwrap()
                .0
        }
    };
    assert_eq!(get("/healthz").await, 200);
    assert_eq!(get("/readyz").await, 503);
    readiness.mark_ready();
    assert_eq!(get("/readyz").await, 200);
    readiness.mark_not_ready();
    assert_eq!(get("/readyz").await, 503);
    assert_eq!(get("/other").await, 404);
    cancel.cancel();
    srv.await.unwrap().unwrap();
}
