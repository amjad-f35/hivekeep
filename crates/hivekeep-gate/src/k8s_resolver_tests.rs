use std::time::Duration;

use k8s_openapi::api::discovery::v1::{EndpointConditions, EndpointPort};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::*;

/// An in-memory set of slices whose changes are streamed.
struct FakeSlices(watch::Sender<Vec<EndpointSlice>>);

impl FakeSlices {
    fn new(initial: Vec<EndpointSlice>) -> Arc<Self> {
        Arc::new(FakeSlices(watch::channel(initial).0))
    }
    fn create(&self, s: EndpointSlice) {
        self.0.send_modify(|v| v.push(s));
    }
}

impl SliceSource for Arc<FakeSlices> {
    fn watch(
        &self,
        namespace: &str,
        service: &str,
    ) -> BoxStream<'static, Result<Vec<EndpointSlice>, String>> {
        let (ns, svc) = (namespace.to_string(), service.to_string());
        let rx = self.0.subscribe();
        futures::stream::unfold((rx, true), |(mut rx, first)| async move {
            if !first && rx.changed().await.is_err() {
                return None;
            }
            let v = rx.borrow_and_update().clone();
            Some((v, (rx, false)))
        })
        .map(move |all| {
            Ok(all
                .into_iter()
                .filter(|s| {
                    s.metadata.namespace.as_deref() == Some(ns.as_str())
                        && s.metadata
                            .labels
                            .as_ref()
                            .and_then(|l| l.get(SERVICE_NAME_LABEL))
                            .map(String::as_str)
                            == Some(svc.as_str())
                })
                .collect())
        })
        .boxed()
    }
}

struct TestSink {
    states: mpsc::UnboundedSender<Vec<String>>,
    errors: Mutex<Vec<String>>,
}

impl ResolverSink for TestSink {
    fn update_state(&self, addrs: Vec<String>) -> Result<(), String> {
        let _ = self.states.send(addrs);
        Ok(())
    }
    fn report_error(&self, err: String) {
        self.errors.lock().push(err);
    }
}

fn sink() -> (Arc<TestSink>, mpsc::UnboundedReceiver<Vec<String>>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (
        Arc::new(TestSink {
            states: tx,
            errors: Mutex::default(),
        }),
        rx,
    )
}

async fn wait_for_addrs(rx: &mut mpsc::UnboundedReceiver<Vec<String>>, want: &[&str]) {
    let mut last = Vec::new();
    let deadline = tokio::time::sleep(Duration::from_secs(2));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            s = rx.recv() => {
                last = s.unwrap();
                if last == want {
                    return;
                }
            }
            _ = &mut deadline => panic!("addresses never converged: last = {last:?}, want {want:?}"),
        }
    }
}

fn slice(name: &str, ip: &str) -> EndpointSlice {
    EndpointSlice {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("hivekeep-system".into()),
            labels: Some([(SERVICE_NAME_LABEL.to_string(), "api".to_string())].into()),
            ..Default::default()
        },
        address_type: "IPv4".into(),
        ports: Some(vec![EndpointPort {
            port: Some(443),
            ..Default::default()
        }]),
        endpoints: Some(vec![Endpoint {
            addresses: vec![ip.into()],
            conditions: Some(EndpointConditions {
                ready: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        }]),
    }
}

#[test]
fn parse_target_cases() {
    for (name, host, path, want) in [
        (
            "slash format namespace/service:port",
            "",
            "/hivekeep-system/api:443",
            ("hivekeep-system", "api", "443"),
        ),
        (
            "host format",
            "hivekeep-system",
            "/api:443",
            ("hivekeep-system", "api", "443"),
        ),
        (
            "fqdn format",
            "",
            "/api.hivekeep-system.svc:443",
            ("hivekeep-system", "api", "443"),
        ),
        (
            "full cluster fqdn format",
            "",
            "/api.hivekeep-system.svc.cluster.local:443",
            ("hivekeep-system", "api", "443"),
        ),
        (
            "simple service without port",
            "",
            "/api",
            ("default", "api", "443"),
        ),
    ] {
        let t = parse_target_parts(host, path).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(
            (t.namespace.as_str(), t.service.as_str(), t.port.as_str()),
            want,
            "{name}"
        );
    }
    assert_eq!(
        parse_target("k8s:///hivekeep-system/api:443")
            .unwrap()
            .service,
        "api"
    );
    assert!(parse_target("dns:///api:443").is_err());
    assert!(parse_target_parts("", "/ns/").is_err());
}

#[tokio::test]
async fn endpoint_slice_updates() {
    let slices = FakeSlices::new(vec![slice("api-slice-1", "10.0.0.1")]);
    let (s, mut rx) = sink();
    let r = K8sResolver::build(&slices, "k8s:///hivekeep-system/api:443", s).unwrap();
    wait_for_addrs(&mut rx, &["10.0.0.1:443"]).await;
    slices.create(slice("api-slice-2", "10.0.0.2"));
    wait_for_addrs(&mut rx, &["10.0.0.1:443", "10.0.0.2:443"]).await;
    r.close();
}

#[tokio::test]
async fn close() {
    let slices = FakeSlices::new(vec![]);
    let (s, mut rx) = sink();
    let r = K8sResolver::build(&slices, "k8s:///hivekeep-system/api:443", s.clone()).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    r.close();
    while rx.try_recv().is_ok() {}
    r.resolve_now();
    slices.create(slice("late", "10.0.0.9"));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(rx.try_recv().is_err(), "state update after close");
    assert!(s.errors.lock().is_empty(), "error report after close");
}

#[test]
fn health_and_ports() {
    let mut s = slice("s", "10.0.0.1");
    s.ports = None;
    s.endpoints.get_or_insert_default().push(Endpoint {
        addresses: vec!["10.0.0.3".into()],
        conditions: Some(EndpointConditions {
            ready: Some(true),
            serving: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    });
    s.endpoints.get_or_insert_default().push(Endpoint {
        addresses: vec!["10.0.0.1".into(), "fd00::1".into()],
        ..Default::default()
    });
    assert_eq!(
        addresses(&[s.clone()], "0"),
        ["10.0.0.1:443", "[fd00::1]:443"]
    );
    assert_eq!(addresses(&[s], "9090"), ["10.0.0.1:9090", "[fd00::1]:9090"]);
}

#[tokio::test]
#[ignore = "needs a live cluster (KUBECONFIG) with the hivekeep-api Service"]
async fn resolves_against_a_live_cluster() {
    let client = kube::Client::try_default().await.unwrap();
    let (s, mut rx) = sink();
    let _r = K8sResolver::build_kube(client, "k8s:///hivekeep-system/hivekeep-api:443", s).unwrap();
    tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .unwrap()
        .unwrap();
}
