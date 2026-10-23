//! Reading the metrics pipeline: the OpenTelemetry Collector's Prometheus
//! exporter and the egress Envoys' admin stats.

use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};

use crate::env::system_namespace;
use crate::install;
use crate::kube_util::is_pod_ready;
use crate::portforward;

/// Namespace of the bundled OpenTelemetry Collector.
pub const COLLECTOR_NAMESPACE: &str = "otel-system";
/// Its Service.
pub const COLLECTOR_SERVICE: &str = "opentelemetry-collector";
/// Its Prometheus exporter port.
pub const COLLECTOR_PROM_PORT: u16 = 8889;

/// Prometheus metric-name prefixes (OTLP dots mapped to underscores) the
/// platform must emit. The exporter appends unit and type suffixes
/// (`_seconds_bucket`, `_bytes_count`), so matching is by prefix.
pub const PLATFORM_METRIC_PREFIXES: &[&str] = &[
    "hivekeep_workerpool_workers",
    "hivekeep_workerpool_desired_workers",
    "hivekeep_workerpool_ready_workers",
    "hivekeep_actor_crashes",
    "hivekeep_actor_lifecycle_operation_duration",
    "hivekeep_scheduler_assignment_duration",
    "hivekeep_actor_restore_duration",
    "hivekeep_actor_checkpoint_duration",
    "hivekeep_keeper_snapshot_size",
    "hivekeep_keeper_snapshot_upload_overdue_actors",
    "hivekeep_keeper_snapshot_undurable_age_max",
    "hivekeep_gate_router_route_duration",
    "hivekeep_filecache_size",
];

/// The route-duration prefix the Envoy dataplane exports through the
/// collector.
pub const ROUTE_DURATION_PREFIX: &str = "hivekeep_gate_router_route_duration";

/// The count connector's view of the Actor lifecycle events. Reading it
/// rather than a log store keeps the check on the metrics harness.
pub const LIFECYCLE_EVENT_METRIC: &str = "hivekeep_actor_state_changes";
/// The label carrying the Actor state on [`LIFECYCLE_EVENT_METRIC`].
pub const LIFECYCLE_STATE_LABEL: &str = "hk_actor_state";

async fn get_text(url: &str, what: &str) -> Result<String, String> {
    let client = reqwest_like::Client::new(Duration::from_secs(30));
    let (status, body) = client
        .get(url)
        .await
        .map_err(|e| format!("scraping {what}: {e}"))?;
    if status != 200 {
        return Err(format!("{what} returned {status}: {body}"));
    }
    Ok(body)
}

/// Reads the collector's Prometheus exporter surface.
pub async fn scrape_collector_metrics(kube: &kube::Client) -> Result<String, String> {
    let fwd = portforward::service_port_forward(
        kube,
        COLLECTOR_NAMESPACE,
        COLLECTOR_SERVICE,
        COLLECTOR_PROM_PORT,
    )
    .await?;
    get_text(
        &format!("http://{}/metrics", fwd.addr()),
        "collector metrics",
    )
    .await
}

/// Reads every ready egress Envoy's `stats/prometheus`, concatenated.
pub async fn scrape_egress_envoy_metrics(kube: &kube::Client) -> Result<String, String> {
    let ns = system_namespace();
    let pods = Api::<Pod>::namespaced(kube.clone(), &ns)
        .list(&ListParams::default().labels(install::EGRESS_APP_LABEL))
        .await
        .map_err(|e| format!("listing gate-egress pods: {e}"))?;
    let mut out = String::new();
    let mut ready = 0;
    for pod in pods.items.iter().filter(|p| is_pod_ready(p)) {
        ready += 1;
        let name = pod.metadata.name.clone().unwrap_or_default();
        let fwd = portforward::pod_port_forward(kube, &ns, &name, install::EGRESS_ENVOY_ADMIN_PORT)
            .await?;
        out.push_str(
            &get_text(
                &format!("http://{}/stats/prometheus", fwd.addr()),
                &format!("egress Envoy metrics on {name}"),
            )
            .await?,
        );
    }
    if ready == 0 {
        return Err(format!("no ready gate-egress pods in {ns}"));
    }
    Ok(out)
}

/// The prefixes with no matching series. A metric matches when its name
/// equals a prefix or starts with prefix + `_`; the underscore boundary
/// stops `..._restore` from matching `..._restored`.
pub fn missing_platform_metrics(scrape: &str, prefixes: &[&str]) -> Vec<String> {
    let mut present = std::collections::HashSet::new();
    for line in scrape.split('\n') {
        let name = metric_name_from_line(line);
        if name.is_empty() {
            continue;
        }
        for p in prefixes {
            if name == *p || name.starts_with(&format!("{p}_")) {
                present.insert(*p);
            }
        }
    }
    prefixes
        .iter()
        .filter(|p| !present.contains(*p))
        .map(|p| p.to_string())
        .collect()
}

/// Lifecycle series keyed by state and then by the series' full name and
/// labels. The counter is cumulative and the collector outlives any test,
/// so compare two reads rather than checking presence; series stay apart,
/// one per emitting instance, so one that expired does not look like a
/// decrease.
pub fn lifecycle_event_series(scrape: &str) -> BTreeMap<String, BTreeMap<String, f64>> {
    let mut series: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    for line in scrape.split('\n') {
        let name = metric_name_from_line(line);
        if name != LIFECYCLE_EVENT_METRIC
            && !name.starts_with(&format!("{LIFECYCLE_EVENT_METRIC}_"))
        {
            continue;
        }
        let state = prom_label_value(line, LIFECYCLE_STATE_LABEL);
        if state.is_empty() {
            continue;
        }
        let line = line.trim();
        let Some(i) = line.rfind([' ', '\t']) else {
            continue;
        };
        let Ok(v) = line[i + 1..].parse::<f64>() else {
            continue;
        };
        series
            .entry(state.to_string())
            .or_default()
            .insert(line[..i].trim().to_string(), v);
    }
    series
}

/// The states with no series that appeared or changed between two reads.
/// A drop counts (a restarted stream); a vanished series does not (the
/// exporter expires idle ones).
pub fn states_not_advanced(
    before: &BTreeMap<String, BTreeMap<String, f64>>,
    after: &BTreeMap<String, BTreeMap<String, f64>>,
    states: &[&str],
) -> Vec<String> {
    let empty = BTreeMap::new();
    states
        .iter()
        .filter(|s| {
            let b = before.get(**s).unwrap_or(&empty);
            !after
                .get(**s)
                .unwrap_or(&empty)
                .iter()
                .any(|(k, v)| b.get(k) != Some(v))
        })
        .map(|s| s.to_string())
        .collect()
}

/// Whether any of `services` has pushed telemetry: the exporter maps each
/// resource's `service.name` onto the `job` label.
pub fn collector_has_service(scrape: &str, services: &[&str]) -> bool {
    services
        .iter()
        .any(|svc| scrape.contains(&format!("job=\"{svc}\"")))
}

/// One resource attribute of `service` from `target_info`, or `""`.
pub fn target_info_label(scrape: &str, service: &str, label: &str) -> String {
    for line in scrape.split('\n') {
        if metric_name_from_line(line) != "target_info"
            || !line.contains(&format!("job=\"{service}\""))
        {
            continue;
        }
        let v = prom_label_value(line, label);
        if !v.is_empty() {
            return v.to_string();
        }
    }
    String::new()
}

/// One label's value on an exposition line.
pub fn prom_label_value<'a>(line: &'a str, label: &str) -> &'a str {
    let key = format!("{label}=\"");
    let Some(i) = line.find(&key) else { return "" };
    let rest = &line[i + key.len()..];
    rest.find('"').map(|end| &rest[..end]).unwrap_or("")
}

/// The metric name of an exposition line (`# HELP name`, `# TYPE name`,
/// or `name{labels} value`); `""` for blanks and other comments.
pub fn metric_name_from_line(line: &str) -> &str {
    let line = line.trim();
    if line.is_empty() {
        return "";
    }
    if line.starts_with('#') {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() >= 3 && (fields[1] == "HELP" || fields[1] == "TYPE") {
            return fields[2];
        }
        return "";
    }
    match line.find(['{', ' ', '\t']) {
        Some(i) => &line[..i],
        None => line,
    }
}

/// A minimal plaintext HTTP/1.1 GET client for scraping forwarded ports.
pub mod reqwest_like {
    use std::time::Duration;

    use http_body_util::{BodyExt, Empty};
    use hyper_util::rt::TokioIo;

    /// A GET client with a whole-request timeout.
    pub struct Client {
        timeout: Duration,
    }

    impl Client {
        /// A client that gives up after `timeout`.
        pub fn new(timeout: Duration) -> Self {
            Client { timeout }
        }

        /// GETs `url` (`http://host:port/path`) and returns status and body.
        pub async fn get(&self, url: &str) -> Result<(u16, String), String> {
            tokio::time::timeout(self.timeout, get(url))
                .await
                .map_err(|_| "timed out".to_string())?
        }
    }

    async fn get(url: &str) -> Result<(u16, String), String> {
        let uri: http::Uri = url.parse().map_err(|e| format!("{e}"))?;
        let authority = uri.authority().ok_or("URL has no host")?.to_string();
        let stream = tokio::net::TcpStream::connect(&authority)
            .await
            .map_err(|e| e.to_string())?;
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(conn);
        let path = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
        let req = http::Request::get(path)
            .header(http::header::HOST, authority)
            .body(Empty::<bytes::Bytes>::new())
            .map_err(|e| e.to_string())?;
        let resp = sender.send_request(req).await.map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| e.to_string())?
            .to_bytes();
        Ok((status, String::from_utf8_lossy(&body).into_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_SCRAPE: &str = r#"# HELP hivekeep_actor_lifecycle_operation_duration_seconds Duration of an actor lifecycle operation.
# TYPE hivekeep_actor_lifecycle_operation_duration_seconds histogram
hivekeep_actor_lifecycle_operation_duration_seconds_bucket{hk_actor_operation_name="resume",le="0.1"} 2
hivekeep_actor_lifecycle_operation_duration_seconds_count{hk_actor_operation_name="resume"} 2
# TYPE hivekeep_workerpool_workers gauge
hivekeep_workerpool_workers{hk_workerpool_name="pool-a",hk_worker_state="idle"} 3
# TYPE hivekeep_workerpool_desired_workers gauge
hivekeep_workerpool_desired_workers{hk_workerpool_name="pool-a",hk_workerpool_namespace="default"} 5
# TYPE hivekeep_workerpool_ready_workers gauge
hivekeep_workerpool_ready_workers{hk_workerpool_name="pool-a",hk_workerpool_namespace="default"} 5
# TYPE hivekeep_gate_router_route_duration_seconds histogram
hivekeep_gate_router_route_duration_seconds_count 1
# TYPE hivekeep_keeper_snapshot_size_bytes histogram
hivekeep_keeper_snapshot_size_bytes_count 1
# TYPE hivekeep_actor_restored_total counter
hivekeep_actor_restored_total 5
"#;

    #[test]
    fn missing_platform_metrics_cases() {
        let sorted = |mut v: Vec<String>| {
            v.sort();
            v
        };
        // all present via suffix, exact, and comment forms
        let all = [
            "hivekeep_actor_lifecycle_operation_duration",
            "hivekeep_workerpool_workers",
            "hivekeep_workerpool_desired_workers",
            "hivekeep_workerpool_ready_workers",
            "hivekeep_gate_router_route_duration",
            "hivekeep_keeper_snapshot_size",
        ];
        assert!(missing_platform_metrics(SAMPLE_SCRAPE, &all).is_empty());
        // absent prefix is reported
        assert_eq!(
            missing_platform_metrics(SAMPLE_SCRAPE, &["hivekeep_scheduler_assignment_duration"]),
            vec!["hivekeep_scheduler_assignment_duration"]
        );
        // underscore boundary avoids restore matching restored
        assert_eq!(
            missing_platform_metrics(SAMPLE_SCRAPE, &["hivekeep_actor_restore_duration"]),
            vec!["hivekeep_actor_restore_duration"]
        );
        // empty scrape misses everything
        assert_eq!(
            sorted(missing_platform_metrics(
                "",
                &[
                    "hivekeep_workerpool_workers",
                    "hivekeep_keeper_snapshot_size"
                ]
            )),
            vec![
                "hivekeep_keeper_snapshot_size",
                "hivekeep_workerpool_workers"
            ]
        );
    }

    #[test]
    fn metric_names_from_lines() {
        let cases = [
            (
                "hivekeep_workerpool_workers{pool=\"a\"} 3",
                "hivekeep_workerpool_workers",
            ),
            (
                "hivekeep_scheduler_assignment_duration_seconds_count 7",
                "hivekeep_scheduler_assignment_duration_seconds_count",
            ),
            (
                "# TYPE hivekeep_workerpool_workers gauge",
                "hivekeep_workerpool_workers",
            ),
            (
                "# HELP hivekeep_workerpool_workers Number of workers.",
                "hivekeep_workerpool_workers",
            ),
            ("   ", ""),
            ("# some other comment", ""),
        ];
        for (line, want) in cases {
            assert_eq!(metric_name_from_line(line), want, "{line:?}");
        }
    }

    const SAMPLE_NODE: &str = "kind-worker2";

    fn sample_target_info() -> String {
        format!(
            "# HELP target_info Target metadata\n# TYPE target_info gauge\n\
target_info{{job=\"keeper\",instance=\"pod-uid-1\",k8s_namespace_name=\"hivekeep-system\",k8s_pod_name=\"keeper-abc\",k8s_node_name=\"{SAMPLE_NODE}\"}} 1\n\
target_info{{job=\"hivekeep-sandbox-gvisor\",instance=\"pod-uid-2\",k8s_namespace_name=\"hivekeep-system\",k8s_pod_name=\"wp-xyz\",k8s_node_name=\"{SAMPLE_NODE}\"}} 1\n\
target_info{{job=\"hivekeep-controller\",instance=\"pod-uid-3\",k8s_namespace_name=\"hivekeep-system\",k8s_pod_name=\"ctrl-def\"}} 1\n\
hivekeep_workerpool_workers{{job=\"keeper\",hk_workerpool_name=\"pool-a\"}} 3\n"
        )
    }

    #[test]
    fn target_info_labels() {
        let s = sample_target_info();
        let cases = [
            (
                "node on the first resource",
                s.as_str(),
                "keeper",
                "k8s_node_name",
                SAMPLE_NODE,
            ),
            (
                "node on a later resource",
                s.as_str(),
                "hivekeep-sandbox-gvisor",
                "k8s_node_name",
                SAMPLE_NODE,
            ),
            (
                "label absent for that service",
                s.as_str(),
                "hivekeep-controller",
                "k8s_node_name",
                "",
            ),
            ("service absent", s.as_str(), "gate", "k8s_node_name", ""),
            (
                "other resource attributes still readable",
                s.as_str(),
                "keeper",
                "k8s_pod_name",
                "keeper-abc",
            ),
            (
                "a non-target_info series is never matched",
                s.as_str(),
                "keeper",
                "hk_workerpool_name",
                "",
            ),
            ("empty scrape", "", "keeper", "k8s_node_name", ""),
        ];
        for (name, scrape, svc, label, want) in cases {
            assert_eq!(target_info_label(scrape, svc, label), want, "{name}");
        }
        assert!(collector_has_service(&s, &["nope", "keeper"]));
        assert!(!collector_has_service(&s, &["nope"]));
    }

    const SAMPLE_LIFECYCLE: &str = r#"# HELP hivekeep_actor_state_changes_total Actor state change events received, by state.
# TYPE hivekeep_actor_state_changes_total counter
hivekeep_actor_state_changes_total{service_name="hivekeep-api",hk_actor_state="resuming"} 2
hivekeep_actor_state_changes_total{service_name="hivekeep-api",hk_actor_state="running"} 2
hivekeep_actor_state_changes_total{service_name="hivekeep-api",instance="other",hk_actor_state="running"} 3
hivekeep_actor_state_changes_total{service_name="hivekeep-api",hk_actor_state="crashed"} 1
# TYPE hivekeep_spans_total counter
hivekeep_spans_total{service_name="hivekeep-api"} 40
"#;

    #[test]
    fn lifecycle_series() {
        type S = BTreeMap<String, BTreeMap<String, f64>>;
        let s = |pairs: &[(&str, &[(&str, f64)])]| -> S {
            pairs
                .iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        v.iter().map(|(a, b)| (a.to_string(), *b)).collect(),
                    )
                })
                .collect()
        };
        let want = s(&[
            (
                "resuming",
                &[(
                    r#"hivekeep_actor_state_changes_total{service_name="hivekeep-api",hk_actor_state="resuming"}"#,
                    2.0,
                )],
            ),
            (
                "running",
                &[
                    (
                        r#"hivekeep_actor_state_changes_total{service_name="hivekeep-api",hk_actor_state="running"}"#,
                        2.0,
                    ),
                    (
                        r#"hivekeep_actor_state_changes_total{service_name="hivekeep-api",instance="other",hk_actor_state="running"}"#,
                        3.0,
                    ),
                ],
            ),
            (
                "crashed",
                &[(
                    r#"hivekeep_actor_state_changes_total{service_name="hivekeep-api",hk_actor_state="crashed"}"#,
                    1.0,
                )],
            ),
        ]);
        assert_eq!(
            lifecycle_event_series(SAMPLE_LIFECYCLE),
            want,
            "every counted state, one entry per instance"
        );
        assert!(
            lifecycle_event_series(SAMPLE_SCRAPE).is_empty(),
            "no events counted"
        );
        assert!(lifecycle_event_series("").is_empty(), "empty scrape");
        assert!(
            lifecycle_event_series(
                "hivekeep_actor_state_changes_total{service_name=\"hivekeep-api\"} 3\n"
            )
            .is_empty(),
            "no state label"
        );
    }

    #[test]
    fn states_not_advanced_cases() {
        type S = BTreeMap<String, BTreeMap<String, f64>>;
        let (a, b) = (r#"m{instance="a"}"#, r#"m{instance="b"}"#);
        let s = |pairs: &[(&str, &[(&str, f64)])]| -> S {
            pairs
                .iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        v.iter().map(|(x, y)| (x.to_string(), *y)).collect(),
                    )
                })
                .collect()
        };
        type Case<'a> = (&'a str, S, S, Vec<&'a str>, Vec<&'a str>);
        let cases: Vec<Case> = vec![
            (
                "every state advanced",
                s(&[("running", &[(a, 1.0)]), ("crashed", &[(a, 0.0)])]),
                s(&[("running", &[(a, 3.0)]), ("crashed", &[(a, 1.0)])]),
                vec!["running", "crashed"],
                vec![],
            ),
            (
                "a stale count from an earlier run is not an advance",
                s(&[("running", &[(a, 3.0)]), ("crashed", &[(a, 1.0)])]),
                s(&[("running", &[(a, 3.0)]), ("crashed", &[(a, 1.0)])]),
                vec!["running", "crashed"],
                vec!["running", "crashed"],
            ),
            (
                "a state that never appeared",
                s(&[]),
                s(&[("running", &[(a, 2.0)])]),
                vec!["running", "suspended"],
                vec!["suspended"],
            ),
            (
                "first observation of a state counts as an advance",
                s(&[]),
                s(&[("running", &[(a, 1.0)])]),
                vec!["running"],
                vec![],
            ),
            (
                "a new instance's series counts as an advance",
                s(&[("crashed", &[(a, 3.0)])]),
                s(&[("crashed", &[(a, 3.0), (b, 1.0)])]),
                vec!["crashed"],
                vec![],
            ),
            (
                "an expired series does not hide an advance on another",
                s(&[("crashed", &[(a, 3.0), (b, 1.0)])]),
                s(&[("crashed", &[(b, 2.0)])]),
                vec!["crashed"],
                vec![],
            ),
            (
                "an expired series alone is not an advance",
                s(&[("crashed", &[(a, 3.0), (b, 1.0)])]),
                s(&[("crashed", &[(b, 1.0)])]),
                vec!["crashed"],
                vec!["crashed"],
            ),
            (
                "a restarted stream counts as an advance",
                s(&[("crashed", &[(a, 3.0)])]),
                s(&[("crashed", &[(a, 1.0)])]),
                vec!["crashed"],
                vec![],
            ),
        ];
        for (name, before, after, states, want) in cases {
            assert_eq!(
                states_not_advanced(&before, &after, &states),
                want,
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn plaintext_get() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf).await;
            s.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nm 1\n\n")
                .await
                .unwrap();
        });
        let body = get_text(&format!("http://{addr}/metrics"), "test")
            .await
            .unwrap();
        assert_eq!(body, "m 1\n\n");
    }
}
