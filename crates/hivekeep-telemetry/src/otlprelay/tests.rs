//! Relay tests against a fake collector.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{Metric, ResourceMetrics, ScopeMetrics};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use tonic::metadata::MetadataMap;
use tonic::{Code, Request};

use super::config::{
    ENDPOINT_ENV, HEADERS_ENV, LOGS_HEADERS_ENV, METRICS_ENDPOINT_ENV, METRICS_HEADERS_ENV,
    TRACES_ENDPOINT_ENV,
};
use super::gate::tests::service_resource;
use super::*;
use crate::env::MapEnv;
use crate::testutil::{start_fake_collector, FakeCollector};

const WAIT: Duration = Duration::from_secs(5);

async fn wait_for_socket(sock: &Path) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while tokio::time::Instant::now() < deadline {
        if tokio::net::UnixStream::connect(sock).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("relay socket {} never became connectable", sock.display());
}

struct Relay {
    server: Arc<Server>,
    sock: PathBuf,
    _dir: tempfile::TempDir,
}

impl Relay {
    async fn stop(&self) {
        self.server.stop().await;
    }
}

async fn start_relay_with(env: MapEnv) -> Relay {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("r.sock");
    let server = Arc::new(
        Server::new(&env, &sock)
            .unwrap()
            .expect("a collector endpoint is set"),
    );
    let s = server.clone();
    tokio::spawn(async move { s.serve().await });
    wait_for_socket(&sock).await;
    Relay {
        server,
        sock,
        _dir: dir,
    }
}

async fn start_relay(collector: std::net::SocketAddr) -> Relay {
    start_relay_with(MapEnv::new([(
        ENDPOINT_ENV,
        collector.to_string().as_str(),
    )]))
    .await
}

fn client(sock: &Path) -> tonic::transport::Channel {
    dial(sock)
        .unwrap()
        .expect("a channel for an existing socket")
}

fn traces(service: &str) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: service_resource(service),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    name: "RunWorkload".into(),
                    trace_id: b"0123456789abcdef".to_vec(),
                    span_id: b"01234567".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn metrics(service: &str) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: service_resource(service),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "worker.workload.runs".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn logs(service: &str) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: service_resource(service),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    event_name: "hk.actor.usage_sampled".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[tokio::test]
async fn relay_forwards_traces_verbatim() {
    let (c, addr) = start_fake_collector().await;
    let relay = start_relay(addr).await;
    let req = traces("hivekeep-sandbox-microvm");
    TraceServiceClient::new(client(&relay.sock))
        .export(req.clone())
        .await
        .unwrap();
    assert!(
        c.wait(WAIT).await,
        "the collector never received the trace export"
    );
    assert_eq!(c.received.lock().unwrap().traces, vec![req]);
    relay.stop().await;
}

#[tokio::test]
async fn relay_forwards_metrics() {
    let (c, addr) = start_fake_collector().await;
    let relay = start_relay(addr).await;
    let req = metrics("hivekeep-sandbox-microvm");
    MetricsServiceClient::new(client(&relay.sock))
        .export(req.clone())
        .await
        .unwrap();
    assert!(c.wait(WAIT).await);
    assert_eq!(c.received.lock().unwrap().metrics, vec![req]);
    relay.stop().await;
}

#[tokio::test]
async fn relay_forwards_logs_verbatim() {
    let (c, addr) = start_fake_collector().await;
    let relay = start_relay(addr).await;
    let req = logs("hivekeep-sandbox-gvisor");
    LogsServiceClient::new(client(&relay.sock))
        .export(req.clone())
        .await
        .unwrap();
    assert!(c.wait(WAIT).await);
    assert_eq!(c.received.lock().unwrap().logs, vec![req]);
    relay.stop().await;
}

#[tokio::test]
async fn stop_removes_socket() {
    let (_c, addr) = start_fake_collector().await;
    let relay = start_relay(addr).await;
    relay.stop().await;
    assert!(!relay.sock.exists(), "the socket outlived stop");
}

#[tokio::test]
async fn serve_replaces_stale_socket() {
    let (_c, addr) = start_fake_collector().await;
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("r.sock");
    std::fs::write(&sock, b"").unwrap();
    let env = MapEnv::new([(ENDPOINT_ENV, addr.to_string().as_str())]);
    let server = Arc::new(Server::new(&env, &sock).unwrap().unwrap());
    let s = server.clone();
    tokio::spawn(async move { s.serve().await });
    wait_for_socket(&sock).await;
    server.stop().await;
}

#[tokio::test]
async fn new_server_disabled() {
    let env = MapEnv::new([(ENDPOINT_ENV, "otel-collector:4317")]);
    assert!(Server::new(&env, Path::new("")).unwrap().is_none());
}

#[tokio::test]
async fn new_server_without_collector() {
    let dir = tempfile::tempdir().unwrap();
    let env = MapEnv::new([
        (ENDPOINT_ENV, ""),
        (TRACES_ENDPOINT_ENV, ""),
        (METRICS_ENDPOINT_ENV, ""),
    ]);
    assert!(
        Server::new(&env, &dir.path().join("r.sock"))
            .unwrap()
            .is_none(),
        "it would accept spans and drop them"
    );
}

#[test]
fn dial_missing_socket_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    assert!(dial(&dir.path().join("absent.sock")).unwrap().is_none());
}

#[test]
fn dial_empty_path() {
    assert!(dial(Path::new("")).unwrap().is_none());
}

#[tokio::test]
async fn relay_refuses_non_worker_source() {
    let (c, addr) = start_fake_collector().await;
    let relay = start_relay(addr).await;
    let ch = client(&relay.sock);
    for (name, service) in [
        ("another component", "keeper"),
        ("actor telemetry", "actor"),
        ("no service name", ""),
    ] {
        let mut cli = TraceServiceClient::new(ch.clone());
        let err = cli.export(ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: service_resource(service),
                ..Default::default()
            }],
        });
        assert_eq!(
            err.await.unwrap_err().code(),
            Code::PermissionDenied,
            "{name}: traces"
        );
        let mut cli = MetricsServiceClient::new(ch.clone());
        let err = cli.export(ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: service_resource(service),
                ..Default::default()
            }],
        });
        assert_eq!(
            err.await.unwrap_err().code(),
            Code::PermissionDenied,
            "{name}: metrics"
        );
        let mut cli = LogsServiceClient::new(ch.clone());
        let err = cli.export(ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: service_resource(service),
                ..Default::default()
            }],
        });
        assert_eq!(
            err.await.unwrap_err().code(),
            Code::PermissionDenied,
            "{name}: logs"
        );
    }
    {
        let r = c.received.lock().unwrap();
        assert!(
            r.traces.is_empty() && r.metrics.is_empty() && r.logs.is_empty(),
            "refused sources were forwarded"
        );
    }
    relay.stop().await;
}

#[tokio::test]
async fn relay_refuses_mixed_batch() {
    let (c, addr) = start_fake_collector().await;
    let relay = start_relay(addr).await;
    let ch = client(&relay.sock);
    let err = TraceServiceClient::new(ch.clone())
        .export(ExportTraceServiceRequest {
            resource_spans: vec![
                ResourceSpans {
                    resource: service_resource("hivekeep-sandbox-gvisor"),
                    ..Default::default()
                },
                ResourceSpans {
                    resource: service_resource("actor"),
                    ..Default::default()
                },
            ],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
    let err = LogsServiceClient::new(ch)
        .export(ExportLogsServiceRequest {
            resource_logs: vec![
                ResourceLogs {
                    resource: service_resource("hivekeep-sandbox-gvisor"),
                    ..Default::default()
                },
                ResourceLogs {
                    resource: service_resource("actor"),
                    ..Default::default()
                },
            ],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
    {
        let r = c.received.lock().unwrap();
        assert!(
            r.traces.is_empty() && r.logs.is_empty(),
            "mixed batches must be refused whole"
        );
    }
    relay.stop().await;
}

#[tokio::test]
async fn relay_accepts_every_worker_service() {
    let (c, addr) = start_fake_collector().await;
    let relay = start_relay(addr).await;
    let ch = client(&relay.sock);
    for service in WORKER_SERVICES {
        TraceServiceClient::new(ch.clone())
            .export(traces(service))
            .await
            .unwrap();
        assert!(c.wait(WAIT).await, "{service}: traces");
        LogsServiceClient::new(ch.clone())
            .export(logs(service))
            .await
            .unwrap();
        assert!(c.wait(WAIT).await, "{service}: logs");
    }
    relay.stop().await;
}

#[test]
fn dial_rejects_relative_socket_path() {
    let err = dial(Path::new("relative/r.sock")).unwrap_err();
    assert!(err.to_string().contains("absolute"), "{err}");
}

#[tokio::test]
async fn new_server_rejects_relative_socket_path() {
    let env = MapEnv::new([(ENDPOINT_ENV, "collector:4317")]);
    let err = Server::new(&env, Path::new("relative/r.sock"))
        .err()
        .expect("an error");
    assert!(err.to_string().contains("absolute"), "{err}");
}

#[tokio::test]
async fn serve_leaves_a_populated_directory_alone() {
    let (_c, addr) = start_fake_collector().await;
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("basepath");
    std::fs::create_dir_all(&base).unwrap();
    let occupant = base.join("worker.sock");
    std::fs::write(&occupant, b"").unwrap();
    let env = MapEnv::new([(ENDPOINT_ENV, addr.to_string().as_str())]);
    let server = Server::new(&env, &base).unwrap().unwrap();
    assert!(
        server.serve().await.is_err(),
        "serving on a populated directory must fail"
    );
    assert!(occupant.exists(), "the neighbouring socket was touched");
}

async fn export_all(
    c: &FakeCollector,
    sock: &Path,
    service: &str,
    md: &MetadataMap,
) -> (MetadataMap, MetadataMap, MetadataMap) {
    let ch = client(sock);
    let with = |msg| {
        let mut r = Request::new(msg);
        *r.metadata_mut() = md.clone();
        r
    };
    TraceServiceClient::new(ch.clone())
        .export(with(ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: service_resource(service),
                ..Default::default()
            }],
        }))
        .await
        .unwrap();
    let mut r = Request::new(ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: service_resource(service),
            ..Default::default()
        }],
    });
    *r.metadata_mut() = md.clone();
    MetricsServiceClient::new(ch.clone())
        .export(r)
        .await
        .unwrap();
    let mut r = Request::new(ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: service_resource(service),
            ..Default::default()
        }],
    });
    *r.metadata_mut() = md.clone();
    LogsServiceClient::new(ch).export(r).await.unwrap();
    let r = c.received.lock().unwrap();
    (
        r.trace_md[0].clone(),
        r.metric_md[0].clone(),
        r.log_md[0].clone(),
    )
}

fn values(md: &MetadataMap, k: &str) -> Vec<String> {
    md.get_all(k)
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect()
}

fn client_md() -> MetadataMap {
    let mut md = MetadataMap::new();
    md.insert("authorization", "Bearer client-token".parse().unwrap());
    md.insert("custom-header", "custom-value".parse().unwrap());
    md
}

#[tokio::test]
async fn export_drops_client_metadata() {
    let (c, addr) = start_fake_collector().await;
    let relay = start_relay(addr).await;
    let (t, m, l) = export_all(&c, &relay.sock, "hivekeep-sandbox-gvisor", &client_md()).await;
    for (signal, md) in [("trace", t), ("metric", m), ("log", l)] {
        for key in ["authorization", "custom-header"] {
            assert!(
                values(&md, key).is_empty(),
                "{signal} export carried the client's {key}"
            );
        }
    }
    relay.stop().await;
}

#[tokio::test]
async fn export_attaches_keeper_headers() {
    let (c, addr) = start_fake_collector().await;
    let relay = start_relay_with(MapEnv::new([
        (ENDPOINT_ENV, addr.to_string().as_str()),
        (
            HEADERS_ENV,
            "authorization=Bearer keeper-token,x-tenant=hivekeep",
        ),
        (METRICS_HEADERS_ENV, "authorization=Bearer metrics-token"),
        (LOGS_HEADERS_ENV, "authorization=Bearer logs-token"),
    ]))
    .await;
    let (t, m, l) = export_all(&c, &relay.sock, "hivekeep-sandbox-gvisor", &client_md()).await;
    assert_eq!(values(&t, "authorization"), vec!["Bearer keeper-token"]);
    assert_eq!(values(&t, "x-tenant"), vec!["hivekeep"]);
    assert_eq!(values(&m, "authorization"), vec!["Bearer metrics-token"]);
    assert!(
        values(&m, "x-tenant").is_empty(),
        "the metrics variable replaces the generic one"
    );
    assert_eq!(values(&l, "authorization"), vec!["Bearer logs-token"]);
    assert!(
        values(&l, "x-tenant").is_empty(),
        "the logs variable replaces the generic one"
    );
    relay.stop().await;
}

#[tokio::test]
async fn new_server_rejects_unparseable_headers() {
    let (_c, addr) = start_fake_collector().await;
    let dir = tempfile::tempdir().unwrap();
    let env = MapEnv::new([
        (ENDPOINT_ENV, addr.to_string().as_str()),
        (HEADERS_ENV, "not-a-pair"),
    ]);
    assert!(Server::new(&env, &dir.path().join("r.sock")).is_err());
}

#[tokio::test]
async fn socket_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let (_c, addr) = start_fake_collector().await;
    let relay = start_relay(addr).await;
    let mode = std::fs::metadata(&relay.sock).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "socket permissions = {mode:o}");
    relay.stop().await;
}

/// The exporters' half: a sandbox worker's tracer exports through the relay
/// with its own identity intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_to_end_through_boot() {
    use crate::attrs::OTLP_RELAY_KEY;
    use crate::boot::sampling::parent_ratio_sampling;
    use crate::boot::tracing::{init_tracing_with_env, TracingOptions};
    use opentelemetry::trace::{Span as _, Tracer, TracerProvider};
    let (c, addr) = start_fake_collector().await;
    let relay = start_relay(addr).await;
    let ch = client(&relay.sock);
    // Pointing the worker's own endpoint nowhere proves the span took the relay.
    let worker_env = MapEnv::new([(ENDPOINT_ENV, "http://127.0.0.1:1")]);
    const SERVICE: &str = "hivekeep-sandbox-microvm";
    tokio::task::spawn_blocking(move || {
        let tp = init_tracing_with_env(
            &worker_env,
            TracingOptions {
                service_name: SERVICE.into(),
                sampling: parent_ratio_sampling(1.0),
                exporter_channel: Some(ch),
                relay_capable: true,
            },
        )
        .unwrap();
        tp.tracer("relay-e2e").start("RunWorkload").end();
        tp.shutdown().unwrap();
    })
    .await
    .unwrap();
    assert!(
        c.wait(Duration::from_secs(10)).await,
        "no span arrived through the relay"
    );
    let (service, span, via) = first_span(&c, OTLP_RELAY_KEY);
    assert_eq!(service, SERVICE, "the relay must not re-attribute the span");
    assert_eq!(span, "RunWorkload");
    assert_eq!(via, "relay");
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_to_end_falls_back_to_direct() {
    use crate::attrs::OTLP_RELAY_KEY;
    use crate::boot::sampling::parent_ratio_sampling;
    use crate::boot::tracing::{init_tracing_with_env, TracingOptions};
    use opentelemetry::trace::{Span as _, Tracer, TracerProvider};
    let (c, addr) = start_fake_collector().await;
    let dir = tempfile::tempdir().unwrap();
    let ch = dial(&dir.path().join("absent-keeper-otlp.sock")).unwrap();
    assert!(ch.is_none());
    let env = MapEnv::new([(ENDPOINT_ENV, format!("http://{addr}").as_str())]);
    tokio::task::spawn_blocking(move || {
        let tp = init_tracing_with_env(
            &env,
            TracingOptions {
                service_name: "hivekeep-sandbox-microvm".into(),
                sampling: parent_ratio_sampling(1.0),
                exporter_channel: ch,
                relay_capable: true,
            },
        )
        .unwrap();
        tp.tracer("relay-e2e").start("RunWorkload").end();
        tp.shutdown().unwrap();
    })
    .await
    .unwrap();
    assert!(
        c.wait(Duration::from_secs(10)).await,
        "no span arrived over the direct path"
    );
    let (service, _, via) = first_span(&c, OTLP_RELAY_KEY);
    assert_eq!(service, "hivekeep-sandbox-microvm");
    assert_eq!(via, "direct");
}

fn first_span(c: &FakeCollector, relay_key: &str) -> (String, String, String) {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    let r = c.received.lock().unwrap();
    let rs = &r.traces[0].resource_spans[0];
    let attr = |k: &str| {
        rs.resource
            .as_ref()
            .unwrap()
            .attributes
            .iter()
            .find(|kv| kv.key == k)
            .and_then(|kv| match kv.value.as_ref()?.value.as_ref()? {
                Value::StringValue(s) => Some(s.clone()),
                _ => None,
            })
            .unwrap_or_default()
    };
    let span = rs.scope_spans[0].spans[0].name.clone();
    (attr("service.name"), span, attr(relay_key))
}
