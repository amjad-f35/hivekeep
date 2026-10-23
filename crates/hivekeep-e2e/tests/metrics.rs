//! Drives an Actor lifecycle, then checks the platform metrics reach the
//! OpenTelemetry Collector, so a renamed or dropped instrument fails here
//! instead of surfacing as an empty dashboard. Needs the counter demo for
//! the sandbox class under test.

mod ops_support;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hivekeep_core::attr::{actor_state, normalize_operation_name};
use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::collector_metrics::{
    collector_has_service, lifecycle_event_series, missing_platform_metrics, prom_label_value,
    scrape_collector_metrics, states_not_advanced, target_info_label, PLATFORM_METRIC_PREFIXES,
};
use hivekeep_e2e::dataplane::Dataplane;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::sandbox::counter_fixture;
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_proto::api::v1 as pb;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams};
use ops_support::*;

const METRICS_HIVE: &str = "hivekeep-metrics-e2e";
/// The worker services whose pushes prove the worker plane reports.
const WORKER_SERVICES: &[&str] = &["hivekeep-sandbox-gvisor", "hivekeep-sandbox-microvm"];
/// The controller's service name.
const CONTROLLER_SERVICE: &str = "hivekeep-controller";
/// Node-scoped services that must carry `k8s_node_name`.
const NODE_SERVICES: &[&str] = &[
    "keeper",
    "hivekeep-sandbox-gvisor",
    "hivekeep-sandbox-microvm",
];

async fn resume(t: &T, id: &str) {
    resume_actor(t, METRICS_HIVE, id).await;
    wait_for_state(
        t,
        METRICS_HIVE,
        id,
        pb::ActorState::Running,
        Duration::from_secs(120),
        Duration::from_secs(2),
    )
    .await;
}

async fn suspend(t: &T, id: &str) {
    suspend_actor(t, METRICS_HIVE, id).await;
    wait_for_state(
        t,
        METRICS_HIVE,
        id,
        pb::ActorState::Suspended,
        Duration::from_secs(120),
        Duration::from_secs(2),
    )
    .await;
}

/// Deletes the Actor's worker pod; the worker sync notices, crashes the
/// Actor and counts the crash.
async fn trigger_actor_crash(t: &T, id: &str) {
    let actor = get_actor(METRICS_HIVE, id)
        .await
        .unwrap_or_else(|e| t.fatal(format!("GetActor failed: {e}")));
    let assignment = actor
        .status
        .and_then(|s| s.worker_assignment)
        .filter(|a| !a.worker_pod.is_empty());
    let Some(a) = assignment else {
        t.fatal(format!("Actor {id} has no assigned worker pod to delete"))
    };
    Api::<Pod>::namespaced(clients().kube(), &a.worker_namespace)
        .delete(&a.worker_pod, &DeleteParams::default())
        .await
        .unwrap_or_else(|e| t.fatal(format!("Delete worker pod failed: {e}")));
    wait_for_state(
        t,
        METRICS_HIVE,
        id,
        pb::ActorState::Crashed,
        Duration::from_secs(120),
        Duration::from_secs(2),
    )
    .await;
}

/// Checks the namespaced pool labels on every line of a workerpool gauge.
fn validate_pool_gauge(scrape: &str, metric: &str, errs: &mut Vec<String>) {
    let mut found = false;
    for line in scrape.split('\n').filter(|l| l.starts_with(metric)) {
        found = true;
        let ns = prom_label_value(line, "hk_workerpool_namespace");
        let pool = prom_label_value(line, "hk_workerpool_name");
        let mut line_errs = Vec::new();
        if ns.is_empty() {
            line_errs.push("hk_workerpool_namespace label is missing or empty");
        }
        if pool.is_empty() {
            line_errs.push("hk_workerpool_name label is missing or empty");
        }
        if !line_errs.is_empty() {
            errs.push(format!(
                "{metric} validation failed on line {line:?}: {} (extracted labels: hk_workerpool_namespace={ns:?}, hk_workerpool_name={pool:?})",
                line_errs.join("; ")
            ));
        }
    }
    if !found {
        errs.push(format!("{metric} validation failed: metric line not found in collector scrape text (no time series emitted by the controller callback)"));
    }
}

/// The crash counter must carry every low-cardinality label, non-empty.
fn validate_crash_labels(scrape: &str, errs: &mut Vec<String>) {
    let mut found = false;
    for line in scrape
        .split('\n')
        .filter(|l| l.starts_with("hivekeep_actor_crashes"))
    {
        found = true;
        let op = prom_label_value(line, "hk_actor_operation_name");
        let labels = [
            "hk_template_hive",
            "hk_template_name",
            "hk_workerpool_namespace",
            "hk_workerpool_name",
            "hk_sandbox_class",
        ];
        let mut crash_errs = Vec::new();
        if op.is_empty() {
            crash_errs.push("hk_actor_operation_name label is missing or empty".to_string());
        } else if normalize_operation_name(op) != op {
            crash_errs.push(format!("hk_actor_operation_name {op:?} is invalid (must be one of the bounded lifecycle operations)"));
        }
        // The pool keys identify one WorkerPool together; the suite crashes
        // an assigned Actor, so both are expected.
        for l in labels {
            if prom_label_value(line, l).is_empty() {
                crash_errs.push(format!("{l} label is missing or empty"));
            }
        }
        if !crash_errs.is_empty() {
            let extracted: Vec<String> = labels
                .iter()
                .map(|l| format!("{l}={:?}", prom_label_value(line, l)))
                .collect();
            errs.push(format!(
                "hivekeep_actor_crashes line {line:?} failed label validation:\n  - {}\n  (extracted labels: op={op:?}, {})",
                crash_errs.join("\n  - "),
                extracted.join(", ")
            ));
        }
    }
    if !found {
        errs.push("hivekeep_actor_crashes metric line not found in collector scrape output".into());
    }
}

/// The cold-start histograms derive kind and sandbox class from the
/// snapshot manifest and omit them when unresolved, so a broken derivation
/// keeps the metric but loses the labels that make it useful.
fn validate_snapshot_phase_labels(scrape: &str) -> Result<(), String> {
    for m in [
        "hivekeep_actor_restore_duration_seconds_count",
        "hivekeep_actor_checkpoint_duration_seconds_count",
    ] {
        let mut labelled = false;
        for line in scrape.split('\n').filter(|l| l.starts_with(m)) {
            if prom_label_value(line, "hk_snapshot_phase").is_empty() {
                return Err(format!("{m} line is missing hk_snapshot_phase: {line:?}"));
            }
            if [
                "hk_snapshot_kind",
                "hk_snapshot_fidelity",
                "hk_sandbox_class",
            ]
            .iter()
            .all(|l| !prom_label_value(line, l).is_empty())
            {
                labelled = true;
            }
        }
        if !labelled {
            return Err(format!("no {m} line carried all of hk_snapshot_kind, hk_snapshot_fidelity and hk_sandbox_class"));
        }
    }
    Ok(())
}

/// The node-scoped resources carry `k8s.node.name` (from the Downward API);
/// no deployed collector restores a dropped one.
fn validate_node_attribution(scrape: &str) -> Result<(), String> {
    let mut checked = 0;
    for svc in NODE_SERVICES {
        if !collector_has_service(scrape, &[svc]) {
            continue;
        }
        checked += 1;
        if target_info_label(scrape, svc, "k8s_node_name").is_empty() {
            return Err(format!("{svc} published telemetry with no k8s_node_name on its target_info; OTEL_RESOURCE_ATTRIBUTES lost k8s.node.name or its $(NODE_NAME) did not expand"));
        }
    }
    // Only one worker runtime runs per cluster; zero services means the
    // check proved nothing.
    if checked == 0 {
        return Err(
            "no node-scoped service reached the collector, so k8s_node_name went unchecked".into(),
        );
    }
    Ok(())
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn platform_metrics_emitted() {
    e2e_test("TestPlatformMetricsEmitted", |t| async move {
        let kube = clients().kube();
        let tmpl = counter_fixture();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let id = format!("metrics-probe-{nanos}");

        // The lifecycle counters are cumulative and the collector outlives
        // the test: take a baseline, or a previous run's counts would pass.
        let baseline = scrape_collector_metrics(&kube).await.unwrap_or_else(|e| {
            t.fatal(format!(
                "scraping the collector for the lifecycle baseline: {e}"
            ))
        });
        let lifecycle_baseline = lifecycle_event_series(&baseline);

        // CreateActor needs the hive; AlreadyExists is fine.
        let _ = clients()
            .api()
            .create_hive(pb::CreateHiveRequest {
                hive: Some(pb::Hive {
                    metadata: Some(pb::ResourceMetadata {
                        name: METRICS_HIVE.into(),
                        ..Default::default()
                    }),
                }),
            })
            .await;
        create_actor(&t, METRICS_HIVE, &id, object_ref(&tmpl.hive, &tmpl.name)).await;

        // A resume assigns a worker, which the worker-count observable reports.
        resume(&t, &id).await;
        // A routed request makes the dataplane record route duration.
        let rc = router(&t).await;
        rc.get(&ActorRef::new(METRICS_HIVE, &id), "/")
            .await
            .unwrap_or_else(|e| t.fatal(format!("router GET: {e}")));
        // The first resume restored the golden snapshot. Suspend writes the
        // Actor's own snapshot and the second resume reads it back, so the
        // checkpoint histogram gets a point and restore covers both kinds.
        suspend(&t, &id).await;
        resume(&t, &id).await;
        // Last, because it deletes the worker pod.
        trigger_actor_crash(&t, &id).await;

        // The crash is the one state only the control plane reports, which
        // proves the events carry more than the worker plane could.
        let want_states = [
            actor_state::RESUMING,
            actor_state::RUNNING,
            actor_state::SUSPENDED,
            actor_state::CRASHED,
        ];
        let dataplane = Dataplane::current();
        let prefixes = dataplane.platform_metric_prefixes(PLATFORM_METRIC_PREFIXES);
        let deadline = Instant::now() + Duration::from_secs(120);
        let (mut missing, mut missing_states) = (Vec::new(), Vec::new());
        let (mut worker_seen, mut controller_seen, mut route_seen) = (false, false, false);
        let mut last_label_err: Option<String> = None;
        while Instant::now() < deadline {
            let scrape = scrape_collector_metrics(&kube)
                .await
                .unwrap_or_else(|e| t.fatal(format!("scraping the collector: {e}")));
            missing = missing_platform_metrics(&scrape, &prefixes);
            route_seen = dataplane
                .route_duration_seen(&kube, &scrape)
                .await
                .unwrap_or_else(|e| t.fatal(format!("checking route-duration metric: {e}")));
            missing_states = states_not_advanced(
                &lifecycle_baseline,
                &lifecycle_event_series(&scrape),
                &want_states,
            );
            worker_seen = collector_has_service(&scrape, WORKER_SERVICES);
            // The controller's reconcile families prove its registry reaches
            // OTLP, not merely that some series arrived. Substring, because
            // the exporter may re-suffix names already ending in _total.
            controller_seen = collector_has_service(&scrape, &[CONTROLLER_SERVICE])
                && scrape.contains("controller_runtime_");
            if missing.is_empty()
                && worker_seen
                && controller_seen
                && route_seen
                && missing_states.is_empty()
            {
                let mut errs = Vec::new();
                validate_pool_gauge(&scrape, "hivekeep_workerpool_desired_workers", &mut errs);
                validate_pool_gauge(&scrape, "hivekeep_workerpool_ready_workers", &mut errs);
                validate_crash_labels(&scrape, &mut errs);
                if let Err(e) = validate_snapshot_phase_labels(&scrape) {
                    errs.push(e);
                }
                if let Err(e) = validate_node_attribution(&scrape) {
                    errs.push(e);
                }
                if errs.is_empty() {
                    return;
                }
                last_label_err = Some(format!(
                    "platform metrics label validation failed:\n  - {}",
                    errs.join("\n  - ")
                ));
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        if let Some(e) = last_label_err {
            t.fatal(format!(
                "platform telemetry validation failed: missing metrics {missing:?}, missing lifecycle states {missing_states:?}, worker pushed={worker_seen}, controller pushed={controller_seen}, error detail: {e}"
            ));
        }
        t.fatal(format!(
            "platform telemetry validation failed: collector missing metrics {missing:?}, missing lifecycle states {missing_states:?}, route duration seen={route_seen}, worker pushed={worker_seen}, controller pushed={controller_seen}"
        ));
    });
}

#[test]
fn label_validators() {
    let mut errs = Vec::new();
    validate_pool_gauge("hivekeep_workerpool_ready_workers{hk_workerpool_namespace=\"n\",hk_workerpool_name=\"p\"} 1\n", "hivekeep_workerpool_ready_workers", &mut errs);
    assert!(errs.is_empty(), "{errs:?}");
    validate_pool_gauge("", "hivekeep_workerpool_ready_workers", &mut errs);
    assert_eq!(errs.len(), 1);
    let good = "hivekeep_actor_restore_duration_seconds_count{hk_snapshot_phase=\"total\",hk_snapshot_kind=\"golden\",hk_snapshot_fidelity=\"full\",hk_sandbox_class=\"gvisor\"} 1\n\
hivekeep_actor_checkpoint_duration_seconds_count{hk_snapshot_phase=\"total\",hk_snapshot_kind=\"latest\",hk_snapshot_fidelity=\"full\",hk_sandbox_class=\"gvisor\"} 1\n";
    assert!(validate_snapshot_phase_labels(good).is_ok());
    assert!(
        validate_snapshot_phase_labels("hivekeep_actor_restore_duration_seconds_count 1\n")
            .is_err()
    );
    assert!(validate_node_attribution("").is_err());
    assert!(
        validate_node_attribution("target_info{job=\"keeper\",k8s_node_name=\"n\"} 1\n").is_ok()
    );
    let _ = NEEDS_CLUSTER;
}
