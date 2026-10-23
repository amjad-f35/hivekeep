//! The controller's per-WorkerPool NetworkPolicy: its shape, its garbage
//! collection, and that it actually admits the gate router while blocking
//! other callers.

mod ops_support;

use std::time::{Duration, Instant};

use hivekeep_core::resource::{network_policy_name, ActorRef};
use hivekeep_e2e::collector_metrics::reqwest_like;
use hivekeep_e2e::crd::{new_worker_pool, worker_pool_resource, SandboxClassEntry, WorkerPoolSpec};
use hivekeep_e2e::env::system_namespace;
use hivekeep_e2e::fixtures::testserver::http::DialResult;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::kube_util::is_not_found;
use hivekeep_e2e::namespace::create_namespace;
use hivekeep_e2e::portforward::{self, Forward};
use hivekeep_e2e::serverpod::{deploy_server_pod, Server, ServerPod};
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_e2e::template::{create_counter_template, template_ref, HiveTemplateOptions};
use hivekeep_proto::api::v1 as pb;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::api::networking::v1::NetworkPolicy;
use kube::api::{Api, DeleteParams, DynamicObject, ListParams, PostParams};
use ops_support::*;

/// The `app` label of the gate router's pods, which the policy admits.
const ROUTER_APP_NAME: &str = "gate-router";
/// The label tying a worker pod (and its policy) to its pool.
const WORKER_POOL_LABEL: &str = "hivekeep.io/worker-pool";
/// The port the probe and control pods listen on.
const TESTSERVER_PORT: u16 = 8080;
/// The port of the worker's /readyz server.
const WORKER_READYZ_PORT: u16 = 8080;

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn network_policy_lifecycle_and_reconciliation() {
    e2e_test(
        "TestNetworkPolicyLifecycleAndReconciliation",
        |t| async move {
            let ns = create_namespace(&t).await.name;
            let kube = clients().kube();
            let wp_name = "test-wp-netpol";
            let spec = WorkerPoolSpec {
                replicas: 1,
                worker_image: "worker:v1".into(),
                template: None,
                sandbox_classes: vec![SandboxClassEntry {
                    name: "gvisor".into(),
                    config_ref: None,
                }],
            };
            t.log(format!("Creating WorkerPool {ns}/{wp_name}..."));
            let pools: Api<DynamicObject> =
                Api::namespaced_with(kube.clone(), &ns, &worker_pool_resource());
            pools
                .create(
                    &PostParams::default(),
                    &new_worker_pool(&ns, wp_name, Default::default(), &spec),
                )
                .await
                .unwrap_or_else(|e| t.fatal(format!("failed to create WorkerPool: {e}")));

            let np_name = network_policy_name(wp_name);
            t.log(format!(
                "Waiting for NetworkPolicy {np_name} to be created..."
            ));
            let policies: Api<NetworkPolicy> = Api::namespaced(kube.clone(), &ns);
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut np = None;
            while Instant::now() < deadline {
                if let Ok(p) = policies.get(&np_name).await {
                    np = Some(p);
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let Some(np) = np else {
                t.fatal(format!(
                    "timed out waiting for NetworkPolicy {np_name} to be created in namespace {ns}"
                ))
            };
            t.log(format!("Successfully retrieved NetworkPolicy {np_name}"));

            // Owner and labels.
            let owners = np.metadata.owner_references.clone().unwrap_or_default();
            if owners.first().map(|o| o.name.as_str()) != Some(wp_name) {
                t.error(format!(
                    "expected OwnerReference to {wp_name:?}, got {owners:?}"
                ));
            }
            let labels = np.metadata.labels.clone().unwrap_or_default();
            if labels.get(WORKER_POOL_LABEL).map(String::as_str) != Some(wp_name) {
                t.error(format!(
                    "expected label {WORKER_POOL_LABEL}={wp_name:?}, got {labels:?}"
                ));
            }
            let spec = np.spec.clone().unwrap_or_default();
            let selector = spec
                .pod_selector
                .clone()
                .unwrap_or_default()
                .match_labels
                .unwrap_or_default();
            if selector.get(WORKER_POOL_LABEL).map(String::as_str) != Some(wp_name) {
                t.error(format!("expected PodSelector match label {WORKER_POOL_LABEL}={wp_name:?}, got {selector:?}"));
            }
            // One ingress rule, from the router pods of the system namespace.
            let ingress = spec.ingress.clone().unwrap_or_default();
            if ingress.len() != 1 {
                t.fatal(format!(
                    "expected exactly 1 ingress rule, got {}",
                    ingress.len()
                ));
            }
            let from = ingress[0].from.clone().unwrap_or_default();
            if from.len() != 1 {
                t.fatal(format!(
                    "expected exactly 1 ingress from peer, got {}",
                    from.len()
                ));
            }
            let peer = &from[0];
            let ns_labels = peer
                .namespace_selector
                .as_ref()
                .and_then(|s| s.match_labels.clone())
                .unwrap_or_default();
            if ns_labels.get("kubernetes.io/metadata.name") != Some(&system_namespace()) {
                t.error(format!(
                    "expected namespace selector for {}, got {:?}",
                    system_namespace(),
                    peer.namespace_selector
                ));
            }
            let pod_labels = peer
                .pod_selector
                .as_ref()
                .and_then(|s| s.match_labels.clone())
                .unwrap_or_default();
            if pod_labels.get("app").map(String::as_str) != Some(ROUTER_APP_NAME) {
                t.error(format!(
                    "expected pod selector for {ROUTER_APP_NAME}, got {:?}",
                    peer.pod_selector
                ));
            }
            // Egress is left unmanaged.
            if !spec.egress.clone().unwrap_or_default().is_empty() {
                t.error(format!(
                    "expected Egress to be empty/unmanaged, got {:?}",
                    spec.egress
                ));
            }

            // Deleting the pool garbage-collects the policy.
            t.log(format!(
                "Deleting WorkerPool {ns}/{wp_name} and verifying NetworkPolicy GC..."
            ));
            if let Err(e) = pools.delete(wp_name, &DeleteParams::default()).await {
                if !is_not_found(&e) {
                    t.fatal(format!("failed to delete WorkerPool: {e}"));
                }
            }
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match policies.get(&np_name).await {
                Err(e) if is_not_found(&e) => break,
                _ if Instant::now() > deadline => t.fatal(format!("timed out waiting for NetworkPolicy {np_name} to be garbage collected after WorkerPool deletion")),
                _ => tokio::time::sleep(Duration::from_secs(2)).await,
            }
            }
            t.log(format!(
                "NetworkPolicy {np_name} was successfully garbage collected"
            ));
        },
    );
}

/// A test-server `http` pod: the probe serves `/dial`, the control is the
/// dial target. An empty namespace deploys into a fresh one.
fn testserver_pod(name: &str, namespace: &str) -> ServerPod {
    ServerPod {
        name: name.into(),
        namespace: namespace.into(),
        args: vec!["http".into()],
        port: TESTSERVER_PORT,
        ..Default::default()
    }
}

/// Calls a test-server pod's `/dial` over a port-forward, so the
/// connection is made from that pod.
struct ProbeClient {
    forward: Forward,
}

impl ProbeClient {
    async fn new(t: &T, probe: &Server) -> ProbeClient {
        let forward = portforward::service_port_forward(
            &clients().kube(),
            &probe.namespace,
            &probe.name,
            TESTSERVER_PORT,
        )
        .await
        .unwrap_or_else(|e| {
            t.fatal(format!(
                "port-forwarding {}/{}: {e}",
                probe.namespace, probe.name
            ))
        });
        ProbeClient { forward }
    }

    /// Asks the probe to open a TCP connection to `address`. Refused or
    /// timed-out connections come back with `ok == false`; anything else
    /// fails the test, so a broken probe is not taken for a blocked dial.
    async fn dial(&self, t: &T, address: &str) -> DialResult {
        let query: String = url::form_urlencoded::byte_serialize(address.as_bytes()).collect();
        let url = format!("http://{}/dial?address={query}", self.forward.addr());
        let (status, body) = reqwest_like::Client::new(Duration::from_secs(30))
            .get(&url)
            .await
            .unwrap_or_else(|e| t.fatal(format!("calling probe /dial for {address}: {e}")));
        if status != 200 {
            let shown: String = body.chars().take(4096).collect();
            t.fatal(format!(
                "probe /dial for {address} returned {status}: {}",
                shown.trim()
            ));
        }
        serde_json::from_str(&body).unwrap_or_else(|e| {
            t.fatal(format!("decoding probe /dial response for {address}: {e}"))
        })
    }
}

fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn network_policy_data_plane_enforcement() {
    e2e_test("TestNetworkPolicyDataPlaneEnforcement", |t| async move {
        let ns = create_namespace(&t).await.name;
        let kube = clients().kube();
        // A per-test pool and template from the counter demo; the template's
        // hive is named after the test namespace.
        let pool_name = "counter";
        let at = create_counter_template(
            &t,
            &clients(),
            &ns,
            HiveTemplateOptions {
                hive: ns.clone(),
                name: "counter".into(),
                pool_name: pool_name.into(),
                pool_replicas: 1,
                labels: [("netpol-test".to_string(), ns.clone())].into(),
                ..Default::default()
            },
        )
        .await;

        let actor = format!("netpol-dataplane-{ns}");
        t.log(format!("Creating Actor {actor:?} in hive {ns:?}..."));
        create_actor(&t, &ns, &actor, template_ref(&at)).await;
        t.log(format!("Resuming Actor {actor:?}..."));
        resume_actor(&t, &ns, &actor).await;
        wait_for_state(
            &t,
            &ns,
            &actor,
            pb::ActorState::Running,
            Duration::from_secs(60),
            Duration::from_secs(1),
        )
        .await;
        t.log(format!("Actor {actor:?} reached RUNNING state"));

        // Authorized ingress, through the gate router.
        t.log("=== Verifying authorized ingress via the gate router ===");
        let rc = router(&t).await;
        let target = ActorRef::new(ns.clone(), actor.clone());
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut last = rc.get(&target, "/").await;
        while !matches!(&last, Ok(r) if r.status == 200) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_secs(1)).await;
            last = rc.get(&target, "/").await;
        }
        let resp = last.unwrap_or_else(|e| t.fatal(format!("Positive data plane verification failed: GET / via the gate router returned error: {e}")));
        if resp.status != 200 {
            t.fatal(format!("Positive data plane verification failed: GET / via the gate router status {}, body {:?}", resp.status, resp.text()));
        }
        t.log(format!("Positive data plane verification PASSED: reached actor {actor:?} via the gate router (HTTP {}): {}", resp.status, resp.text().trim()));

        // Unauthorized ingress is blocked.
        t.log("=== Verifying unauthorized ingress blocking ===");
        let pods = Api::<Pod>::namespaced(kube.clone(), &ns)
            .list(&ListParams::default().labels(&format!("{WORKER_POOL_LABEL}={pool_name}")))
            .await
            .unwrap_or_else(|e| {
                t.fatal(format!(
                    "failed to list worker pods for worker pool {pool_name:?}: {e}"
                ))
            });
        let worker_ip = pods
            .items
            .iter()
            .find_map(|p| {
                let s = p.status.as_ref()?;
                (s.phase.as_deref() == Some("Running"))
                    .then(|| s.pod_ip.clone())
                    .flatten()
                    .filter(|ip| !ip.is_empty())
                    .map(|ip| (p.metadata.name.clone().unwrap_or_default(), ip))
            })
            .unwrap_or_else(|| {
                t.fatal(format!(
                    "no running worker pod with IP found for worker pool {pool_name:?}"
                ))
            });
        t.log(format!(
            "Target worker pod {ns}/{} has IP {}",
            worker_ip.0, worker_ip.1
        ));

        // The probe runs in a new namespace the policy does not admit. The
        // control runs beside the worker but no policy selects it: if the
        // probe reaches the control, a failed dial to the worker is the
        // policy's doing, not a probe that cannot connect at all.
        let probe = deploy_server_pod(&t, &testserver_pod("unauthorized-probe", "")).await;
        let control = deploy_server_pod(&t, &testserver_pod("netpol-control", &ns)).await;
        let control_pod = Api::<Pod>::namespaced(kube.clone(), &control.namespace)
            .get(&control.name)
            .await
            .unwrap_or_else(|e| {
                t.fatal(format!(
                    "failed to get control pod {}/{}: {e}",
                    control.namespace, control.name
                ))
            });
        let control_ip = control_pod
            .status
            .and_then(|s| s.pod_ip)
            .unwrap_or_default();
        if control_ip.is_empty() {
            t.fatal(format!(
                "control pod {}/{} has no IP",
                control.namespace, control.name
            ));
        }
        let pc = ProbeClient::new(&t, &probe).await;
        let control_address = join_host_port(&control_ip, TESTSERVER_PORT);
        t.log(format!(
            "Dialing control pod {}/{} at {control_address} from probe pod {}/{}...",
            control.namespace, control.name, probe.namespace, probe.name
        ));
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut result = pc.dial(&t, &control_address).await;
        while !result.ok && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_secs(1)).await;
            result = pc.dial(&t, &control_address).await;
        }
        if !result.ok {
            t.fatal(format!(
                "Probe control failed: probe pod {}/{} cannot reach control pod {control_address}, which no NetworkPolicy selects ({}), so a blocked dial to the worker would prove nothing",
                probe.namespace, probe.name, result.error
            ));
        }

        let worker_address = join_host_port(&worker_ip.1, WORKER_READYZ_PORT);
        t.log(format!(
            "Attempting unauthorized connection from {}/{} to worker {worker_address}...",
            probe.namespace, probe.name
        ));
        let result = pc.dial(&t, &worker_address).await;
        if result.ok {
            t.fatal(format!(
                "Negative data plane verification failed: unauthorized probe pod {}/{} connected to worker pod {worker_address}; the NetworkPolicy did not block it",
                probe.namespace, probe.name
            ));
        }
        t.log(format!("Negative data plane verification PASSED: connection from {}/{} to {worker_address} was blocked as expected ({})", probe.namespace, probe.name, result.error));
    });
}

#[test]
fn host_port_joining() {
    assert_eq!(join_host_port("10.0.0.1", 80), "10.0.0.1:80");
    assert_eq!(join_host_port("fd00::1", 80), "[fd00::1]:80");
    assert_eq!(testserver_pod("p", "").port, TESTSERVER_PORT);
    let _ = NEEDS_CLUSTER;
}
