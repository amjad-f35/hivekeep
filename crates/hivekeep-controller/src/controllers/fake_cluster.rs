//! End-to-end tests of the WorkerPool, SandboxConfig protection and
//! NetworkPolicy controllers running together against an in-memory API
//! server, the way they run in the controller binary.

// Table-driven cases spell out their tuple types.
#![allow(clippy::type_complexity)]

use std::collections::BTreeMap;
use std::time::Duration;

use hivekeep_k8s::crd::sandboxconfig::{
    CLASS_DEFAULT_ANNOTATION, WORKER_POOL_PROTECTION_FINALIZER,
};
use hivekeep_k8s::crd::workerpool::{
    CONDITION_RESOLVED, REASON_DEFAULT_CONFIG_NOT_FOUND, REASON_SANDBOX_CLASS_MISMATCH,
    REASON_SANDBOX_CONFIG_DELETING, REASON_SANDBOX_CONFIG_NOT_FOUND,
};
use hivekeep_k8s::crd::{
    SandboxClass, SandboxConfig, SandboxConfigReference, WorkerPool, WorkerPoolPodTemplate,
};
use hivekeep_k8s::fake::FakeApiServer;
use hivekeep_k8s::install_defaults::{
    KEEPER_SERVICE_ACCOUNT, ROUTER_SERVICE_ACCOUNT, SYSTEM_NAMESPACE, WORKER_SERVICE_ACCOUNT,
};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{
    NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, ResourceRequirements,
    ServiceAccount, Toleration,
};
use k8s_openapi::api::events::v1::Event;
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::{Api, DeleteParams, PostParams};
use kube::{Client, ResourceExt};
use tokio_util::sync::CancellationToken;

use super::manager::{run_controllers, Controllers};
use super::meta::{controller_of, find_condition, has_finalizer};
use super::networkpolicy::{network_policy_name, GATE_ROUTER_APP_NAME};
use super::test_support::*;
use super::worker_env::{
    DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL, SANDBOX_CLASS_TAINT_KEY, WORKER_POOL_LABEL,
};
use super::workerpool_apply::{
    NODE_BASE_VOLUME, TUNNEL_EGRESS_TRUST_VOLUME, TUNNEL_IDENTITY_VOLUME, WORKER_CAPACITY_VOLUME,
};
use super::workerpool_sandboxconfig::sandbox_class_status;
use super::{ControllerConfig, WorkerOtelSettings};

/// The gvisor class default every test environment starts with, so a
/// gvisor pool without `configRef` resolves. Tests that control the class
/// default use microvm.
const TEST_GVISOR_DEFAULT: &str = "gvisor-default-test";

/// A fake API server with the controllers running against it.
struct Env {
    fake: FakeApiServer,
    client: Client,
    stop: CancellationToken,
}

impl Drop for Env {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

async fn start() -> Env {
    let fake = FakeApiServer::new();
    fake.insert(&make_sandbox_config(
        TEST_GVISOR_DEFAULT,
        SandboxClass::Gvisor,
        true,
    ));
    let client = fake.client();
    let stop = CancellationToken::new();
    let cfg = ControllerConfig {
        system_namespace: SYSTEM_NAMESPACE.into(),
        keeper_service_account: KEEPER_SERVICE_ACCOUNT.into(),
        router_service_account: ROUTER_SERVICE_ACCOUNT.into(),
        dns_egress_policy: DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL.into(),
        otel: WorkerOtelSettings::default(),
    };
    let which = Controllers {
        egress_mitm_trust: false,
        ..Controllers::all()
    };
    tokio::spawn({
        let (client, stop) = (client.clone(), stop.clone());
        async move {
            run_controllers(client, cfg, prometheus::Registry::new(), stop, which)
                .await
                .expect("controllers run");
        }
    });
    Env { fake, client, stop }
}

impl Env {
    fn pools(&self, ns: &str) -> Api<WorkerPool> {
        Api::namespaced(self.client.clone(), ns)
    }

    fn configs(&self) -> Api<SandboxConfig> {
        Api::all(self.client.clone())
    }

    fn deployments(&self, ns: &str) -> Api<Deployment> {
        Api::namespaced(self.client.clone(), ns)
    }

    async fn create_pool(&self, wp: &WorkerPool) {
        self.pools(&wp.namespace().unwrap())
            .create(&PostParams::default(), wp)
            .await
            .expect("create WorkerPool");
    }

    async fn create_config(&self, sc: &SandboxConfig) {
        self.configs()
            .create(&PostParams::default(), sc)
            .await
            .expect("create SandboxConfig");
    }

    fn deployment(&self, wp: &WorkerPool) -> Option<Deployment> {
        self.fake
            .get::<Deployment>(&wp.namespace().unwrap(), &wp.name_any())
    }

    fn pool(&self, wp: &WorkerPool) -> Option<WorkerPool> {
        self.fake
            .get::<WorkerPool>(&wp.namespace().unwrap(), &wp.name_any())
    }

    fn config(&self, name: &str) -> Option<SandboxConfig> {
        self.fake.get::<SandboxConfig>("", name)
    }

    /// Re-reads the pool, applies `mutate` and updates it, retrying on
    /// conflicts with the controller's own writes.
    async fn update_pool_spec(&self, wp: &WorkerPool, mutate: impl Fn(&mut WorkerPool)) {
        let api = self.pools(&wp.namespace().unwrap());
        for _ in 0..50 {
            let mut current = api.get(&wp.name_any()).await.expect("get WorkerPool");
            mutate(&mut current);
            match api
                .replace(&wp.name_any(), &PostParams::default(), &current)
                .await
            {
                Ok(_) => return,
                Err(e) if hivekeep_k8s::api_errors::is_conflict(&e) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(e) => panic!("update WorkerPool: {e}"),
            }
        }
        panic!("update WorkerPool: too many conflicts");
    }

    /// Like [`Env::update_pool_spec`] for the pool's Deployment, through
    /// the main resource or the status subresource.
    async fn update_deployment(
        &self,
        wp: &WorkerPool,
        status: bool,
        mutate: impl Fn(&mut Deployment),
    ) {
        let api = self.deployments(&wp.namespace().unwrap());
        for _ in 0..50 {
            let mut dep = api.get(&wp.name_any()).await.expect("get Deployment");
            mutate(&mut dep);
            let res = if status {
                api.replace_status(&wp.name_any(), &PostParams::default(), &dep)
                    .await
            } else {
                api.replace(&wp.name_any(), &PostParams::default(), &dep)
                    .await
            };
            match res {
                Ok(_) => return,
                Err(e) if hivekeep_k8s::api_errors::is_conflict(&e) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(e) => panic!("update Deployment: {e}"),
            }
        }
        panic!("update Deployment: too many conflicts");
    }

    async fn update_config(&self, name: &str, mutate: impl Fn(&mut SandboxConfig)) {
        let api = self.configs();
        for _ in 0..50 {
            let mut sc = api.get(name).await.expect("get SandboxConfig");
            mutate(&mut sc);
            match api.replace(name, &PostParams::default(), &sc).await {
                Ok(_) => return,
                Err(e) if hivekeep_k8s::api_errors::is_conflict(&e) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(e) => panic!("update SandboxConfig: {e}"),
            }
        }
        panic!("update SandboxConfig: too many conflicts");
    }

    async fn delete_config(&self, name: &str) {
        self.configs()
            .delete(name, &DeleteParams::default())
            .await
            .expect("delete SandboxConfig");
    }

    async fn delete_pool(&self, wp: &WorkerPool) {
        self.pools(&wp.namespace().unwrap())
            .delete(&wp.name_any(), &DeleteParams::default())
            .await
            .expect("delete WorkerPool");
    }

    async fn wait_deployment(&self, what: &str, wp: &WorkerPool, ok: impl Fn(&Deployment) -> bool) {
        wait_for(what, || self.deployment(wp).is_some_and(|d| ok(&d))).await;
    }

    /// Waits for `wp`'s class to report a Resolved condition for its
    /// current generation that `ok` accepts.
    async fn wait_condition(
        &self,
        what: &str,
        wp: &WorkerPool,
        ok: impl Fn(&WorkerPool, bool, &str) -> bool,
    ) -> WorkerPool {
        let mut found = None;
        wait_for(what, || {
            let Some(current) = self.pool(wp) else {
                return false;
            };
            let class = current.spec.default_sandbox_class().unwrap_or_default();
            let Some(status) = sandbox_class_status(&current, class) else {
                return false;
            };
            let Some(cond) = find_condition(&status.conditions, CONDITION_RESOLVED) else {
                return false;
            };
            let done = cond.observed_generation == current.metadata.generation
                && ok(&current, cond.status == "True", &cond.reason);
            if done {
                found = Some(current.clone());
            }
            done
        })
        .await;
        found.unwrap()
    }

    async fn wait_resolved(&self, wp: &WorkerPool, config: &str) -> WorkerPool {
        self.wait_condition(
            &format!("{} resolves to {config}", wp.name_any()),
            wp,
            |cur, ok, _| {
                ok && status_config(cur, cur.spec.default_sandbox_class().unwrap_or_default())
                    == config
            },
        )
        .await
    }

    async fn wait_unresolved(&self, wp: &WorkerPool, reason: &str) -> WorkerPool {
        self.wait_condition(
            &format!("{} reports {reason}", wp.name_any()),
            wp,
            |_, ok, r| !ok && r == reason,
        )
        .await
    }

    async fn wait_protection_finalizer(&self, name: &str, want: bool) {
        wait_for(&format!("{name} protected = {want}"), || {
            self.config(name).is_some_and(|sc| {
                has_finalizer(&sc.metadata, WORKER_POOL_PROTECTION_FINALIZER) == want
            })
        })
        .await;
    }

    async fn wait_resolved_and_deployed(&self, wp: &WorkerPool, config: &str) {
        self.wait_resolved(wp, config).await;
        self.wait_protection_finalizer(config, true).await;
        self.wait_deployment("Deployment exists", wp, |_| true)
            .await;
    }

    async fn wait_warning_event(&self, wp: &WorkerPool, reason: &str) {
        wait_for(&format!("Warning {reason} about {}", wp.name_any()), || {
            self.fake.list::<Event>().iter().any(|ev| {
                ev.regarding.as_ref().and_then(|r| r.name.as_deref())
                    == Some(wp.name_any().as_str())
                    && ev.type_.as_deref() == Some("Warning")
                    && ev.reason.as_deref() == Some(reason)
            })
        })
        .await;
    }

    async fn wait_config_gone(&self, name: &str) {
        wait_for(&format!("{name} gone"), || self.config(name).is_none()).await;
    }

    async fn delete_config_and_wait(&self, name: &str) {
        self.delete_config(name).await;
        self.wait_config_gone(name).await;
    }

    async fn delete_pool_and_wait(&self, wp: &WorkerPool) {
        self.delete_pool(wp).await;
        wait_for(&format!("{} gone", wp.name_any()), || {
            self.pool(wp).is_none()
        })
        .await;
    }

    async fn wait_deployment_replicas(&self, wp: &WorkerPool, replicas: i32) {
        self.wait_deployment(&format!("Deployment asks for {replicas}"), wp, |d| {
            replicas_of(d) == Some(replicas)
        })
        .await;
    }

    /// Fails if a Deployment controlled by `wp` exists.
    fn assert_no_deployment(&self, wp: &WorkerPool) {
        let Some(dep) = self.deployment(wp) else {
            return;
        };
        let uid = self.pool(wp).and_then(|p| p.metadata.uid);
        let controlled = controller_of(&dep.metadata).is_some_and(|o| Some(&o.uid) == uid.as_ref());
        assert!(
            !controlled,
            "unresolved WorkerPool {} has a Deployment",
            wp.name_any()
        );
    }
}

/// Polls `cond` every 50ms until it holds, failing after 15s.
async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    eventually(what, || std::future::ready(cond())).await;
}

fn replicas_of(d: &Deployment) -> Option<i32> {
    d.spec.as_ref()?.replicas
}

fn pod_spec(d: &Deployment) -> &k8s_openapi::api::core::v1::PodSpec {
    d.spec.as_ref().unwrap().template.spec.as_ref().unwrap()
}

fn quantity<'a>(r: &'a Option<ResourceRequirements>, which: &str, name: &str) -> Option<&'a str> {
    let r = r.as_ref()?;
    let list = if which == "requests" {
        &r.requests
    } else {
        &r.limits
    };
    list.as_ref()?.get(name).map(|q| q.0.as_str())
}

fn sample_pod_template() -> WorkerPoolPodTemplate {
    WorkerPoolPodTemplate {
        node_selector: BTreeMap::from([("workload".into(), "hivekeep".into())]),
        tolerations: vec![Toleration {
            key: Some("nvidia.com/gpu".into()),
            operator: Some("Exists".into()),
            effect: Some("NoSchedule".into()),
            ..Default::default()
        }],
        priority_class_name: "hivekeep-workers".into(),
        service_account_name: Some("hivekeep-worker".into()),
        node_affinity: Some(NodeAffinity {
            required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                node_selector_terms: vec![NodeSelectorTerm {
                    match_expressions: Some(vec![NodeSelectorRequirement {
                        key: "workload".into(),
                        operator: "In".into(),
                        values: Some(vec!["hivekeep".into()]),
                    }]),
                    match_fields: None,
                }],
            }),
            preferred_during_scheduling_ignored_during_execution: None,
        }),
        resources: Some(ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".into(), Quantity("500m".into())),
                ("memory".into(), Quantity("1Gi".into())),
            ])),
            limits: Some(BTreeMap::from([
                ("cpu".into(), Quantity("1".into())),
                ("memory".into(), Quantity("2Gi".into())),
            ])),
            claims: None,
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn worker_pool_creates_deployment() {
    let env = start().await;
    let wp = make_worker_pool("test-create", "default", 3, "worker:v1");
    env.create_pool(&wp).await;
    env.wait_deployment("configured Deployment", &wp, |d| {
        let ps = pod_spec(d);
        let Some(c) = ps.containers.first() else {
            return false;
        };
        let vols: Vec<&str> = ps
            .volumes
            .iter()
            .flatten()
            .map(|v| v.name.as_str())
            .collect();
        replicas_of(d) == Some(3)
            && c.image.as_deref() == Some("worker:v1")
            && c.name == "worker"
            && d.spec
                .as_ref()
                .unwrap()
                .template
                .metadata
                .as_ref()
                .and_then(|m| m.labels.as_ref())
                .and_then(|l| l.get(WORKER_POOL_LABEL))
                == Some(&wp.name_any())
            && d.metadata
                .owner_references
                .as_ref()
                .and_then(|o| o.first())
                .map(|o| o.name.as_str())
                == Some("test-create")
            && vols
                == [
                    WORKER_CAPACITY_VOLUME,
                    NODE_BASE_VOLUME,
                    TUNNEL_IDENTITY_VOLUME,
                    TUNNEL_EGRESS_TRUST_VOLUME,
                ]
    })
    .await;
}

#[tokio::test]
async fn worker_pool_replicas_update() {
    let env = start().await;
    let wp = make_worker_pool("test-replicas", "default", 2, "worker:v1");
    env.create_pool(&wp).await;
    env.wait_deployment("Deployment exists", &wp, |_| true)
        .await;
    env.update_pool_spec(&wp, |p| p.spec.replicas = 5).await;
    env.wait_deployment_replicas(&wp, 5).await;
}

#[tokio::test]
async fn worker_pool_image_update() {
    let env = start().await;
    let wp = make_worker_pool("test-image", "default", 1, "worker:v1");
    env.create_pool(&wp).await;
    env.wait_deployment("Deployment exists", &wp, |_| true)
        .await;
    env.update_pool_spec(&wp, |p| p.spec.worker_image = "worker:v2".into())
        .await;
    env.wait_deployment("image updated", &wp, |d| {
        pod_spec(d).containers[0].image.as_deref() == Some("worker:v2")
    })
    .await;
}

#[tokio::test]
async fn ssa_preserves_unowned_fields() {
    let env = start().await;
    let wp = make_worker_pool("test-ssa-unowned", "default", 2, "worker:v1");
    env.create_pool(&wp).await;
    env.wait_deployment("Deployment exists", &wp, |_| true)
        .await;
    // Another manager sets a field the controller never declares.
    env.update_deployment(&wp, false, |d| {
        d.spec.as_mut().unwrap().revision_history_limit = Some(7)
    })
    .await;
    env.wait_deployment("revisionHistoryLimit kept", &wp, |d| {
        replicas_of(d) == Some(2) && d.spec.as_ref().unwrap().revision_history_limit == Some(7)
    })
    .await;
}

#[tokio::test]
async fn ssa_reverts_owned_fields() {
    let env = start().await;
    let wp = make_worker_pool("test-ssa-owned", "default", 2, "worker:v1");
    env.create_pool(&wp).await;
    env.wait_deployment_replicas(&wp, 2).await;
    env.update_deployment(&wp, false, |d| d.spec.as_mut().unwrap().replicas = Some(99))
        .await;
    // The controller re-applies with force, reclaiming replicas.
    env.wait_deployment_replicas(&wp, 2).await;
}

#[tokio::test]
async fn deleted_deployment_recreated() {
    let env = start().await;
    let wp = make_worker_pool("test-recreate", "default", 2, "worker:v1");
    env.create_pool(&wp).await;
    env.wait_deployment("Deployment exists", &wp, |_| true)
        .await;
    env.deployments("default")
        .delete("test-recreate", &DeleteParams::default())
        .await
        .unwrap();
    wait_for("Deployment deleted or recreated", || {
        env.deployment(&wp)
            .is_none_or(|d| d.metadata.deletion_timestamp.is_none())
    })
    .await;
    env.wait_deployment("Deployment recreated", &wp, |_| true)
        .await;
}

#[tokio::test]
async fn status_replicas_propagation() {
    let env = start().await;
    let wp = make_worker_pool("test-status", "default", 3, "worker:v1");
    env.create_pool(&wp).await;
    env.wait_deployment("Deployment exists", &wp, |_| true)
        .await;
    // As the Deployment controller would report 3 running pods.
    env.update_deployment(&wp, true, |d| {
        d.status.get_or_insert_with(Default::default).replicas = Some(3)
    })
    .await;
    wait_for("status replicas and selector", || {
        env.pool(&wp).and_then(|p| p.status).is_some_and(|s| {
            s.selector == format!("{WORKER_POOL_LABEL}=test-status") && s.replicas == 3
        })
    })
    .await;
}

#[tokio::test]
async fn sync_status_ready_replicas() {
    let env = start().await;
    let wp = make_worker_pool("test-sync-ready", "default", 3, "worker:v1");
    env.create_pool(&wp).await;
    env.wait_deployment("Deployment exists", &wp, |_| true)
        .await;
    env.update_deployment(&wp, true, |d| {
        let s = d.status.get_or_insert_with(Default::default);
        s.replicas = Some(3);
        s.ready_replicas = Some(2);
    })
    .await;
    wait_for("3 replicas, 2 ready", || {
        env.pool(&wp)
            .and_then(|p| p.status)
            .is_some_and(|s| s.replicas == 3 && s.ready_replicas == 2)
    })
    .await;
}

#[tokio::test]
async fn worker_pool_runs_workers_as_worker_service_account() {
    let env = start().await;
    let wp = make_worker_pool("test-worker-sa", "default", 1, "worker:v1");
    env.create_pool(&wp).await;
    env.wait_deployment("runs as the worker ServiceAccount", &wp, |d| {
        pod_spec(d).service_account_name.as_deref() == Some(WORKER_SERVICE_ACCOUNT)
    })
    .await;
    // Applied before the Deployment, so it exists by now.
    let sa = env
        .fake
        .get::<ServiceAccount>("default", WORKER_SERVICE_ACCOUNT)
        .expect("worker ServiceAccount");
    // Pools share it, so deleting one must not delete it.
    assert!(
        sa.metadata.owner_references.unwrap_or_default().is_empty(),
        "worker ServiceAccount has owners"
    );
}

#[tokio::test]
async fn worker_pool_with_service_account_skips_default() {
    let env = start().await;
    let mut wp = make_worker_pool("test-custom-sa", "test-custom-worker-sa", 1, "worker:v1");
    wp.spec.template = Some(WorkerPoolPodTemplate {
        service_account_name: Some("hivekeep-worker".into()),
        ..Default::default()
    });
    env.create_pool(&wp).await;
    env.wait_deployment("runs as the pool's ServiceAccount", &wp, |d| {
        pod_spec(d).service_account_name.as_deref() == Some("hivekeep-worker")
    })
    .await;
    assert!(
        env.fake
            .get::<ServiceAccount>("test-custom-worker-sa", WORKER_SERVICE_ACCOUNT)
            .is_none(),
        "the default worker ServiceAccount was created"
    );
}

#[tokio::test]
async fn worker_pool_pod_template_propagation() {
    let env = start().await;
    let mut wp = make_worker_pool("test-template-propagate", "default", 1, "worker:v1");
    wp.spec.template = Some(sample_pod_template());
    env.create_pool(&wp).await;
    env.wait_deployment("template propagated", &wp, |d| {
        let ps = pod_spec(d);
        let c = &ps.containers[0];
        ps.node_selector.as_ref().and_then(|n| n.get("workload")).map(String::as_str) == Some("hivekeep")
            // The controller adds its sandbox class toleration as well.
            && ps.tolerations.iter().flatten().any(|t| t.key.as_deref() == Some("nvidia.com/gpu"))
            && ps.priority_class_name.as_deref() == Some("hivekeep-workers")
            && ps.service_account_name.as_deref() == Some("hivekeep-worker")
            && ps.affinity.as_ref().is_some_and(|a| a.node_affinity.is_some())
            && quantity(&c.resources, "requests", "cpu") == Some("500m")
            && quantity(&c.resources, "requests", "memory") == Some("1Gi")
            && quantity(&c.resources, "limits", "cpu") == Some("1")
            && quantity(&c.resources, "limits", "memory") == Some("2Gi")
    })
    .await;
}

#[tokio::test]
async fn worker_pool_pod_template_update() {
    let env = start().await;
    let mut wp = make_worker_pool("test-template-update", "default", 1, "worker:v1");
    wp.spec.template = Some(sample_pod_template());
    env.create_pool(&wp).await;
    let selector = |d: &Deployment, v: &str| {
        pod_spec(d)
            .node_selector
            .as_ref()
            .and_then(|n| n.get("workload"))
            .map(String::as_str)
            == Some(v)
    };
    env.wait_deployment("template propagated", &wp, |d| selector(d, "hivekeep"))
        .await;
    env.update_pool_spec(&wp, |p| {
        p.spec.template.as_mut().unwrap().node_selector =
            BTreeMap::from([("workload".into(), "updated".into())]);
    })
    .await;
    env.wait_deployment("template updated", &wp, |d| {
        selector(d, "updated")
            && quantity(&pod_spec(d).containers[0].resources, "requests", "cpu") == Some("500m")
    })
    .await;
}

#[tokio::test]
async fn worker_pool_pod_template_clear() {
    let env = start().await;
    let mut wp = make_worker_pool("test-template-clear", "default", 1, "worker:v1");
    wp.spec.template = Some(sample_pod_template());
    env.create_pool(&wp).await;
    env.wait_deployment("template propagated", &wp, |d| {
        pod_spec(d).node_selector.is_some()
    })
    .await;
    env.update_pool_spec(&wp, |p| {
        p.spec.template.as_mut().unwrap().node_selector.clear()
    })
    .await;
    env.wait_deployment("nodeSelector cleared", &wp, |d| {
        pod_spec(d)
            .node_selector
            .as_ref()
            .is_none_or(|n| n.is_empty())
    })
    .await;
}

#[tokio::test]
async fn worker_pool_pod_template_clear_all() {
    let env = start().await;
    let mut wp = make_worker_pool("test-template-clear-all", "default", 1, "worker:v1");
    wp.spec.template = Some(sample_pod_template());
    env.create_pool(&wp).await;
    env.wait_deployment("template propagated", &wp, |d| {
        let ps = pod_spec(d);
        // The template's toleration plus the controller's class one.
        ps.node_selector
            .as_ref()
            .and_then(|n| n.get("workload"))
            .map(String::as_str)
            == Some("hivekeep")
            && ps.tolerations.as_ref().map(Vec::len) == Some(2)
            && ps.priority_class_name.as_deref() == Some("hivekeep-workers")
            && ps.service_account_name.as_deref() == Some("hivekeep-worker")
            && ps
                .affinity
                .as_ref()
                .is_some_and(|a| a.node_affinity.is_some())
            && quantity(&ps.containers[0].resources, "requests", "cpu") == Some("500m")
    })
    .await;
    env.update_pool_spec(&wp, |p| p.spec.template = None).await;
    env.wait_deployment("template cleared", &wp, |d| {
        let ps = pod_spec(d);
        let r = ps.containers[0].resources.clone().unwrap_or_default();
        // Only the controller's sandbox class toleration survives.
        ps.node_selector.as_ref().is_none_or(|n| n.is_empty())
            && ps.tolerations.as_ref().is_some_and(|t| {
                t.len() == 1 && t[0].key.as_deref() == Some(SANDBOX_CLASS_TAINT_KEY)
            })
            && ps.priority_class_name.as_deref().unwrap_or("").is_empty()
            && ps.service_account_name.as_deref() == Some(WORKER_SERVICE_ACCOUNT)
            && ps
                .affinity
                .as_ref()
                .is_none_or(|a| a.node_affinity.is_none())
            && r.limits.unwrap_or_default().is_empty()
            && r.requests.unwrap_or_default().is_empty()
    })
    .await;
}

#[tokio::test]
async fn ssa_reverts_owned_pod_template_fields() {
    let env = start().await;
    let mut wp = make_worker_pool("test-ssa-template", "default", 1, "worker:v1");
    wp.spec.template = Some(sample_pod_template());
    env.create_pool(&wp).await;
    let selector = |d: &Deployment, v: &str| {
        pod_spec(d)
            .node_selector
            .as_ref()
            .and_then(|n| n.get("workload"))
            .map(String::as_str)
            == Some(v)
    };
    env.wait_deployment("template propagated", &wp, |d| selector(d, "hivekeep"))
        .await;
    env.update_deployment(&wp, false, |d| {
        d.spec
            .as_mut()
            .unwrap()
            .template
            .spec
            .as_mut()
            .unwrap()
            .node_selector = Some(BTreeMap::from([("workload".into(), "rogue".into())]));
    })
    .await;
    env.wait_deployment("nodeSelector reverted", &wp, |d| selector(d, "hivekeep"))
        .await;
}

#[tokio::test]
async fn worker_pool_creates_network_policy() {
    let env = start().await;
    let wp = make_worker_pool("test-netpolicy-create", "default", 2, "worker:v1");
    env.create_pool(&wp).await;
    wait_for("NetworkPolicy", || {
        let Some(np) = env
            .fake
            .get::<NetworkPolicy>("default", &network_policy_name(&wp.name_any()))
        else {
            return false;
        };
        let spec = np.spec.clone().unwrap_or_default();
        let pool = wp.name_any();
        let owner_ok = np
            .metadata
            .owner_references
            .as_ref()
            .and_then(|o| o.first())
            .is_some_and(|o| o.name == pool);
        let label_ok = np.labels().get(WORKER_POOL_LABEL) == Some(&pool);
        let selector_ok = spec
            .pod_selector
            .as_ref()
            .and_then(|s| s.match_labels.as_ref())
            .and_then(|l| l.get(WORKER_POOL_LABEL))
            == Some(&pool);
        let ingress_type = spec.policy_types.iter().flatten().any(|t| t == "Ingress");
        let ingress = spec.ingress.clone().unwrap_or_default();
        let peer_ok = ingress.len() == 1
            && ingress[0].from.as_ref().is_some_and(|from| {
                from.len() == 1
                    && from[0]
                        .namespace_selector
                        .as_ref()
                        .and_then(|s| s.match_labels.as_ref())
                        .and_then(|l| l.get("kubernetes.io/metadata.name"))
                        .map(String::as_str)
                        == Some(SYSTEM_NAMESPACE)
                    && from[0]
                        .pod_selector
                        .as_ref()
                        .and_then(|s| s.match_labels.as_ref())
                        .and_then(|l| l.get("app"))
                        .map(String::as_str)
                        == Some(GATE_ROUTER_APP_NAME)
            });
        // Egress is unmanaged.
        let egress_ok = spec.egress.unwrap_or_default().is_empty();
        owner_ok && label_ok && selector_ok && ingress_type && peer_ok && egress_ok
    })
    .await;
}

fn with_ref(mut wp: WorkerPool, config: &str) -> WorkerPool {
    wp.spec.sandbox_classes[0].config_ref = Some(SandboxConfigReference::new(config));
    wp
}

#[tokio::test]
async fn sandbox_config_released_when_unused() {
    let cases = [
        (
            "never used",
            ["unused-sc", "unused-sc-other"],
            "test-unused-sc",
            "unused-sc-other",
            "",
            "unused-sc",
        ),
        (
            "pool switched away",
            ["switch-from", "switch-to"],
            "test-switch-ref",
            "switch-from",
            "switch-to",
            "switch-from",
        ),
    ];
    for (name, configs, pool, r, switch_to, release) in cases {
        let env = start().await;
        for c in configs {
            env.create_config(&make_sandbox_config(c, SandboxClass::Gvisor, false))
                .await;
        }
        let wp = with_ref(make_worker_pool(pool, "default", 1, "worker:v1"), r);
        env.create_pool(&wp).await;
        env.wait_resolved_and_deployed(&wp, r).await;
        if !switch_to.is_empty() {
            env.update_pool_spec(&wp, |p| {
                p.spec.sandbox_classes[0].config_ref = Some(SandboxConfigReference::new(switch_to))
            })
            .await;
            env.wait_resolved(&wp, switch_to).await;
            env.wait_protection_finalizer(switch_to, true).await;
        }
        if release != r {
            let got = env.config(release).unwrap();
            assert!(
                !has_finalizer(&got.metadata, WORKER_POOL_PROTECTION_FINALIZER),
                "{name}: unused SandboxConfig {release} carries the protection finalizer"
            );
        }
        env.delete_config_and_wait(release).await;
    }
}

#[tokio::test]
async fn worker_pool_config_ref() {
    let cases: [(
        &str,
        Option<SandboxConfig>,
        bool,
        &str,
        &str,
        Option<SandboxConfig>,
    ); 4] = [
        (
            "existing config of the same class",
            Some(make_sandbox_config("ref-ok", SandboxClass::Gvisor, false)),
            false,
            "ref-ok",
            "",
            None,
        ),
        (
            "missing config, created later",
            None,
            false,
            "ref-late",
            REASON_SANDBOX_CONFIG_NOT_FOUND,
            Some(make_sandbox_config("ref-late", SandboxClass::Gvisor, false)),
        ),
        (
            "config of another class",
            Some(make_sandbox_config("ref-vm", SandboxClass::Microvm, false)),
            false,
            "ref-vm",
            REASON_SANDBOX_CLASS_MISMATCH,
            None,
        ),
        (
            "config being deleted",
            Some(make_sandbox_config(
                "ref-dying",
                SandboxClass::Gvisor,
                false,
            )),
            true,
            "ref-dying",
            REASON_SANDBOX_CONFIG_DELETING,
            None,
        ),
    ];
    for (name, config, deleting, r, want_reason, late) in cases {
        let env = start().await;
        if let Some(mut c) = config {
            if deleting {
                c = with_finalizer(c, TEST_HOLD_FINALIZER);
            }
            env.create_config(&c).await;
            if deleting {
                env.delete_config(&c.name_any()).await;
            }
        }
        let wp = with_ref(
            make_worker_pool(&format!("test-configref-{r}"), "default", 1, "worker:v1"),
            r,
        );
        env.create_pool(&wp).await;
        if want_reason.is_empty() {
            env.wait_resolved_and_deployed(&wp, r).await;
            continue;
        }
        let current = env.wait_unresolved(&wp, want_reason).await;
        assert_eq!(
            status_config(&current, SandboxClass::Gvisor),
            "",
            "{name}: a pool that never resolved names a config"
        );
        env.assert_no_deployment(&wp);
        env.wait_warning_event(&wp, want_reason).await;
        if let Some(l) = late {
            env.create_config(&l).await;
            env.wait_resolved_and_deployed(&wp, r).await;
        }
    }
}

#[tokio::test]
async fn worker_pool_unresolved_keeps_config_in_use() {
    let cases = [
        (
            "spec points at a missing config",
            "test-unresolved-keeps",
            None,
            "never-created",
            5,
            REASON_SANDBOX_CONFIG_NOT_FOUND,
        ),
        (
            "config in use marked for deletion",
            "test-ref-deleting-in-use",
            Some(make_sandbox_config(
                "dying-in-use",
                SandboxClass::Gvisor,
                false,
            )),
            "",
            3,
            REASON_SANDBOX_CONFIG_DELETING,
        ),
    ];
    for (name, pool, config, missing_ref, replicas, want_reason) in cases {
        let env = start().await;
        let mut wp = make_worker_pool(pool, "default", 1, "worker:v1");
        let mut in_use = TEST_GVISOR_DEFAULT.to_string();
        if let Some(c) = &config {
            env.create_config(&with_finalizer(c.clone(), TEST_HOLD_FINALIZER))
                .await;
            in_use = c.name_any();
            wp = with_ref(wp, &in_use);
        }
        env.create_pool(&wp).await;
        env.wait_resolved_and_deployed(&wp, &in_use).await;

        if !missing_ref.is_empty() {
            env.update_pool_spec(&wp, |p| {
                p.spec.replicas = replicas;
                p.spec.sandbox_classes[0].config_ref =
                    Some(SandboxConfigReference::new(missing_ref));
            })
            .await;
        } else {
            env.delete_config(&in_use).await;
        }
        let current = env.wait_unresolved(&wp, want_reason).await;
        assert_eq!(
            status_config(&current, SandboxClass::Gvisor),
            in_use,
            "{name}: the config in use was dropped"
        );
        env.wait_warning_event(&wp, want_reason).await;
        env.wait_protection_finalizer(&in_use, true).await;
        if missing_ref.is_empty() {
            env.update_pool_spec(&wp, |p| p.spec.replicas = replicas)
                .await;
        }
        env.wait_deployment_replicas(&wp, replicas).await;
    }
}

/// A config shared by several pools survives a delete until all of them
/// are gone, and the pools themselves delete without waiting.
///
/// A sentinel pool uses a sentinel config deleted together with the shared
/// one. The pool deletions reach the protection reconciler through the
/// same watch, in order, and it reconciles one config at a time, so once
/// the sentinel is gone the shared config has been reconciled for every
/// user deleted before the sentinel pool and found still in use.
#[tokio::test]
async fn sandbox_config_in_use_protection() {
    let env = start().await;
    let (shared, sentinel) = ("shared-sc", "shared-sentinel");
    env.create_config(&make_sandbox_config(shared, SandboxClass::Gvisor, false))
        .await;
    env.create_config(&make_sandbox_config(sentinel, SandboxClass::Gvisor, false))
        .await;
    let mut users = Vec::new();
    for name in ["test-shared-a", "test-shared-b"] {
        let wp = with_ref(make_worker_pool(name, "default", 1, "worker:v1"), shared);
        env.create_pool(&wp).await;
        env.wait_resolved(&wp, shared).await;
        users.push(wp);
    }
    let sentinel_pool = with_ref(
        make_worker_pool("test-shared-sentinel", "default", 1, "worker:v1"),
        sentinel,
    );
    env.create_pool(&sentinel_pool).await;
    env.wait_resolved(&sentinel_pool, sentinel).await;
    env.wait_protection_finalizer(shared, true).await;
    env.wait_protection_finalizer(sentinel, true).await;

    env.delete_config(shared).await;
    env.delete_config(sentinel).await;
    let last = users.pop().unwrap();
    for wp in &users {
        env.delete_pool_and_wait(wp).await;
    }
    env.delete_pool_and_wait(&sentinel_pool).await;
    env.wait_config_gone(sentinel).await;
    // Reconciled for every other user's deletion; it must stay for the last.
    let got = env
        .config(shared)
        .unwrap_or_else(|| panic!("{shared} gone while {} uses it", last.name_any()));
    assert!(
        got.metadata.deletion_timestamp.is_some(),
        "no deletionTimestamp after delete"
    );

    env.delete_pool_and_wait(&last).await;
    env.wait_config_gone(shared).await;
}

/// A pool without `configRef` follows the newest class default and keeps
/// the config it uses, reporting why, once no default is left. Each step
/// builds on the previous one.
#[tokio::test]
async fn worker_pool_default_newest_wins() {
    let env = start().await;
    let mut wp = make_worker_pool("test-default-newest", "default", 1, "worker:v1");
    wp.spec.sandbox_classes[0].name = SandboxClass::Microvm;
    env.create_pool(&wp).await;
    let mv = |name: &str| make_sandbox_config(name, SandboxClass::Microvm, true);

    // No class default.
    let current = env
        .wait_unresolved(&wp, REASON_DEFAULT_CONFIG_NOT_FOUND)
        .await;
    assert_eq!(status_config(&current, SandboxClass::Microvm), "");
    env.wait_warning_event(&wp, REASON_DEFAULT_CONFIG_NOT_FOUND)
        .await;
    env.assert_no_deployment(&wp);

    // A class default appears.
    env.create_config(&mv("mvdef-a")).await;
    env.wait_resolved_and_deployed(&wp, "mvdef-a").await;

    // A newer class default wins. Creation times have one-second
    // resolution and the name tie-break would pick mvdef-a, so only a later
    // creation time can make mvdef-b win.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    env.create_config(&mv("mvdef-b")).await;
    env.wait_resolved_and_deployed(&wp, "mvdef-b").await;

    // The default no longer used deletes at once.
    env.delete_config_and_wait("mvdef-a").await;
    env.wait_resolved_and_deployed(&wp, "mvdef-b").await;

    // The only default being deleted is kept, held by its finalizer.
    env.delete_config("mvdef-b").await;
    let current = env
        .wait_unresolved(&wp, REASON_SANDBOX_CONFIG_DELETING)
        .await;
    assert_eq!(status_config(&current, SandboxClass::Microvm), "mvdef-b");
    env.wait_warning_event(&wp, REASON_SANDBOX_CONFIG_DELETING)
        .await;

    // No default left: the config in use is kept.
    env.update_config("mvdef-b", |sc| {
        if let Some(a) = sc.metadata.annotations.as_mut() {
            a.remove(CLASS_DEFAULT_ANNOTATION);
        }
    })
    .await;
    let current = env
        .wait_unresolved(&wp, REASON_DEFAULT_CONFIG_NOT_FOUND)
        .await;
    assert_eq!(status_config(&current, SandboxClass::Microvm), "mvdef-b");
    env.wait_warning_event(&wp, REASON_DEFAULT_CONFIG_NOT_FOUND)
        .await;

    // Once the pool is gone, nothing holds mvdef-b.
    env.delete_pool_and_wait(&wp).await;
    env.wait_config_gone("mvdef-b").await;
}

/// `run` with every controller: the egress MITM trust controller follows
/// the pool Secret through its watch, reverts a deleted bundle, and the
/// worker pool gauges are registered.
#[tokio::test]
async fn run_starts_every_controller() {
    use super::egress_mitm_trust::{EGRESS_MITM_CA_POOL_KEY, EGRESS_MITM_TRUST_BUNDLE_NAME};
    use hivekeep_identity::ca::{Ca, CaPool};
    use hivekeep_identity::key::KeyAlgorithm;
    use hivekeep_k8s::cluster_trust_bundle::ClusterTrustBundle;
    use k8s_openapi::api::core::v1::Secret;

    let fake = FakeApiServer::new();
    fake.serve(
        "certificates.k8s.io/v1",
        &[("clustertrustbundles", "ClusterTrustBundle", false)],
    );
    let client = fake.client();
    let stop = CancellationToken::new();
    let registry = prometheus::Registry::new();
    let cfg = ControllerConfig {
        system_namespace: SYSTEM_NAMESPACE.into(),
        keeper_service_account: KEEPER_SERVICE_ACCOUNT.into(),
        router_service_account: ROUTER_SERVICE_ACCOUNT.into(),
        dns_egress_policy: DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL.into(),
        otel: WorkerOtelSettings::default(),
    };
    let task = tokio::spawn(super::run(
        client.clone(),
        cfg,
        registry.clone(),
        stop.clone(),
    ));

    let pool = CaPool::new(
        vec![Ca::generate("mitm", KeyAlgorithm::Ed25519, Duration::from_secs(3600)).unwrap()],
        "",
    );
    let (ns, name) = super::egress_mitm_ca_pool_ref(SYSTEM_NAMESPACE);
    let mut secret = Secret {
        data: Some(BTreeMap::from([(
            EGRESS_MITM_CA_POOL_KEY.to_string(),
            k8s_openapi::ByteString(pool.to_json().unwrap()),
        )])),
        ..Default::default()
    };
    secret.metadata.name = Some(name.clone());
    secret.metadata.namespace = Some(ns.clone());
    Api::<Secret>::namespaced(client.clone(), &ns)
        .create(&PostParams::default(), &secret)
        .await
        .unwrap();
    let want = pool.cas()[0].root_pem();
    let published = || {
        fake.get::<ClusterTrustBundle>("", EGRESS_MITM_TRUST_BUNDLE_NAME)
            .is_some_and(|b| b.spec.trust_bundle == want)
    };
    wait_for("trust bundle published", published).await;

    // A deleted bundle is reverted through the bundle watch.
    let bundles: Api<kube::api::DynamicObject> = Api::all_with(
        client.clone(),
        &kube::discovery::ApiResource {
            group: "certificates.k8s.io".into(),
            version: "v1".into(),
            api_version: "certificates.k8s.io/v1".into(),
            kind: "ClusterTrustBundle".into(),
            plural: "clustertrustbundles".into(),
        },
    );
    bundles
        .delete(EGRESS_MITM_TRUST_BUNDLE_NAME, &DeleteParams::default())
        .await
        .unwrap();
    wait_for("trust bundle recreated", published).await;

    // The pool's controllers run too.
    let wp = make_worker_pool("test-run", "default", 2, "worker:v1");
    Api::<WorkerPool>::namespaced(client.clone(), "default")
        .create(&PostParams::default(), &wp)
        .await
        .unwrap();
    fake.insert(&make_sandbox_config("def", SandboxClass::Gvisor, true));
    wait_for("Deployment", || {
        fake.get::<Deployment>("default", "test-run").is_some()
    })
    .await;
    let names: Vec<String> = registry
        .gather()
        .iter()
        .map(|mf| mf.name().to_string())
        .collect();
    assert!(
        names
            .iter()
            .any(|n| n == "hivekeep_workerpool_desired_workers"),
        "{names:?}"
    );

    stop.cancel();
    task.await.unwrap().unwrap();
}
