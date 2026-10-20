//! Wires the controllers to shared informer caches and runs them.
//!
//! Each controller has its own work queue and one worker. A WorkerPool is
//! reconciled when it changes, when a Deployment it controls changes, and
//! when a SandboxConfig that can change its resolution changes. A
//! SandboxConfig is reconciled for protection when it changes and when a
//! pool whose status names it changes. Every cached object is queued once
//! at startup.

use std::sync::Arc;

use anyhow::Context as _;
use futures::StreamExt;
use hivekeep_core::preview::PreviewGates;
use hivekeep_k8s::cluster_trust_bundle::ClusterTrustBundleClient;
use hivekeep_k8s::crd::{SandboxConfig, WorkerPool, GROUP};
use hivekeep_k8s::informer::{Informer, InformerEvent};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::api::networking::v1::NetworkPolicy;
use kube::api::Api;
use kube::runtime::watcher;
use kube::{Client, Resource, ResourceExt};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

use super::egress_mitm_trust::{
    egress_mitm_ca_pool_ref, EgressMitmTrustReconciler, EGRESS_MITM_TRUST_BUNDLE_NAME,
};
use super::events::KubeEventRecorder;
use super::meta::{controller_of, in_group};
use super::networkpolicy::NetworkPolicyReconciler;
use super::queue::{ObjectKey, WorkQueue};
use super::reader::Reader;
use super::runner::{spawn_watch, spawn_workers};
use super::sandboxconfig_protection::{
    sandbox_configs_for_worker_pool, SandboxConfigProtectionReconciler,
};
use super::worker_env::preview_gates;
use super::workerpool::{worker_pools_for_sandbox_config, WorkerPoolReconciler};
use super::workerpool_apply::WorkerPodSettings;
use super::workerpool_metrics::WorkerPoolMetrics;
use super::ControllerConfig;

/// Which controllers to run.
#[derive(Debug, Clone, Copy)]
pub struct Controllers {
    /// The WorkerPool controller.
    pub worker_pool: bool,
    /// The SandboxConfig protection controller.
    pub sandbox_config_protection: bool,
    /// The NetworkPolicy controller.
    pub network_policy: bool,
    /// The egress MITM trust controller.
    pub egress_mitm_trust: bool,
}

impl Controllers {
    /// Every controller.
    pub fn all() -> Self {
        Controllers {
            worker_pool: true,
            sandbox_config_protection: true,
            network_policy: true,
            egress_mitm_trust: true,
        }
    }
}

/// The key of the WorkerPool controlling `obj`, if one does.
fn owning_pool<K: Resource>(obj: &K) -> Vec<ObjectKey> {
    let meta = obj.meta();
    match controller_of(meta) {
        Some(o) if o.kind == "WorkerPool" && in_group(&o.api_version, GROUP) => {
            vec![ObjectKey::new(
                meta.namespace.clone().unwrap_or_default(),
                o.name.clone(),
            )]
        }
        _ => Vec::new(),
    }
}

fn events<K>(informer: &Informer<K>) -> UnboundedReceiverStream<InformerEvent<K>>
where
    K: Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug + Send + Sync + 'static,
{
    UnboundedReceiverStream::new(informer.events())
}

/// Runs the selected controllers until `shutdown` fires.
pub async fn run_controllers(
    client: Client,
    cfg: ControllerConfig,
    registry: prometheus::Registry,
    shutdown: CancellationToken,
    which: Controllers,
) -> anyhow::Result<()> {
    run_with_preview(client, cfg, registry, shutdown, which, preview_gates()).await
}

/// [`run_controllers`] with explicit preview gates for worker pods.
pub async fn run_with_preview(
    client: Client,
    cfg: ControllerConfig,
    registry: prometheus::Registry,
    shutdown: CancellationToken,
    which: Controllers,
    preview: PreviewGates,
) -> anyhow::Result<()> {
    let pools = Informer::new(
        Api::<WorkerPool>::all(client.clone()),
        watcher::Config::default(),
    );
    let deployments = Informer::new(
        Api::<Deployment>::all(client.clone()),
        watcher::Config::default(),
    );
    let configs = Informer::new(
        Api::<SandboxConfig>::all(client.clone()),
        watcher::Config::default(),
    );
    let policies = Informer::new(
        Api::<NetworkPolicy>::all(client.clone()),
        watcher::Config::default(),
    );
    let (pool_ns, pool_name) = egress_mitm_ca_pool_ref(&cfg.system_namespace);
    // Only the pool Secret is cached, which RBAC grants by name.
    let secrets = Informer::new(
        Api::<Secret>::namespaced(client.clone(), &pool_ns),
        watcher::Config::default().fields(&format!("metadata.name={pool_name}")),
    );
    let bundles = if which.egress_mitm_trust {
        Some(
            ClusterTrustBundleClient::new(client.clone(), None)
                .await
                .context("discovering the ClusterTrustBundle API")?,
        )
    } else {
        None
    };

    WorkerPoolMetrics::register(&registry, {
        let pools = pools.clone();
        move || pools.list()
    })
    .context("registering the worker pool metrics")?;
    super::runner::register_metrics(&registry).context("registering the reconcile metrics")?;

    let mut tasks: Vec<JoinHandle<()>> = Vec::new();
    let mut queues: Vec<Arc<WorkQueue<ObjectKey>>> = Vec::new();

    let wp_queue = WorkQueue::new();
    let prot_queue = WorkQueue::new();
    let np_queue = WorkQueue::new();
    let egress_queue = WorkQueue::new();

    // Watches subscribe before the informers start so the initial lists
    // queue every object.
    if which.worker_pool {
        tasks.push(spawn_watch(
            events(&pools),
            wp_queue.clone(),
            shutdown.clone(),
            |wp: Arc<WorkerPool>| async move { vec![ObjectKey::of(wp.as_ref())] },
        ));
        tasks.push(spawn_watch(
            events(&deployments),
            wp_queue.clone(),
            shutdown.clone(),
            |d: Arc<Deployment>| async move { owning_pool(d.as_ref()) },
        ));
        let reader = Reader::cached(pools.clone());
        tasks.push(spawn_watch(
            events(&configs),
            wp_queue.clone(),
            shutdown.clone(),
            move |sc: Arc<SandboxConfig>| {
                let reader = reader.clone();
                async move { worker_pools_for_sandbox_config(&reader, &sc).await }
            },
        ));
    }
    if which.sandbox_config_protection {
        tasks.push(spawn_watch(
            events(&configs),
            prot_queue.clone(),
            shutdown.clone(),
            |sc: Arc<SandboxConfig>| async move { vec![ObjectKey::cluster(sc.name_any())] },
        ));
        // For an update this maps the old and the new pool, so the config
        // a pool switches away from is reconciled too.
        tasks.push(spawn_watch(
            events(&pools),
            prot_queue.clone(),
            shutdown.clone(),
            |wp: Arc<WorkerPool>| async move { sandbox_configs_for_worker_pool(&wp) },
        ));
    }
    if which.network_policy {
        tasks.push(spawn_watch(
            events(&pools),
            np_queue.clone(),
            shutdown.clone(),
            |wp: Arc<WorkerPool>| async move { vec![ObjectKey::of(wp.as_ref())] },
        ));
        tasks.push(spawn_watch(
            events(&policies),
            np_queue.clone(),
            shutdown.clone(),
            |np: Arc<NetworkPolicy>| async move { owning_pool(np.as_ref()) },
        ));
    }
    if let Some(b) = &bundles {
        let pool_key = ObjectKey::new(pool_ns.clone(), pool_name.clone());
        let key = pool_key.clone();
        tasks.push(spawn_watch(
            events(&secrets),
            egress_queue.clone(),
            shutdown.clone(),
            move |s: Arc<Secret>| {
                let key = key.clone();
                async move {
                    if ObjectKey::of(s.as_ref()) == key {
                        vec![key]
                    } else {
                        Vec::new()
                    }
                }
            },
        ));
        // The derived bundle is watched too, so deleting or hand-editing it
        // is reverted.
        let key = pool_key.clone();
        let bundle_events = b.events().filter(|ev| {
            futures::future::ready(
                ev.object().metadata.name.as_deref() == Some(EGRESS_MITM_TRUST_BUNDLE_NAME),
            )
        });
        tasks.push(spawn_watch(
            bundle_events,
            egress_queue.clone(),
            shutdown.clone(),
            move |_| {
                let key = key.clone();
                async move { vec![key] }
            },
        ));
        tasks.push(b.informer().spawn());
    }

    let mut informer_tasks = vec![
        pools.spawn(),
        deployments.spawn(),
        configs.spawn(),
        policies.spawn(),
    ];
    if bundles.is_some() {
        informer_tasks.push(secrets.spawn());
    }
    let synced = async {
        pools.wait_synced().await;
        deployments.wait_synced().await;
        configs.wait_synced().await;
        policies.wait_synced().await;
        if let Some(b) = &bundles {
            secrets.wait_synced().await;
            b.informer().wait_synced().await;
        }
    };
    tokio::select! {
        _ = synced => {}
        _ = shutdown.cancelled() => {
            for t in informer_tasks.into_iter().chain(tasks) {
                t.abort();
            }
            return Ok(());
        }
    }
    tracing::info!("Controller caches synced; starting workers");

    let mut workers = Vec::new();
    if which.worker_pool {
        let pod_settings = WorkerPodSettings {
            otel: cfg.otel.clone(),
            system_namespace: cfg.system_namespace.clone(),
            keeper_service_account: cfg.keeper_service_account.clone(),
            router_service_account: cfg.router_service_account.clone(),
            dns_egress_policy: cfg.dns_egress_policy.clone(),
            preview,
        };
        let r = WorkerPoolReconciler::new(
            client.clone(),
            client.clone(),
            Reader::cached(pools.clone()),
            Reader::cached(deployments.clone()),
            Reader::cached(configs.clone()),
            Arc::new(KubeEventRecorder::new(
                client.clone(),
                "workerpool-controller",
            )),
            pod_settings,
        );
        workers.extend(spawn_workers(
            "workerpool",
            Arc::new(r),
            wp_queue.clone(),
            1,
            shutdown.clone(),
        ));
        queues.push(wp_queue);
    }
    if which.sandbox_config_protection {
        let r = SandboxConfigProtectionReconciler::new(
            client.clone(),
            client.clone(),
            Reader::cached(configs.clone()),
            Reader::cached(pools.clone()),
        );
        workers.extend(spawn_workers(
            "sandboxconfig-protection",
            Arc::new(r),
            prot_queue.clone(),
            1,
            shutdown.clone(),
        ));
        queues.push(prot_queue);
    }
    if which.network_policy {
        let r = NetworkPolicyReconciler::new(
            client.clone(),
            Reader::cached(pools.clone()),
            Reader::cached(policies.clone()),
            cfg.system_namespace.clone(),
            Arc::new(KubeEventRecorder::new(
                client.clone(),
                "networkpolicy-controller",
            )),
        );
        workers.extend(spawn_workers(
            "networkpolicy",
            Arc::new(r),
            np_queue.clone(),
            1,
            shutdown.clone(),
        ));
        queues.push(np_queue);
    }
    if let Some(b) = bundles {
        let r = EgressMitmTrustReconciler::new(client.clone(), Reader::cached(secrets.clone()), b);
        workers.extend(spawn_workers(
            "egressmitmtrust",
            Arc::new(r),
            egress_queue.clone(),
            1,
            shutdown.clone(),
        ));
        queues.push(egress_queue);
    }

    shutdown.cancelled().await;
    tracing::info!("Stopping controllers");
    for q in &queues {
        q.shut_down();
    }
    for w in workers {
        let _ = w.await;
    }
    for t in informer_tasks.into_iter().chain(tasks) {
        t.abort();
    }
    Ok(())
}
