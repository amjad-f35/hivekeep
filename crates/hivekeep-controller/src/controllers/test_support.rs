//! Builders and polling helpers shared by the controller tests.

use std::future::Future;
use std::time::Duration;

use hivekeep_k8s::crd::sandboxconfig::CLASS_DEFAULT_ANNOTATION;
use hivekeep_k8s::crd::{
    SandboxClass, SandboxConfig, SandboxConfigSpec, SandboxVersionConfig, WorkerPool,
    WorkerPoolSandboxClass, WorkerPoolSandboxClassStatus, WorkerPoolSpec, WorkerPoolStatus,
};
use hivekeep_k8s::install_defaults::{
    KEEPER_SERVICE_ACCOUNT, ROUTER_SERVICE_ACCOUNT, SYSTEM_NAMESPACE,
};
use std::collections::BTreeMap;

use super::worker_env::{WorkerOtelSettings, DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL};
use super::workerpool_apply::WorkerPodSettings;
use super::workerpool_sandboxconfig::sandbox_class_status;

/// A finalizer that keeps a config on the API server after its delete, so
/// a test can observe how a config being deleted is treated.
pub const TEST_HOLD_FINALIZER: &str = "test.hivekeep.io/hold";

/// A one-class gvisor pool.
pub fn make_worker_pool(name: &str, ns: &str, replicas: i32, image: &str) -> WorkerPool {
    let mut wp = WorkerPool::new(
        name,
        WorkerPoolSpec {
            replicas,
            worker_image: image.into(),
            template: None,
            sandbox_classes: vec![WorkerPoolSandboxClass {
                name: SandboxClass::Gvisor,
                config_ref: None,
            }],
        },
    );
    wp.metadata.namespace = Some(ns.into());
    wp
}

/// A pool in `ns` with the given spec and status classes.
pub fn make_worker_pool_classes(
    ns: &str,
    name: &str,
    spec: Vec<WorkerPoolSandboxClass>,
    status: Vec<WorkerPoolSandboxClassStatus>,
) -> WorkerPool {
    let mut wp = make_worker_pool(name, ns, 1, "worker:v1");
    wp.spec.sandbox_classes = spec;
    if !status.is_empty() {
        wp.status = Some(WorkerPoolStatus {
            sandbox_classes: status,
            ..Default::default()
        });
    }
    wp
}

/// A one-version config of `class`, annotated as the class default when
/// `is_default`.
pub fn make_sandbox_config(name: &str, class: SandboxClass, is_default: bool) -> SandboxConfig {
    let mut sc = SandboxConfig::new(
        name,
        SandboxConfigSpec {
            sandbox_class: class,
            default_version: "v1".into(),
            versions: vec![SandboxVersionConfig {
                name: "v1".into(),
                ..Default::default()
            }],
        },
    );
    if is_default {
        sc.metadata.annotations = Some(BTreeMap::from([(
            CLASS_DEFAULT_ANNOTATION.into(),
            "true".into(),
        )]));
    }
    sc
}

/// Adds `finalizer` to `sc`.
pub fn with_finalizer(mut sc: SandboxConfig, finalizer: &str) -> SandboxConfig {
    sc.metadata
        .finalizers
        .get_or_insert_with(Vec::new)
        .push(finalizer.into());
    sc
}

/// A status entry of `class` using `config` and claiming `pending`.
pub fn class_status(
    class: SandboxClass,
    config: Option<&str>,
    pending: Option<&str>,
) -> WorkerPoolSandboxClassStatus {
    WorkerPoolSandboxClassStatus {
        name: class,
        config_ref: config.map(hivekeep_k8s::crd::SandboxConfigReference::new),
        pending_config_ref: pending.map(hivekeep_k8s::crd::SandboxConfigReference::new),
        ..Default::default()
    }
}

/// The config `wp`'s status uses for `class`, or "".
pub fn status_config(wp: &WorkerPool, class: SandboxClass) -> String {
    sandbox_class_status(wp, class)
        .and_then(|s| s.config_ref.as_ref())
        .map(|r| r.name.clone())
        .unwrap_or_default()
}

/// Worker pod settings of the canonical install.
pub fn pod_settings() -> WorkerPodSettings {
    WorkerPodSettings {
        otel: WorkerOtelSettings::default(),
        system_namespace: SYSTEM_NAMESPACE.into(),
        keeper_service_account: KEEPER_SERVICE_ACCOUNT.into(),
        router_service_account: ROUTER_SERVICE_ACCOUNT.into(),
        dns_egress_policy: DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL.into(),
        preview: Default::default(),
    }
}

/// Polls `cond` every 50ms until it holds, failing after 15s.
pub async fn eventually<F, Fut>(what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if cond().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within timeout: {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Runs `fut` to completion from synchronous code on a multi-thread
/// runtime, e.g. inside a fake API server reactor.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
}

/// Which finalizer operation a [`ProtectionFinalizerCase`] runs.
#[derive(Debug, Clone, Copy)]
pub enum FinalizerOp {
    /// `add_protection_finalizer`.
    Add,
    /// `remove_protection_finalizer`.
    Remove,
}

/// One case of a test applying a protection finalizer operation.
pub struct ProtectionFinalizerCase {
    pub name: &'static str,
    pub op: FinalizerOp,
    pub sc: SandboxConfig,
    /// Changes the stored config after it is read, so the patch carries an
    /// outdated resourceVersion.
    pub stale: bool,
    pub want_patches: usize,
    pub want: bool,
    pub want_conflict: bool,
}

/// A config for a [`ProtectionFinalizerCase`]: it always carries
/// [`TEST_HOLD_FINALIZER`], so it stays readable while deleting.
pub fn protected_sandbox_config(protected: bool, deleting: bool) -> SandboxConfig {
    let mut sc = with_finalizer(
        make_sandbox_config("sc", SandboxClass::Gvisor, false),
        TEST_HOLD_FINALIZER,
    );
    if protected {
        sc = with_finalizer(
            sc,
            hivekeep_k8s::crd::sandboxconfig::WORKER_POOL_PROTECTION_FINALIZER,
        );
    }
    if deleting {
        sc.metadata.deletion_timestamp = Some(super::meta::now());
    }
    sc
}

/// Runs each case's operation against a fake API server and checks the
/// patches made, the error, the protection finalizer, and that
/// [`TEST_HOLD_FINALIZER`] survived.
pub async fn run_protection_finalizer_cases(cases: Vec<ProtectionFinalizerCase>) {
    use hivekeep_k8s::crd::sandboxconfig::WORKER_POOL_PROTECTION_FINALIZER;
    use hivekeep_k8s::fake::{FakeApiServer, Reaction};
    use kube::api::{Api, PostParams};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::meta::has_finalizer;
    use super::workerpool::{add_protection_finalizer, remove_protection_finalizer};

    for tc in cases {
        let fake = FakeApiServer::new();
        fake.insert(&tc.sc);
        let patches = Arc::new(AtomicUsize::new(0));
        let p = patches.clone();
        fake.prepend_reactor("patch", "sandboxconfigs", move |_| {
            p.fetch_add(1, Ordering::SeqCst);
            Reaction::Pass
        });
        let client = fake.client();
        let api: Api<SandboxConfig> = Api::all(client.clone());
        let mut read = api.get("sc").await.unwrap();
        if tc.stale {
            let mut stored = read.clone();
            stored.metadata.labels = Some(BTreeMap::from([("changed".into(), "true".into())]));
            api.replace("sc", &PostParams::default(), &stored)
                .await
                .unwrap();
        }
        let res = match tc.op {
            FinalizerOp::Add => add_protection_finalizer(&client, &mut read).await,
            FinalizerOp::Remove => remove_protection_finalizer(&client, &mut read).await,
        };
        match (&res, tc.want_conflict) {
            (Ok(()), false) => {}
            (Err(e), true) => assert!(e.is_conflict(), "{}: error {e} is not a conflict", tc.name),
            (r, _) => panic!("{}: unexpected result {r:?}", tc.name),
        }
        assert_eq!(
            patches.load(Ordering::SeqCst),
            tc.want_patches,
            "{}: patches",
            tc.name
        );
        let got = fake
            .get::<SandboxConfig>("", "sc")
            .expect("config still stored");
        assert_eq!(
            has_finalizer(&got.metadata, WORKER_POOL_PROTECTION_FINALIZER),
            tc.want,
            "{}: protected",
            tc.name
        );
        assert!(
            has_finalizer(&got.metadata, TEST_HOLD_FINALIZER),
            "{}: lost {TEST_HOLD_FINALIZER}",
            tc.name
        );
    }
}
