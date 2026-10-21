//! The installer's cluster client: discovery-cached resource mapping,
//! retrying server-side apply, deletes, and polling waits.

use super::cluster::{Cluster, Propagation, ResourceInfo};
use super::{decode_manifest_bytes, describe, load_path, stub, Gvk, KubeError, Object, ObjectExt};
use crate::log;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

const CRD_KIND: &str = "CustomResourceDefinition";

/// Exponential backoff for retried applies.
#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    /// Total attempts, the first included.
    pub steps: u32,
    pub initial: Duration,
    pub factor: f64,
    /// Fraction of each delay added at random.
    pub jitter: f64,
}

impl Backoff {
    /// The retry budget for applies: the failures that routinely hit one -- a
    /// webhook whose pod is still starting, an object racing the CRD or
    /// namespace that admits it -- clear within a few seconds.
    pub const APPLY: Backoff = Backoff {
        steps: 5,
        initial: Duration::from_millis(500),
        factor: 2.0,
        jitter: 0.1,
    };

    fn delay(&self, attempt: u32) -> Duration {
        let base = self.initial.as_secs_f64() * self.factor.powi(attempt as i32);
        let jitter = if self.jitter > 0.0 {
            // Cheap pseudo-randomness is enough to de-synchronise retries.
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0);
            base * self.jitter * f64::from(nanos % 1000) / 1000.0
        } else {
            0.0
        };
        Duration::from_secs_f64(base + jitter)
    }
}

/// Default interval between polls of a wait.
pub const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Cluster client. Cheap to clone; clones share the discovery cache.
#[derive(Clone)]
pub struct Client {
    cluster: Arc<dyn Cluster>,
    mapper: Arc<Mutex<HashMap<Gvk, ResourceInfo>>>,
    pub(crate) poll_interval: Duration,
    pub(crate) apply_backoff: Backoff,
    cancel: CancellationToken,
}

impl Client {
    pub fn new(cluster: Arc<dyn Cluster>) -> Self {
        Self {
            cluster,
            mapper: Arc::default(),
            poll_interval: POLL_INTERVAL,
            apply_backoff: Backoff::APPLY,
            cancel: CancellationToken::new(),
        }
    }

    /// Connects to the cluster `kubeconfig` and `context` select.
    pub async fn connect(kubeconfig: &str, context: &str) -> Result<Self, KubeError> {
        let live = super::live::LiveCluster::connect(kubeconfig, context).await?;
        Ok(Self::new(Arc::new(live)))
    }

    /// Shortens the wait and retry timings, for tests.
    pub fn with_fast_timings(mut self) -> Self {
        self.poll_interval = Duration::from_millis(1);
        self.apply_backoff = Backoff {
            steps: self.apply_backoff.steps,
            initial: Duration::from_millis(1),
            factor: 1.0,
            jitter: 0.0,
        };
        self
    }

    /// The token that stops retries and waits, cancelled on interrupt.
    pub fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub fn with_cancellation(mut self, token: CancellationToken) -> Self {
        self.cancel = token;
        self
    }

    pub fn cluster(&self) -> &Arc<dyn Cluster> {
        &self.cluster
    }

    /// Drops every cached resource mapping, so the next request rediscovers.
    ///
    /// Needed after installing CRDs (their kinds become mappable) and after
    /// removing them (a stale mapping would send requests to a resource that
    /// no longer exists, which no lookup failure would reveal).
    pub fn invalidate_discovery(&self) {
        self.mapper.lock().clear();
    }

    /// Maps a gvk to its resource, rediscovering once on a miss.
    pub async fn resource_for(&self, gvk: &Gvk) -> Result<ResourceInfo, KubeError> {
        if let Some(r) = self.mapper.lock().get(gvk) {
            return Ok(r.clone());
        }
        let resolved = match self.cluster.resolve(gvk).await {
            Ok(r) => r,
            Err(e) if e.is_no_match() => {
                self.invalidate_discovery();
                self.cluster
                    .resolve(gvk)
                    .await
                    .map_err(|e| e.context(format!("while resolving {gvk}")))?
            }
            Err(e) => return Err(e.context(format!("while resolving {gvk}"))),
        };
        self.mapper.lock().insert(gvk.clone(), resolved.clone());
        Ok(resolved)
    }

    pub async fn server_version(&self) -> Result<String, KubeError> {
        self.cluster.server_version().await
    }

    /// Applies objects in order. Discovery is invalidated after each run of
    /// CRDs, so instances later in the same list map to the new kinds.
    pub async fn apply(&self, objs: &[Object]) -> Result<(), KubeError> {
        let _t = log::Timer::new(format!("apply of {} objects", objs.len()));
        let mut saw_crd = false;
        for obj in objs {
            if saw_crd && obj.kind() != CRD_KIND {
                self.invalidate_discovery();
                saw_crd = false;
            }
            self.apply_one(obj).await?;
            if obj.kind() == CRD_KIND {
                saw_crd = true;
            }
        }
        if saw_crd {
            self.invalidate_discovery();
        }
        Ok(())
    }

    /// Server-side applies one object, retrying transient failures and
    /// conflicts.
    pub async fn apply_one(&self, obj: &Object) -> Result<(), KubeError> {
        let res = self.resource_for(&obj.gvk()).await?;
        let mut attempt = 0;
        loop {
            match self.cluster.apply(&res, obj.obj_namespace(), obj).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    let retryable =
                        !self.cancel.is_cancelled() && (e.is_conflict() || e.is_retryable());
                    if !retryable || attempt >= self.apply_backoff.steps {
                        return Err(e.context(format!("while applying {}", describe(obj))));
                    }
                    let delay = self.apply_backoff.delay(attempt - 1);
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = self.cancel.cancelled() => {
                            return Err(e.context(format!("while applying {}", describe(obj))));
                        }
                    }
                }
            }
        }
    }

    pub async fn apply_path(&self, path: &Path) -> Result<(), KubeError> {
        self.apply(&load_path(path)?).await
    }

    pub async fn apply_bytes(&self, data: &[u8]) -> Result<(), KubeError> {
        self.apply(&decode_manifest_bytes(data)?).await
    }

    /// Applies objects, skipping (and reporting through `on_skip`) any whose
    /// kind the cluster does not serve.
    pub async fn apply_tolerant(
        &self,
        objs: &[Object],
        mut on_skip: impl FnMut(&Object, &KubeError),
    ) -> Result<(), KubeError> {
        for obj in objs {
            match self.apply_one(obj).await {
                Err(e) if e.is_no_match() => on_skip(obj, &e),
                Err(e) => return Err(e),
                Ok(()) => {}
            }
        }
        Ok(())
    }

    pub async fn delete(&self, objs: &[Object]) -> Result<(), KubeError> {
        let _t = log::Timer::new(format!("delete of {} objects", objs.len()));
        for obj in objs {
            self.delete_one(obj).await?;
        }
        Ok(())
    }

    /// Deletes one object. A kind the cluster does not serve, or an object
    /// already gone, is not an error.
    pub async fn delete_one(&self, obj: &Object) -> Result<(), KubeError> {
        let res = match self.resource_for(&obj.gvk()).await {
            Ok(r) => r,
            Err(e) if e.is_no_match() || e.is_not_found() => return Ok(()),
            Err(e) => return Err(e),
        };
        match self
            .cluster
            .delete(&res, obj.obj_namespace(), obj.name(), None)
            .await
        {
            Ok(()) => Ok(()),
            Err(e) if e.is_not_found() => Ok(()),
            Err(e) => Err(e.context(format!("while deleting {}", describe(obj)))),
        }
    }

    /// Deletes what a manifest path names. A path that does not exist is not
    /// an error: teardown walks every manifest any install shape uses.
    pub async fn delete_path(&self, path: &Path) -> Result<(), KubeError> {
        match load_path(path) {
            Err(e) if e.is_not_exist() => Ok(()),
            Err(e) => Err(e),
            Ok(objs) => self.delete(&objs).await,
        }
    }

    pub async fn delete_bytes(&self, data: &[u8]) -> Result<(), KubeError> {
        self.delete(&decode_manifest_bytes(data)?).await
    }

    /// Deletes by identity, ignoring absence.
    pub async fn delete_named(
        &self,
        gvk: &Gvk,
        namespace: &str,
        name: &str,
        propagation: Option<Propagation>,
    ) -> Result<(), KubeError> {
        let res = match self.resource_for(gvk).await {
            Ok(r) => r,
            Err(e) if e.is_no_match() => return Ok(()),
            Err(e) => return Err(e),
        };
        match self
            .cluster
            .delete(&res, namespace, name, propagation)
            .await
        {
            Err(e) if !e.is_not_found() => Err(e.context(format!(
                "while deleting {}",
                describe(&stub(gvk, namespace, name))
            ))),
            _ => Ok(()),
        }
    }

    /// Whether the object exists. An unserved kind means it does not.
    pub async fn exists(&self, gvk: &Gvk, namespace: &str, name: &str) -> Result<bool, KubeError> {
        Ok(self.get(gvk, namespace, name).await?.is_some())
    }

    /// Fetches an object; `None` when it or its kind does not exist.
    pub async fn get(
        &self,
        gvk: &Gvk,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Object>, KubeError> {
        let res = match self.resource_for(gvk).await {
            Ok(r) => r,
            Err(e) if e.is_no_match() => return Ok(None),
            Err(e) => return Err(e),
        };
        match self.cluster.get(&res, namespace, name).await {
            Ok(o) => Ok(Some(o)),
            Err(e) if e.is_not_found() => Ok(None),
            Err(e) => Err(e.context(format!(
                "while getting {}",
                describe(&stub(gvk, namespace, name))
            ))),
        }
    }

    /// Fetches a typed object; `None` when it does not exist.
    pub async fn get_typed<K>(&self, namespace: &str, name: &str) -> Result<Option<K>, KubeError>
    where
        K: k8s_openapi::Resource + serde::de::DeserializeOwned,
    {
        match self.get(&Gvk::of::<K>(), namespace, name).await? {
            None => Ok(None),
            Some(obj) => serde_json::from_value(obj)
                .map(Some)
                .map_err(|e| KubeError::Decode(e.to_string())),
        }
    }

    /// Lists objects of a kind. An unserved kind lists nothing.
    pub async fn list(
        &self,
        gvk: &Gvk,
        namespace: &str,
        label_selector: Option<&str>,
    ) -> Result<Vec<Object>, KubeError> {
        let res = match self.resource_for(gvk).await {
            Ok(r) => r,
            Err(e) if e.is_no_match() => return Ok(vec![]),
            Err(e) => return Err(e),
        };
        self.cluster.list(&res, namespace, label_selector).await
    }

    pub async fn patch(
        &self,
        gvk: &Gvk,
        namespace: &str,
        name: &str,
        patch: &super::Patch,
    ) -> Result<Object, KubeError> {
        let res = self.resource_for(gvk).await?;
        self.cluster.patch(&res, namespace, name, patch).await
    }

    pub async fn create(&self, obj: &Object) -> Result<Object, KubeError> {
        let res = self.resource_for(&obj.gvk()).await?;
        self.cluster.create(&res, obj.obj_namespace(), obj).await
    }

    pub async fn raw_get(&self, path: &str) -> Result<Vec<u8>, KubeError> {
        self.cluster.raw_get(path).await
    }

    /// Calls `check` immediately and then every poll interval until it
    /// reports done, fails permanently, or `timeout` passes.
    ///
    /// Transient failures (throttling, an API server or webhook still coming
    /// up, connection resets) are polled through; if the wait times out while
    /// they persist, the last one is named so the operator is not left with a
    /// bare deadline for a cluster that refused every request.
    pub async fn poll<F, Fut>(&self, timeout: Duration, mut check: F) -> Result<(), KubeError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<bool, KubeError>>,
    {
        let deadline = Instant::now() + timeout;
        let mut last_err: Option<KubeError>;
        loop {
            match check().await {
                Ok(true) => return Ok(()),
                Ok(false) => last_err = None,
                Err(e) if e.is_retryable() => last_err = Some(e),
                Err(e) => return Err(e),
            }
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let wait = self.poll_interval.min(deadline - now);
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = self.cancel.cancelled() => return Err(KubeError::Cancelled),
            }
            if Instant::now() >= deadline {
                break;
            }
        }
        let base = KubeError::Deadline("context deadline exceeded".into());
        Err(match last_err {
            Some(e) => KubeError::Deadline(format!("{base} (last error: {e})")),
            None => base,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube::fake::FakeCluster;
    use crate::kube::ResourceInfo;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn client(f: &FakeCluster) -> Client {
        Client::new(Arc::new(f.clone())).with_fast_timings()
    }

    fn test_config_map() -> Object {
        stub(
            &Gvk::new("", "v1", "ConfigMap"),
            "hivekeep-system",
            "hivekeep-api-envvars",
        )
    }

    /// Queues errors for successive applies, then succeeds.
    fn queue_apply_errors(f: &FakeCluster, errors: Vec<KubeError>) -> Arc<AtomicUsize> {
        let attempts = Arc::new(AtomicUsize::new(0));
        let a = attempts.clone();
        f.prepend_reactor("apply", "*", move |_| {
            let n = a.fetch_add(1, Ordering::SeqCst);
            errors.get(n).cloned().map(Err)
        });
        attempts
    }

    #[tokio::test]
    async fn delete_path_ignores_missing_path() {
        // No cluster behind this client: a request would fail the test.
        let f = FakeCluster::new();
        f.prepend_reactor("*", "*", |a| panic!("unexpected request {a:?}"));
        let c = client(&f);
        let dir = tempfile::tempdir().unwrap();
        for name in ["absent.yaml", "absent-dir"] {
            c.delete_path(&dir.path().join(name)).await.unwrap();
        }
    }

    #[tokio::test]
    async fn delete_path_reports_unparseable_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broken.yaml");
        std::fs::write(&path, "kind: [unterminated\n").unwrap();
        assert!(client(&FakeCluster::new())
            .delete_path(&path)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn apply_retries_transient_failures() {
        let f = FakeCluster::new();
        let errors = vec![
            KubeError::internal("failed calling webhook: connection refused"),
            KubeError::service_unavailable("apiserver is starting"),
            KubeError::conflict(
                "configmaps",
                "hivekeep-api-envvars",
                "the object has been modified",
            ),
        ];
        let n = errors.len();
        let attempts = queue_apply_errors(&f, errors);
        client(&f).apply_one(&test_config_map()).await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), n + 1);
    }

    #[tokio::test]
    async fn apply_reports_rejections_unchanged() {
        let f = FakeCluster::new();
        let attempts = queue_apply_errors(
            &f,
            vec![KubeError::invalid(
                "ConfigMap",
                "hivekeep-api-envvars",
                "bad",
            )],
        );
        let err = client(&f).apply_one(&test_config_map()).await.unwrap_err();
        assert!(err.is_invalid(), "{err}");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn apply_stops_on_cancellation() {
        let f = FakeCluster::new();
        let attempts = queue_apply_errors(
            &f,
            vec![
                KubeError::service_unavailable("apiserver is starting"),
                KubeError::service_unavailable("again"),
            ],
        );
        let c = client(&f);
        c.cancellation().cancel();
        assert!(c.apply_one(&test_config_map()).await.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn invalidate_discovery_rebuilds_the_mapping() {
        let f = FakeCluster::new();
        let secrets = ResourceInfo {
            gvk: Gvk::new("", "v1", "Secret"),
            plural: "secrets".into(),
            namespaced: true,
        };
        f.set_resources(vec![secrets.clone()]);
        let c = client(&f);
        let sandbox_config = Gvk::new("hivekeep.io", "v1", "SandboxConfig");
        assert!(
            c.resource_for(&sandbox_config).await.is_err(),
            "resolved before its CRD was installed"
        );

        let crd = ResourceInfo {
            gvk: sandbox_config.clone(),
            plural: "sandboxconfigs".into(),
            namespaced: true,
        };
        f.set_resources(vec![secrets.clone(), crd.clone()]);
        c.invalidate_discovery();
        assert_eq!(c.resource_for(&sandbox_config).await.unwrap(), crd);

        // The silently wrong direction: a mapping that still resolves is
        // returned stale unless the cache is dropped.
        f.set_resources(vec![secrets]);
        c.invalidate_discovery();
        assert!(
            c.resource_for(&sandbox_config).await.is_err(),
            "still resolved after its CRD left discovery"
        );
    }

    #[tokio::test]
    async fn poll_recovers_from_retryable_errors() {
        let c = client(&FakeCluster::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let k = calls.clone();
        c.poll(Duration::from_secs(60), move || {
            let n = k.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                match n {
                    1 => Err(KubeError::too_many_requests("slow down")),
                    2 => Err(KubeError::service_unavailable("apiserver is starting")),
                    _ => Ok(true),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn poll_reports_the_last_retryable_error() {
        let c = client(&FakeCluster::new());
        let err = c
            .poll(Duration::from_millis(10), || async {
                Err(KubeError::too_many_requests("slow down"))
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("slow down"), "{err}");
    }

    #[tokio::test]
    async fn poll_aborts_on_permanent_errors() {
        let c = client(&FakeCluster::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let k = calls.clone();
        let err = c
            .poll(Duration::from_secs(60), move || {
                k.fetch_add(1, Ordering::SeqCst);
                async { Err(KubeError::forbidden("deployments.apps", "api", "nope")) }
            })
            .await
            .unwrap_err();
        assert!(err.is_forbidden(), "{err}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
