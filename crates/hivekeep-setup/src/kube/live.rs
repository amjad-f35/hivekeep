//! [`Cluster`] over a real API server, through the `kube` client.

use super::cluster::{Cluster, ExecOutput, Patch, Propagation, ResourceInfo};
use super::{Gvk, KubeError, Object, FIELD_MANAGER};
use async_trait::async_trait;
use kube::api::{
    Api, AttachParams, DeleteParams, DynamicObject, ListParams, PatchParams, PostParams,
};
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::discovery::{ApiResource, Scope};
use std::path::Path;
use tokio::io::AsyncReadExt;

/// The live cluster connection.
#[derive(Clone)]
pub struct LiveCluster {
    client: kube::Client,
}

impl LiveCluster {
    /// Connects using `kubeconfig` (a single file; empty means the default
    /// loading rules, which honour a `$KUBECONFIG` list) and `context` (empty
    /// means the current context).
    pub async fn connect(kubeconfig: &str, context: &str) -> Result<Self, KubeError> {
        let opts = KubeConfigOptions {
            context: (!context.is_empty()).then(|| context.to_string()),
            ..Default::default()
        };
        let config = if kubeconfig.is_empty() {
            kube::Config::from_kubeconfig(&opts).await
        } else {
            let kc = Kubeconfig::read_from(Path::new(kubeconfig))
                .map_err(|e| KubeError::Config(format!("while reading kubeconfig: {e}")))?;
            kube::Config::from_custom_kubeconfig(kc, &opts).await
        }
        .map_err(|e| KubeError::Config(format!("while reading kubeconfig: {e}")))?;
        let client = kube::Client::try_from(config)
            .map_err(|e| KubeError::Config(format!("while creating the Kubernetes client: {e}")))?;
        Ok(Self { client })
    }

    pub fn from_client(client: kube::Client) -> Self {
        Self { client }
    }

    fn api(&self, res: &ResourceInfo, namespace: &str) -> Api<DynamicObject> {
        let ar = ApiResource {
            group: res.gvk.group.clone(),
            version: res.gvk.version.clone(),
            api_version: res.gvk.api_version(),
            kind: res.gvk.kind.clone(),
            plural: res.plural.clone(),
        };
        let ns = res.effective_namespace(namespace);
        if ns.is_empty() {
            Api::all_with(self.client.clone(), &ar)
        } else {
            Api::namespaced_with(self.client.clone(), ns, &ar)
        }
    }
}

fn to_object(obj: DynamicObject) -> Result<Object, KubeError> {
    serde_json::to_value(obj).map_err(|e| KubeError::Decode(e.to_string()))
}

fn to_dynamic(obj: &Object) -> Result<DynamicObject, KubeError> {
    serde_json::from_value(obj.clone()).map_err(|e| KubeError::Decode(e.to_string()))
}

#[async_trait]
impl Cluster for LiveCluster {
    async fn resolve(&self, gvk: &Gvk) -> Result<ResourceInfo, KubeError> {
        let kgvk = kube::api::GroupVersionKind::gvk(&gvk.group, &gvk.version, &gvk.kind);
        match kube::discovery::pinned_kind(&self.client, &kgvk).await {
            Ok((ar, caps)) => Ok(ResourceInfo {
                gvk: gvk.clone(),
                plural: ar.plural,
                namespaced: matches!(caps.scope, Scope::Namespaced),
            }),
            Err(kube::Error::Api(status)) if status.code == 404 => Err(KubeError::no_match(gvk)),
            Err(kube::Error::Discovery(_)) => Err(KubeError::no_match(gvk)),
            Err(e) => Err(e.into()),
        }
    }

    async fn group_versions(&self, group: &str) -> Result<Vec<String>, KubeError> {
        if group.is_empty() {
            let v = self.client.list_core_api_versions().await?;
            return Ok(v.versions);
        }
        let groups = self.client.list_api_groups().await?;
        Ok(groups
            .groups
            .into_iter()
            .filter(|g| g.name == group)
            .flat_map(|g| g.versions.into_iter().map(|v| v.version))
            .collect())
    }

    async fn server_version(&self) -> Result<String, KubeError> {
        let info = self
            .client
            .apiserver_version()
            .await
            .map_err(|e| KubeError::from(e).context("while contacting the cluster"))?;
        Ok(info.git_version)
    }

    async fn apply(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        obj: &Object,
    ) -> Result<Object, KubeError> {
        let name = obj
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("");
        let pp = PatchParams::apply(FIELD_MANAGER).force();
        let out = self
            .api(res, namespace)
            .patch(name, &pp, &kube::api::Patch::Apply(obj))
            .await?;
        to_object(out)
    }

    async fn get(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        name: &str,
    ) -> Result<Object, KubeError> {
        to_object(self.api(res, namespace).get(name).await?)
    }

    async fn list(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        label_selector: Option<&str>,
    ) -> Result<Vec<Object>, KubeError> {
        let mut lp = ListParams::default();
        if let Some(sel) = label_selector {
            lp = lp.labels(sel);
        }
        let api = if res.namespaced && namespace.is_empty() {
            let ar = ApiResource {
                group: res.gvk.group.clone(),
                version: res.gvk.version.clone(),
                api_version: res.gvk.api_version(),
                kind: res.gvk.kind.clone(),
                plural: res.plural.clone(),
            };
            Api::all_with(self.client.clone(), &ar)
        } else {
            self.api(res, namespace)
        };
        let list = api.list(&lp).await?;
        let kind = res.gvk.kind.clone();
        let api_version = res.gvk.api_version();
        list.items
            .into_iter()
            .map(|mut o| {
                // List items arrive without their type fields.
                if o.types.is_none() {
                    o.types = Some(kube::api::TypeMeta {
                        api_version: api_version.clone(),
                        kind: kind.clone(),
                    });
                }
                to_object(o)
            })
            .collect()
    }

    async fn create(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        obj: &Object,
    ) -> Result<Object, KubeError> {
        let out = self
            .api(res, namespace)
            .create(&PostParams::default(), &to_dynamic(obj)?)
            .await?;
        to_object(out)
    }

    async fn delete(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        name: &str,
        propagation: Option<Propagation>,
    ) -> Result<(), KubeError> {
        let dp = match propagation {
            Some(Propagation::Foreground) => DeleteParams::foreground(),
            Some(Propagation::Background) => DeleteParams::background(),
            Some(Propagation::Orphan) => DeleteParams::orphan(),
            None => DeleteParams::default(),
        };
        self.api(res, namespace).delete(name, &dp).await?;
        Ok(())
    }

    async fn patch(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        name: &str,
        patch: &Patch,
    ) -> Result<Object, KubeError> {
        let pp = PatchParams::default();
        let api = self.api(res, namespace);
        let out = match patch {
            Patch::Merge(v) => api.patch(name, &pp, &kube::api::Patch::Merge(v)).await?,
            Patch::Strategic(v) => {
                api.patch(name, &pp, &kube::api::Patch::Strategic(v))
                    .await?
            }
        };
        to_object(out)
    }

    async fn raw_get(&self, path: &str) -> Result<Vec<u8>, KubeError> {
        let req = http::Request::get(path)
            .body(Vec::new())
            .map_err(|e| KubeError::Other(e.to_string()))?;
        let text = self.client.request_text(req).await?;
        Ok(text.into_bytes())
    }

    async fn exec(
        &self,
        namespace: &str,
        pod: &str,
        container: &str,
        command: &[String],
        stdin: Option<Vec<u8>>,
    ) -> Result<ExecOutput, KubeError> {
        use tokio::io::AsyncWriteExt;
        let pods: Api<k8s_openapi::api::core::v1::Pod> =
            Api::namespaced(self.client.clone(), namespace);
        let ap = AttachParams::default()
            .container(container)
            .stdin(stdin.is_some())
            .stdout(true)
            .stderr(true);
        let wrap =
            |e: kube::Error| KubeError::from(e).context(format!("exec in pod {namespace}/{pod}"));
        let mut proc = pods.exec(pod, command.to_vec(), &ap).await.map_err(wrap)?;
        if let (Some(data), Some(mut w)) = (stdin, proc.stdin()) {
            w.write_all(&data)
                .await
                .map_err(|e| KubeError::io(&e, "while writing exec stdin"))?;
            w.shutdown()
                .await
                .map_err(|e| KubeError::io(&e, "while closing exec stdin"))?;
        }
        let mut out = ExecOutput::default();
        if let Some(mut r) = proc.stdout() {
            r.read_to_end(&mut out.stdout)
                .await
                .map_err(|e| KubeError::io(&e, "while reading exec stdout"))?;
        }
        if let Some(mut r) = proc.stderr() {
            r.read_to_end(&mut out.stderr)
                .await
                .map_err(|e| KubeError::io(&e, "while reading exec stderr"))?;
        }
        let status = proc.take_status();
        proc.join()
            .await
            .map_err(|e| KubeError::Other(format!("exec in pod {namespace}/{pod}: {e}")))?;
        if let Some(status) = status {
            if let Some(s) = status.await {
                if s.status.as_deref() == Some("Failure") {
                    return Err(KubeError::Other(format!(
                        "exec in pod {namespace}/{pod}: {}",
                        s.message.unwrap_or_default()
                    )));
                }
            }
        }
        Ok(out)
    }
}
