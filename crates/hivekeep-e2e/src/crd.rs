//! The `hivekeep.io/v1` custom resources the harness reads and creates,
//! as untyped kube objects plus the WorkerPool fields it touches.

use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use serde::{Deserialize, Serialize};

/// The CRD group.
pub const GROUP: &str = "hivekeep.io";
/// The CRD version.
pub const VERSION: &str = "v1";

/// The WorkerPool resource (namespaced).
pub fn worker_pool_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk(GROUP, VERSION, "WorkerPool"),
        "workerpools",
    )
}

/// The SandboxConfig resource (cluster-scoped).
pub fn sandbox_config_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk(GROUP, VERSION, "SandboxConfig"),
        "sandboxconfigs",
    )
}

/// One sandbox class of a pool.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SandboxClassEntry {
    /// The runtime family.
    pub name: String,
    /// The SandboxConfig it pins, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_ref: Option<serde_json::Value>,
}

/// The WorkerPool spec, strict so a misplaced manifest field is caught.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerPoolSpec {
    /// Number of worker pods.
    #[serde(default)]
    pub replicas: i32,
    /// The worker container image.
    pub worker_image: String,
    /// Pod template overrides, passed through.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<serde_json::Value>,
    /// The runtime families the pool runs (one today).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sandbox_classes: Vec<SandboxClassEntry>,
}

impl WorkerPoolSpec {
    /// The class of the first entry.
    pub fn default_sandbox_class(&self) -> Option<&str> {
        self.sandbox_classes.first().map(|c| c.name.as_str())
    }
}

/// A WorkerPool document as a manifest declares it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerPoolDoc {
    /// `hivekeep.io/v1`.
    pub api_version: String,
    /// `WorkerPool`.
    pub kind: String,
    /// Object metadata.
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    /// The spec.
    pub spec: WorkerPoolSpec,
}

/// The typed spec of a fetched WorkerPool.
pub fn worker_pool_spec(obj: &DynamicObject) -> Result<WorkerPoolSpec, String> {
    let spec = obj.data.get("spec").cloned().unwrap_or_default();
    // Fetched objects may carry fields this client does not model.
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Loose {
        #[serde(default)]
        replicas: i32,
        #[serde(default)]
        worker_image: String,
        #[serde(default)]
        template: Option<serde_json::Value>,
        #[serde(default)]
        sandbox_classes: Vec<serde_json::Value>,
    }
    let loose: Loose = serde_json::from_value(spec).map_err(|e| e.to_string())?;
    Ok(WorkerPoolSpec {
        replicas: loose.replicas,
        worker_image: loose.worker_image,
        template: loose.template,
        sandbox_classes: loose
            .sandbox_classes
            .into_iter()
            .map(|v| SandboxClassEntry {
                name: v
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or_default()
                    .to_string(),
                config_ref: v.get("configRef").cloned(),
            })
            .collect(),
    })
}

/// A new WorkerPool object for `namespace/name`.
pub fn new_worker_pool(
    namespace: &str,
    name: &str,
    labels: std::collections::BTreeMap<String, String>,
    spec: &WorkerPoolSpec,
) -> DynamicObject {
    let mut obj = DynamicObject::new(name, &worker_pool_resource()).within(namespace);
    obj.metadata.labels = Some(labels);
    obj.data = serde_json::json!({ "spec": spec });
    obj
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_pool_documents() {
        let doc = "apiVersion: hivekeep.io/v1\nkind: WorkerPool\nmetadata:\n  name: p\nspec:\n  replicas: 1\n  workerImage: img\n  sandboxClasses:\n  - name: gvisor\n";
        let pool: WorkerPoolDoc = serde_yaml_ng::from_str(doc).unwrap();
        assert_eq!(pool.spec.default_sandbox_class(), Some("gvisor"));
        let misplaced = "apiVersion: hivekeep.io/v1\nkind: WorkerPool\nmetadata:\n  name: p\nspec:\n  workerImage: img\nsandboxClasses:\n- name: gvisor\n";
        assert!(serde_yaml_ng::from_str::<WorkerPoolDoc>(misplaced).is_err());
        let obj = new_worker_pool("ns", "p", Default::default(), &pool.spec);
        assert_eq!(worker_pool_spec(&obj).unwrap(), pool.spec);
    }
}
