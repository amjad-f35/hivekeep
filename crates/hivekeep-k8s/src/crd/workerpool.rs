//! `WorkerPool`: a scalable set of worker pods running one sandbox class.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{NodeAffinity, ResourceRequirements, Toleration};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::{CustomResource, KubeSchema};
use serde::{Deserialize, Serialize};

use super::sandbox_class::SandboxClass;

/// Maximum number of template labels or annotations.
pub const MAX_TEMPLATE_METADATA: usize = 64;
/// Maximum number of template tolerations.
pub const MAX_TOLERATIONS: usize = 16;
/// Maximum WorkerPool name length: worker pods carry it as a label value.
pub const MAX_NAME_LEN: usize = 63;

/// The class condition that is True when the class's spec resolves to a
/// usable SandboxConfig. While it is False for any class, the controller
/// applies the worker Deployment only if one exists already or every class
/// has a config in use.
pub const CONDITION_RESOLVED: &str = "Resolved";
/// The class resolved.
pub const REASON_RESOLVED: &str = "Resolved";
/// `configRef` names a SandboxConfig that does not exist.
pub const REASON_SANDBOX_CONFIG_NOT_FOUND: &str = "SandboxConfigNotFound";
/// The SandboxConfig `configRef` names, or the only class default, is being
/// deleted (whether or not the pool already uses it).
pub const REASON_SANDBOX_CONFIG_DELETING: &str = "SandboxConfigDeleting";
/// `configRef` names a SandboxConfig of another sandbox class.
pub const REASON_SANDBOX_CLASS_MISMATCH: &str = "SandboxClassMismatch";
/// `configRef` is omitted and no SandboxConfig of the class is annotated
/// as the class default.
pub const REASON_DEFAULT_CONFIG_NOT_FOUND: &str = "DefaultConfigNotFound";

const RESERVED_RULE: &str =
    "self.all(key, !key.startsWith('hivekeep.io/') && !key.contains('.hivekeep.io/'))";
const RESERVED_MSG: &str = "hivekeep.io and its subdomains are reserved";

/// Schema of template labels: at most 64, each value a Kubernetes label
/// value.
fn label_map_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "maxProperties": MAX_TEMPLATE_METADATA,
        "additionalProperties": {
            "type": "string",
            "maxLength": 63,
            "pattern": "^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$"
        }
    })
}

/// Optional metadata, scheduling and resource settings for worker pods.
/// `node_affinity` maps to `spec.affinity.nodeAffinity` on the pod.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, KubeSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerPoolPodTemplate {
    /// Added to the generated Deployment and worker pods. Keys in the
    /// `hivekeep.io` domain and its subdomains are reserved.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(schema_with = "label_map_schema")]
    #[x_kube(
        validation = Rule::new(RESERVED_RULE).message(RESERVED_MSG),
        validation = Rule::new("self.all(key, !format.qualifiedName().validate(key).hasValue())").message("label keys must be valid Kubernetes qualified names")
    )]
    pub labels: BTreeMap<String, String>,
    /// Added to the generated Deployment and worker pods. Keys in the
    /// `hivekeep.io` domain and its subdomains are reserved.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(extend("maxProperties" = 64))]
    #[x_kube(
        validation = Rule::new(RESERVED_RULE).message(RESERVED_MSG),
        validation = Rule::new("self.all(key, !format.qualifiedName().validate(key).hasValue())").message("annotation keys must be valid Kubernetes qualified names")
    )]
    pub annotations: BTreeMap<String, String>,
    /// A selector which must be true for a worker pod to fit on a node.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_selector: BTreeMap<String, String>,
    /// Tolerations of the worker pods.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(length(max = 16))]
    #[x_kube(merge_strategy = ListMerge::Atomic)]
    pub tolerations: Vec<Toleration>,
    /// Priority class of the worker pods.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub priority_class_name: String,
    /// Node affinity of the worker pods.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_affinity: Option<NodeAffinity>,
    /// Compute resources of each worker pod.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
    /// ServiceAccount of the worker pods; it must exist in the pool's
    /// namespace. When omitted, pods run as the controller-created default
    /// worker ServiceAccount.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub service_account_name: Option<String>,
}

/// Names a cluster-scoped SandboxConfig.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, KubeSchema)]
pub struct SandboxConfigReference {
    /// The SandboxConfig's `metadata.name`.
    #[schemars(length(min = 1, max = 253))]
    #[serde(default)]
    pub name: String,
}

impl SandboxConfigReference {
    /// A reference to `name`.
    pub fn new(name: impl Into<String>) -> Self {
        SandboxConfigReference { name: name.into() }
    }
}

/// One sandbox runtime family a WorkerPool runs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, KubeSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerPoolSandboxClass {
    /// The sandbox runtime family. It drives the worker pod shape (device
    /// mounts and node placement); the sandbox binaries come from the
    /// SandboxConfig `config_ref` names, or the class default.
    pub name: SandboxClass,
    /// A SandboxConfig of this class. When omitted the pool uses the newest
    /// config of the class annotated as the class default. The resolved
    /// config is reported in `status.sandboxClasses`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_ref: Option<SandboxConfigReference>,
}

/// A scalable set of worker pods.
#[derive(CustomResource, Debug, Clone, Default, PartialEq, Serialize, Deserialize, KubeSchema)]
#[kube(
    group = "hivekeep.io",
    version = "v1",
    kind = "WorkerPool",
    namespaced,
    status = "WorkerPoolStatus",
    shortname = "workerpool",
    scale(
        spec_replicas_path = ".spec.replicas",
        status_replicas_path = ".status.replicas",
        label_selector_path = ".status.selector"
    ),
    printcolumn(name = "Desired", type_ = "integer", json_path = ".spec.replicas"),
    printcolumn(name = "Replicas", type_ = "integer", json_path = ".status.replicas"),
    printcolumn(name = "Ready", type_ = "integer", json_path = ".status.readyReplicas"),
    printcolumn(name = "SandboxConfig", type_ = "string", json_path = ".status.sandboxClasses[*].configRef.name"),
    printcolumn(name = "Resolved", type_ = "string", json_path = r#".status.sandboxClasses[*].conditions[?(@.type=="Resolved")].status"#),
    printcolumn(name = "Age", type_ = "date", json_path = ".metadata.creationTimestamp"),
    validation = Rule::new("size(self.metadata.name) <= 63").message("metadata.name must be at most 63 characters, because worker pods use it as a label value")
)]
#[serde(rename_all = "camelCase")]
pub struct WorkerPoolSpec {
    /// Number of worker pods to run.
    #[schemars(range(min = 0))]
    pub replicas: i32,
    /// The worker container image.
    #[schemars(length(min = 1))]
    pub worker_image: String,
    /// Optional metadata, scheduling and resource settings for worker pods.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<WorkerPoolPodTemplate>,
    /// The sandbox runtime families this pool runs. Exactly one entry is
    /// allowed today; the names cannot change after creation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(length(min = 1, max = 1))]
    #[x_kube(
        merge_strategy = ListMerge::Map(vec!["name".into()]),
        validation = Rule::new("self.map(c, c.name) == oldSelf.map(c, c.name)").message("sandboxClasses names are immutable")
    )]
    pub sandbox_classes: Vec<WorkerPoolSandboxClass>,
}

impl WorkerPoolSpec {
    /// The class of the first `sandbox_classes` entry; `None` without one.
    /// The list always has exactly one entry today.
    pub fn default_sandbox_class(&self) -> Option<SandboxClass> {
        self.sandbox_classes.first().map(|c| c.name)
    }
}

/// The SandboxConfig and resolution state of one sandbox class of a pool.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, KubeSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerPoolSandboxClassStatus {
    /// The current state of this sandbox class.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[x_kube(merge_strategy = ListMerge::Map(vec!["type".into()]))]
    pub conditions: Vec<Condition>,
    /// The sandbox runtime family.
    pub name: SandboxClass,
    /// The SandboxConfig in use: the one the spec names, or the class
    /// default, or the last resolved config while the spec does not
    /// resolve. Omitted when the class never resolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_ref: Option<SandboxConfigReference>,
    /// The SandboxConfig the class is switching to while the controller
    /// confirms it against the API server. It protects the config from
    /// deletion like `config_ref` does; the same reconcile clears it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_config_ref: Option<SandboxConfigReference>,
}

impl WorkerPoolSandboxClassStatus {
    /// The condition of `type_`.
    pub fn condition(&self, type_: &str) -> Option<&Condition> {
        self.conditions.iter().find(|c| c.type_ == type_)
    }
}

/// The observed state of a WorkerPool.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, KubeSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerPoolStatus {
    /// Total number of worker pods.
    #[serde(default)]
    #[schemars(range(min = 0))]
    pub replicas: i32,
    /// Number of ready worker pods.
    #[serde(default, skip_serializing_if = "is_zero")]
    #[schemars(range(min = 0))]
    pub ready_replicas: i32,
    /// Label selector of the worker pods.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub selector: String,
    /// The SandboxConfig each sandbox class uses and whether the class's
    /// spec resolves.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(length(max = 1))]
    #[x_kube(merge_strategy = ListMerge::Map(vec!["name".into()]))]
    pub sandbox_classes: Vec<WorkerPoolSandboxClassStatus>,
}

fn is_zero(v: &i32) -> bool {
    *v == 0
}

impl WorkerPool {
    /// The SandboxConfig the controller confirmed for the class the pool's
    /// workers run, or `None` before one resolved. Never a
    /// `pending_config_ref`: that config is not confirmed yet.
    pub fn resolved_sandbox_config(&self) -> Option<&str> {
        let class = self.spec.default_sandbox_class()?;
        self.status
            .as_ref()?
            .sandbox_classes
            .iter()
            .find(|c| c.name == class)?
            .config_ref
            .as_ref()
            .map(|r| r.name.as_str())
    }

    /// Every SandboxConfig the pool's status names, in `config_ref` or
    /// `pending_config_ref`: the configs it protects from deletion.
    pub fn sandbox_configs_in_use(&self) -> Vec<&str> {
        let mut out = Vec::new();
        for c in self.status.iter().flat_map(|s| &s.sandbox_classes) {
            for r in [&c.config_ref, &c.pending_config_ref].into_iter().flatten() {
                if !out.contains(&r.name.as_str()) {
                    out.push(r.name.as_str());
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(spec: SandboxClass, status: Vec<WorkerPoolSandboxClassStatus>) -> WorkerPool {
        let mut wp = WorkerPool::new(
            "p",
            WorkerPoolSpec {
                sandbox_classes: vec![WorkerPoolSandboxClass {
                    name: spec,
                    config_ref: None,
                }],
                ..Default::default()
            },
        );
        wp.status = Some(WorkerPoolStatus {
            sandbox_classes: status,
            ..Default::default()
        });
        wp
    }

    fn status(
        class: SandboxClass,
        cfg: Option<&str>,
        pending: Option<&str>,
    ) -> WorkerPoolSandboxClassStatus {
        WorkerPoolSandboxClassStatus {
            name: class,
            config_ref: cfg.map(SandboxConfigReference::new),
            pending_config_ref: pending.map(SandboxConfigReference::new),
            ..Default::default()
        }
    }

    #[test]
    fn resolved_sandbox_config() {
        use SandboxClass::*;
        let cases = [
            (
                "confirmed config of the pool's class",
                pool(Gvisor, vec![status(Gvisor, Some("gvisor-default"), None)]),
                Some("gvisor-default"),
            ),
            (
                "not resolved yet",
                pool(Gvisor, vec![status(Gvisor, None, None)]),
                None,
            ),
            (
                "only a pending config",
                pool(Gvisor, vec![status(Gvisor, None, Some("gvisor-next"))]),
                None,
            ),
            (
                "status resolved another class than the pool's",
                pool(Microvm, vec![status(Gvisor, Some("gvisor-default"), None)]),
                None,
            ),
            (
                "empty sandboxClasses in spec",
                WorkerPool::new("p", WorkerPoolSpec::default()),
                None,
            ),
        ];
        for (name, wp, want) in cases {
            assert_eq!(wp.resolved_sandbox_config(), want, "{name}");
        }
    }

    #[test]
    fn configs_in_use() {
        use SandboxClass::*;
        let wp = pool(Gvisor, vec![status(Gvisor, Some("a"), Some("b"))]);
        assert_eq!(wp.sandbox_configs_in_use(), vec!["a", "b"]);
    }
}
