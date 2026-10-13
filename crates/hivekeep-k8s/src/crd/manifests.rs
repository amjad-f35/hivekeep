//! CustomResourceDefinition manifests for the `hivekeep.io/v1` resources.

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::CustomResourceExt;

use super::{CSIDriverConfig, SandboxConfig, WorkerPool};

/// Every Hivekeep CRD.
pub fn crds() -> Vec<CustomResourceDefinition> {
    vec![
        CSIDriverConfig::crd(),
        SandboxConfig::crd(),
        WorkerPool::crd(),
    ]
}

/// Every Hivekeep CRD as one multi-document YAML stream.
pub fn crds_yaml() -> Result<String, serde_yaml::Error> {
    let mut out = String::new();
    for crd in crds() {
        out.push_str("---\n");
        out.push_str(&serde_yaml::to_string(&crd)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crds_carry_schema_rules() {
        let yaml = crds_yaml().unwrap();
        for want in [
            "name: workerpools.hivekeep.io",
            "name: sandboxconfigs.hivekeep.io",
            "name: csidriverconfigs.hivekeep.io",
            "sandboxClasses names are immutable",
            "defaultVersion must name an entry in versions",
            "the version named by defaultVersion must not be Disabled",
            "hivekeep.io and its subdomains are reserved",
            "label keys must be valid Kubernetes qualified names",
            "maxLength: 63",
            "metadata.name must be at most 63 characters",
            "tls.serverName is required when tls.enabled is true",
            "x-kubernetes-list-type: map",
            "specReplicasPath: .spec.replicas",
            "^[a-f0-9]{64}$",
            "^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$",
        ] {
            assert!(yaml.contains(want), "CRD YAML lacks {want:?}");
        }
        let wp = WorkerPool::crd();
        assert_eq!(wp.spec.scope, "Namespaced");
        assert_eq!(SandboxConfig::crd().spec.scope, "Cluster");
        assert_eq!(
            wp.spec.names.short_names.as_deref(),
            Some(&["workerpool".to_string()][..])
        );
    }
}
