//! `CSIDriverConfig`: how workers reach a CSI driver for external volumes.

use kube::{CustomResource, KubeSchema};
use serde::{Deserialize, Serialize};

/// TLS and mTLS configuration of a CSI driver connection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, KubeSchema)]
#[serde(rename_all = "camelCase")]
#[x_kube(
    validation = Rule::new("!self.enabled || (has(self.usePodIdentity) && self.usePodIdentity)").message("tls.usePodIdentity must be true when tls.enabled is true; manual certificates are not yet supported"),
    validation = Rule::new("!self.enabled || (has(self.serverName) && size(self.serverName) > 0)").message("tls.serverName is required when tls.enabled is true")
)]
pub struct CsiDriverTlsConfig {
    /// Whether TLS is used.
    pub enabled: bool,
    /// Whether to present the pod identity (SPIFFE) certificate.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub use_pod_identity: bool,
    /// The server name expected on the driver's certificate; required with
    /// TLS.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub server_name: String,
}

/// Where a CSI driver's controller and node services are.
#[derive(
    CustomResource, Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, KubeSchema,
)]
#[kube(
    group = "hivekeep.io",
    version = "v1",
    kind = "CSIDriverConfig",
    plural = "csidriverconfigs",
    shortname = "csidriverconfig",
    printcolumn(name = "Driver", type_ = "string", json_path = ".spec.driverName"),
    printcolumn(
        name = "Age",
        type_ = "date",
        json_path = ".metadata.creationTimestamp"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct CsiDriverConfigSpec {
    /// The CSI driver name (e.g. `hostpath.csi.k8s.io`), matching the
    /// StorageClass ActorTemplate volumes reference.
    #[schemars(
        length(min = 1, max = 63),
        regex(
            pattern = r"^(hivekeep\.io/)?([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*)$"
        )
    )]
    pub driver_name: String,
    /// The gRPC endpoint of the CSI Controller service, a `tcp://` or
    /// `dns://` URI.
    #[schemars(regex(pattern = r"^(tcp|dns)://.+$"))]
    pub controller_endpoint: String,
    /// Overrides the CSI Node socket on worker nodes; defaults to
    /// `unix:///var/lib/kubelet/plugins/<driverName>/csi.sock`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[schemars(regex(pattern = r"^unix://.+$"))]
    pub node_socket_override: String,
    /// TLS for the controller endpoint connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<CsiDriverTlsConfig>,
}

impl CsiDriverConfigSpec {
    /// The node socket: the override, or the kubelet plugin default.
    pub fn node_socket(&self) -> String {
        if self.node_socket_override.is_empty() {
            format!(
                "unix:///var/lib/kubelet/plugins/{}/csi.sock",
                self.driver_name
            )
        } else {
            self.node_socket_override.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_socket_default() {
        let mut s = CsiDriverConfigSpec {
            driver_name: "hostpath.csi.k8s.io".into(),
            ..Default::default()
        };
        assert_eq!(
            s.node_socket(),
            "unix:///var/lib/kubelet/plugins/hostpath.csi.k8s.io/csi.sock"
        );
        s.node_socket_override = "unix:///x.sock".into();
        assert_eq!(s.node_socket(), "unix:///x.sock");
    }
}
