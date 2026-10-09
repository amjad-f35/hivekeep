//! How to reach a CSI driver: its controller endpoint, node socket and TLS
//! settings, looked up by driver name (in a cluster, from the driver's
//! `CSIDriverConfig` resource).

use async_trait::async_trait;

/// TLS settings of the controller connection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CsiDriverTls {
    /// Whether the connection uses TLS.
    pub enabled: bool,
    /// Whether the client presents the pod identity certificate. The only
    /// supported mode when TLS is enabled.
    pub use_pod_identity: bool,
    /// The name the driver's serving certificate must carry.
    pub server_name: String,
}

/// One driver's connection settings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CsiDriverConfig {
    /// The CSI driver name, as `GetPluginInfo` reports it.
    pub driver_name: String,
    /// The controller service endpoint (`tcp://` or `dns://`).
    pub controller_endpoint: String,
    /// The node service socket on worker nodes; empty for the kubelet
    /// plugin directory default.
    pub node_socket_override: String,
    /// Controller TLS; `None` for plaintext.
    pub tls: Option<CsiDriverTls>,
}

/// The kubelet plugin directory socket a node driver serves on by default.
pub fn kubelet_plugin_socket_path(driver_name: &str) -> String {
    format!("/var/lib/kubelet/plugins/{driver_name}/csi.sock")
}

impl CsiDriverConfig {
    /// The node service endpoint: the override, or the kubelet plugin
    /// socket.
    pub fn node_endpoint(&self) -> String {
        if self.node_socket_override.is_empty() {
            format!("unix://{}", kubelet_plugin_socket_path(&self.driver_name))
        } else {
            self.node_socket_override.clone()
        }
    }
}

/// Looks up a driver's connection settings by name.
#[async_trait]
pub trait CsiDriverConfigGetter: Send + Sync + 'static {
    /// The settings of `driver_name`; an error when there are none.
    async fn get(&self, driver_name: &str) -> Result<CsiDriverConfig, String>;
}

/// A getter over a fixed set of configs, for tests and static setups.
#[derive(Debug, Default, Clone)]
pub struct StaticCsiDriverConfigs(pub Vec<CsiDriverConfig>);

#[async_trait]
impl CsiDriverConfigGetter for StaticCsiDriverConfigs {
    async fn get(&self, driver_name: &str) -> Result<CsiDriverConfig, String> {
        self.0
            .iter()
            .find(|c| c.driver_name == driver_name)
            .cloned()
            .ok_or_else(|| format!("no CSIDriverConfig named {driver_name:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_endpoint_defaults_to_the_kubelet_plugin_socket() {
        let mut c = CsiDriverConfig {
            driver_name: "hostpath.csi.k8s.io".into(),
            ..Default::default()
        };
        assert_eq!(
            c.node_endpoint(),
            "unix:///var/lib/kubelet/plugins/hostpath.csi.k8s.io/csi.sock"
        );
        c.node_socket_override = "unix:///run/csi.sock".into();
        assert_eq!(c.node_endpoint(), "unix:///run/csi.sock");
    }

    #[tokio::test]
    async fn static_configs_find_by_name() {
        let g = StaticCsiDriverConfigs(vec![CsiDriverConfig {
            driver_name: "a".into(),
            ..Default::default()
        }]);
        assert_eq!(g.get("a").await.unwrap().driver_name, "a");
        assert!(g.get("b").await.is_err());
    }
}
