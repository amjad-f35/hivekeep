//! CSI driver settings read from `CSIDriverConfig` resources, for the
//! keeper's node plugins and the control plane's controller plugins.
//!
//! Each lookup is a direct GET of the named (cluster-scoped) resource
//! rather than a cluster-wide watch: plugins are opened once per driver and
//! cached, so lookups are rare, and nodes need no list/watch permission.

use async_trait::async_trait;
use hivekeep_node::csi::{CsiDriverConfig, CsiDriverConfigGetter, CsiDriverTls};
use kube::Api;

use crate::crd::{CSIDriverConfig, CsiDriverConfigSpec};

impl From<&CsiDriverConfigSpec> for CsiDriverConfig {
    fn from(spec: &CsiDriverConfigSpec) -> Self {
        CsiDriverConfig {
            driver_name: spec.driver_name.clone(),
            controller_endpoint: spec.controller_endpoint.clone(),
            node_socket_override: spec.node_socket_override.clone(),
            tls: spec.tls.as_ref().map(|t| CsiDriverTls {
                enabled: t.enabled,
                use_pod_identity: t.use_pod_identity,
                server_name: t.server_name.clone(),
            }),
        }
    }
}

/// Reads `CSIDriverConfig` resources by name.
#[derive(Clone)]
pub struct KubeCsiDriverConfigs {
    api: Api<CSIDriverConfig>,
}

impl KubeCsiDriverConfigs {
    /// A getter over `client`.
    pub fn new(client: kube::Client) -> Self {
        KubeCsiDriverConfigs {
            api: Api::all(client),
        }
    }
}

#[async_trait]
impl CsiDriverConfigGetter for KubeCsiDriverConfigs {
    async fn get(&self, driver_name: &str) -> Result<CsiDriverConfig, String> {
        let obj = self.api.get(driver_name).await.map_err(|e| e.to_string())?;
        Ok((&obj.spec).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::CsiDriverTlsConfig;
    use crate::fake::FakeApiServer;

    #[tokio::test]
    async fn direct_csi_driver_config_getter() {
        let fake = FakeApiServer::new();
        let mut obj = CSIDriverConfig::new(
            "test.csi.k8s.io",
            CsiDriverConfigSpec {
                driver_name: "test.csi.k8s.io".into(),
                controller_endpoint: "tcp://127.0.0.1:9000".into(),
                node_socket_override: String::new(),
                tls: None,
            },
        );
        fake.insert(&obj);
        let getter = KubeCsiDriverConfigs::new(fake.client());

        let cfg = getter.get("test.csi.k8s.io").await.unwrap();
        assert_eq!(cfg.driver_name, "test.csi.k8s.io");
        assert_eq!(cfg.controller_endpoint, "tcp://127.0.0.1:9000");
        assert_eq!(
            cfg.node_endpoint(),
            "unix:///var/lib/kubelet/plugins/test.csi.k8s.io/csi.sock"
        );
        assert!(getter.get("missing.csi.k8s.io").await.is_err());

        obj.metadata.name = Some("tls.csi.k8s.io".into());
        obj.spec.driver_name = "tls.csi.k8s.io".into();
        obj.spec.node_socket_override = "unix:///run/csi.sock".into();
        obj.spec.tls = Some(CsiDriverTlsConfig {
            enabled: true,
            use_pod_identity: true,
            server_name: "csi.svc".into(),
        });
        fake.insert(&obj);
        let cfg = getter.get("tls.csi.k8s.io").await.unwrap();
        assert_eq!(cfg.node_endpoint(), "unix:///run/csi.sock");
        assert_eq!(
            cfg.tls,
            Some(CsiDriverTls {
                enabled: true,
                use_pod_identity: true,
                server_name: "csi.svc".into()
            })
        );
    }
}
