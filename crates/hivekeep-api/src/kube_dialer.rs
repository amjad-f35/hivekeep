//! Finding keepers through the Kubernetes API: the keeper of a node is the
//! one pod of the keeper DaemonSet scheduled there. Each dial is pinned to
//! that pod's UID (its certificate's PodIdentity must name it), and the
//! channel is cached per pod and redialed when the pod's IP changes.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_proto::keeper::v1::keeper_client::KeeperClient;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::{Api, ListParams};
use parking_lot::Mutex;
use rustls::crypto::CryptoProvider;
use tonic::transport::Channel;

use crate::error::{status, Error, Result};
use crate::keeper_dialer::{Keeper, KeeperDialer};
use crate::keeper_tls::{config_with, tls_channel, KeeperServerVerifier, ReloadingClientCert};

/// Label selecting keeper pods.
pub const DEFAULT_KEEPER_SELECTOR: &str = "app=hivekeep-keeper";
/// Port keepers serve the Keeper API on; must match hivekeep-keeper's `DEFAULT_PORT`.
pub const DEFAULT_KEEPER_PORT: u16 = 7443;

/// TLS material for keeper dials.
#[derive(Clone)]
pub struct KeeperTls {
    /// Trust anchors of the pod-identity CA (DER).
    pub roots: Vec<Vec<u8>>,
    /// The keeper SPIFFE ID expected on its certificate.
    pub keeper_spiffe_id: String,
    /// The control plane's client certificate.
    pub client_cert: Arc<ReloadingClientCert>,
    /// The crypto provider.
    pub provider: Arc<CryptoProvider>,
}

/// A dialer that looks keepers up with the Kubernetes API.
pub struct KubeKeeperDialer {
    pods: Api<Pod>,
    nodes: Api<Node>,
    selector: String,
    port: u16,
    tls: KeeperTls,
    conns: Mutex<HashMap<String, (String, Channel)>>,
}

impl KubeKeeperDialer {
    /// A dialer over keeper pods in `namespace` matching `selector`.
    pub fn new(
        client: kube::Client,
        namespace: &str,
        selector: impl Into<String>,
        port: u16,
        tls: KeeperTls,
    ) -> Self {
        KubeKeeperDialer {
            pods: Api::namespaced(client.clone(), namespace),
            nodes: Api::all(client),
            selector: selector.into(),
            port,
            tls,
            conns: Mutex::new(HashMap::new()),
        }
    }

    async fn keepers_on(&self, node: &str) -> Result<Vec<Pod>> {
        let lp = ListParams::default()
            .labels(&self.selector)
            .fields(&format!("spec.nodeName={node}"));
        let list = self.pods.list(&lp).await.map_err(|e| {
            status::unavailable(format!("while finding keeper on node {node:?}: {e}"))
        })?;
        Ok(list
            .items
            .into_iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
            .collect())
    }
}

#[async_trait]
impl KeeperDialer for KubeKeeperDialer {
    async fn dial(&self, node: &str) -> Result<Keeper> {
        let pods = self.keepers_on(node).await?;
        let pod = match pods.as_slice() {
            [] => return Err(Error::NoKeeperOnNode(node.to_string())),
            [p] => p,
            more => {
                return Err(status::internal(format!(
                    "found {} keeper pods on node {node:?}, expected 1",
                    more.len()
                )))
            }
        };
        let uid = pod.metadata.uid.clone().unwrap_or_default();
        let ip = pod
            .status
            .as_ref()
            .and_then(|s| s.pod_ips.as_ref())
            .and_then(|ips| ips.first())
            .map(|i| i.ip.clone())
            .ok_or_else(|| {
                status::unavailable(format!("keeper on node {node:?} has no assigned IPs"))
            })?;
        if let Some((cached_ip, ch)) = self.conns.lock().get(&uid) {
            if *cached_ip == ip {
                return Ok(KeeperClient::new(ch.clone()));
            }
        }
        let verifier = KeeperServerVerifier::new(
            self.tls.roots.clone(),
            &self.tls.keeper_spiffe_id,
            self.tls.provider.clone(),
        )
        .with_pod_uid(&uid);
        let cfg = config_with(
            self.tls.provider.clone(),
            verifier,
            self.tls.client_cert.clone(),
        )
        .map_err(status::internal)?;
        let host = if ip.contains(':') {
            format!("[{ip}]")
        } else {
            ip.clone()
        };
        let ch = tls_channel(&format!("{host}:{}", self.port), Arc::new(cfg))?;
        self.conns.lock().insert(uid, (ip, ch.clone()));
        Ok(KeeperClient::new(ch))
    }

    async fn node_gone(&self, node: &str) -> Result<bool> {
        match self.nodes.get_opt(node).await {
            Ok(Some(_)) => Ok(false),
            Ok(None) => Ok(self.keepers_on(node).await?.is_empty()),
            Err(e) => Err(status::unavailable(format!(
                "while reading node {node:?}: {e}"
            ))),
        }
    }
}
