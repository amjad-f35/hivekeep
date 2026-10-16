//! Reaching the keeper (node agent) on a node.
//!
//! Workflows ask a [`KeeperDialer`] for a client to the keeper of the node an
//! Actor is on. [`StaticKeeperDialer`] maps node names to endpoints given up
//! front (development, tests); [`crate::keeper_tls::TlsKeeperDialer`]
//! resolves the address from a template and dials over mTLS, verifying the
//! keeper's SPIFFE ID.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use hivekeep_proto::keeper::v1::keeper_client::KeeperClient;
use parking_lot::RwLock;
use tonic::transport::{Channel, Endpoint};

use crate::error::{status, Error, Result};

/// A client to one node's keeper.
pub type Keeper = KeeperClient<Channel>;

/// Finds and dials node keepers.
#[async_trait]
pub trait KeeperDialer: Send + Sync {
    /// A client to the keeper on `node`. [`Error::NoKeeperOnNode`] when no
    /// keeper is known there (it is restarting, or the node is gone).
    async fn dial(&self, node: &str) -> Result<Keeper>;

    /// Whether `node` is deleted for good and no keeper runs there. A dialer
    /// that cannot tell answers `false`.
    async fn node_gone(&self, node: &str) -> Result<bool>;
}

/// A dialer over a fixed node-to-endpoint map, with lazily connected,
/// cached channels.
#[derive(Default)]
pub struct StaticKeeperDialer {
    endpoints: RwLock<HashMap<String, String>>,
    channels: RwLock<HashMap<String, Channel>>,
    gone: RwLock<HashSet<String>>,
}

impl StaticKeeperDialer {
    /// An empty dialer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Routes `node` to `endpoint` (e.g. `http://127.0.0.1:7443`).
    pub fn set(&self, node: impl Into<String>, endpoint: impl Into<String>) {
        let node = node.into();
        self.channels.write().remove(&node);
        self.gone.write().remove(&node);
        self.endpoints.write().insert(node, endpoint.into());
    }

    /// Forgets the keeper on `node`.
    pub fn remove(&self, node: &str) {
        self.channels.write().remove(node);
        self.endpoints.write().remove(node);
    }

    /// Records that `node` was deleted.
    pub fn mark_gone(&self, node: impl Into<String>) {
        let node = node.into();
        self.remove(&node);
        self.gone.write().insert(node);
    }

    /// Parses `node=endpoint` pairs.
    pub fn from_pairs<I, S>(pairs: I) -> std::result::Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let d = Self::new();
        for p in pairs {
            let (node, ep) = p
                .as_ref()
                .split_once('=')
                .ok_or_else(|| format!("{:?} is not node=endpoint", p.as_ref()))?;
            d.set(node.trim(), ep.trim());
        }
        Ok(d)
    }
}

#[async_trait]
impl KeeperDialer for StaticKeeperDialer {
    async fn dial(&self, node: &str) -> Result<Keeper> {
        if let Some(ch) = self.channels.read().get(node) {
            return Ok(KeeperClient::new(ch.clone()));
        }
        let Some(ep) = self.endpoints.read().get(node).cloned() else {
            return Err(Error::NoKeeperOnNode(node.to_string()));
        };
        let channel = Endpoint::from_shared(ep.clone())
            .map_err(|e| status::internal(format!("invalid keeper endpoint {ep:?}: {e}")))?
            .connect_lazy();
        self.channels
            .write()
            .insert(node.to_string(), channel.clone());
        Ok(KeeperClient::new(channel))
    }

    async fn node_gone(&self, node: &str) -> Result<bool> {
        Ok(self.gone.read().contains(node) && !self.endpoints.read().contains_key(node))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn routes_known_nodes() {
        let d = StaticKeeperDialer::from_pairs(["n1=http://127.0.0.1:1"]).unwrap();
        assert!(d.dial("n1").await.is_ok());
        assert!(matches!(d.dial("n2").await, Err(Error::NoKeeperOnNode(_))));
        assert!(!d.node_gone("n2").await.unwrap());
        d.mark_gone("n1");
        assert!(matches!(d.dial("n1").await, Err(Error::NoKeeperOnNode(_))));
        assert!(d.node_gone("n1").await.unwrap());
        assert!(StaticKeeperDialer::from_pairs(["nope"]).is_err());
    }
}
