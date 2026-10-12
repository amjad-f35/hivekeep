//! The control plane's side of the control plugin: whole-snapshot cleanup
//! and copy over the plugin's Unix socket ([`PluginControlStore`], the
//! deployed shape), or against the backend in process
//! ([`DirectControlStore`], for single-binary setups).
//!
//! Both take snapshot URIs (`<scheme>://<bucket>/<path>`) and keep the
//! plugin's status codes: FAILED_PRECONDITION when a copy's source holds no
//! objects, INVALID_ARGUMENT for a malformed URI.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_core::snapshot::StoragePrefix;
use tonic::Status;

use crate::plugin::ControlPlugin;
use crate::pluginsocket::{call_error, dial, wait_ready, PluginChannel, READY_WAIT};
use crate::proto::v1::control_provider_client::ControlProviderClient;
use crate::proto::v1::{CleanupSnapshotRequest, CopySnapshotRequest};
use crate::store::Store;

/// The control plugin over its socket.
#[derive(Clone)]
pub struct PluginControlStore {
    client: ControlProviderClient<PluginChannel>,
    channel: PluginChannel,
}

impl PluginControlStore {
    /// A client of the plugin at `socket`, connecting on first use; each
    /// call waits up to `ready_wait` for a restarting plugin.
    pub fn new(socket: &Path, ready_wait: Duration) -> Result<Self, String> {
        let ch = dial(socket, ready_wait)
            .map_err(|e| format!("while dialing the object-store control plugin: {e}"))?;
        Ok(PluginControlStore {
            client: ControlProviderClient::new(ch.clone()),
            channel: ch,
        })
    }

    /// A client of the plugin at `socket` with the default ready wait.
    pub fn connect(socket: &Path) -> Result<Self, String> {
        Self::new(socket, READY_WAIT)
    }

    /// Waits until the plugin reports it is serving.
    pub async fn wait_ready(&self) -> Result<(), Status> {
        wait_ready(&self.channel).await
    }

    /// Deletes every object under `uri`; an absent prefix succeeds.
    pub async fn cleanup_snapshot(&self, uri: &str) -> Result<(), Status> {
        let mut c = self.client.clone();
        c.cleanup_snapshot(CleanupSnapshotRequest {
            snapshot_uri: uri.to_string(),
        })
        .await
        .map(|_| ())
        .map_err(call_error)
    }

    /// Copies every object of `src` to the same relative name under `dst`.
    pub async fn copy_snapshot(&self, src: &str, dst: &str) -> Result<(), Status> {
        let mut c = self.client.clone();
        c.copy_snapshot(CopySnapshotRequest {
            src_uri: src.to_string(),
            dst_uri: dst.to_string(),
        })
        .await
        .map(|_| ())
        .map_err(call_error)
    }
}

/// The control plugin's logic in process, on a [`Store`].
#[derive(Clone)]
pub struct DirectControlStore {
    plugin: Arc<ControlPlugin>,
}

impl DirectControlStore {
    /// Cleanup and copy on `store`.
    pub fn new(store: Arc<dyn Store>) -> Self {
        DirectControlStore {
            plugin: Arc::new(ControlPlugin::new(store)),
        }
    }

    /// Cleanup and copy on the backend the environment selects.
    pub fn from_env() -> crate::error::Result<Self> {
        Ok(Self::new(crate::store::env::new_store_from_env()?))
    }

    /// Deletes every object under `uri`; an absent prefix succeeds.
    pub async fn cleanup_snapshot(&self, uri: &str) -> Result<(), Status> {
        self.plugin
            .cleanup(&CleanupSnapshotRequest {
                snapshot_uri: uri.to_string(),
            })
            .await
    }

    /// Copies every object of `src` to the same relative name under `dst`.
    pub async fn copy_snapshot(&self, src: &str, dst: &str) -> Result<(), Status> {
        self.plugin
            .copy(&CopySnapshotRequest {
                src_uri: src.to_string(),
                dst_uri: dst.to_string(),
            })
            .await
    }
}

/// Parses a snapshot URI the way the plugin does, for callers that want to
/// fail before a round trip.
pub fn parse_prefix(uri: &str) -> Result<StoragePrefix, Status> {
    StoragePrefix::parse(uri).map_err(|e| Status::invalid_argument(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::fake::FakeStore;

    #[tokio::test]
    async fn direct_copy_and_cleanup() {
        let fake = Arc::new(FakeStore::new());
        fake.put("b", "a/snap/manifest.json", "m");
        fake.put("b", "a/snap/memory", "mem");
        let s = DirectControlStore::new(fake.clone());
        s.copy_snapshot("gs://b/a/snap", "gs://b/t/snap")
            .await
            .unwrap();
        assert_eq!(
            fake.objects(),
            vec![
                "b/a/snap/manifest.json",
                "b/a/snap/memory",
                "b/t/snap/manifest.json",
                "b/t/snap/memory"
            ]
        );
        let err = s
            .copy_snapshot("gs://b/none", "gs://b/x")
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        s.cleanup_snapshot("gs://b/a/snap").await.unwrap();
        s.cleanup_snapshot("gs://b/a/snap").await.unwrap();
        assert_eq!(
            fake.objects(),
            vec!["b/t/snap/manifest.json", "b/t/snap/memory"]
        );
        assert_eq!(
            s.cleanup_snapshot("not a uri").await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}
