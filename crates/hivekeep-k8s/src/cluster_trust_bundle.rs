//! ClusterTrustBundles through whichever of v1 and v1beta1 the cluster
//! serves.
//!
//! [`ClusterTrustBundleClient`] uses one discovered version for live
//! operations and for its watch cache; objects are always returned in the
//! typed v1beta1 representation. The cache is allocated lazily, so a client
//! used only for live operations never watches.

use std::sync::{Arc, OnceLock};

use futures::StreamExt;
use kube::api::{DeleteParams, DynamicObject, ListParams, PostParams};
use kube::runtime::watcher;
use kube::Client;

use crate::cert_api::{self, CertVersion, DiscoverError, VersionedApi};
use crate::informer::{Informer, InformerEvent};

/// The ClusterTrustBundle type used in memory.
pub type ClusterTrustBundle = k8s_openapi::api::certificates::v1beta1::ClusterTrustBundle;

/// Finds the ClusterTrustBundle version the cluster serves, preferring v1.
pub async fn discover(client: &Client) -> Result<CertVersion, DiscoverError> {
    cert_api::discover(client, "clustertrustbundles", "ClusterTrustBundle").await
}

/// Live and cached access to ClusterTrustBundles.
pub struct ClusterTrustBundleClient {
    api: VersionedApi<ClusterTrustBundle>,
    label_selector: Option<String>,
    informer: OnceLock<Arc<Informer<DynamicObject>>>,
}

impl ClusterTrustBundleClient {
    /// Discovers the served version. `label_selector` narrows the cache.
    pub async fn new(
        client: Client,
        label_selector: Option<String>,
    ) -> Result<Self, DiscoverError> {
        let version = discover(&client).await?;
        Ok(Self::with_version(client, version, label_selector))
    }

    /// A client for a known version.
    pub fn with_version(
        client: Client,
        version: CertVersion,
        label_selector: Option<String>,
    ) -> Self {
        ClusterTrustBundleClient {
            api: VersionedApi::all(client, version),
            label_selector,
            informer: OnceLock::new(),
        }
    }

    /// The version in use.
    pub fn version(&self) -> CertVersion {
        self.api.version()
    }

    /// Whether the watch cache was allocated.
    pub fn has_informer(&self) -> bool {
        self.informer.get().is_some()
    }

    /// The watch cache, allocated on first use. The caller runs it
    /// (`informer().spawn()`) once if cached reads are needed.
    pub fn informer(&self) -> &Arc<Informer<DynamicObject>> {
        self.informer.get_or_init(|| {
            let mut cfg = watcher::Config::default();
            if let Some(sel) = &self.label_selector {
                cfg = cfg.labels(sel);
            }
            Informer::new(self.api.dynamic().clone(), cfg)
        })
    }

    /// A bundle from the cache.
    pub fn get_cached(&self, name: &str) -> Option<ClusterTrustBundle> {
        let obj = self.informer().get("", name)?;
        VersionedApi::<ClusterTrustBundle>::from_dynamic((*obj).clone()).ok()
    }

    /// Typed cache events.
    pub fn events(&self) -> impl futures::Stream<Item = InformerEvent<ClusterTrustBundle>> {
        tokio_stream::wrappers::UnboundedReceiverStream::new(self.informer().events()).filter_map(
            |ev| async move {
                let conv = |o: &Arc<DynamicObject>| {
                    VersionedApi::<ClusterTrustBundle>::from_dynamic((**o).clone())
                        .ok()
                        .map(Arc::new)
                };
                Some(match &ev {
                    InformerEvent::Added(o) => InformerEvent::Added(conv(o)?),
                    InformerEvent::Deleted(o) => InformerEvent::Deleted(conv(o)?),
                    InformerEvent::Updated { old, new } => InformerEvent::Updated {
                        old: conv(old)?,
                        new: conv(new)?,
                    },
                })
            },
        )
    }

    /// Lists bundles.
    pub async fn list(&self, lp: &ListParams) -> Result<Vec<ClusterTrustBundle>, kube::Error> {
        self.api.list(lp).await
    }

    /// Reads a bundle.
    pub async fn get(&self, name: &str) -> Result<ClusterTrustBundle, kube::Error> {
        self.api.get(name).await
    }

    /// Creates a bundle.
    pub async fn create(
        &self,
        pp: &PostParams,
        b: &ClusterTrustBundle,
    ) -> Result<ClusterTrustBundle, kube::Error> {
        self.api.create(pp, b).await
    }

    /// Replaces a bundle.
    pub async fn update(
        &self,
        pp: &PostParams,
        b: &ClusterTrustBundle,
    ) -> Result<ClusterTrustBundle, kube::Error> {
        let name = b.metadata.name.clone().unwrap_or_default();
        self.api.replace(&name, pp, b).await
    }

    /// Deletes a bundle.
    pub async fn delete(&self, name: &str, dp: &DeleteParams) -> Result<(), kube::Error> {
        self.api.delete(name, dp).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::fake::{FakeApiServer, Reaction};
    use k8s_openapi::api::certificates::v1beta1::ClusterTrustBundleSpec;

    fn want() -> ClusterTrustBundle {
        let mut b = ClusterTrustBundle {
            spec: ClusterTrustBundleSpec {
                signer_name: Some("example.com/signer".into()),
                trust_bundle: "root-a".into(),
            },
            ..Default::default()
        };
        b.metadata.name = Some("example.com:signer:bundle".into());
        b.metadata.labels = Some([("live".to_string(), "true".to_string())].into());
        b
    }

    fn check(got: &ClusterTrustBundle, want: &ClusterTrustBundle) {
        assert_eq!(got.metadata.name, want.metadata.name);
        assert_eq!(got.metadata.labels, want.metadata.labels);
        assert_eq!(got.spec, want.spec);
    }

    async fn receive(
        events: &mut (impl futures::Stream<Item = InformerEvent<ClusterTrustBundle>> + Unpin),
    ) -> InformerEvent<ClusterTrustBundle> {
        tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .expect("informer event timeout")
            .expect("stream open")
    }

    #[tokio::test]
    async fn client_round_trips_each_version() {
        for version in [CertVersion::V1, CertVersion::V1beta1] {
            let fake = FakeApiServer::new();
            fake.serve(
                &version.api_version(),
                &[("clustertrustbundles", "ClusterTrustBundle", false)],
            );
            let c = ClusterTrustBundleClient::new(fake.client(), Some("live=true".into()))
                .await
                .unwrap();
            assert_eq!(c.version(), version);
            let mut want = want();
            let created = c.create(&PostParams::default(), &want).await.unwrap();
            check(&created, &want);
            check(&c.get("example.com:signer:bundle").await.unwrap(), &want);
            let list = c
                .list(&ListParams::default().labels("live=true"))
                .await
                .unwrap();
            assert_eq!(list.len(), 1);
            check(&list[0], &want);
            assert!(!c.has_informer(), "live operations allocated an informer");
            assert!(c.get_cached("missing").is_none());

            let mut events = Box::pin(c.events());
            let task = c.informer().spawn();
            receive(&mut events).await;
            check(&c.get_cached("example.com:signer:bundle").unwrap(), &want);

            want = created.clone();
            want.spec.trust_bundle = "root-b".into();
            check(
                &c.update(&PostParams::default(), &want).await.unwrap(),
                &want,
            );
            receive(&mut events).await;
            check(&c.get_cached("example.com:signer:bundle").unwrap(), &want);

            c.delete("example.com:signer:bundle", &DeleteParams::default())
                .await
                .unwrap();
            assert!(matches!(
                receive(&mut events).await,
                InformerEvent::Deleted(_)
            ));
            assert!(c.get_cached("example.com:signer:bundle").is_none());

            for a in fake
                .actions()
                .iter()
                .filter(|a| a.resource == "clustertrustbundles")
            {
                assert_eq!(a.version, version.as_str(), "wrong endpoint: {a:?}");
                if a.verb == "watch" {
                    assert_eq!(
                        a.query.get("labelSelector").map(String::as_str),
                        Some("live=true"),
                        "watch lost selector"
                    );
                }
            }
            fake.prepend_reactor("get", "clustertrustbundles", |_| {
                Reaction::fail(403, "Forbidden", "denied")
            });
            let err = c.get("example.com:signer:bundle").await.unwrap_err();
            assert!(
                crate::api_errors::is_forbidden(&err),
                "lost API error: {err}"
            );
            task.abort();
        }
    }

    #[test]
    fn conversion_copies_metadata() {
        let mut original = ClusterTrustBundle::default();
        original.metadata.labels = Some([("key".to_string(), "original".to_string())].into());
        let mut copy = original.clone();
        copy.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("key".into(), "copy".into());
        assert_eq!(original.metadata.labels.unwrap()["key"], "original");
    }
}
