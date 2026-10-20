//! Reads of one resource, from an informer cache or straight from the API
//! server.
//!
//! The controllers read most objects from the shared informer caches,
//! which can lag behind the API server. The same reads can be pointed at
//! the API server instead, which is what tests driving a reconcile
//! directly do.

use std::fmt::Debug;
use std::sync::Arc;

use hivekeep_k8s::informer::Informer;
use kube::api::{Api, ListParams};
use kube::core::{ClusterResourceScope, NamespaceResourceScope};
use kube::{Client, Resource};
use serde::de::DeserializeOwned;

/// Builds the API handle for one namespace ("" for cluster-scoped).
type ScopedApi<K> = fn(Client, &str) -> Api<K>;

enum Source<K: Resource> {
    Live {
        client: Client,
        scoped: ScopedApi<K>,
    },
    Cached(Arc<Informer<K>>),
}

/// Get and list access to one resource.
pub struct Reader<K: Resource> {
    source: Source<K>,
}

impl<K: Resource> Clone for Reader<K> {
    fn clone(&self) -> Self {
        Reader {
            source: match &self.source {
                Source::Live { client, scoped } => Source::Live {
                    client: client.clone(),
                    scoped: *scoped,
                },
                Source::Cached(i) => Source::Cached(i.clone()),
            },
        }
    }
}

impl<K> Reader<K>
where
    K: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Clone
        + DeserializeOwned
        + Debug
        + Send
        + Sync
        + 'static,
{
    /// Live reads of a namespaced resource.
    pub fn live_namespaced(client: Client) -> Self {
        Reader {
            source: Source::Live {
                client,
                scoped: |c, ns| Api::namespaced(c, ns),
            },
        }
    }
}

impl<K> Reader<K>
where
    K: Resource<DynamicType = (), Scope = ClusterResourceScope>
        + Clone
        + DeserializeOwned
        + Debug
        + Send
        + Sync
        + 'static,
{
    /// Live reads of a cluster-scoped resource.
    pub fn live_cluster(client: Client) -> Self {
        Reader {
            source: Source::Live {
                client,
                scoped: |c, _| Api::all(c),
            },
        }
    }
}

impl<K> Reader<K>
where
    K: Resource<DynamicType = ()> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
{
    /// Reads from `informer`'s cache.
    pub fn cached(informer: Arc<Informer<K>>) -> Self {
        Reader {
            source: Source::Cached(informer),
        }
    }

    /// The object, or `None` if it does not exist.
    pub async fn get(&self, namespace: &str, name: &str) -> Result<Option<K>, kube::Error> {
        match &self.source {
            Source::Live { client, scoped } => {
                scoped(client.clone(), namespace).get_opt(name).await
            }
            Source::Cached(i) => Ok(i.get(namespace, name).map(|o| (*o).clone())),
        }
    }

    /// Every object, in every namespace.
    pub async fn list(&self) -> Result<Vec<K>, kube::Error> {
        self.list_filtered(|_| true).await
    }

    /// The objects `filter` accepts.
    pub async fn list_filtered(&self, filter: impl Fn(&K) -> bool) -> Result<Vec<K>, kube::Error> {
        match &self.source {
            Source::Live { client, .. } => Ok(Api::<K>::all(client.clone())
                .list(&ListParams::default())
                .await?
                .items
                .into_iter()
                .filter(|o| filter(o))
                .collect()),
            Source::Cached(i) => Ok(i
                .list_filtered(filter)
                .into_iter()
                .map(|o| (*o).clone())
                .collect()),
        }
    }
}
