//! A watch-backed object cache with event handlers.
//!
//! [`Informer`] lists and watches one resource (optionally narrowed by
//! label and field selectors), keeps every object in a local store, and
//! calls registered handlers on each add, update and delete *after* the
//! store reflects it, so a handler that only enqueues a key can rely on the
//! cache when the key is processed. A relist (after a watch expires or
//! fails) reports objects that vanished meanwhile as deletes, and an
//! optional resync period re-delivers every cached object as an update.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kube::api::Api;
use kube::runtime::watcher::{self, watcher, Event};
use kube::runtime::WatchStreamExt;
use kube::{Resource, ResourceExt};
use parking_lot::{Mutex, RwLock};
use serde::de::DeserializeOwned;
use tokio::sync::watch;

/// A change the informer observed.
#[derive(Debug)]
pub enum InformerEvent<K> {
    /// The object appeared.
    Added(Arc<K>),
    /// The object changed (or was re-delivered by a relist or resync).
    Updated {
        /// The cached object before the change.
        old: Arc<K>,
        /// The object now.
        new: Arc<K>,
    },
    /// The object was deleted; this is its last known state.
    Deleted(Arc<K>),
}

impl<K> Clone for InformerEvent<K> {
    fn clone(&self) -> Self {
        match self {
            InformerEvent::Added(o) => InformerEvent::Added(o.clone()),
            InformerEvent::Updated { old, new } => InformerEvent::Updated {
                old: old.clone(),
                new: new.clone(),
            },
            InformerEvent::Deleted(o) => InformerEvent::Deleted(o.clone()),
        }
    }
}

impl<K> InformerEvent<K> {
    /// The object the event is about (the new state for an update).
    pub fn object(&self) -> &Arc<K> {
        match self {
            InformerEvent::Added(o) | InformerEvent::Deleted(o) => o,
            InformerEvent::Updated { new, .. } => new,
        }
    }
}

type Handler<K> = Arc<dyn Fn(&InformerEvent<K>) + Send + Sync>;
type ErrorHandler = Arc<dyn Fn(&watcher::Error) + Send + Sync>;

/// Cache key: namespace ("" when cluster-scoped) and name.
pub type CacheKey = (String, String);

fn key_of<K: Resource>(obj: &K) -> CacheKey {
    (obj.namespace().unwrap_or_default(), obj.name_any())
}

/// A watch-backed cache of one resource.
pub struct Informer<K: Resource> {
    api: Api<K>,
    config: watcher::Config,
    resync: Option<Duration>,
    store: RwLock<BTreeMap<CacheKey, Arc<K>>>,
    handlers: Mutex<Vec<Handler<K>>>,
    error_handlers: Mutex<Vec<ErrorHandler>>,
    synced: watch::Sender<bool>,
}

impl<K> Informer<K>
where
    K: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
{
    /// An informer over `api` narrowed by `config`'s selectors.
    pub fn new(api: Api<K>, config: watcher::Config) -> Arc<Self> {
        Self::with_resync(api, config, None)
    }

    /// Like [`Informer::new`], re-delivering every cached object as an
    /// update each `resync` period.
    pub fn with_resync(
        api: Api<K>,
        config: watcher::Config,
        resync: Option<Duration>,
    ) -> Arc<Self> {
        Arc::new(Informer {
            api,
            config,
            resync,
            store: RwLock::new(BTreeMap::new()),
            handlers: Mutex::new(Vec::new()),
            error_handlers: Mutex::new(Vec::new()),
            synced: watch::channel(false).0,
        })
    }

    /// Registers a handler. Handlers run on the informer task and should
    /// only record or enqueue. Register before [`Informer::run`] starts to
    /// see the initial list as adds.
    pub fn add_handler(&self, handler: impl Fn(&InformerEvent<K>) + Send + Sync + 'static) {
        self.handlers.lock().push(Arc::new(handler));
    }

    /// Registers a handler for list/watch failures (which are retried).
    pub fn add_error_handler(&self, handler: impl Fn(&watcher::Error) + Send + Sync + 'static) {
        self.error_handlers.lock().push(Arc::new(handler));
    }

    /// A channel receiving every event.
    pub fn events(&self) -> tokio::sync::mpsc::UnboundedReceiver<InformerEvent<K>> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.add_handler(move |e| {
            let _ = tx.send(e.clone());
        });
        rx
    }

    /// The cached object.
    pub fn get(&self, namespace: &str, name: &str) -> Option<Arc<K>> {
        self.store
            .read()
            .get(&(namespace.to_string(), name.to_string()))
            .cloned()
    }

    /// Every cached object, in namespace/name order.
    pub fn list(&self) -> Vec<Arc<K>> {
        self.store.read().values().cloned().collect()
    }

    /// The cached objects `filter` accepts.
    pub fn list_filtered(&self, filter: impl Fn(&K) -> bool) -> Vec<Arc<K>> {
        self.store
            .read()
            .values()
            .filter(|o| filter(o))
            .cloned()
            .collect()
    }

    /// Puts `obj` in the cache without notifying handlers, as if the watch
    /// had delivered it. For tests that drive a consumer synchronously.
    pub fn cache_put(&self, obj: K) {
        self.store.write().insert(key_of(&obj), Arc::new(obj));
    }

    /// Removes an object from the cache without notifying handlers.
    pub fn cache_remove(&self, namespace: &str, name: &str) -> Option<Arc<K>> {
        self.store
            .write()
            .remove(&(namespace.to_string(), name.to_string()))
    }

    /// Whether the initial list completed.
    pub fn has_synced(&self) -> bool {
        *self.synced.borrow()
    }

    /// Waits until the initial list completed.
    pub async fn wait_synced(&self) {
        let mut rx = self.synced.subscribe();
        let _ = rx.wait_for(|s| *s).await;
    }

    fn notify(&self, event: InformerEvent<K>) {
        let handlers: Vec<_> = self.handlers.lock().clone();
        for h in handlers {
            h(&event);
        }
    }

    fn apply(&self, obj: K) {
        let key = key_of(&obj);
        let new = Arc::new(obj);
        let old = self.store.write().insert(key, new.clone());
        self.notify(match old {
            Some(old) => InformerEvent::Updated { old, new },
            None => InformerEvent::Added(new),
        });
    }

    fn delete(&self, obj: K) {
        let key = key_of(&obj);
        let last = self
            .store
            .write()
            .remove(&key)
            .unwrap_or_else(|| Arc::new(obj));
        self.notify(InformerEvent::Deleted(last));
    }

    /// Re-delivers every cached object as an update.
    pub fn resync_now(&self) {
        for obj in self.list() {
            self.notify(InformerEvent::Updated {
                old: obj.clone(),
                new: obj,
            });
        }
    }

    /// Lists and watches until the returned future is dropped. Watch
    /// failures are retried with backoff.
    pub async fn run(self: Arc<Self>) {
        let mut stream = watcher(self.api.clone(), self.config.clone())
            .default_backoff()
            .boxed();
        let mut seen: Option<HashSet<CacheKey>> = None;
        let mut resync = self.resync.map(|p| {
            let mut i = tokio::time::interval(p);
            i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            i
        });
        if let Some(i) = resync.as_mut() {
            i.tick().await;
        }
        loop {
            let ev = match resync.as_mut() {
                Some(i) => tokio::select! {
                    ev = stream.next() => ev,
                    _ = i.tick() => {
                        self.resync_now();
                        continue;
                    }
                },
                None => stream.next().await,
            };
            let Some(ev) = ev else { return };
            match ev {
                Ok(Event::Init) => seen = Some(HashSet::new()),
                Ok(Event::InitApply(obj)) => {
                    if let Some(s) = seen.as_mut() {
                        s.insert(key_of(&obj));
                    }
                    self.apply(obj);
                }
                Ok(Event::InitDone) => {
                    if let Some(seen) = seen.take() {
                        let gone: Vec<Arc<K>> = self
                            .store
                            .read()
                            .iter()
                            .filter(|(k, _)| !seen.contains(*k))
                            .map(|(_, v)| v.clone())
                            .collect();
                        for obj in gone {
                            self.delete((*obj).clone());
                        }
                    }
                    self.synced.send_replace(true);
                }
                Ok(Event::Apply(obj)) => self.apply(obj),
                Ok(Event::Delete(obj)) => self.delete(obj),
                Err(err) => {
                    tracing::warn!(%err, "Watch failed; retrying");
                    let handlers: Vec<_> = self.error_handlers.lock().clone();
                    for h in handlers {
                        h(&err);
                    }
                }
            }
        }
    }

    /// Spawns [`Informer::run`] on the current runtime.
    pub fn spawn(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.clone().run())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeApiServer;
    use k8s_openapi::api::core::v1::ConfigMap;
    use kube::api::{DeleteParams, PostParams};

    fn cm(name: &str, label: &str) -> ConfigMap {
        let mut c = ConfigMap::default();
        c.metadata.name = Some(name.into());
        c.metadata.labels = Some([("sel".to_string(), label.to_string())].into());
        c
    }

    async fn next<K>(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<InformerEvent<K>>,
    ) -> InformerEvent<K> {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("event in time")
            .expect("channel open")
    }

    #[tokio::test]
    async fn caches_and_notifies() {
        let fake = FakeApiServer::new();
        let api: Api<ConfigMap> = Api::namespaced(fake.client(), "ns");
        api.create(&PostParams::default(), &cm("a", "yes"))
            .await
            .unwrap();
        api.create(&PostParams::default(), &cm("skip", "no"))
            .await
            .unwrap();
        let inf = Informer::new(api.clone(), watcher::Config::default().labels("sel=yes"));
        let mut rx = inf.events();
        let task = inf.spawn();
        assert!(matches!(next(&mut rx).await, InformerEvent::Added(_)));
        inf.wait_synced().await;
        assert!(inf.get("ns", "a").is_some());
        assert!(inf.get("ns", "skip").is_none());

        api.create(&PostParams::default(), &cm("b", "yes"))
            .await
            .unwrap();
        let ev = next(&mut rx).await;
        assert_eq!(ev.object().metadata.name.as_deref(), Some("b"));
        assert!(
            inf.get("ns", "b").is_some(),
            "store updated before the handler ran"
        );

        api.delete("a", &DeleteParams::default()).await.unwrap();
        assert!(matches!(next(&mut rx).await, InformerEvent::Deleted(_)));
        assert!(inf.get("ns", "a").is_none());

        inf.resync_now();
        assert!(matches!(next(&mut rx).await, InformerEvent::Updated { .. }));
        task.abort();
    }
}
