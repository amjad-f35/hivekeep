//! An in-memory view of every Worker, kept current by the store's worker
//! watch plus a periodic full relist that repairs drift from missed events.
//! Scheduling reads it instead of the store.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{ListOptions, Store, WorkerEvent, WorkerEventType, WorkerWatch};
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;

use crate::scheduling::WorkerSource;

const RELIST_PAGE_SIZE: i32 = 1000;

/// A callback for Worker changes. Must not block.
pub type WorkerHandler = Arc<dyn Fn(&pb::Worker) + Send + Sync>;

struct State {
    workers: HashMap<String, Arc<pb::Worker>>,
    handlers: Vec<WorkerHandler>,
}

/// The cache.
pub struct WorkerCache {
    store: Arc<dyn Store>,
    relist_interval: Duration,
    state: RwLock<State>,
    ready: AtomicBool,
}

impl WorkerCache {
    /// A cache over `store` that relists every `relist_interval`.
    pub fn new(store: Arc<dyn Store>, relist_interval: Duration) -> Arc<Self> {
        Arc::new(WorkerCache {
            store,
            relist_interval,
            state: RwLock::new(State {
                workers: HashMap::new(),
                handlers: Vec::new(),
            }),
            ready: AtomicBool::new(false),
        })
    }

    /// Syncs once, then keeps the cache current in the background until
    /// `stop` is cancelled. Returns once the first sync succeeded.
    pub async fn start(self: &Arc<Self>, stop: CancellationToken) -> Result<(), String> {
        let watch = self.sync().await?;
        self.ready.store(true, Ordering::SeqCst);
        let me = self.clone();
        tokio::spawn(async move { me.watch_events(watch, stop).await });
        Ok(())
    }

    /// Registers `handler` and calls it with every Worker held now; it is
    /// then called for every Worker a watch event or relist changes.
    pub fn add_handler(&self, handler: WorkerHandler) {
        let current: Vec<Arc<pb::Worker>> = {
            let mut st = self.state.write();
            st.handlers.push(handler.clone());
            st.workers.values().cloned().collect()
        };
        for w in current {
            handler(&w);
        }
    }

    /// Every known Worker. Fails while the cache is resyncing.
    pub fn workers(&self) -> Result<Vec<Arc<pb::Worker>>, String> {
        if !self.ready.load(Ordering::SeqCst) {
            return Err("worker cache not ready".into());
        }
        Ok(self.state.read().workers.values().cloned().collect())
    }

    /// The Worker named `name`, if known.
    pub fn worker(&self, name: &str) -> Option<Arc<pb::Worker>> {
        self.state.read().workers.get(name).cloned()
    }

    /// Drops a Worker a store write proved gone, closing the window before
    /// its delete event arrives.
    pub fn forget(&self, name: &str) {
        self.state.write().workers.remove(name);
    }

    async fn sync(&self) -> Result<WorkerWatch, String> {
        let mut watch = self
            .store
            .watch_workers()
            .await
            .map_err(|e| format!("watching workers: {e}"))?;
        if let Err(e) = self.relist().await {
            watch.close();
            return Err(e);
        }
        Ok(watch)
    }

    /// Replaces the cache with a full listing, notifying handlers of every
    /// Worker that is new or at a different version.
    pub async fn relist(&self) -> Result<(), String> {
        let mut all = Vec::new();
        let mut token = String::new();
        loop {
            let page = self
                .store
                .list_workers(&ListOptions::new(RELIST_PAGE_SIZE, token.clone()))
                .await
                .map_err(|e| format!("listing workers: {e}"))?;
            all.extend(page.items);
            if page.next_page_token.is_empty() {
                break;
            }
            token = page.next_page_token;
        }
        let fresh: HashMap<String, Arc<pb::Worker>> =
            all.into_iter().map(|w| (key(&w), Arc::new(w))).collect();
        let (changed, handlers) = {
            let mut st = self.state.write();
            let changed: Vec<Arc<pb::Worker>> = fresh
                .iter()
                .filter(|(k, w)| {
                    st.workers
                        .get(*k)
                        .is_none_or(|old| version(old) != version(w))
                })
                .map(|(_, w)| w.clone())
                .collect();
            st.workers = fresh;
            (changed, st.handlers.clone())
        };
        tracing::debug!(changed = changed.len(), "worker cache synced");
        notify(&handlers, &changed);
        Ok(())
    }

    fn apply(&self, event: WorkerEvent) {
        let k = key(&event.worker);
        let worker = Arc::new(event.worker);
        let handlers = {
            let mut st = self.state.write();
            match event.kind {
                WorkerEventType::Deleted => {
                    st.workers.remove(&k);
                    Vec::new()
                }
                WorkerEventType::Created | WorkerEventType::Updated => {
                    let newer = st
                        .workers
                        .get(&k)
                        .is_none_or(|old| version(&worker) >= version(old));
                    if newer {
                        st.workers.insert(k, worker.clone());
                        st.handlers.clone()
                    } else {
                        Vec::new()
                    }
                }
            }
        };
        notify(&handlers, std::slice::from_ref(&worker));
    }

    async fn watch_events(self: Arc<Self>, mut watch: WorkerWatch, stop: CancellationToken) {
        let mut ticker = tokio::time::interval(self.relist_interval);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = stop.cancelled() => {
                    self.ready.store(false, Ordering::SeqCst);
                    watch.close();
                    return;
                }
                ev = watch.recv() => match ev {
                    Some(ev) => self.apply(ev),
                    None => {
                        self.ready.store(false, Ordering::SeqCst);
                        tracing::warn!("worker watch ended; resyncing");
                        match self.resync(&stop).await {
                            Some(w) => {
                                watch = w;
                                self.ready.store(true, Ordering::SeqCst);
                            }
                            None => return,
                        }
                    }
                },
                _ = ticker.tick() => {
                    if let Err(e) = self.relist().await {
                        tracing::warn!(error = %e, "periodic worker relist failed");
                    }
                }
            }
        }
    }

    async fn resync(&self, stop: &CancellationToken) -> Option<WorkerWatch> {
        let mut delay = Duration::from_secs(1);
        loop {
            match self.sync().await {
                Ok(w) => return Some(w),
                Err(e) => tracing::warn!(error = %e, "worker cache resync failed"),
            }
            tokio::select! {
                _ = stop.cancelled() => return None,
                _ = tokio::time::sleep(delay) => {}
            }
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    }
}

impl WorkerSource for WorkerCache {
    fn workers(&self) -> Result<Vec<Arc<pb::Worker>>, String> {
        WorkerCache::workers(self)
    }
}

fn notify(handlers: &[WorkerHandler], workers: &[Arc<pb::Worker>]) {
    for h in handlers {
        for w in workers {
            h(w);
        }
    }
}

fn key(w: &pb::Worker) -> String {
    w.metadata
        .as_ref()
        .map(|m| m.name.clone())
        .unwrap_or_default()
}

fn version(w: &pb::Worker) -> i64 {
    w.metadata.as_ref().map(|m| m.version).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_store::{MemoryStore, Precondition};
    use std::sync::atomic::AtomicUsize;

    fn worker(name: &str) -> pb::Worker {
        pb::Worker {
            metadata: Some(pb::ResourceMetadata {
                name: name.into(),
                ..Default::default()
            }),
            worker_namespace: "ns".into(),
            worker_pool: "pool".into(),
            worker_pod: name.into(),
            worker_pod_uid: uuid::Uuid::new_v4().to_string(),
            node_name: "node".into(),
            status: Some(pb::WorkerStatus {
                state: pb::WorkerState::Unavailable as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    async fn eventually(f: impl Fn() -> bool) {
        for _ in 0..200 {
            if f() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("condition never held");
    }

    #[tokio::test]
    async fn follows_store_changes() {
        let store: Arc<dyn Store> = Arc::new(MemoryStore::new());
        store.create_worker(&worker("a")).await.unwrap();
        let cache = WorkerCache::new(store.clone(), Duration::from_secs(3600));
        assert!(cache.workers().is_err());
        let stop = CancellationToken::new();
        cache.start(stop.clone()).await.unwrap();
        assert_eq!(cache.workers().unwrap().len(), 1);

        let seen = Arc::new(AtomicUsize::new(0));
        let s = seen.clone();
        cache.add_handler(Arc::new(move |_| {
            s.fetch_add(1, Ordering::SeqCst);
        }));
        assert_eq!(seen.load(Ordering::SeqCst), 1);

        let b = store.create_worker(&worker("b")).await.unwrap();
        eventually(|| cache.worker("b").is_some()).await;
        store
            .update_worker("b", Precondition::of(&b), &mut |w| {
                w.labels.insert("k".into(), "v".into());
                Ok(())
            })
            .await
            .unwrap();
        eventually(|| {
            cache
                .worker("b")
                .is_some_and(|w| w.labels.contains_key("k"))
        })
        .await;
        store
            .delete_worker("b", hivekeep_store::DeletePreconditions::none())
            .await
            .unwrap();
        eventually(|| cache.worker("b").is_none()).await;
        assert!(seen.load(Ordering::SeqCst) >= 3);

        cache.forget("a");
        assert!(cache.worker("a").is_none());
        cache.relist().await.unwrap();
        assert!(cache.worker("a").is_some());
        stop.cancel();
    }
}
