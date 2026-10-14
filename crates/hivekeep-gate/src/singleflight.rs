//! Collapsing concurrent calls for the same key into one, whose result
//! every caller shares. The call runs in its own task, so a caller that
//! stops waiting does not cancel it for the others (or for the cache it
//! fills).

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use futures::future::{BoxFuture, FutureExt, Shared};
use parking_lot::Mutex;

type Inflight<K, V> = Arc<Mutex<HashMap<K, Shared<BoxFuture<'static, V>>>>>;

/// Calls in flight by key.
pub struct Group<K, V: Clone> {
    inflight: Inflight<K, V>,
}

impl<K, V: Clone> Default for Group<K, V> {
    fn default() -> Self {
        Group {
            inflight: Arc::default(),
        }
    }
}

impl<K, V> Group<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Runs `f` for `key` unless a call for it is in flight, and returns
    /// the (shared) result. `on_panic` is returned if the call panics.
    pub async fn run<F, Fut>(&self, key: K, f: F, on_panic: V) -> V
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = V> + Send + 'static,
    {
        let shared = {
            let mut map = self.inflight.lock();
            match map.get(&key) {
                Some(s) => s.clone(),
                None => {
                    let fut = f();
                    let inflight = self.inflight.clone();
                    let k = key.clone();
                    let task = tokio::spawn(async move {
                        let v = fut.await;
                        inflight.lock().remove(&k);
                        v
                    });
                    let s = async move { task.await.unwrap_or(on_panic) }
                        .boxed()
                        .shared();
                    map.insert(key, s.clone());
                    s
                }
            }
        };
        shared.await
    }
}
