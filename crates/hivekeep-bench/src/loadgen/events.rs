//! The worker's event bus: things the locust master tells the worker that
//! other parts of the process want to react to.

use std::sync::Arc;

use futures::future::BoxFuture;
use parking_lot::Mutex;

/// A spawn handler: receives the user count and spawn rate of each spawn
/// message.
pub type SpawnHandler = Arc<dyn Fn(usize, f64) -> BoxFuture<'static, ()> + Send + Sync>;

/// Subscribers to the master's spawn messages. The locust runner publishes
/// every spawn message here before it adjusts the number of users, and
/// waits for each handler in subscription order.
#[derive(Default, Clone)]
pub struct Events {
    spawn: Arc<Mutex<Vec<SpawnHandler>>>,
}

impl Events {
    /// An empty bus.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a spawn handler.
    pub fn subscribe_spawn(&self, handler: SpawnHandler) {
        self.spawn.lock().push(handler);
    }

    /// Runs every spawn handler, one after another.
    pub async fn publish_spawn(&self, user_count: usize, spawn_rate: f64) {
        let handlers = self.spawn.lock().clone();
        for h in handlers {
            h(user_count, spawn_rate).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn handlers_run_in_order_and_see_the_message() {
        let events = Events::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        for tag in 0..2 {
            let seen = seen.clone();
            events.subscribe_spawn(Arc::new(move |n, rate| {
                let seen = seen.clone();
                Box::pin(async move { seen.lock().push((tag, n, rate)) })
            }));
        }
        events.publish_spawn(3, 1.5).await;
        assert_eq!(*seen.lock(), vec![(0, 3, 1.5), (1, 3, 1.5)]);

        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let clone = events.clone();
        clone.subscribe_spawn(Arc::new(move |_, _| {
            c.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {})
        }));
        events.publish_spawn(1, 1.0).await;
        assert_eq!(count.load(Ordering::SeqCst), 1, "clones share subscribers");
    }
}
