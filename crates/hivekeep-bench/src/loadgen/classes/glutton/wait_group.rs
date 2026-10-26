//! A count of in-flight tasks that can be waited on from anywhere.

use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

/// Counts running tasks; [`WaitGroup::wait`] resolves once none are left.
#[derive(Default)]
pub struct WaitGroup {
    n: AtomicUsize,
    zero: Notify,
}

impl WaitGroup {
    /// Counts one more task.
    pub fn add(&self) {
        self.n.fetch_add(1, Ordering::SeqCst);
    }

    /// Marks one task done.
    pub fn done(&self) {
        if self.n.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.zero.notify_waiters();
        }
    }

    /// Resolves when the count is zero.
    pub async fn wait(&self) {
        loop {
            let notified = self.zero.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.n.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// Marks a task done when dropped, so an early return or a dropped future
/// still counts down.
pub struct Done<'a>(pub &'a WaitGroup);

impl Drop for Done<'_> {
    fn drop(&mut self) {
        self.0.done();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn waits_for_every_task() {
        let wg = Arc::new(WaitGroup::default());
        wg.wait().await;
        for _ in 0..3 {
            wg.add();
            let wg = wg.clone();
            tokio::spawn(async move {
                let _d = Done(&wg);
                tokio::time::sleep(Duration::from_millis(20)).await;
            });
        }
        tokio::time::timeout(Duration::from_secs(2), wg.wait())
            .await
            .unwrap();
    }
}
