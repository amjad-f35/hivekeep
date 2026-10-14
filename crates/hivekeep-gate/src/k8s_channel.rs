//! A load-balanced tonic [`Channel`] for `k8s:///` targets: the resolver's
//! address updates become `Change::Insert` / `Change::Remove` events for
//! the channel's balancer, so calls spread across the Service's ready pods
//! and follow them as they come and go.

use std::collections::BTreeSet;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tonic::transport::channel::Change;
use tonic::transport::{Channel, Endpoint};

use crate::k8s_resolver::{K8sResolver, SliceSource, WatchSink};

/// How many pending endpoint changes the balancer buffers.
const CHANGE_CAPACITY: usize = 64;

/// Whether `target` names a Kubernetes Service (`k8s:` scheme).
pub fn is_k8s_target(target: &str) -> bool {
    target.starts_with("k8s:")
}

/// Builds a balanced channel for `target`, resolved from `source`.
///
/// `endpoint` turns one resolved `ip:port` into the endpoint to dial (TLS,
/// keep-alives). The resolver lives as long as the channel: it stops once
/// every clone of the channel is dropped. Must be called inside a tokio
/// runtime.
pub fn balanced_channel<F>(
    source: &dyn SliceSource,
    target: &str,
    endpoint: F,
) -> Result<Channel, String>
where
    F: Fn(&str) -> Result<Endpoint, String> + Send + 'static,
{
    let (channel, changes) = Channel::balance_channel::<String>(CHANGE_CAPACITY);
    resolve_changes(source, target, changes, endpoint)?;
    Ok(channel)
}

/// Starts resolving `target` and feeds the resulting endpoint changes to
/// `changes` until it closes.
pub fn resolve_changes<E, F>(
    source: &dyn SliceSource,
    target: &str,
    changes: mpsc::Sender<Change<String, E>>,
    endpoint: F,
) -> Result<(), String>
where
    E: Send + 'static,
    F: Fn(&str) -> Result<E, String> + Send + 'static,
{
    let (tx, rx) = watch::channel(Vec::new());
    let resolver = K8sResolver::build(source, target, Arc::new(WatchSink(tx)))?;
    tokio::spawn(async move {
        // Owning the resolver here ties its watch to the balancer's life.
        let _resolver = resolver;
        drive_changes(rx, changes, endpoint).await;
    });
    Ok(())
}

/// Diffs each published address list against the previous one and sends
/// the removals, then the insertions. Returns when the address source or
/// the change receiver goes away.
pub async fn drive_changes<E, F>(
    mut addrs: watch::Receiver<Vec<String>>,
    changes: mpsc::Sender<Change<String, E>>,
    endpoint: F,
) where
    F: Fn(&str) -> Result<E, String>,
{
    let mut current: BTreeSet<String> = BTreeSet::new();
    loop {
        tokio::select! {
            _ = changes.closed() => return,
            r = addrs.changed() => if r.is_err() { return },
        }
        let next: BTreeSet<String> = addrs.borrow_and_update().iter().cloned().collect();
        for gone in current.difference(&next) {
            if changes.send(Change::Remove(gone.clone())).await.is_err() {
                return;
            }
        }
        let mut accepted = BTreeSet::new();
        for addr in next.difference(&current) {
            match endpoint(addr) {
                Ok(ep) => {
                    if changes
                        .send(Change::Insert(addr.clone(), ep))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    accepted.insert(addr.clone());
                }
                Err(e) => tracing::warn!(%addr, err = %e, "skipping unusable resolved address"),
            }
        }
        current.retain(|a| next.contains(a));
        current.extend(accepted);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::stream::BoxStream;
    use futures::StreamExt;
    use k8s_openapi::api::discovery::v1::{Endpoint as SliceEndpoint, EndpointPort, EndpointSlice};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use parking_lot::Mutex;

    use super::*;

    /// Slice sets pushed by the test, streamed to the resolver.
    struct FakeSource(Mutex<Option<mpsc::UnboundedReceiver<Vec<EndpointSlice>>>>);

    impl SliceSource for FakeSource {
        fn watch(
            &self,
            _: &str,
            _: &str,
        ) -> BoxStream<'static, Result<Vec<EndpointSlice>, String>> {
            let rx = self.0.lock().take().expect("watched once");
            tokio_stream::wrappers::UnboundedReceiverStream::new(rx)
                .map(Ok)
                .boxed()
        }
    }

    fn slice(ips: &[&str]) -> EndpointSlice {
        EndpointSlice {
            metadata: ObjectMeta {
                name: Some("s".into()),
                ..Default::default()
            },
            address_type: "IPv4".into(),
            endpoints: Some(
                ips.iter()
                    .map(|ip| SliceEndpoint {
                        addresses: vec![ip.to_string()],
                        ..Default::default()
                    })
                    .collect(),
            ),
            ports: Some(vec![EndpointPort {
                port: Some(8443),
                ..Default::default()
            }]),
        }
    }

    fn describe(c: Change<String, String>) -> String {
        match c {
            Change::Insert(k, v) => format!("+{k}={v}"),
            Change::Remove(k) => format!("-{k}"),
        }
    }

    async fn next(rx: &mut mpsc::Receiver<Change<String, String>>) -> String {
        describe(
            tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("change")
                .expect("open"),
        )
    }

    #[test]
    fn k8s_targets_are_recognised() {
        assert!(is_k8s_target("k8s:///hivekeep-system/api:443"));
        assert!(!is_k8s_target("dns:///api:443"));
        assert!(!is_k8s_target("api:443"));
    }

    #[tokio::test]
    async fn drive_changes_diffs_address_lists() {
        let (tx, rx) = watch::channel(Vec::new());
        let (ctx, mut crx) = mpsc::channel(16);
        let task = tokio::spawn(drive_changes(rx, ctx, |a: &str| {
            if a.starts_with("bad") {
                Err("unusable".to_string())
            } else {
                Ok(format!("https://{a}"))
            }
        }));
        tx.send_replace(vec![
            "10.0.0.1:443".into(),
            "10.0.0.2:443".into(),
            "bad:1".into(),
        ]);
        assert_eq!(next(&mut crx).await, "+10.0.0.1:443=https://10.0.0.1:443");
        assert_eq!(next(&mut crx).await, "+10.0.0.2:443=https://10.0.0.2:443");
        tx.send_replace(vec!["10.0.0.2:443".into(), "10.0.0.3:443".into()]);
        assert_eq!(next(&mut crx).await, "-10.0.0.1:443");
        assert_eq!(next(&mut crx).await, "+10.0.0.3:443=https://10.0.0.3:443");
        // An unchanged list sends nothing.
        tx.send_replace(vec!["10.0.0.3:443".into(), "10.0.0.2:443".into()]);
        tx.send_replace(vec![]);
        assert_eq!(next(&mut crx).await, "-10.0.0.2:443");
        assert_eq!(next(&mut crx).await, "-10.0.0.3:443");
        drop(tx);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn resolver_watch_feeds_the_balancer() {
        let (stx, srx) = mpsc::unbounded_channel();
        let source = FakeSource(Mutex::new(Some(srx)));
        let (ctx, mut crx) = mpsc::channel(16);
        resolve_changes(&source, "k8s:///hivekeep-system/api", ctx, |a: &str| {
            Ok(a.to_string())
        })
        .unwrap();
        stx.send(vec![slice(&["10.0.0.1", "10.0.0.2"])]).unwrap();
        assert_eq!(next(&mut crx).await, "+10.0.0.1:443=10.0.0.1:443");
        assert_eq!(next(&mut crx).await, "+10.0.0.2:443=10.0.0.2:443");
        stx.send(vec![slice(&["10.0.0.2"])]).unwrap();
        assert_eq!(next(&mut crx).await, "-10.0.0.1:443");
        // Dropping the balancer's receiver stops the resolver.
        drop(crx);
        tokio::time::timeout(Duration::from_secs(2), stx.closed())
            .await
            .expect("resolver stopped");
    }

    #[tokio::test]
    async fn balanced_channel_builds_for_k8s_targets() {
        let (_stx, srx) = mpsc::unbounded_channel();
        let source = FakeSource(Mutex::new(Some(srx)));
        let ch = balanced_channel(&source, "k8s:///hivekeep-system/api:443", |a: &str| {
            Endpoint::from_shared(format!("http://{a}")).map_err(|e| e.to_string())
        });
        assert!(ch.is_ok());
        assert!(balanced_channel(&source, "k8s:///", |_: &str| Err("unused".to_string())).is_err());
    }
}
