use super::*;

fn dialer(d: &tempfile::TempDir, capacity: usize) -> UnixWorkerDialer {
    UnixWorkerDialer::new(Arc::new(NodePaths::new(d.path())), capacity)
}

/// A connection pushed out of the cache is let go, not kept: the cache
/// holds only what fits, so worker pod churn cannot grow it without bound.
/// The channel closes once the last caller holding it drops it.
#[tokio::test]
async fn worker_dialer_closes_evicted_conns() {
    let d = tempfile::tempdir().unwrap();
    let dialer = dialer(&d, 1);
    dialer.connect("pod-a").await.unwrap();
    dialer.connect("pod-b").await.unwrap();
    let c = dialer.cache.lock();
    assert!(!c.map.contains_key("pod-a"), "evicted conn still cached");
    assert!(c.map.contains_key("pod-b"));
    assert_eq!(c.map.len(), 1);
    assert_eq!(c.order.len(), 1);
}

/// Dialing the same pod twice reuses the cached connection.
#[tokio::test]
async fn worker_dialer_reuses_cached_conn() {
    let d = tempfile::tempdir().unwrap();
    let dialer = dialer(&d, 2);
    dialer.connect("pod-a").await.unwrap();
    dialer.connect("pod-a").await.unwrap();
    let c = dialer.cache.lock();
    assert_eq!(c.map.len(), 1, "a second dial made a new connection");
    assert_eq!(c.order.len(), 1);
}

/// A worker that is unavailable, refusing workloads until it registers or
/// unreachable after a restart, reaches the caller as Unavailable so the
/// resume is retried; any other failure is Internal.
#[test]
fn worker_call_error_cases() {
    for (name, err, want) in [
        (
            "worker refuses",
            Status::unavailable("worker has not registered yet"),
            tonic::Code::Unavailable,
        ),
        (
            "worker unreachable",
            Status::unavailable("dial: connection refused"),
            tonic::Code::Unavailable,
        ),
        (
            "worker fails",
            Status::internal("sandbox would not start"),
            tonic::Code::Internal,
        ),
        (
            "plain error",
            Status::unknown("boom"),
            tonic::Code::Internal,
        ),
    ] {
        assert_eq!(
            worker_call_error("RestoreWorkload", err).code(),
            want,
            "{name}"
        );
    }
}
