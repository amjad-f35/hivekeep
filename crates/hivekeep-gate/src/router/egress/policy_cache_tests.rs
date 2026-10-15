use std::sync::atomic::Ordering;

use super::*;
use crate::egress_policy::Destination;
use crate::router::egress::test_support::*;

fn actor() -> ActorRef {
    ActorRef::new(HIVE, ACTOR)
}

fn cache(api: Arc<MockApi>, ttl: Duration) -> PolicyCache {
    PolicyCache::new(Arc::new(Api(api)), ttl)
}

#[tokio::test(start_paused = true)]
async fn serves_from_cache_within_ttl() {
    let api = MockApi::with(None, Some(allow_all_policy()));
    let c = cache(api.clone(), Duration::from_secs(10));
    c.get(&actor()).await.unwrap();
    c.get(&actor()).await.unwrap();
    assert_eq!(api.policy_calls.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(10)).await;
    c.get(&actor()).await.unwrap();
    assert_eq!(api.policy_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn caches_no_policy() {
    let api = MockApi::with(None, None);
    let c = cache(api.clone(), Duration::from_secs(10));
    for _ in 0..2 {
        assert!(matches!(c.get(&actor()).await, Err(PolicyError::NoPolicy)));
    }
    assert_eq!(api.policy_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn does_not_cache_fetch_errors() {
    let api = MockApi::with(None, Some(allow_all_policy()));
    *api.policy_err.lock() = Some(tonic::Status::unavailable("down"));
    let c = cache(api.clone(), Duration::from_secs(10));
    assert!(
        matches!(c.get(&actor()).await, Err(PolicyError::Fetch(s)) if s.code() == tonic::Code::Unavailable)
    );
    *api.policy_err.lock() = None;
    c.get(&actor()).await.unwrap();
    assert_eq!(api.policy_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn disabled_fetches_every_time() {
    let api = MockApi::with(None, Some(allow_all_policy()));
    let c = cache(api.clone(), Duration::ZERO);
    for _ in 0..3 {
        c.get(&actor()).await.unwrap();
    }
    assert_eq!(api.policy_calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn collapses_concurrent_fetches() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let api = Arc::new(MockApi {
        policy: Mutex::new(Some(allow_all_policy())),
        policy_gate: Some(gate.clone()),
        ..Default::default()
    });
    let c = Arc::new(cache(api.clone(), Duration::from_secs(10)));
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let c = c.clone();
            tokio::spawn(async move { c.get(&actor()).await.map(|_| ()) })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(20)).await;
    gate.notify_waiters();
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    assert_eq!(api.policy_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fetch_outlives_canceled_caller() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let api = Arc::new(MockApi {
        policy: Mutex::new(Some(allow_all_policy())),
        policy_gate: Some(gate.clone()),
        ..Default::default()
    });
    let c = Arc::new(cache(api.clone(), Duration::from_secs(10)));
    let c1 = c.clone();
    let t = tokio::spawn(async move { c1.get(&actor()).await.map(|_| ()) });
    tokio::time::sleep(Duration::from_millis(20)).await;
    t.abort();
    assert!(t.await.unwrap_err().is_cancelled());
    gate.notify_waiters();
    tokio::time::sleep(Duration::from_millis(20)).await;
    c.get(&actor()).await.unwrap();
    assert_eq!(
        api.policy_calls.load(Ordering::SeqCst),
        1,
        "the canceled caller's fetch is reused"
    );
}

#[tokio::test(start_paused = true)]
async fn expiry_boundary_and_update_visibility() {
    let api = MockApi::with(None, Some(http_policy(&["old.example"])));
    let c = cache(api.clone(), Duration::from_secs(10));
    let before = c.get(&actor()).await.unwrap();
    *api.policy.lock() = Some(http_policy(&["new.example"]));
    tokio::time::advance(Duration::from_secs(10) - Duration::from_millis(1)).await;
    assert!(
        Arc::ptr_eq(&c.get(&actor()).await.unwrap(), &before),
        "refetched before the TTL elapsed"
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    let after = c.get(&actor()).await.unwrap();
    assert!(!Arc::ptr_eq(&after, &before));
    assert!(
        after
            .evaluate_request(&Destination::host("new.example"), false)
            .allowed
    );
}

#[tokio::test(start_paused = true)]
async fn removes_expired_entries() {
    let api = MockApi::with(None, Some(allow_all_policy()));
    let c = cache(api, Duration::from_secs(10));
    for i in 0..MIN_CLEANUP_SIZE {
        c.get(&ActorRef::new("a", format!("actor-{i}")))
            .await
            .unwrap();
    }
    tokio::time::advance(Duration::from_secs(11)).await;
    for i in 0..MIN_CLEANUP_SIZE {
        c.get(&ActorRef::new("b", format!("actor-{i}")))
            .await
            .unwrap();
    }
    assert_eq!(c.len(), MIN_CLEANUP_SIZE);
}
