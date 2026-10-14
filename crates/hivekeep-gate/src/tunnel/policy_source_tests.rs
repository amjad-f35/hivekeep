use std::sync::atomic::{AtomicI32, Ordering};

use tokio::sync::Notify;

use super::*;

fn http_policy(host: &str) -> pb::EgressPolicy {
    pb::EgressPolicy {
        rules: vec![pb::EgressRule {
            http: Some(pb::EgressDestinations {
                hostnames: vec![host.into()],
                ports: None,
            }),
            ..Default::default()
        }],
    }
}

#[derive(Clone)]
struct Clock(Arc<Mutex<Instant>>);

impl Clock {
    fn new() -> Self {
        Clock(Arc::new(Mutex::new(Instant::now())))
    }
    fn advance(&self, d: Duration) {
        *self.0.lock() += d;
    }
    fn now_fn(&self) -> Arc<dyn Fn() -> Instant + Send + Sync> {
        let c = self.0.clone();
        Arc::new(move || *c.lock())
    }
}

type Scripted = Vec<Result<Option<pb::EgressPolicy>, String>>;

fn scripted(results: Scripted) -> (Fetch, Arc<AtomicI32>) {
    let fetches = Arc::new(AtomicI32::new(0));
    let f = fetches.clone();
    let results = Arc::new(results);
    let fetch: Fetch = Arc::new(move || {
        let i = f.fetch_add(1, Ordering::SeqCst) as usize;
        let r = results[i].clone();
        Box::pin(async move { r })
    });
    (fetch, fetches)
}

fn quiet() -> Log {
    Arc::new(|_, _| {})
}

fn source(fetch: Fetch, clock: &Clock, log: Log) -> ActorEgressPolicy {
    ActorEgressPolicy::with_clock(fetch, EGRESS_POLICY_TTL, clock.now_fn(), log)
}

fn deref(p: &Policy) -> Option<&pb::EgressPolicy> {
    p.as_deref()
}

#[tokio::test]
async fn reuses_a_fetch_for_its_ttl() {
    let policy = http_policy("example.com");
    let (fetch, fetches) = scripted(vec![Ok(Some(policy.clone())), Ok(Some(policy.clone()))]);
    let clock = Clock::new();
    let s = source(fetch, &clock, quiet());
    for (advance, want) in [
        (Duration::ZERO, 1),
        (EGRESS_POLICY_TTL - Duration::from_nanos(1), 1),
        (Duration::from_nanos(1), 2),
    ] {
        clock.advance(advance);
        let got = s.get().await.unwrap();
        assert_eq!(deref(&got), Some(&policy));
        assert_eq!(
            fetches.load(Ordering::SeqCst),
            want,
            "after {advance:?} more"
        );
    }
}

#[tokio::test]
async fn reuses_no_policy() {
    let (fetch, fetches) = scripted(vec![Ok(None)]);
    let s = source(fetch, &Clock::new(), quiet());
    for _ in 0..2 {
        assert!(s.get().await.unwrap().is_none());
    }
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retries_after_a_failure() {
    let (fetch, _) = scripted(vec![
        Err("keeper is unavailable".into()),
        Ok(Some(pb::EgressPolicy::default())),
    ]);
    let s = source(fetch, &Clock::new(), quiet());
    assert_eq!(&*s.get().await.unwrap_err(), "keeper is unavailable");
    assert_eq!(
        deref(&s.get().await.unwrap()),
        Some(&pb::EgressPolicy::default())
    );
}

#[tokio::test]
async fn fails_static_on_refresh_failure() {
    let (p1, p2) = (
        http_policy("one.example.com"),
        http_policy("two.example.com"),
    );
    let err = || Err::<Option<pb::EgressPolicy>, String>("keeper is unavailable".into());
    let (fetch, _) = scripted(vec![
        Ok(Some(p1.clone())),
        err(),
        err(),
        Ok(Some(p2.clone())),
        Ok(None),
        err(),
    ]);
    let clock = Clock::new();
    let s = source(fetch, &clock, quiet());
    for (i, want) in [Some(&p1), Some(&p1), Some(&p1), Some(&p2), None, None]
        .into_iter()
        .enumerate()
    {
        let got = s.get().await.unwrap_or_else(|e| panic!("step {i}: {e}"));
        assert_eq!(deref(&got), want, "step {i}");
        clock.advance(EGRESS_POLICY_TTL);
    }
}

#[tokio::test]
async fn shares_one_fetch() {
    let release = Arc::new(Notify::new());
    let fetches = Arc::new(AtomicI32::new(0));
    let (r, f) = (release.clone(), fetches.clone());
    let fetch: Fetch = Arc::new(move || {
        f.fetch_add(1, Ordering::SeqCst);
        let r = r.clone();
        Box::pin(async move {
            r.notified().await;
            Ok(Some(pb::EgressPolicy::default()))
        })
    });
    let s = source(fetch, &Clock::new(), quiet());
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let s = s.clone();
            tokio::spawn(async move { s.get().await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(20)).await;
    release.notify_one();
    for t in tasks {
        assert!(t.await.unwrap().unwrap().is_some());
    }
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        1,
        "one fetch for concurrent queries"
    );
}

#[tokio::test]
async fn query_stops_waiting_when_canceled() {
    let release = Arc::new(Notify::new());
    let started = Arc::new(Notify::new());
    let fetches = Arc::new(AtomicI32::new(0));
    let (r, st, f) = (release.clone(), started.clone(), fetches.clone());
    let fetch: Fetch = Arc::new(move || {
        f.fetch_add(1, Ordering::SeqCst);
        let (r, st) = (r.clone(), st.clone());
        Box::pin(async move {
            st.notify_one();
            r.notified().await;
            Ok(Some(pb::EgressPolicy::default()))
        })
    });
    let s = source(fetch, &Clock::new(), quiet());
    let s2 = s.clone();
    let query = tokio::spawn(async move { s2.get().await });
    started.notified().await;
    query.abort();
    assert!(
        query.await.unwrap_err().is_cancelled(),
        "the query kept waiting"
    );
    release.notify_one();
    // The fetch outlives the abandoned query and is recorded.
    let got = tokio::time::timeout(Duration::from_secs(5), s.get())
        .await
        .unwrap()
        .unwrap();
    assert!(got.is_some());
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn logs_changes() {
    let err = || Err::<Option<pb::EgressPolicy>, String>("keeper is unavailable".into());
    let ok = || Ok(Some(pb::EgressPolicy::default()));
    let none = || Ok::<Option<pb::EgressPolicy>, String>(None);
    let ttl = EGRESS_POLICY_TTL;
    let warn = EGRESS_POLICY_STALE_WARN_INTERVAL;
    let ns = Duration::from_nanos(1);
    let steps = vec![
        (Duration::ZERO, err()),
        (Duration::ZERO, err()),
        (Duration::ZERO, none()),
        (ttl, none()),
        (ttl, err()),
        (warn - ns, err()),
        (ns, err()),
        (ttl, err()),
        (warn - ttl, err()),
        (ttl, ok()),
        (ttl, ok()),
        (ttl, err()),
        (ttl, none()),
    ];
    let (fetch, _) = scripted(steps.iter().map(|(_, r)| r.clone()).collect());
    let records = Arc::new(Mutex::new(Vec::new()));
    let rec = records.clone();
    let log: Log = Arc::new(move |l, m| rec.lock().push(format!("{l:?} {m}")));
    let clock = Clock::new();
    let s = source(fetch, &clock, log);
    for (advance, _) in &steps {
        clock.advance(*advance);
        let _ = s.get().await;
    }
    let want: Vec<String> = [
        ("Error", msg::FETCH_FAILED),
        ("Debug", msg::FETCH_FAILED),
        ("Info", msg::NO_POLICY),
        ("Error", msg::REFRESH_FAILED),
        ("Debug", msg::REFRESH_FAILED),
        ("Warn", msg::REFRESH_FAILED),
        ("Debug", msg::REFRESH_FAILED),
        ("Warn", msg::REFRESH_FAILED),
        ("Info", msg::HAS_POLICY),
        ("Error", msg::REFRESH_FAILED),
        ("Info", msg::NO_POLICY),
    ]
    .iter()
    .map(|(l, m)| format!("{l} {m}"))
    .collect();
    assert_eq!(*records.lock(), want);
}
