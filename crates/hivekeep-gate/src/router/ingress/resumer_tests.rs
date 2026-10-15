use std::sync::atomic::AtomicU32;

use tokio::sync::Notify;
use tonic::{Code, Status};

use super::*;
use crate::attr::outcome;

pub(crate) struct FnApi<F>(pub F);

impl<F> ActorApi for FnApi<F>
where
    F: Fn(pb::ResumeActorRequest) -> BoxFuture<'static, Result<pb::ResumeActorResponse, Status>>
        + Send
        + Sync
        + 'static,
{
    fn resume_actor(
        &self,
        req: pb::ResumeActorRequest,
    ) -> BoxFuture<'static, Result<pb::ResumeActorResponse, Status>> {
        (self.0)(req)
    }
}

pub(crate) fn running(ip: &str, resumed: bool) -> pb::ResumeActorResponse {
    pb::ResumeActorResponse {
        actor: Some(pb::Actor {
            status: Some(pb::ActorStatus {
                state: pb::ActorState::Running as i32,
                worker_assignment: Some(pb::WorkerAssignment {
                    worker_pod_ips: vec![ip.into()],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        resumed,
    }
}

fn ips(a: &pb::Actor) -> Vec<String> {
    a.status
        .as_ref()
        .and_then(|s| s.worker_assignment.as_ref())
        .map(|w| w.worker_pod_ips.clone())
        .unwrap_or_default()
}

fn api<F, Fut>(f: F) -> Arc<dyn ActorApi>
where
    F: Fn(u32) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<pb::ResumeActorResponse, Status>> + Send + 'static,
{
    let calls = Arc::new(AtomicU32::new(0));
    Arc::new(FnApi(move |_req| {
        let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(f(n)) as BoxFuture<'static, _>
    }))
}

fn counting<F, Fut>(f: F) -> (Arc<dyn ActorApi>, Arc<AtomicU32>)
where
    F: Fn(u32) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<pb::ResumeActorResponse, Status>> + Send + 'static,
{
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    (
        Arc::new(FnApi(move |_req| {
            let n = c.fetch_add(1, Ordering::SeqCst) + 1;
            Box::pin(f(n)) as BoxFuture<'static, _>
        })),
        calls,
    )
}

fn actor() -> ActorRef {
    ActorRef::new("team-a", "actor-a")
}

const IP: &str = "10.0.0.52";

fn none() -> RequestCtx {
    RequestCtx::default()
}

#[tokio::test]
async fn resume_actor_suspended_resumed_successfully() {
    let (a, calls) = counting(|_| async { Ok(running(IP, true)) });
    let r = Arc::new(ActorResumer::new(a));
    let (got, outcome) = r.resume_actor(&actor(), &none()).await.unwrap();
    assert_eq!(ips(&got), vec![IP]);
    assert_eq!(outcome, ResumeOutcome::Triggered);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn resume_actor_warm_routing() {
    let r = Arc::new(ActorResumer::new(api(|_| async { Ok(running(IP, false)) })));
    assert_eq!(
        r.resume_actor(&actor(), &none()).await.unwrap().1,
        ResumeOutcome::None
    );
}

#[tokio::test(start_paused = true)]
async fn resume_actor_retry_on_aborted_conflict() {
    let (a, calls) = counting(|n| async move {
        if n < 3 {
            Err(Status::new(Code::Aborted, "concurrent update conflict"))
        } else {
            Ok(running(IP, true))
        }
    });
    let r = Arc::new(ActorResumer::new(a));
    let (got, outcome) = r.resume_actor(&actor(), &none()).await.unwrap();
    assert_eq!(ips(&got), vec![IP]);
    assert_eq!(outcome, ResumeOutcome::Triggered);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn resume_actor_not_found() {
    let r = Arc::new(ActorResumer::new(api(|_| async {
        Err(Status::new(Code::NotFound, "not found"))
    })));
    let err = r.resume_actor(&actor(), &none()).await.unwrap_err();
    assert_eq!(err.code(), Some(Code::NotFound));
}

#[tokio::test]
async fn resume_actor_caller_context_canceled() {
    let gate = Arc::new(Notify::new());
    let g = gate.clone();
    let r = Arc::new(ActorResumer::new(api(move |_| {
        let g = g.clone();
        async move {
            g.notified().await;
            Ok(running(IP, true))
        }
    })));
    let cancel = CancellationToken::new();
    cancel.cancel();
    let err = r
        .resume_actor(
            &actor(),
            &RequestCtx {
                cancel: Some(cancel),
                deadline: None,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ResumeError::Canceled));
    gate.notify_one();
}

#[tokio::test]
async fn singleflight_deduplication_failed_flight() {
    let gate = Arc::new(Notify::new());
    let g = gate.clone();
    let (a, calls) = counting(move |_| {
        let g = g.clone();
        async move {
            g.notified().await;
            Err(Status::new(
                Code::ResourceExhausted,
                "no worker has room for the actor",
            ))
        }
    });
    let r = Arc::new(ActorResumer::new(a));
    let tasks: Vec<_> = (0..10)
        .map(|_| {
            let r = r.clone();
            tokio::spawn(async move { r.resume_actor(&actor(), &none()).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(20)).await;
    gate.notify_one();
    for t in tasks {
        assert_eq!(
            t.await.unwrap().unwrap_err().code(),
            Some(Code::ResourceExhausted)
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn singleflight_deduplication_disambiguation() {
    let (a, calls) = counting(|_| async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(running(IP, true))
    });
    let r = Arc::new(ActorResumer::new(a));
    let tasks: Vec<_> = (0..10)
        .map(|_| {
            let r = r.clone();
            tokio::spawn(async move { r.resume_actor(&actor(), &none()).await })
        })
        .collect();
    let mut triggered = 0;
    let mut joined = 0;
    for t in tasks {
        let (a, o) = t.await.unwrap().unwrap();
        assert_eq!(ips(&a), vec![IP]);
        match o {
            ResumeOutcome::Triggered => triggered += 1,
            ResumeOutcome::Joined => joined += 1,
            other => panic!("unexpected outcome {other:?}"),
        }
    }
    assert_eq!((triggered, joined), (1, 9));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

fn parked(max: usize, budget: Duration) -> ParkedRequestConfig {
    ParkedRequestConfig::with(max, budget)
}

#[tokio::test(start_paused = true)]
async fn parks_then_succeeds_on_capacity_error() {
    let (a, calls) = counting(|n| async move {
        if n < 3 {
            Err(Status::new(
                Code::FailedPrecondition,
                "no worker has room for the actor",
            ))
        } else {
            Ok(running("10.0.0.77", false))
        }
    });
    let r = Arc::new(ActorResumer::new(a).with_parking(parked(1, Duration::from_secs(5))));
    assert_eq!(
        ips(&r.resume_actor(&actor(), &none()).await.unwrap().0),
        vec!["10.0.0.77"]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn budget_expiry_returns_underlying_capacity_error() {
    let (a, calls) = counting(|_| async {
        Err(Status::new(
            Code::FailedPrecondition,
            "no worker has room for the actor",
        ))
    });
    let r = Arc::new(ActorResumer::new(a).with_parking(parked(1, Duration::from_millis(1500))));
    let err = r.resume_actor(&actor(), &none()).await.unwrap_err();
    assert_eq!(err.code(), Some(Code::FailedPrecondition));
    assert!(matches!(err, ResumeError::BudgetExhausted(_)));
    assert!(calls.load(Ordering::SeqCst) >= 2);
}

#[tokio::test(start_paused = true)]
async fn parks_through_unavailable_blip() {
    let (a, calls) = counting(|n| async move {
        if n < 3 {
            Err(Status::new(Code::Unavailable, "connection refused"))
        } else {
            Ok(running("10.0.0.77", false))
        }
    });
    let r = Arc::new(ActorResumer::new(a).with_parking(parked(1, Duration::from_secs(5))));
    r.resume_actor(&actor(), &none()).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn disabled_fails_fast_on_unavailable() {
    let (a, calls) =
        counting(|_| async { Err(Status::new(Code::Unavailable, "connection refused")) });
    let r = Arc::new(ActorResumer::new(a));
    assert_eq!(
        r.resume_actor(&actor(), &none()).await.unwrap_err().code(),
        Some(Code::Unavailable)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn disabled_fails_fast_on_capacity_error() {
    let (a, calls) = counting(|_| async {
        Err(Status::new(
            Code::FailedPrecondition,
            "no worker has room for the actor",
        ))
    });
    let r = Arc::new(ActorResumer::new(a));
    assert_eq!(
        r.resume_actor(&actor(), &none()).await.unwrap_err().code(),
        Some(Code::FailedPrecondition)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn in_flight_attempt_runs_to_completion() {
    let budget = Duration::from_millis(300);
    let base = Instant::now();
    let starts = Arc::new(Mutex::new(Vec::new()));
    let s = starts.clone();
    let (a, _) = counting(move |n| {
        s.lock().push(base.elapsed());
        async move {
            if n == 1 {
                return Err(Status::new(
                    Code::ResourceExhausted,
                    "no worker has room for the actor",
                ));
            }
            tokio::time::sleep(budget).await;
            Ok(running("10.0.0.77", true))
        }
    });
    let r = Arc::new(ActorResumer::new(a).with_parking(parked(1, budget)));
    let (got, _) = r
        .resume_actor(&actor(), &none())
        .await
        .expect("the overshooting resume is served");
    assert_eq!(ips(&got), vec!["10.0.0.77"]);
    for (i, s) in starts.lock().iter().enumerate() {
        assert!(*s < budget, "attempt {} started after the budget", i + 1);
    }
}

#[tokio::test(start_paused = true)]
async fn late_non_retryable_error_is_preserved() {
    for (name, code) in [
        ("NotFound", Code::NotFound),
        ("PermissionDenied", Code::PermissionDenied),
        ("Internal", Code::Internal),
    ] {
        let budget = Duration::from_millis(300);
        let (a, _) = counting(move |n| async move {
            if n == 1 {
                return Err(Status::new(
                    Code::ResourceExhausted,
                    "no free workers available",
                ));
            }
            tokio::time::sleep(budget).await;
            Err(Status::new(code, "terminal"))
        });
        let r = Arc::new(ActorResumer::new(a).with_parking(parked(1, budget)));
        let err = r.resume_actor(&actor(), &none()).await.unwrap_err();
        assert_eq!(err.code(), Some(code), "{name}");
        assert!(
            !matches!(err, ResumeError::BudgetExhausted(_)),
            "{name}: classified as budget exhaustion"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn late_retryable_error_is_budget_exhaustion() {
    let budget = Duration::from_millis(300);
    let (a, calls) = counting(move |_| async move {
        tokio::time::sleep(budget + Duration::from_millis(100)).await;
        Err(Status::new(
            Code::ResourceExhausted,
            "no worker has room for the actor",
        ))
    });
    let r = Arc::new(ActorResumer::new(a).with_parking(parked(1, budget)));
    let err = r.resume_actor(&actor(), &none()).await.unwrap_err();
    assert!(matches!(err, ResumeError::BudgetExhausted(_)), "{err:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "no retry after the budget");
}

#[tokio::test(start_paused = true)]
async fn caller_cancel_does_not_abort_flight() {
    let proceed = Arc::new(Notify::new());
    let p = proceed.clone();
    let (a, calls) = counting(move |n| {
        let p = p.clone();
        async move {
            if n == 1 {
                return Err(Status::new(
                    Code::ResourceExhausted,
                    "no worker has room for the actor",
                ));
            }
            p.notified().await;
            Ok(running("10.0.0.88", true))
        }
    });
    let reg = prometheus::Registry::new();
    let lot = Arc::new(ParkingLot::new(
        parked(2, Duration::from_secs(5)),
        Some(super::super::parking::ParkingMetrics::new(&reg).unwrap()),
    ));
    let r = Arc::new(
        ActorResumer::new(a)
            .with_parking(parked(2, Duration::from_secs(5)))
            .with_lot(lot.clone()),
    );
    let cancel = CancellationToken::new();
    let (r1, c1) = (r.clone(), cancel.clone());
    let first = tokio::spawn(async move {
        r1.resume_actor(
            &actor(),
            &RequestCtx {
                cancel: Some(c1),
                deadline: None,
            },
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    cancel.cancel();
    assert!(matches!(
        first.await.unwrap().unwrap_err(),
        ResumeError::Canceled
    ));
    let wait = reg
        .gather()
        .into_iter()
        .find(|m| m.name() == super::super::parking::PARKING_WAIT_METRIC)
        .unwrap();
    assert_eq!(wait.get_metric()[0].get_label()[0].value(), "canceled");
    let r2 = r.clone();
    let second = tokio::spawn(async move { r2.resume_actor(&actor(), &none()).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    proceed.notify_one();
    let (got, _) = second.await.unwrap().unwrap();
    assert_eq!(ips(&got), vec!["10.0.0.88"]);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the canceled caller's flight is shared"
    );
}

#[tokio::test(start_paused = true)]
async fn fast_flight_never_enters_lot() {
    let cfg = parked(1, Duration::from_secs(5));
    let lot = Arc::new(ParkingLot::new(cfg, None));
    let _priming = lot.enter(outcome::UNAVAILABLE).unwrap();
    let r = Arc::new(
        ActorResumer::new(api(|_| async { Ok(running("10.0.0.99", true)) }))
            .with_parking(cfg)
            .with_lot(lot.clone()),
    );
    assert_eq!(
        ips(&r.resume_actor(&actor(), &none()).await.unwrap().0),
        vec!["10.0.0.99"]
    );
    assert_eq!(lot.active_count(), 1, "fast path must not take a slot");
}

#[tokio::test(start_paused = true)]
async fn park_transition_acquires_slot() {
    let cfg = parked(2, Duration::from_secs(5));
    let lot = Arc::new(ParkingLot::new(cfg, None));
    let during = Arc::new(Mutex::new(None));
    let (l, d) = (lot.clone(), during.clone());
    let a = api(move |n| {
        let (l, d) = (l.clone(), d.clone());
        async move {
            if n == 1 {
                return Err(Status::new(
                    Code::ResourceExhausted,
                    "no worker has room for the actor",
                ));
            }
            *d.lock() = Some(l.active_count());
            Ok(running("10.0.0.99", true))
        }
    });
    let r = Arc::new(ActorResumer::new(a).with_parking(cfg).with_lot(lot.clone()));
    r.resume_actor(&actor(), &none()).await.unwrap();
    assert_eq!(*during.lock(), Some(1), "caller holds a slot while parked");
    assert_eq!(lot.active_count(), 0, "slot released when the wait ends");
}

#[tokio::test(start_paused = true)]
async fn shed_when_lot_full_at_park_transition() {
    let cfg = parked(1, Duration::from_millis(500));
    let lot = Arc::new(ParkingLot::new(cfg, None));
    let _priming = lot.enter(outcome::UNAVAILABLE).unwrap();
    let (a, calls) = counting(|_| async {
        Err(Status::new(
            Code::ResourceExhausted,
            "no worker has room for the actor",
        ))
    });
    let r = Arc::new(ActorResumer::new(a).with_parking(cfg).with_lot(lot.clone()));
    match r.resume_actor(&actor(), &none()).await.unwrap_err() {
        ResumeError::Route(e) => assert_eq!(e.http_status(), 503),
        other => panic!("want a router-at-capacity denial, got {other:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(600)).await;
}

#[tokio::test(start_paused = true)]
async fn joiner_to_parked_flight_needs_slot() {
    let cfg = parked(1, Duration::from_secs(5));
    let lot = Arc::new(ParkingLot::new(cfg, None));
    let proceed = Arc::new(Notify::new());
    let p = proceed.clone();
    let a = api(move |n| {
        let p = p.clone();
        async move {
            if n == 1 {
                return Err(Status::new(
                    Code::ResourceExhausted,
                    "no worker has room for the actor",
                ));
            }
            p.notified().await;
            Ok(running("10.0.0.99", true))
        }
    });
    let r = Arc::new(ActorResumer::new(a).with_parking(cfg).with_lot(lot.clone()));
    let r1 = r.clone();
    let leader = tokio::spawn(async move { r1.resume_actor(&actor(), &none()).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    match r.resume_actor(&actor(), &none()).await.unwrap_err() {
        ResumeError::Route(e) => assert_eq!(e.http_status(), 503),
        other => panic!("joiner: want 503, got {other:?}"),
    }
    proceed.notify_one();
    let (got, outcome) = leader.await.unwrap().unwrap();
    assert_eq!(outcome, ResumeOutcome::Triggered);
    assert_eq!(ips(&got), vec!["10.0.0.99"]);
    assert_eq!(lot.active_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn budget_exhaustion_releases_slot() {
    let cfg = parked(1, Duration::from_secs(1));
    let lot = Arc::new(ParkingLot::new(cfg, None));
    let r = Arc::new(
        ActorResumer::new(api(|_| async {
            Err(Status::new(
                Code::ResourceExhausted,
                "no worker has room for the actor",
            ))
        }))
        .with_parking(cfg)
        .with_lot(lot.clone()),
    );
    let err = r.resume_actor(&actor(), &none()).await.unwrap_err();
    assert!(matches!(err, ResumeError::BudgetExhausted(_)));
    assert_eq!(err.code(), Some(Code::ResourceExhausted));
    assert_eq!(lot.active_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn cancel_while_parked_releases_slot() {
    let cfg = parked(1, Duration::from_secs(5));
    let lot = Arc::new(ParkingLot::new(cfg, None));
    let proceed = Arc::new(Notify::new());
    let p = proceed.clone();
    let a = api(move |n| {
        let p = p.clone();
        async move {
            if n == 1 {
                return Err(Status::new(
                    Code::ResourceExhausted,
                    "no worker has room for the actor",
                ));
            }
            p.notified().await;
            Ok(running("10.0.0.99", true))
        }
    });
    let r = Arc::new(ActorResumer::new(a).with_parking(cfg).with_lot(lot.clone()));
    let cancel = CancellationToken::new();
    let (r1, c1) = (r.clone(), cancel.clone());
    let t = tokio::spawn(async move {
        r1.resume_actor(
            &actor(),
            &RequestCtx {
                cancel: Some(c1),
                deadline: None,
            },
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(lot.active_count(), 1, "parked caller holds a slot");
    cancel.cancel();
    assert!(matches!(
        t.await.unwrap().unwrap_err(),
        ResumeError::Canceled
    ));
    assert_eq!(lot.active_count(), 0);
    proceed.notify_one();
    tokio::time::sleep(Duration::from_millis(200)).await;
}

#[tokio::test(start_paused = true)]
async fn deadline_while_parked_is_a_timeout() {
    let cfg = parked(1, Duration::from_secs(5));
    let lot = Arc::new(ParkingLot::new(cfg, None));
    let r = Arc::new(
        ActorResumer::new(api(|_| async {
            Err(Status::new(Code::ResourceExhausted, "no room"))
        }))
        .with_parking(cfg)
        .with_lot(lot.clone()),
    );
    let ctx = RequestCtx {
        cancel: None,
        deadline: Some(Instant::now() + Duration::from_secs(1)),
    };
    assert!(matches!(
        r.resume_actor(&actor(), &ctx).await.unwrap_err(),
        ResumeError::Timeout
    ));
    assert_eq!(lot.active_count(), 0);
}

#[tokio::test]
async fn completed_flight_is_forgotten() {
    let (a, calls) = counting(|_| async { Ok(running(IP, true)) });
    let r = Arc::new(ActorResumer::new(a).with_parking(parked(1, Duration::from_secs(1))));
    for i in 0..2 {
        let (_, o) = r.resume_actor(&actor(), &none()).await.unwrap();
        assert_eq!(o, ResumeOutcome::Triggered, "call {i}");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn resume_backoff_has_no_cap() {
    let mut b = Backoff {
        interval: Duration::from_millis(100),
        factor: 1.1,
        jitter: 0.0,
    };
    let mut last = Duration::ZERO;
    for _ in 0..200 {
        let d = b.step();
        assert!(d >= last, "backoff must keep growing");
        last = d;
    }
    assert!(last > Duration::from_secs(60), "no cap: {last:?}");
}

#[test]
fn park_outcome_for() {
    let other = ResumeError::Other("boom".into());
    let budget = ResumeError::BudgetExhausted(Status::new(Code::Unknown, "boom"));
    for (name, err, want) in [
        ("nil is served", None, ParkOutcome::Served),
        (
            "budget exhaustion is explicit",
            Some(&budget),
            ParkOutcome::BudgetExhausted,
        ),
        (
            "canceled",
            Some(&ResumeError::Canceled),
            ParkOutcome::Canceled,
        ),
        (
            "deadline is timeout",
            Some(&ResumeError::Timeout),
            ParkOutcome::Timeout,
        ),
        ("other is error", Some(&other), ParkOutcome::Error),
    ] {
        assert_eq!(ResumeError::park_outcome(err), want, "{name}");
    }
}
