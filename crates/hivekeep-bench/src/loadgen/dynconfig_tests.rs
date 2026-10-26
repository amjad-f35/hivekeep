use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use parking_lot::Mutex;

use super::*;
use crate::testutil::{HttpServer, Resp};

/// The kind of struct a user class declares: its slice of the payload with
/// its own defaults and rules.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Knobs {
    #[serde(flatten)]
    wait: WaitTime,
    #[serde(flatten)]
    lifecycle: Lifecycle,
    #[serde(rename = "test_template")]
    template: String,
    #[serde(rename = "test_workers")]
    workers: i64,
}

fn defaults() -> Knobs {
    Knobs {
        wait: WaitTime::default(),
        lifecycle: Lifecycle::default(),
        template: "stock".into(),
        workers: 1,
    }
}

fn knobs_codec() -> Typed<Knobs> {
    Typed::new(defaults()).with_validate(|k| {
        k.wait.validate()?;
        k.lifecycle.validate()?;
        if k.workers < 1 {
            return Err(format!("test_workers must be positive: {}", k.workers));
        }
        Ok(())
    })
}

fn holder() -> Holder {
    Holder::new(Box::new(knobs_codec()))
}

fn must_apply(h: &Holder, payload: &str) -> bool {
    h.apply(payload.as_bytes())
        .unwrap_or_else(|e| panic!("apply({payload}): {e}"))
        .1
}

#[test]
fn apply_decodes_over_defaults() {
    let h = holder();
    assert_eq!(h.get::<Knobs>(), defaults(), "fresh holder");
    must_apply(
        &h,
        r#"{
            "min_wait_time": 0.5, "max_wait_time": 2,
            "resume_mode": "implicit", "lifecycle_mode": "none",
            "test_workers": 3,
            "someone_elses_key": "ignored"
        }"#,
    );
    let want = Knobs {
        wait: WaitTime {
            min_wait: Seconds::from_millis(500),
            max_wait: Seconds::from_secs(2),
        },
        lifecycle: Lifecycle {
            resume_mode: RESUME_MODE_IMPLICIT.into(),
            lifecycle_mode: LIFECYCLE_MODE_NONE.into(),
        },
        template: "stock".into(),
        workers: 3,
    };
    assert_eq!(h.get::<Knobs>(), want, "after first payload");

    // A cleared form field comes as null, and a key the master does not
    // know is absent; both are back at the default, not at the last value.
    must_apply(
        &h,
        r#"{"test_template": "big", "test_workers": null, "max_wait_time": null}"#,
    );
    let mut want = defaults();
    want.template = "big".into();
    assert_eq!(h.get::<Knobs>(), want, "after nulls and omissions");
    assert!(
        !must_apply(&h, r#"{"test_template": "big"}"#),
        "a payload that changes nothing reported a change"
    );
    assert!(
        must_apply(&h, "{}"),
        "an empty object did not report the return to the defaults"
    );
    assert_eq!(h.get::<Knobs>(), defaults());
}

#[test]
fn apply_returns_what_it_stored() {
    let h = holder();
    let (applied, changed) = h
        .apply(br#"{"test_workers": 4, "trace_probability": 0.5}"#)
        .unwrap();
    assert!(changed);
    assert!(applied.class.same_as(h.load().as_ref()));
    assert_eq!(applied.common, h.common());
    assert_eq!(
        applied
            .class
            .as_any()
            .downcast_ref::<Knobs>()
            .unwrap()
            .workers,
        4
    );
}

#[test]
fn apply_refuses() {
    let h = holder();
    must_apply(&h, r#"{"test_workers": 2, "trace_probability": 0.5}"#);
    let before = h.get::<Knobs>();
    for (name, payload) in [
        ("class rule", r#"{"test_workers": 0}"#),
        ("embedded", r#"{"max_wait_time": 1, "min_wait_time": 2}"#),
        ("mode", r#"{"lifecycle_mode": "hibernate"}"#),
        ("wrong type", r#"{"test_workers": "three"}"#),
        ("bad seconds", r#"{"min_wait_time": "soon"}"#),
        ("common rule", r#"{"trace_probability": 1.5}"#),
        ("not json", "<html>"),
        ("empty body", ""),
    ] {
        let res = h.apply(payload.as_bytes());
        assert!(
            res.is_err(),
            "{name}: apply({payload}) = {res:?}, want a refusal"
        );
        assert_eq!(
            h.get::<Knobs>(),
            before,
            "{name}: a refused payload changed the config"
        );
        assert_eq!(h.common().trace_probability, 0.5, "{name}");
    }
    // A type error names the key it came from.
    let err = h.apply(br#"{"test_workers": "three"}"#).unwrap_err();
    assert!(err.0.contains("test_workers"), "{err}");
}

#[test]
fn get_type_checks() {
    assert_eq!(
        get::<Common>(None),
        Common::default(),
        "no holder reads as the default"
    );
    let h = holder();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h.get::<Common>()));
    assert!(res.is_err(), "get with the wrong type did not panic");
}

#[test]
fn static_and_nil_codec() {
    let mut k = defaults();
    k.template = "t".into();
    k.wait.max_wait = Seconds::from_secs(3);
    let h = Holder::fixed(k.clone());
    let got = h.get::<Knobs>();
    assert_eq!(got.template, "t");
    assert_eq!(got.wait.max_wait, Seconds::from_secs(3));

    // A class without knobs still gets the common slice.
    let h = Holder::without_knobs();
    must_apply(&h, r#"{"trace_probability": 0.25, "anything": 1}"#);
    assert_eq!(h.common().trace_probability, 0.25);
}

#[test]
fn seconds_prints_as_duration() {
    let mut k = defaults();
    k.wait.max_wait = Seconds::from_millis(500);
    let line = format!("{k:?}");
    assert!(line.contains("max_wait: 500ms"), "printed config {line:?}");
}

#[test]
fn wait_time_draw() {
    let window = WaitTime {
        min_wait: Seconds::from_millis(10),
        max_wait: Seconds::from_millis(50),
    };
    for _ in 0..100 {
        let got = window.draw();
        assert!(
            got >= Duration::from_millis(10) && got <= Duration::from_millis(50),
            "{got:?}"
        );
    }
    let inverted = WaitTime {
        min_wait: Seconds::from_millis(100),
        max_wait: Seconds::from_millis(50),
    };
    assert_eq!(inverted.draw(), Duration::from_millis(100));
    assert_eq!(uniform(Duration::ZERO, Duration::ZERO), Duration::ZERO);
    assert_eq!(
        uniform(Duration::from_secs(3), Duration::from_secs(3)),
        Duration::from_secs(3)
    );
}

#[tokio::test]
async fn fetch_reports_errors() {
    let srv = HttpServer::start(|req| match req.uri.path() {
        "/valid" => Resp::ok(r#"{"resume_mode": "implicit"}"#),
        _ => Resp::status(503, "busy"),
    })
    .await;
    let body = fetch(&format!("{}/valid", srv.url()), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(body, br#"{"resume_mode": "implicit"}"#);
    assert!(
        fetch(&format!("{}/busy", srv.url()), Duration::from_secs(5))
            .await
            .is_err()
    );
}

/// Records each probability the loop applies.
#[derive(Default)]
struct FakeSampler {
    seen: Mutex<Vec<f64>>,
}

impl ProbabilityUpdater for FakeSampler {
    fn update_probability(&self, p: f64) {
        self.seen.lock().push(p);
    }
}

impl FakeSampler {
    fn last(&self) -> (f64, usize) {
        let s = self.seen.lock();
        (s.last().copied().unwrap_or(-1.0), s.len())
    }
}

/// A load-shape step can change the sample rate and hold the user count;
/// the master sends no spawn message for it, so the poll is the only way
/// the new rate reaches the worker.
#[tokio::test]
async fn start_poll_applies_a_change() {
    let probability = Arc::new(AtomicU64::new(0f64.to_bits()));
    let p = probability.clone();
    let srv = HttpServer::start(move |_| {
        Resp::ok(format!(
            r#"{{"trace_probability": {}}}"#,
            f64::from_bits(p.load(Ordering::SeqCst))
        ))
    })
    .await;
    let cancel = CancellationToken::new();
    let holder = Arc::new(holder());
    let sampler = Arc::new(FakeSampler::default());
    start_poll(
        cancel.clone(),
        srv.url(),
        holder.clone(),
        sampler.clone(),
        Duration::from_millis(10),
        Duration::from_secs(1),
        Arc::new(|e| panic!("unexpected poll error: {e}")),
    );
    probability.store(0.5f64.to_bits(), Ordering::SeqCst);
    let s = sampler.clone();
    crate::testutil::eventually(Duration::from_secs(2), || s.last().0 == 0.5).await;
    assert_eq!(holder.common().trace_probability, 0.5);

    // A value that does not change must not go to the sampler again.
    let (_, count) = sampler.last();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        sampler.last().1,
        count,
        "sampler got updates with no change"
    );
    cancel.cancel();
}

/// A poll that brings a value the class refuses goes to on_error and leaves
/// the holder as it was.
#[tokio::test]
async fn start_poll_reports_refused_config() {
    let srv = HttpServer::start(|_| Resp::ok(r#"{"test_workers": 0}"#)).await;
    let cancel = CancellationToken::new();
    let holder = Arc::new(holder());
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(1);
    start_poll(
        cancel.clone(),
        srv.url(),
        holder.clone(),
        Arc::new(FakeSampler::default()),
        Duration::from_millis(10),
        Duration::from_secs(1),
        Arc::new(move |e| {
            let _ = tx.try_send(e);
        }),
    );
    let err = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("a refused poll never reached on_error")
        .unwrap();
    assert!(err.contains("test_workers"), "{err}");
    assert_eq!(holder.get::<Knobs>(), defaults());
    cancel.cancel();
}

#[tokio::test]
async fn start_poll_stops_with_the_context() {
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let srv = HttpServer::start(move |_| {
        h.fetch_add(1, Ordering::SeqCst);
        Resp::ok("{}")
    })
    .await;
    let cancel = CancellationToken::new();
    start_poll(
        cancel.clone(),
        srv.url(),
        Arc::new(Holder::without_knobs()),
        Arc::new(FakeSampler::default()),
        Duration::from_millis(10),
        Duration::from_secs(1),
        Arc::new(|_| {}),
    );
    tokio::time::sleep(Duration::from_millis(60)).await;
    cancel.cancel();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let stopped = hits.load(Ordering::SeqCst);
    assert!(stopped > 0, "the poll never ran");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        stopped,
        "poll continued after cancellation"
    );
}

/// A spawn fetch that fails after an earlier success reports fetched=true
/// and keeps the last applied value; a failure before any success reports
/// fetched=false.
#[tokio::test]
async fn subscribe_spawn_reports_prior_success() {
    let fail = Arc::new(AtomicBool::new(false));
    let f = fail.clone();
    let srv = HttpServer::start(move |_| {
        if f.load(Ordering::SeqCst) {
            Resp::status(503, "busy")
        } else {
            Resp::ok(r#"{"trace_probability": 0.25}"#)
        }
    })
    .await;
    let calls: Arc<Mutex<Vec<(String, bool)>>> = Arc::default();
    let c = calls.clone();
    let holder = Arc::new(Holder::without_knobs());
    let events = Events::new();
    subscribe_spawn(
        &events,
        srv.url(),
        holder.clone(),
        Arc::new(FakeSampler::default()),
        Duration::from_secs(1),
        Arc::new(move |e, fetched| c.lock().push((e, fetched))),
    );

    fail.store(true, Ordering::SeqCst);
    events.publish_spawn(1, 1.0).await;
    {
        let c = calls.lock();
        assert_eq!(c.len(), 1, "{c:?}");
        assert!(!c[0].1, "fetched must be false before any success");
    }

    fail.store(false, Ordering::SeqCst);
    events.publish_spawn(2, 1.0).await;
    assert_eq!(calls.lock().len(), 1, "a successful fetch invoked on_error");
    assert_eq!(holder.common().trace_probability, 0.25);

    fail.store(true, Ordering::SeqCst);
    events.publish_spawn(3, 1.0).await;
    {
        let c = calls.lock();
        assert_eq!(c.len(), 2, "{c:?}");
        assert!(c[1].1, "fetched must be true after a success");
    }
    assert_eq!(
        holder.common().trace_probability,
        0.25,
        "a failed fetch changed the holder"
    );
}

/// An interval of zero starts no loop.
#[tokio::test]
async fn start_poll_zero_interval() {
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let srv = HttpServer::start(move |_| {
        h.fetch_add(1, Ordering::SeqCst);
        Resp::ok("{}")
    })
    .await;
    start_poll(
        CancellationToken::new(),
        srv.url(),
        Arc::new(Holder::without_knobs()),
        Arc::new(FakeSampler::default()),
        Duration::ZERO,
        Duration::from_secs(1),
        Arc::new(|_| {}),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}
