use std::sync::atomic::AtomicBool as Flag;

use serde_json::json;
use tokio::sync::{mpsc, watch};
use tonic::Status;

use super::super::chunks::Chunk;
use super::super::session::Session;
use super::super::test_support::*;
use super::super::{CYCLE_CEL, RESUME_TO_FIRST_EXEC, TASK_CEL, TASK_WALL_CLOCK};
use super::*;
use crate::loadgen::duration::Seconds;
use crate::loadgen::dynconfig::WaitTime;

const TASK_RECORD: &str = "sweperf task record";
const COMPLETED_CYCLE: &str = "Completed sweperf cycle";
const SHUTDOWN_STARTED: &str = "sweperf shutdown started";
const STARTUP_FAILED: &str = "sweperf on_start failed; the user will retry next iteration";

fn rt(h: &Harness) -> Arc<SweperfRuntime> {
    Arc::new(SweperfRuntime::new(h.cfg.clone(), &h.class))
}

fn session(h: &Harness, actor: &str, chunks: Vec<Chunk>) -> Arc<Session> {
    Arc::new(Session::new(
        h.cfg.clone(),
        actor,
        DEFAULT_TEMPLATE,
        &h.class,
        0,
        chunks,
    ))
}

fn wait(min_ms: i64, max_ms: i64) -> WaitTime {
    WaitTime {
        min_wait: Seconds::from_millis(min_ms),
        max_wait: Seconds::from_millis(max_ms),
    }
}

#[tokio::test]
async fn resolve_config() {
    let h = harness(not_found(), Knobs::default(), "SWRResolve").await;
    assert_eq!(
        rt(&h).resolve_config(),
        (
            DEFAULT_TEMPLATE.to_string(),
            DEFAULT_TOTAL_STEPS,
            DEFAULT_NUM_CYCLES
        )
    );
    let k = Knobs {
        template: "custom-template".into(),
        total_steps: 50,
        num_cycles: 5,
        ..Default::default()
    };
    let h = harness(not_found(), k, "SWRResolve").await;
    assert_eq!(
        rt(&h).resolve_config(),
        ("custom-template".to_string(), 50, 5)
    );
}

#[tokio::test]
async fn dynamic_wait() {
    let h = harness(
        not_found(),
        Knobs {
            wait: wait(100, 50),
            ..Default::default()
        },
        "SWRWait",
    )
    .await;
    assert_eq!(
        rt(&h).dynamic_wait(),
        Duration::from_millis(100),
        "an inverted window yields its lower bound"
    );
    let h = harness(
        not_found(),
        Knobs {
            wait: wait(10, 50),
            ..Default::default()
        },
        "SWRWait",
    )
    .await;
    let got = rt(&h).dynamic_wait();
    assert!(
        got >= Duration::from_millis(10) && got <= Duration::from_millis(50),
        "{got:?}"
    );
}

#[tokio::test]
async fn hive_created_once_per_worker() {
    let h = harness(async_job_handler(), Knobs::default(), "SWRHiveOnce").await;
    let r = rt(&h);
    for vu in 0..2 {
        r.start_user(&Ctx::background(), vu).await.unwrap();
    }
    assert_eq!(
        h.fake.count("CreateHive"),
        1,
        "one CreateHive for two actors"
    );
    // A missing Hive comes back from CreateActor as FAILED_PRECONDITION;
    // the next start recreates it.
    h.fake.with(|s| {
        s.create_actor_err = Some(Status::failed_precondition("Hive benchmark-test not found"))
    });
    assert!(r.start_user(&Ctx::background(), 2).await.is_err());
    h.fake.with(|s| s.create_actor_err = None);
    r.start_user(&Ctx::background(), 3).await.unwrap();
    assert_eq!(h.fake.count("CreateHive"), 2);
}

#[tokio::test]
async fn startup_failure_logs_user_actor_and_template() {
    let (logs, _g) = capture_logs();
    let h = harness(async_job_handler(), Knobs::default(), "SWRStartLog").await;
    h.fake
        .with(|s| s.create_actor_err = Some(Status::resource_exhausted("quota exceeded")));
    let r = rt(&h);
    assert!(r.start_user(&Ctx::background(), 7).await.is_err());
    let failure = logs.only(STARTUP_FAILED);
    assert!(
        failure["actor"].as_str().unwrap().starts_with("sb-"),
        "{failure:?}"
    );
    assert_eq!(failure["template"], json!(DEFAULT_TEMPLATE));
    assert_eq!(failure["user"], json!(7.0));
}

#[tokio::test]
async fn init_and_task_fn() {
    let h = harness(
        sync_handler(|r| match r.path.as_str() {
            "/status" => Resp::json(json!({"status": "up"})),
            _ => Resp::json(json!({"job_id": "", "status": "COMPLETED", "exit_code": 0})),
        }),
        Knobs {
            wait: wait(1, 2),
            ..Default::default()
        },
        "SWRInit",
    )
    .await;
    let entry = super::super::entries().remove(0);
    assert_eq!(
        (entry.name, entry.locust_file, entry.user_class),
        ("sweperf", "sweperf.py", super::super::USER_CLASS)
    );
    let task = (entry.init)(h.cfg.clone());
    task.iterate(0).await;
    task.shutdown().await;
}

#[tokio::test]
async fn noop_first_resume_not_recorded() {
    let class = "SWRNoop";
    let h = harness(async_job_handler(), Knobs::default(), class).await;
    h.fake.with(|s| s.resume_elapsed_us = Some("5000".into()));
    let r = rt(&h);
    let u = r.start_user(&Ctx::background(), 0).await.unwrap();
    // No extra lifecycle calls at setup.
    assert_eq!(h.fake.calls(), vec!["CreateHive", "CreateActor"]);
    let mut p = u.progress.lock().await;
    p.chunks = vec![Chunk::new(0, 5), Chunk::new(5, 10)];
    let rows = ["ResumeActor", "ResumeActor_rtt", RESUME_TO_FIRST_EXEC];
    let before = read_stats(class);
    u.step(&Ctx::background(), &mut p).await;
    for name in rows {
        assert_eq!(
            recorded_since(&before, class, name, "success").count,
            0.0,
            "cycle 1: {name}"
        );
    }
    assert_eq!(
        recorded_since(&before, class, CYCLE_CEL, "success").count,
        1.0
    );
    let before = read_stats(class);
    u.step(&Ctx::background(), &mut p).await;
    for name in rows {
        assert_eq!(
            recorded_since(&before, class, name, "success").count,
            1.0,
            "cycle 2: {name}"
        );
    }
}

#[tokio::test]
async fn startup_failure_deletes_without_suspend() {
    let h = harness(
        sync_handler(|_| Resp::status(500, "server not ready")),
        Knobs::default(),
        "SWRStartFail",
    )
    .await;
    let r = rt(&h);
    assert!(r
        .start_user(
            &Ctx::background().with_timeout(Duration::from_millis(100)),
            0
        )
        .await
        .is_err());
    assert_eq!(
        h.fake.calls(),
        vec!["CreateHive", "CreateActor", "DeleteActor"]
    );
}

#[tokio::test]
async fn shutdown_deletes_without_suspend() {
    let h = harness(not_found(), Knobs::default(), "SWRShutdownNoSuspend").await;
    let r = rt(&h);
    r.bind(1, session(&h, "swe-actor", vec![]));
    r.shutdown_within(None).await;
    assert_eq!(h.fake.calls(), vec!["DeleteActor"]);
}

#[tokio::test]
async fn task_cel_only_on_full_trajectory() {
    let class = "SWRTaskCel";
    let cycle = Arc::new(AtomicUsizeLike::default());
    let c = cycle.clone();
    let h = harness(
        sync_handler(move |r| match r.path.as_str() {
            "/execute" => {
                let n = c.next();
                Resp::text(&format!(r#"{{"job_id":"job-{n}","status":"ACCEPTED"}}"#))
            }
            "/status" => {
                let id = r.query.get("job_id").cloned().unwrap_or_default();
                let ms = if id == "job-1" { 1200 } else { 1800 };
                Resp::text(&format!(r#"{{"job_id":"{id}","status":"COMPLETED","exit_code":0,"execution_duration_ms":{ms}}}"#))
            }
            _ => Resp::status(404, ""),
        }),
        Knobs::default(),
        class,
    )
    .await;
    let r = rt(&h);
    let u = session(&h, "act", vec![Chunk::new(0, 5), Chunk::new(5, 10)]);
    r.bind(0, u.clone());
    let before = read_stats(class);

    r.iterate_user(0).await;
    assert_eq!(
        u.progress.lock().await.loop_cel,
        Duration::from_millis(1200)
    );
    assert_eq!(
        recorded_since(&before, class, TASK_CEL, "success").count,
        0.0,
        "no TaskCEL mid-trajectory"
    );

    r.iterate_user(0).await;
    let cel = recorded_since(&before, class, CYCLE_CEL, "success");
    assert_eq!((cel.count, cel.sum_ms), (2.0, 3000.0));
    let task = recorded_since(&before, class, TASK_CEL, "success");
    assert_eq!((task.count, task.sum_ms), (1.0, 3000.0));
    assert_eq!(
        recorded_since(&before, class, TASK_WALL_CLOCK, "success").count,
        1.0
    );
    let p = u.progress.lock().await;
    assert!(
        p.loop_cel.is_zero() && p.loop_wall.is_zero() && !p.loop_failed,
        "loop counters rearmed"
    );
}

/// A counter handing out 1, 2, 3, ...
#[derive(Default)]
struct AtomicUsizeLike(std::sync::atomic::AtomicUsize);

impl AtomicUsizeLike {
    fn next(&self) -> usize {
        self.0.fetch_add(1, Ordering::SeqCst) + 1
    }
}

#[tokio::test]
async fn finished_trajectory_deletes_actor_and_starts_fresh() {
    let h = harness(async_job_handler(), Knobs::default(), "SWRFresh").await;
    let r = rt(&h);
    let u = session(&h, "act-1", vec![Chunk::new(0, 5)]);
    r.bind(0, u.clone());
    // The single cycle finishes the trajectory, so the Actor must go.
    r.iterate_user(0).await;
    assert!(
        u.cleaned_up.load(Ordering::SeqCst),
        "the actor was not deleted"
    );
    // The final cycle is not suspended.
    assert_eq!(h.fake.calls(), vec!["ResumeActor", "DeleteActor"]);
    assert!(
        r.session(0).is_none(),
        "the finished session is still bound"
    );

    let before = h.fake.calls().len();
    r.iterate_user(0).await;
    let next = r
        .session(0)
        .expect("a fresh session after the next iterate");
    assert!(
        !Arc::ptr_eq(&next, &u) && next.actor_name != u.actor_name,
        "the next session reuses the actor"
    );
    let new_calls = h.fake.calls()[before..].to_vec();
    assert!(
        new_calls.len() >= 2 && new_calls[0] == "CreateHive" && new_calls[1] == "CreateActor",
        "{new_calls:?}"
    );
    r.shutdown_within(None).await;
}

#[tokio::test]
async fn retire_retries_failed_delete_once() {
    let h = harness(async_job_handler(), Knobs::default(), "SWRRetireRetry").await;
    h.fake
        .with(|s| s.delete_err = Some(Status::unavailable("unavailable")));
    let r = rt(&h);
    r.bind(0, session(&h, "act-1", vec![Chunk::new(0, 5)]));
    r.iterate_user(0).await;
    // One retry, then the Actor is given up on.
    assert_eq!(
        h.fake.calls(),
        vec!["ResumeActor", "DeleteActor", "DeleteActor"]
    );
}

#[tokio::test]
async fn retire_retry_gets_fresh_teardown_ctx() {
    let class = "SWRRetireFresh";
    let h = harness(async_job_handler(), Knobs::default(), class).await;
    // The first DeleteActor outlasts its teardown context; the retry is quick.
    let first = Arc::new(Flag::new(true));
    h.fake.with(|s| {
        s.before_delete = Some(hook(move || {
            let slow = first.swap(false, Ordering::SeqCst);
            async move {
                if slow {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }))
    });
    let mut r = SweperfRuntime::new(h.cfg.clone(), class);
    r.teardown_timeout = Duration::from_millis(50);
    let r = Arc::new(r);
    r.bind(0, session(&h, "act-1", vec![Chunk::new(0, 5)]));
    let before = read_stats(class);
    r.iterate_user(0).await;
    assert_eq!(
        recorded_since(&before, class, "DeleteActor", "failure").count,
        1.0,
        "the timed-out first attempt"
    );
    assert_eq!(
        recorded_since(&before, class, "DeleteActor", "success").count,
        1.0,
        "the retry on a fresh context"
    );
}

#[tokio::test]
async fn only_final_cycle_skips_suspend() {
    let h = harness(async_job_handler(), Knobs::default(), "SWRFinalSkip").await;
    let r = rt(&h);
    r.bind(
        0,
        session(&h, "act-1", vec![Chunk::new(0, 2), Chunk::new(2, 5)]),
    );
    r.iterate_user(0).await;
    r.iterate_user(0).await;
    assert_eq!(
        h.fake.calls(),
        vec!["ResumeActor", "SuspendActor", "ResumeActor", "DeleteActor"]
    );
}

#[tokio::test]
async fn shutdown_and_retire_delete_once() {
    let h = harness(async_job_handler(), Knobs::default(), "SWRDeleteOnce").await;
    let r = rt(&h);
    let u = session(&h, "act-1", vec![Chunk::new(0, 5)]);
    r.bind(0, u.clone());
    // Shutdown claims the Actor first; a late retire must not delete again.
    r.shutdown_within(None).await;
    r.retire_user(0, &u).await;
    assert_eq!(h.fake.count("DeleteActor"), 1);
}

#[tokio::test]
async fn poll_liveness_failure_decrements_users_once() {
    let class = "SWRGauge";
    let h = harness(
        sync_handler(|_| Resp::status(500, "server not ready")),
        Knobs::default(),
        class,
    )
    .await;
    let r = rt(&h);
    let before = users_gauge(class);
    assert!(r
        .start_user(
            &Ctx::background().with_timeout(Duration::from_millis(100)),
            0
        )
        .await
        .is_err());
    assert_eq!(
        users_gauge(class),
        before,
        "a failed start leaves the gauge unchanged"
    );
}

#[tokio::test]
async fn no_new_actor_after_shutdown() {
    let h = harness(async_job_handler(), Knobs::default(), "SWRNoNew").await;
    let r = rt(&h);
    r.shutdown_within(None).await;
    r.iterate_user(0).await;
    assert!(
        h.fake.calls().is_empty(),
        "iterate after shutdown sent {:?}",
        h.fake.calls()
    );
    assert!(r.session(0).is_none());
}

/// A handler that signals `entered` on its first request and then never
/// answers.
fn hanging(paths: &'static [&'static str]) -> (Handler, mpsc::UnboundedReceiver<()>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let once = Arc::new(Flag::new(false));
    let handler: Handler = Arc::new(move |r: Req| {
        let (tx, once) = (tx.clone(), once.clone());
        Box::pin(async move {
            let hang = paths.is_empty()
                || paths.iter().any(|p| match p.split_once('?') {
                    Some((path, _)) => r.path == path && r.query.contains_key("job_id"),
                    None => r.path == *p,
                });
            if !hang {
                return match r.path.as_str() {
                    "/execute" => Resp::json(json!({"job_id": "job-1", "status": "ACCEPTED"})),
                    _ => Resp::status(404, ""),
                };
            }
            if !once.swap(true, Ordering::SeqCst) {
                let _ = tx.send(());
            }
            std::future::pending::<Resp>().await
        })
    });
    (handler, rx)
}

#[tokio::test]
async fn shutdown_cancels_slow_startup() {
    let class = "SWRSlowStart";
    // The sandbox never comes up.
    let (handler, mut entered) = hanging(&[]);
    let h = harness(handler, Knobs::default(), class).await;
    let r = rt(&h);
    let before = users_gauge(class);
    let it = {
        let r = r.clone();
        tokio::spawn(async move { r.iterate_user(0).await })
    };
    entered.recv().await.unwrap();
    let start = Instant::now();
    r.shutdown_within(Some(Duration::from_secs(5))).await;
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "the liveness poll was not canceled: {:?}",
        start.elapsed()
    );
    it.await.unwrap();
    assert_eq!(
        h.fake.calls(),
        vec!["CreateHive", "CreateActor", "DeleteActor"],
        "the starting actor is deleted"
    );
    assert_eq!(users_gauge(class), before);
}

#[tokio::test]
async fn shutdown_mid_cycle_records_no_failures() {
    let class = "SWRMidCycle";
    // The job never finishes.
    let (handler, mut entered) = hanging(&["/status?job_id"]);
    let h = harness(
        handler,
        Knobs {
            poll_interval_ms: 10,
            ..Default::default()
        },
        class,
    )
    .await;
    let r = rt(&h);
    // The last cycle, so an interrupted task would also emit task rows.
    r.bind(0, session(&h, "act-1", vec![Chunk::new(0, 5)]));
    let before = read_stats(class);
    let it = {
        let r = r.clone();
        tokio::spawn(async move { r.iterate_user(0).await })
    };
    entered.recv().await.unwrap();
    r.shutdown_within(Some(Duration::from_secs(5))).await;
    for name in [
        "ResumeActor",
        "ResumeActor_rtt",
        RESUME_TO_FIRST_EXEC,
        "Workload_Cycle_1",
        CYCLE_CEL,
        TASK_CEL,
        TASK_WALL_CLOCK,
        "SuspendActor",
        "DeleteActor",
    ] {
        assert_eq!(
            recorded_since(&before, class, name, "failure").count,
            0.0,
            "{name}"
        );
    }
    assert_eq!(
        h.fake.calls(),
        vec!["ResumeActor", "DeleteActor"],
        "the interrupted actor is deleted by the sweep"
    );
    it.await.unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_retire() {
    let h = harness(async_job_handler(), Knobs::default(), "SWRWaitRetire").await;
    let (etx, mut entered) = mpsc::unbounded_channel::<()>();
    let (otx, orx) = watch::channel(false);
    h.fake.with(|s| {
        s.before_delete = Some(hook(move || {
            let (etx, mut orx) = (etx.clone(), orx.clone());
            async move {
                let _ = etx.send(());
                let _ = orx.wait_for(|o| *o).await;
            }
        }))
    });
    let r = rt(&h);
    r.bind(0, session(&h, "act-1", vec![Chunk::new(0, 5)]));
    let it = {
        let r = r.clone();
        tokio::spawn(async move { r.iterate_user(0).await })
    };
    entered.recv().await.unwrap(); // the retire is inside DeleteActor
    let shutdown = {
        let r = r.clone();
        tokio::spawn(async move { r.shutdown_within(None).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!shutdown.is_finished(), "shutdown returned mid-DeleteActor");
    otx.send(true).unwrap();
    shutdown.await.unwrap();
    it.await.unwrap();
    assert_eq!(h.fake.count("DeleteActor"), 1);
}

#[tokio::test]
async fn shutdown_sweeps_concurrently() {
    let h = harness(not_found(), Knobs::default(), "SWRSweep").await;
    h.fake
        .with(|s| s.before_delete = Some(hook(|| tokio::time::sleep(Duration::from_millis(50)))));
    let r = rt(&h);
    const N: u64 = 20;
    for i in 0..N {
        r.bind(i, session(&h, &format!("act-{i}"), vec![]));
    }
    let start = Instant::now();
    r.shutdown_within(None).await;
    // A serial sweep would take 20 * 50ms = 1s.
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "the sweep is not concurrent: {:?}",
        start.elapsed()
    );
    assert_eq!(h.fake.count("DeleteActor"), N as usize);
}

#[tokio::test]
async fn shutdown_sweeps_after_inflight_wait_cap() {
    let h = harness(not_found(), Knobs::default(), "SWRInflightCap").await;
    let mut r = SweperfRuntime::new(h.cfg.clone(), &h.class);
    r.inflight_wait_timeout = Duration::from_millis(50);
    r.bind(1, session(&h, "act-1", vec![]));
    // An iteration that never finishes.
    let _stuck = r.enter().unwrap();
    let start = Instant::now();
    r.shutdown_within(Some(Duration::from_secs(5))).await;
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "the in-flight wait used the whole budget"
    );
    assert_eq!(
        h.fake.calls(),
        vec!["DeleteActor"],
        "the bound actor is swept despite the stuck iteration"
    );
}

#[tokio::test]
async fn task_record_for_clean_task() {
    let (logs, _g) = capture_logs();
    let k = Knobs {
        wait: wait(3, 3),
        total_steps: 10,
        num_cycles: 2,
        ..Default::default()
    };
    let h = harness(async_job_handler(), k, "SWRRecordClean").await;
    h.fake.with(|s| s.resume_elapsed_us = Some("5000".into()));
    let r = rt(&h);
    r.iterate_user(0).await; // starts the Actor and runs cycle 1 (no-op resume)
    r.iterate_user(0).await; // cycle 2, the last, ends the task
    let cycles = logs.with_msg(COMPLETED_CYCLE);
    assert_eq!(cycles.len(), 2);
    let (first, last) = (&cycles[0], &cycles[1]);
    assert!(
        first.get("resume_ms").is_none() && first.get("ttfe_ms").is_none(),
        "a no-op resume reports neither"
    );
    assert_eq!(
        (first["cel_ms"].clone(), first["ok"].clone()),
        (json!(1200.0), json!(true))
    );
    // The last cycle is not suspended, and is still ok.
    assert_eq!(last["ok"], json!(true));
    assert_eq!(last["resume_ms"], json!(5.0));
    assert!(last.get("ttfe_ms").is_some());

    let rec = logs.only(TASK_RECORD);
    for (k, v) in [
        ("v", json!(1.0)),
        ("user", json!(0.0)),
        ("template", json!(DEFAULT_TEMPLATE)),
        ("cycles", json!(2.0)),
        ("steps", json!(10.0)),
        ("cel_ms", json!(2400.0)),
        ("think_ms", json!(3.0)),
        ("resume_max_ms", json!(5.0)),
        ("retries", json!(0.0)),
        ("failed", json!(false)),
        ("fail_step", json!("")),
        ("fail_cycle", json!(0.0)),
    ] {
        assert_eq!(rec[k], v, "record {k}");
    }
    assert!(rec["actor"].as_str().unwrap().starts_with("sb-"));
    assert!(rec["startup_ms"].as_f64().unwrap() > 0.0);
    assert!(rec.get("ttfe_max_ms").is_some());
    let sum = first["wall_ms"].as_f64().unwrap() + last["wall_ms"].as_f64().unwrap();
    assert!(
        rec["wall_ms"].as_f64().unwrap() >= sum,
        "wall_ms covers the cycles"
    );
    let start = chrono::DateTime::parse_from_rfc3339(rec["task_start"].as_str().unwrap()).unwrap();
    let end = chrono::DateTime::parse_from_rfc3339(rec["end"].as_str().unwrap()).unwrap();
    assert!(end > start);
    drop(_g);
    r.shutdown_within(None).await;
}

#[tokio::test]
async fn task_record_resume_failure_and_retry() {
    let (logs, _g) = capture_logs();
    let h = harness(
        async_job_handler(),
        Knobs {
            wait: wait(4, 4),
            ..Default::default()
        },
        "SWRRecordRetry",
    )
    .await;
    let r = rt(&h);
    r.bind(
        0,
        session(&h, "act", vec![Chunk::new(0, 5), Chunk::new(5, 10)]),
    );
    h.fake
        .with(|s| s.resume_err = Some(Status::unknown("resume failed")));
    r.iterate_user(0).await; // cycle 1 fails at resume
    h.fake.with(|s| s.resume_err = None);
    r.iterate_user(0).await; // cycle 1 retried
    r.iterate_user(0).await; // cycle 2 ends the task
    let rec = logs.only(TASK_RECORD);
    // The wait after the failed resume is a retry, not think time.
    for (k, v) in [
        ("retries", json!(1.0)),
        ("failed", json!(true)),
        ("fail_step", json!("resume")),
        ("fail_cycle", json!(1.0)),
        ("think_ms", json!(4.0)),
        ("cycles", json!(2.0)),
        ("steps", json!(10.0)),
    ] {
        assert_eq!(rec[k], v, "record {k}");
    }
    // The failed attempt has no cycle line, but its wall time counts.
    let mut sum = logs.only("ResumeActor failed in step")["wall_ms"]
        .as_f64()
        .unwrap();
    for c in logs.with_msg(COMPLETED_CYCLE) {
        sum += c["wall_ms"].as_f64().unwrap();
    }
    assert!(rec["wall_ms"].as_f64().unwrap() >= sum);
}

#[tokio::test]
async fn task_record_counts_follow_config_change() {
    let (logs, _g) = capture_logs();
    let h = harness(
        async_job_handler(),
        Knobs {
            total_steps: 4,
            num_cycles: 2,
            ..Default::default()
        },
        "SWRRecordCfg",
    )
    .await;
    let r = rt(&h);
    r.iterate_user(0).await; // task 1, cycle 1
                             // A change mid-task applies from the next task on.
    h.cfg
        .dyn_config
        .apply(br#"{"sweperf_total_steps": 3, "sweperf_num_cycles": 5}"#)
        .unwrap();
    r.iterate_user(0).await; // task 1, cycle 2, the last
    for _ in 0..3 {
        r.iterate_user(0).await; // task 2: five cycles over three steps run as three
    }
    let recs = logs.with_msg(TASK_RECORD);
    assert_eq!(recs.len(), 2);
    for (i, (cycles, steps)) in [(2.0, 4.0), (3.0, 3.0)].into_iter().enumerate() {
        assert_eq!(
            (recs[i]["cycles"].clone(), recs[i]["steps"].clone()),
            (json!(cycles), json!(steps)),
            "record {}",
            i + 1
        );
    }
    drop(_g);
    r.shutdown_within(None).await;
}

#[tokio::test]
async fn shutdown_cuts_off_in_flight_cycle() {
    for (name, chunks) in [
        ("last cycle", vec![Chunk::new(0, 5)]),
        ("earlier cycle", vec![Chunk::new(0, 5), Chunk::new(5, 10)]),
    ] {
        let (logs, _g) = capture_logs();
        // The chunk is still running when shutdown begins.
        let (handler, mut entered) = hanging(&["/execute"]);
        let h = harness(handler, Knobs::default(), "SWRCutOff").await;
        let r = rt(&h);
        r.bind(0, session(&h, "act", chunks));
        let it = {
            let r = r.clone();
            tokio::spawn(async move { r.iterate_user(0).await })
        };
        entered.recv().await.unwrap();
        r.shutdown_within(None).await;
        it.await.unwrap();
        logs.only(SHUTDOWN_STARTED);
        assert!(
            logs.with_msg(COMPLETED_CYCLE).is_empty(),
            "{name}: a canceled cycle logged completion"
        );
        assert!(
            logs.with_msg(TASK_RECORD).is_empty(),
            "{name}: a canceled task logged a record"
        );
    }
}
