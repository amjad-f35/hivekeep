use std::sync::atomic::AtomicUsize;

use serde_json::json;

use super::super::chunks::Chunk;
use super::super::test_support::*;
use super::*;
use crate::loadgen::dynconfig::{Lifecycle, LIFECYCLE_MODE_ASYNC};

fn session(h: &Harness, actor: &str, chunks: Vec<Chunk>) -> Arc<Session> {
    Arc::new(Session::new(
        h.cfg.clone(),
        actor,
        super::super::DEFAULT_TEMPLATE,
        &h.class,
        0,
        chunks,
    ))
}

fn bg() -> Ctx {
    Ctx::background()
}

#[tokio::test]
async fn actor_routing_header() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s = seen.clone();
    let h = harness(
        sync_handler(move |r| {
            s.lock().push(
                r.headers
                    .get(TARGET_ACTOR_HEADER)
                    .map(|v| v.to_str().unwrap().to_string()),
            );
            if r.path == "/status" {
                Resp::json(json!({"status": "up"}))
            } else {
                Resp::json(json!({"status": "COMPLETED", "exit_code": 0}))
            }
        }),
        Knobs::default(),
        "SWRouting",
    )
    .await;
    let u = session(&h, "test-actor", vec![]);
    u.poll_liveness(&bg()).await.unwrap();
    u.execute(&bg(), 1, 0, 5).await.2.unwrap();
    let seen = seen.lock();
    assert!(!seen.is_empty(), "no requests reached the router");
    for (i, got) in seen.iter().enumerate() {
        assert_eq!(
            got.as_deref(),
            Some("benchmark-test/test-actor"),
            "request {i}"
        );
    }
}

#[tokio::test]
async fn poll_interval_table() {
    for (ms, want) in [
        (0, DEFAULT_POLL_INTERVAL),
        (250, Duration::from_millis(250)),
    ] {
        let h = harness(
            not_found(),
            Knobs {
                poll_interval_ms: ms,
                ..Default::default()
            },
            "SWPoll",
        )
        .await;
        assert_eq!(poll_interval(&h.cfg), want, "{ms}");
    }
}

#[tokio::test]
async fn cycle_sequence() {
    let http_calls = Arc::new(Mutex::new(Vec::new()));
    let c = http_calls.clone();
    let h = harness(
        sync_handler(move |r| {
            c.lock().push(format!("{}?{}", r.path, r.raw_query));
            match (r.path.as_str(), r.query.get("job_id")) {
                ("/status", None) => Resp::json(json!({"status": "up"})),
                ("/status", Some(id)) => Resp::json(json!({"job_id": id, "status": "COMPLETED", "exit_code": 0})),
                ("/execute", _) => {
                    let req: ExecuteRequest = serde_json::from_slice(&r.body).unwrap();
                    Resp::json(json!({"job_id": format!("job-{}-{}", req.start_step, req.end_step), "status": "RUNNING"}))
                }
                _ => Resp::status(404, "not found"),
            }
        }),
        Knobs::default(),
        "SWSequence",
    )
    .await;
    let u = session(&h, "test-actor", vec![Chunk::new(0, 5), Chunk::new(5, 10)]);
    let mut p = u.progress.lock().await;
    u.step(&bg(), &mut p).await;
    assert_eq!(p.cycle_index, 1);
    assert_eq!(h.fake.calls(), vec!["ResumeActor", "SuspendActor"]);
    for req in &h.fake.s.lock().suspend_reqs {
        assert_eq!(
            req.options.as_ref().unwrap().mode(),
            api::DurabilityMode::Asynchronous
        );
    }
    assert_eq!(
        *http_calls.lock(),
        vec![
            "/execute?".to_string(),
            "/status?job_id=job-1-5".to_string()
        ]
    );
}

/// lifecycle_mode picks the durability of the suspend that ends a cycle.
#[tokio::test]
async fn suspend_lifecycle_mode() {
    for (mode, want, metric) in [
        ("", api::DurabilityMode::Asynchronous, "SuspendActorAsync"),
        (
            LIFECYCLE_MODE_SYNC,
            api::DurabilityMode::Synchronous,
            "SuspendActor",
        ),
        (
            LIFECYCLE_MODE_ASYNC,
            api::DurabilityMode::Asynchronous,
            "SuspendActorAsync",
        ),
    ] {
        let k = Knobs {
            lifecycle: Lifecycle {
                lifecycle_mode: mode.into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let class = format!("SWSuspendMode{mode}");
        let h = harness(not_found(), k, &class).await;
        let u = session(&h, "test-actor", vec![]);
        let before = read_stats(&class);
        assert!(u.suspend(&bg()).await, "mode={mode}");
        assert_eq!(
            recorded_since(&before, &class, metric, "success").count,
            1.0,
            "mode={mode}"
        );
        let reqs = h.fake.s.lock().suspend_reqs.clone();
        assert_eq!(reqs.len(), 1);
        assert_eq!(
            reqs[0].options.as_ref().unwrap().mode(),
            want,
            "mode={mode}"
        );
    }
}

#[tokio::test]
async fn ensure_hive_handling() {
    // AlreadyExists is success.
    let h = harness(not_found(), Knobs::default(), "SWHive").await;
    h.fake
        .with(|s| s.create_hive_err = Some(Status::already_exists("hive already exists")));
    session(&h, "act", vec![])
        .ensure_hive(&bg())
        .await
        .expect("AlreadyExists is success");
    // Any other error is returned.
    h.fake
        .with(|s| s.create_hive_err = Some(Status::internal("internal database error")));
    assert!(session(&h, "act", vec![]).ensure_hive(&bg()).await.is_err());
}

#[tokio::test]
async fn control_client_errors() {
    let h = harness(not_found(), Knobs::default(), "SWControlErrors").await;
    let u = session(&h, "act", vec![]);
    h.fake
        .with(|s| s.resume_err = Some(Status::unknown("resume RPC failed")));
    assert!(
        !u.resume(&bg(), false).await.1,
        "resume = true on RPC failure"
    );
    h.fake
        .with(|s| s.suspend_err = Some(Status::unknown("suspend failed")));
    assert!(!u.suspend(&bg()).await, "suspend = true on RPC failure");
    h.fake
        .with(|s| s.delete_err = Some(Status::unknown("delete failed")));
    assert!(
        u.delete(&bg()).await.is_err(),
        "the delete error is handed back"
    );
}

#[tokio::test]
async fn execute_sync_exit_code_failure() {
    let h = harness(
        sync_handler(|_| Resp::json(json!({"job_id": "", "status": "FAILED", "exit_code": 2, "stderr": "command not found"}))),
        Knobs::default(),
        "SWSyncExit",
    )
    .await;
    let err = session(&h, "act", vec![])
        .execute(&bg(), 1, 0, 5)
        .await
        .2
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("exit code 2, stderr: command not found"),
        "{err}"
    );
    assert!(matches!(err, ExecError::SyncExit(_)));
}

#[tokio::test]
async fn execute_http_status_error() {
    let h = harness(
        sync_handler(|_| Resp::status(500, "internal server error")),
        Knobs::default(),
        "SWHttp500",
    )
    .await;
    assert!(session(&h, "act", vec![])
        .execute(&bg(), 1, 0, 5)
        .await
        .2
        .is_err());
}

#[tokio::test]
async fn poll_liveness() {
    // Succeeds when the server is up.
    let h = harness(
        sync_handler(|_| Resp::json(json!({"status": "up"}))),
        Knobs::default(),
        "SWLive",
    )
    .await;
    session(&h, "act", vec![])
        .poll_liveness(&bg())
        .await
        .unwrap();

    // Fails when the context ends.
    let h = harness(
        sync_handler(|_| Resp::status(503, "")),
        Knobs::default(),
        "SWLive",
    )
    .await;
    assert!(session(&h, "act", vec![])
        .poll_liveness(&bg().with_timeout(Duration::from_millis(50)))
        .await
        .is_err());

    // Retries at the configured poll interval.
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let h = harness(
        sync_handler(move |_| {
            if c.fetch_add(1, Ordering::SeqCst) + 1 < 3 {
                Resp::status(503, "")
            } else {
                Resp::json(json!({"status": "up"}))
            }
        }),
        Knobs {
            poll_interval_ms: 10,
            ..Default::default()
        },
        "SWLive",
    )
    .await;
    let start = Instant::now();
    session(&h, "act", vec![])
        .poll_liveness(&bg())
        .await
        .unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "two retries at 10ms took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn poll_job_completion() {
    // A literal payload, so a wrong field name fails here.
    let h = harness(
        sync_handler(|_| {
            Resp::text(r#"{"job_id":"job-1","status":"COMPLETED","exit_code":0,"completed_step":5,"execution_duration_ms":1234.5}"#)
        }),
        Knobs::default(),
        "SWJob",
    )
    .await;
    let got = session(&h, "act", vec![])
        .poll_job_completion(&bg(), "job-1", 1)
        .await
        .unwrap();
    assert_eq!(got, Duration::from_micros(1_234_500));

    let h = harness(
        sync_handler(|_| Resp::json(json!({"job_id": "job-1", "status": "FAILED", "exit_code": 1, "error": "step execution failed"}))),
        Knobs::default(),
        "SWJob",
    )
    .await;
    assert!(session(&h, "act", vec![])
        .poll_job_completion(&bg(), "job-1", 1)
        .await
        .is_err());
}

#[test]
fn user_is_done() {
    let mut p = Progress::new(vec![Chunk::new(0, 5), Chunk::new(5, 10)]);
    assert!(!p.is_done());
    p.cycle_index = 2;
    assert!(p.is_done());
}

#[tokio::test]
async fn noop_first_resume_failure_still_recorded() {
    let class = "SWNoopFail";
    let h = harness(async_job_handler(), Knobs::default(), class).await;
    h.fake
        .with(|s| s.resume_err = Some(Status::unknown("resume failed")));
    let u = session(&h, "act", vec![Chunk::new(0, 5)]);
    let mut p = u.progress.lock().await;
    p.awake = true;
    let before = read_stats(class);
    u.step(&bg(), &mut p).await;
    for name in ["ResumeActor", RESUME_TO_FIRST_EXEC] {
        assert_eq!(
            recorded_since(&before, class, name, "failure").count,
            1.0,
            "{name}"
        );
    }
}

#[tokio::test]
async fn traced_call_records_both_latencies() {
    let class = "SWBothLatencies";
    let h = harness(not_found(), Knobs::default(), class).await;
    // 30ms on the wire, 5ms reported by the server.
    h.fake.with(|s| {
        s.resume_delay = Duration::from_millis(30);
        s.resume_elapsed_us = Some("5000".into());
    });
    let u = session(&h, "act", vec![]);
    let before = read_stats(class);
    assert!(u.resume(&bg(), false).await.1);
    let server = recorded_since(&before, class, "ResumeActor", "success");
    let client = recorded_since(&before, class, "ResumeActor_rtt", "success");
    assert_eq!((server.count, client.count), (1.0, 1.0));
    assert_eq!(server.sum_ms, 5.0, "the trailer's 5ms");
    assert!(
        client.sum_ms >= 30.0,
        "rtt = {}ms, want at least 30",
        client.sum_ms
    );
}

#[tokio::test]
async fn traced_call_skips_rtt_row_without_trailer() {
    let class = "SWNoRtt";
    let h = harness(not_found(), Knobs::default(), class).await;
    let u = session(&h, "act", vec![]);
    let before = read_stats(class);
    assert!(u.resume(&bg(), false).await.1);
    // Without a trailer both figures are the client's.
    assert_eq!(
        recorded_since(&before, class, "ResumeActor_rtt", "success").count,
        0.0
    );
}

#[tokio::test]
async fn resume_to_first_exec_stops_before_job_polling() {
    let class = "SWTtfe";
    let poll_delay = Duration::from_millis(60);
    let handler: Handler = Arc::new(move |r: Req| {
        Box::pin(async move {
            match (r.path.as_str(), r.query.get("job_id")) {
                ("/execute", _) => Resp::json(json!({"job_id": "job-1", "status": "ACCEPTED"})),
                ("/status", Some(_)) => {
                    tokio::time::sleep(poll_delay).await;
                    Resp::json(json!({"job_id": "job-1", "status": "COMPLETED", "exit_code": 0}))
                }
                _ => Resp::status(404, ""),
            }
        })
    });
    let h = harness(handler, Knobs::default(), class).await;
    let u = session(&h, "act", vec![Chunk::new(0, 5)]);
    let before = read_stats(class);
    u.step(&bg(), &mut *u.progress.lock().await).await;
    let ttfe = recorded_since(&before, class, RESUME_TO_FIRST_EXEC, "success");
    let cycle = recorded_since(&before, class, "Workload_Cycle_1", "success");
    assert_eq!(ttfe.count, 1.0);
    assert!(
        cycle.sum_ms >= poll_delay.as_millis() as f64,
        "cycle = {}ms",
        cycle.sum_ms
    );
    assert!(
        ttfe.sum_ms < cycle.sum_ms,
        "ResumeToFirstExec {} must be well under the cycle {}",
        ttfe.sum_ms,
        cycle.sum_ms
    );
}

#[tokio::test]
async fn cycle_cel_uses_container_duration_not_wall_clock() {
    let class = "SWCel";
    // Three polls, so the wall clock dwarfs the reported 10ms.
    let polls = Arc::new(AtomicUsize::new(0));
    let p2 = polls.clone();
    let h = harness(
        sync_handler(move |r| match r.path.as_str() {
            "/execute" => Resp::text(r#"{"job_id":"job-1","status":"ACCEPTED"}"#),
            "/status" => {
                if p2.fetch_add(1, Ordering::SeqCst) < 2 {
                    Resp::text(r#"{"job_id":"job-1","status":"RUNNING"}"#)
                } else {
                    Resp::text(r#"{"job_id":"job-1","status":"COMPLETED","exit_code":0,"execution_duration_ms":10}"#)
                }
            }
            _ => Resp::status(404, ""),
        }),
        Knobs::default(),
        class,
    )
    .await;
    let u = session(&h, "act", vec![Chunk::new(0, 5)]);
    let before = read_stats(class);
    let mut p = u.progress.lock().await;
    u.step(&bg(), &mut p).await;
    assert_eq!(
        p.loop_cel,
        Duration::from_millis(10),
        "the reported time, not the client's"
    );
    let got = recorded_since(&before, class, CYCLE_CEL, "success");
    assert_eq!((got.count, got.sum_ms), (1.0, 10.0));
    assert!(
        p.loop_wall >= Duration::from_millis(75),
        "loop_wall = {:?}",
        p.loop_wall
    );
}

#[tokio::test]
async fn step_suspend_failure_fails_task() {
    let h = harness(async_job_handler(), Knobs::default(), "SWSuspendFail").await;
    h.fake
        .with(|s| s.suspend_err = Some(Status::unknown("suspend failed")));
    // Two cycles: the final one is never suspended.
    let u = session(&h, "act", vec![Chunk::new(0, 2), Chunk::new(2, 5)]);
    let mut p = u.progress.lock().await;
    u.step(&bg(), &mut p).await;
    assert!(p.loop_failed, "a failed suspend must fail the task");
    assert_eq!((p.fail_step.as_str(), p.fail_cycle), ("suspend", 1));
}

#[tokio::test]
async fn step_sync_exit_skips_resume_to_first_exec() {
    let class = "SWSyncSkip";
    let h = harness(
        sync_handler(|_| Resp::json(json!({"status": "FAILED", "exit_code": 2, "stderr": "boom"}))),
        Knobs::default(),
        class,
    )
    .await;
    let u = session(&h, "act", vec![Chunk::new(0, 5)]);
    let before = read_stats(class);
    let mut p = u.progress.lock().await;
    u.step(&bg(), &mut p).await;
    for status in ["success", "failure"] {
        assert_eq!(
            recorded_since(&before, class, RESUME_TO_FIRST_EXEC, status).count,
            0.0,
            "{status}"
        );
    }
    assert!(p.loop_failed);
}

#[tokio::test]
async fn teardown_claims_once() {
    let h = harness(not_found(), Knobs::default(), "SWTeardownOnce").await;
    let u = session(&h, "act", vec![]);
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let u = u.clone();
            tokio::spawn(async move { u.teardown(&Ctx::background()).await })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(h.fake.count("DeleteActor"), 1);
}

#[tokio::test]
async fn traced_call_skips_failure_when_canceled() {
    let class = "SWCanceled";
    let h = harness(not_found(), Knobs::default(), class).await;
    h.fake.with(|s| {
        s.resume_err = Some(Status::cancelled("context canceled"));
        s.resume_elapsed_us = Some("5000".into());
    });
    let u = session(&h, "act", vec![]);
    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();
    let before = read_stats(class);
    assert!(!u.resume(&Ctx::new(token), false).await.1);
    for name in ["ResumeActor", "ResumeActor_rtt"] {
        assert_eq!(
            recorded_since(&before, class, name, "failure").count,
            0.0,
            "{name}"
        );
    }
}

#[tokio::test]
async fn task_record_execute_failure_on_last_cycle() {
    let (logs, _g) = capture_logs();
    let h = harness(
        sync_handler(|_| Resp::status(500, "boom")),
        Knobs::default(),
        "SWLastExecFail",
    )
    .await;
    let u = session(&h, "act", vec![Chunk::new(0, 5)]);
    let mut p = u.progress.lock().await;
    u.step(&bg(), &mut p).await;
    u.record_task_metrics(&mut p);
    let cyc = logs.only("Completed sweperf cycle");
    assert_eq!(cyc["ok"], json!(false));
    assert_eq!(cyc["cel_ms"], json!(0.0));
    assert!(cyc.get("ttfe_ms").is_none());
    let rec = logs.only("sweperf task record");
    assert_eq!(rec["failed"], json!(true));
    assert_eq!(rec["fail_step"], json!("execute"));
    assert_eq!(rec["fail_cycle"], json!(1.0));
    assert!(rec.get("ttfe_max_ms").is_none());
}
