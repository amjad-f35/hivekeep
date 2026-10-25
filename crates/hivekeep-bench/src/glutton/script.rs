//! RunScript: a tree of blocks (steps in order, optionally looped for a
//! wall-clock budget) and operations (requests at the same time), run
//! in-process as one request. The response mirrors the tree with stats
//! summed at every node.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::time::Instant;
use tonic::Status;

use super::ctx::Ctx;
use super::service::Service;
use super::sizes::parse_bytes;
use crate::pb::glutton::v1 as pb;
use pb::request::Kind as Req;
use pb::step::Kind as StepKind;

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

impl Service {
    /// Checks the tree's shape, then runs it and holds the request until it
    /// finishes. A request's own argument errors surface when it runs, with
    /// its path in the tree and kind prepended and its code kept, so a
    /// caller can tell a bad script from a failed request.
    pub async fn run_script(
        &self,
        ctx: &Ctx,
        req: &pb::RunScriptRequest,
    ) -> Result<pb::RunScriptResponse, Status> {
        let script = req
            .script
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("script is required"))?;
        validate_block(script, "script")?;
        let (result, err) = self
            .run_block(ctx, script, "script".to_string(), None)
            .await;
        match err {
            Some(e) => Err(e),
            None => Ok(pb::RunScriptResponse {
                result: Some(result),
            }),
        }
    }

    /// Runs the block's steps in order and, when it loops, again until a
    /// budget has elapsed. `deadline` is the earliest budget among the
    /// block's ancestors; the block's own budget can tighten it for
    /// everything beneath. Budgets are checked between passes only, so a
    /// loop overruns by at most one pass and nothing is cut mid-flight. The
    /// lifetime is checked before every step, since most requests run to
    /// completion without looking at it.
    fn run_block<'a>(
        &'a self,
        ctx: &'a Ctx,
        block: &'a pb::Block,
        path: String,
        deadline: Option<Instant>,
    ) -> BoxFut<'a, (pb::StepResult, Option<Status>)> {
        Box::pin(async move {
            let start = Instant::now();
            let mut result = new_result(block.steps.len());
            let mut deadline = deadline;
            if block.loop_duration_ms > 0 {
                let own = start + Duration::from_millis(block.loop_duration_ms as u64);
                deadline = Some(deadline.map_or(own, |d| d.min(own)));
            }
            loop {
                stats_mut(&mut result).passes += 1;
                for (i, step) in block.steps.iter().enumerate() {
                    if let Some(err) = ctx.err() {
                        stats_mut(&mut result).elapsed_ms = elapsed_ms(start);
                        return (result, Some(err));
                    }
                    let (child, err) = self
                        .run_step(ctx, step, format!("{path}.steps[{i}]"), deadline)
                        .await;
                    if let Some(child) = child {
                        add_result(&mut result.children[i], &child);
                        add_totals(stats_mut(&mut result), child.stats.as_ref());
                    }
                    if let Some(err) = err {
                        stats_mut(&mut result).elapsed_ms = elapsed_ms(start);
                        return (result, Some(err));
                    }
                }
                let more =
                    block.loop_duration_ms != 0 && deadline.is_some_and(|d| Instant::now() < d);
                if !more {
                    break;
                }
            }
            stats_mut(&mut result).elapsed_ms = elapsed_ms(start);
            (result, None)
        })
    }

    async fn run_step(
        &self,
        ctx: &Ctx,
        step: &pb::Step,
        path: String,
        deadline: Option<Instant>,
    ) -> (Option<pb::StepResult>, Option<Status>) {
        match &step.kind {
            Some(StepKind::Block(b)) => {
                let (r, e) = self
                    .run_block(ctx, b, format!("{path}.block"), deadline)
                    .await;
                (Some(r), e)
            }
            Some(StepKind::Operation(op)) => {
                let (r, e) = self
                    .run_operation(ctx, op, format!("{path}.operation"))
                    .await;
                (Some(r), e)
            }
            None => (
                None,
                Some(Status::invalid_argument(format!("{path}: no kind set"))),
            ),
        }
    }

    /// Runs the operation's requests at the same time. The first failure
    /// cancels the rest and is the error returned; the result still carries
    /// the stats of every request that ran, failed ones included.
    async fn run_operation(
        &self,
        ctx: &Ctx,
        op: &pb::Operation,
        path: String,
    ) -> (pb::StepResult, Option<Status>) {
        let start = Instant::now();
        let mut result = new_result(op.requests.len());
        let group = ctx.child();
        let mut running: FuturesUnordered<_> = op
            .requests
            .iter()
            .enumerate()
            .map(|(i, request)| {
                let group = &group;
                async move { (i, request, self.run_request(group, request).await) }
            })
            .collect();
        let mut first_err = None;
        while let Some((i, request, (stats, err))) = running.next().await {
            result.children[i].stats = Some(stats);
            if let Some(err) = err {
                if first_err.is_none() {
                    group.cancel();
                    first_err = Some(prefix_error(
                        &format!("{path}.requests[{i}] ({})", request_kind(request)),
                        err,
                    ));
                }
            }
        }
        drop(running);
        let children: Vec<Option<pb::Stats>> = result.children.iter().map(|c| c.stats).collect();
        let st = stats_mut(&mut result);
        for child in &children {
            add_totals(st, child.as_ref());
        }
        st.passes = 1;
        st.elapsed_ms = elapsed_ms(start);
        (result, first_err)
    }

    /// Runs one request and reports what it did; results other than the
    /// stats are dropped.
    async fn run_request(&self, ctx: &Ctx, request: &pb::Request) -> (pb::Stats, Option<Status>) {
        let start = Instant::now();
        let mut stats = pb::Stats {
            passes: 1,
            requests_run: 1,
            ..Default::default()
        };
        let err = match &request.kind {
            Some(Req::WriteRam(r)) => match self.write_ram(r) {
                Ok(_) => {
                    stats.ram_bytes_written = parse_bytes(&r.size).unwrap_or(0);
                    None
                }
                Err(e) => Some(e),
            },
            Some(Req::ReadRam(r)) => match self.read_ram(r) {
                Ok(resp) => {
                    stats.ram_bytes_read = resp.size;
                    None
                }
                Err(e) => Some(e),
            },
            Some(Req::WriteDisk(r)) => match self.write_disk(r).await {
                Ok(_) => {
                    stats.disk_bytes_written = r.size as i64;
                    None
                }
                Err(e) => Some(e),
            },
            Some(Req::ReadDisk(r)) => match self.read_disk(r).await {
                Ok(resp) => {
                    stats.disk_bytes_read = resp.size;
                    None
                }
                Err(e) => Some(e),
            },
            Some(Req::BurnCpu(r)) => match self.burn_cpu(ctx, r).await {
                Ok(resp) => {
                    stats.burn_iterations = resp.iterations;
                    None
                }
                Err(e) => Some(e),
            },
            Some(Req::Ingest(r)) => match self.ingest(r).await {
                Ok(_) => {
                    stats.disk_bytes_written = r.payload.len() as i64;
                    None
                }
                Err(e) => Some(e),
            },
            Some(Req::Sleep(r)) => {
                let e = sleep(ctx, Duration::from_millis(r.duration_ms.max(0) as u64))
                    .await
                    .err();
                stats.slept_ms = elapsed_ms(start);
                e
            }
            None => Some(Status::invalid_argument("unknown request")),
        };
        stats.elapsed_ms = elapsed_ms(start);
        (stats, err)
    }
}

/// Rejects what can be seen without running anything: an empty block or
/// operation, a step or request with nothing set, a negative budget or
/// sleep. `path` names the node in messages.
fn validate_block(block: &pb::Block, path: &str) -> Result<(), Status> {
    if block.steps.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{path}: steps is required"
        )));
    }
    if block.loop_duration_ms < 0 {
        return Err(Status::invalid_argument(format!(
            "{path}: loop_duration_ms must be non-negative"
        )));
    }
    for (i, step) in block.steps.iter().enumerate() {
        let step_path = format!("{path}.steps[{i}]");
        match &step.kind {
            Some(StepKind::Block(b)) => validate_block(b, &format!("{step_path}.block"))?,
            Some(StepKind::Operation(op)) => {
                validate_operation(op, &format!("{step_path}.operation"))?
            }
            None => {
                return Err(Status::invalid_argument(format!(
                    "{step_path}: no kind set"
                )))
            }
        }
    }
    Ok(())
}

fn validate_operation(op: &pb::Operation, path: &str) -> Result<(), Status> {
    if op.requests.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{path}: requests is required"
        )));
    }
    for (i, request) in op.requests.iter().enumerate() {
        let request_path = format!("{path}.requests[{i}]");
        match &request.kind {
            None => {
                return Err(Status::invalid_argument(format!(
                    "{request_path}: no kind set"
                )))
            }
            Some(Req::Sleep(s)) if s.duration_ms < 0 => {
                return Err(Status::invalid_argument(format!(
                    "{request_path} (sleep): duration_ms must be non-negative"
                )))
            }
            _ => {}
        }
    }
    Ok(())
}

/// Idles for `duration` or until the lifetime ends, whichever is first.
async fn sleep(ctx: &Ctx, duration: Duration) -> Result<(), Status> {
    if duration.is_zero() {
        return Ok(());
    }
    tokio::select! {
        _ = tokio::time::sleep(duration) => Ok(()),
        _ = ctx.done() => Err(ctx.err().unwrap_or_else(|| Status::cancelled("context canceled"))),
    }
}

fn elapsed_ms(start: Instant) -> i64 {
    start.elapsed().as_millis() as i64
}

fn stats_mut(r: &mut pb::StepResult) -> &mut pb::Stats {
    r.stats.get_or_insert_with(Default::default)
}

/// A node with zeroed stats and `n` zeroed children.
fn new_result(n: usize) -> pb::StepResult {
    pb::StepResult {
        stats: Some(pb::Stats::default()),
        children: (0..n)
            .map(|_| pb::StepResult {
                stats: Some(pb::Stats::default()),
                children: Vec::new(),
            })
            .collect(),
    }
}

/// Folds one entry's result tree into the accumulated node of the same
/// shape.
fn add_result(dst: &mut pb::StepResult, src: &pb::StepResult) {
    add_stats(stats_mut(dst), src.stats.as_ref());
    while dst.children.len() < src.children.len() {
        dst.children.push(pb::StepResult {
            stats: Some(pb::Stats::default()),
            children: Vec::new(),
        });
    }
    for (i, child) in src.children.iter().enumerate() {
        add_result(&mut dst.children[i], child);
    }
}

/// Sums a later entry of one node into its earlier ones, field by field.
fn add_stats(dst: &mut pb::Stats, src: Option<&pb::Stats>) {
    let Some(src) = src else { return };
    dst.elapsed_ms += src.elapsed_ms;
    dst.passes += src.passes;
    add_totals(dst, Some(src));
}

/// Sums a child's work into its parent: every field but passes (the
/// parent's own entries) and elapsed (which the parent measures itself).
fn add_totals(dst: &mut pb::Stats, src: Option<&pb::Stats>) {
    let Some(src) = src else { return };
    dst.requests_run += src.requests_run;
    dst.disk_bytes_written += src.disk_bytes_written;
    dst.disk_bytes_read += src.disk_bytes_read;
    dst.ram_bytes_written += src.ram_bytes_written;
    dst.ram_bytes_read += src.ram_bytes_read;
    dst.burn_iterations += src.burn_iterations;
    dst.slept_ms += src.slept_ms;
}

/// The proto field name of the request that is set.
fn request_kind(request: &pb::Request) -> &'static str {
    match &request.kind {
        Some(Req::WriteRam(_)) => "write_ram",
        Some(Req::ReadRam(_)) => "read_ram",
        Some(Req::WriteDisk(_)) => "write_disk",
        Some(Req::ReadDisk(_)) => "read_disk",
        Some(Req::BurnCpu(_)) => "burn_cpu",
        Some(Req::Ingest(_)) => "ingest",
        Some(Req::Sleep(_)) => "sleep",
        None => "unset",
    }
}

/// Puts the failing request's path ahead of the message, keeping the code.
fn prefix_error(where_: &str, err: Status) -> Status {
    Status::new(err.code(), format!("{where_}: {}", err.message()))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::glutton::service::tests::new_service;
    use tonic::Code;

    fn request(kind: Req) -> pb::Request {
        pb::Request { kind: Some(kind) }
    }
    pub(crate) fn write_disk(key: &str, size: i32) -> pb::Request {
        request(Req::WriteDisk(pb::WriteDiskRequest {
            key: key.into(),
            size,
            write_mode: 0,
        }))
    }
    pub(crate) fn read_disk(key: &str) -> pb::Request {
        request(Req::ReadDisk(pb::ReadDiskRequest {
            key: key.into(),
            read_mode: pb::ReadMode::DigestOnly as i32,
        }))
    }
    fn write_ram(key: &str, size: &str) -> pb::Request {
        request(Req::WriteRam(pb::WriteRamRequest {
            key: key.into(),
            size: size.into(),
            write_mode: 0,
        }))
    }
    fn read_ram(key: &str) -> pb::Request {
        request(Req::ReadRam(pb::ReadRamRequest {
            key: key.into(),
            size: String::new(),
        }))
    }
    fn burn(ms: i64) -> pb::Request {
        request(Req::BurnCpu(pb::BurnCpuRequest {
            duration_ms: ms,
            parallelism: 0,
        }))
    }
    fn sleep_for(ms: i64) -> pb::Request {
        request(Req::Sleep(pb::SleepRequest { duration_ms: ms }))
    }
    pub(crate) fn op(requests: Vec<pb::Request>) -> pb::Step {
        pb::Step {
            kind: Some(StepKind::Operation(pb::Operation { requests })),
        }
    }
    fn seq(steps: Vec<pb::Step>) -> pb::Step {
        lp(0, steps)
    }
    fn lp(budget_ms: i64, steps: Vec<pb::Step>) -> pb::Step {
        pb::Step {
            kind: Some(StepKind::Block(pb::Block {
                steps,
                loop_duration_ms: budget_ms,
            })),
        }
    }
    pub(crate) fn script(steps: Vec<pb::Step>) -> pb::RunScriptRequest {
        pb::RunScriptRequest {
            script: Some(pb::Block {
                steps,
                loop_duration_ms: 0,
            }),
        }
    }

    async fn run(svc: &Service, req: pb::RunScriptRequest) -> pb::StepResult {
        svc.run_script(&Ctx::background(), &req)
            .await
            .unwrap()
            .result
            .unwrap()
    }

    fn st(r: &pb::StepResult) -> pb::Stats {
        r.stats.unwrap_or_default()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_script_runs_steps_in_order_and_sums_stats() {
        let (svc, _d) = new_service();
        let result = run(
            &svc,
            script(vec![
                op(vec![write_disk("scripted", 1024)]),
                op(vec![read_disk("scripted")]),
                seq(vec![
                    op(vec![write_ram("arr", "8Ki")]),
                    op(vec![read_ram("arr")]),
                ]),
                op(vec![burn(1), sleep_for(1)]),
            ]),
        )
        .await;
        let root = st(&result);
        assert_eq!((root.passes, root.requests_run), (1, 6));
        assert_eq!(
            (root.disk_bytes_written, root.disk_bytes_read),
            (1024, 1024)
        );
        assert_eq!((root.ram_bytes_written, root.ram_bytes_read), (8192, 8192));
        assert!(root.burn_iterations >= 1 && root.slept_ms >= 1, "{root:?}");
        assert_eq!(result.children.len(), 4);
        let nested = &result.children[2];
        assert_eq!(nested.children.len(), 2);
        assert_eq!(st(nested).requests_run, 2);
        assert_eq!(st(nested).ram_bytes_written, 8192);
        let parallel = &result.children[3];
        assert_eq!(parallel.children.len(), 2);
        assert!(st(&parallel.children[0]).burn_iterations >= 1);
        assert!(st(&parallel.children[1]).slept_ms >= 1);
    }

    // The timing tests run on a paused clock: it advances only when every
    // task is idle on a timer, and then exactly to that timer, so a pass
    // costs its sleep and nothing else and the counts are exact.

    #[tokio::test(start_paused = true)]
    async fn run_script_operation_runs_requests_together() {
        let (svc, _d) = new_service();
        let start = Instant::now();
        let result = run(
            &svc,
            script(vec![op(vec![
                sleep_for(100),
                sleep_for(100),
                sleep_for(100),
            ])]),
        )
        .await;
        assert_eq!(start.elapsed(), Duration::from_millis(100));
        let s = st(&result.children[0]);
        assert_eq!((s.slept_ms, s.elapsed_ms), (300, 100));
    }

    #[tokio::test(start_paused = true)]
    async fn run_script_loops_until_the_budget() {
        let (svc, _d) = new_service();
        let start = Instant::now();
        let result = run(
            &svc,
            script(vec![lp(
                100,
                vec![op(vec![sleep_for(30)]), op(vec![write_disk("churn", 16)])],
            )]),
        )
        .await;
        assert_eq!(start.elapsed(), Duration::from_millis(120));
        let looped = st(&result.children[0]);
        assert_eq!(looped.passes, 4);
        assert_eq!(looped.disk_bytes_written, 16 * 4);
        assert_eq!(looped.slept_ms, 30 * 4);
        assert_eq!(looped.elapsed_ms, 120);
        let root = st(&result);
        assert_eq!(root.passes, 1);
        assert_eq!(root.disk_bytes_written, looped.disk_bytes_written);
    }

    #[tokio::test(start_paused = true)]
    async fn run_script_nested_loops_share_the_budget() {
        let (svc, _d) = new_service();
        let start = Instant::now();
        let result = run(
            &svc,
            script(vec![lp(100, vec![lp(5000, vec![op(vec![sleep_for(20)])])])]),
        )
        .await;
        assert_eq!(start.elapsed(), Duration::from_millis(100));
        let outer = st(&result.children[0]);
        let inner = st(&result.children[0].children[0]);
        assert_eq!(outer.passes, 1);
        assert_eq!(inner.passes, 5);
        assert_eq!(inner.slept_ms, 100);
    }

    #[tokio::test]
    async fn run_script_rejects_shape() {
        let (svc, _d) = new_service();
        let cases: Vec<(&str, pb::RunScriptRequest, &str)> = vec![
            (
                "no script",
                pb::RunScriptRequest::default(),
                "script is required",
            ),
            ("empty block", script(vec![]), "script: steps is required"),
            (
                "unset step",
                script(vec![pb::Step::default()]),
                "script.steps[0]: no kind set",
            ),
            (
                "empty operation",
                script(vec![op(vec![])]),
                "script.steps[0].operation: requests is required",
            ),
            (
                "unset request",
                script(vec![op(vec![pb::Request::default()])]),
                "script.steps[0].operation.requests[0]: no kind set",
            ),
            (
                "negative sleep",
                script(vec![op(vec![sleep_for(-1)])]),
                "requests[0] (sleep)",
            ),
            (
                "negative budget",
                script(vec![lp(-1, vec![op(vec![sleep_for(0)])])]),
                "script.steps[0].block: loop_duration_ms",
            ),
            (
                "deep empty block",
                script(vec![seq(vec![seq(vec![seq(vec![])])])]),
                "script.steps[0].block.steps[0].block.steps[0].block: steps is required",
            ),
        ];
        for (name, req, msg) in cases {
            let err = svc.run_script(&Ctx::background(), &req).await.unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument, "{name}");
            assert!(
                err.message().contains(msg),
                "{name}: {:?} lacks {msg:?}",
                err.message()
            );
        }
        let err = svc
            .read_disk(&pb::ReadDiskRequest {
                key: "scripted".into(),
                read_mode: 0,
            })
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::NotFound,
            "a rejected script left a file behind"
        );
    }

    #[tokio::test]
    async fn run_script_request_errors_keep_their_code() {
        let (svc, _d) = new_service();
        let cases = vec![
            (
                "bad key",
                script(vec![
                    op(vec![sleep_for(0)]),
                    op(vec![write_disk("../x", 1)]),
                ]),
                Code::InvalidArgument,
                "script.steps[1].operation.requests[0] (write_disk)",
            ),
            (
                "missing file",
                script(vec![seq(vec![op(vec![read_disk("nothing")])])]),
                Code::NotFound,
                "script.steps[0].block.steps[0].operation.requests[0] (read_disk)",
            ),
        ];
        for (name, req, code, msg) in cases {
            let err = svc.run_script(&Ctx::background(), &req).await.unwrap_err();
            assert_eq!(err.code(), code, "{name}");
            assert!(err.message().contains(msg), "{name}: {:?}", err.message());
        }
        let err = svc
            .run_script(
                &Ctx::background(),
                &script(vec![lp(100, vec![op(vec![read_disk("nothing")])])]),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
        let start = std::time::Instant::now();
        let err = svc
            .run_script(
                &Ctx::background(),
                &script(vec![op(vec![read_disk("nothing"), sleep_for(5000)])]),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "sibling not cancelled promptly"
        );
    }

    #[tokio::test]
    async fn run_script_stops_when_the_context_ends() {
        let (svc, _d) = new_service();
        let ctx = Ctx::with_timeout(Duration::from_millis(20));
        let start = std::time::Instant::now();
        let err = svc
            .run_script(
                &ctx,
                &script(vec![lp(10000, vec![op(vec![sleep_for(5000)])])]),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::DeadlineExceeded);
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_script_stops_between_steps_when_the_context_ends() {
        let (svc, _d) = new_service();
        let ctx = Ctx::with_timeout(Duration::from_millis(20));
        let start = std::time::Instant::now();
        let err = svc
            .run_script(
                &ctx,
                &script(vec![lp(
                    10000,
                    vec![
                        op(vec![write_ram("churn", "4Ki")]),
                        op(vec![write_disk("churn", 16)]),
                        op(vec![burn(1)]),
                    ],
                )]),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::DeadlineExceeded);
        assert!(start.elapsed() < Duration::from_secs(1));
    }
}
