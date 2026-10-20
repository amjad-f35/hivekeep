//! `get worker` and `top worker` against a fake Control server and a fake
//! metrics source.

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::*;
use hivekeep_ctl::age::timestamp_before;
use hivekeep_ctl::cli::WorkerFilter;
use hivekeep_ctl::client::Api;
use hivekeep_ctl::commands::worker::{list_filtered, validate_filter};
use hivekeep_ctl::commands::worker_top::{run_with, ContainerMetrics, MetricsSource, PodMetrics};
use hivekeep_ctl::connection::{ConnectionConfig, RequestDecorator};
use hivekeep_ctl::error::Result;
use hivekeep_ctl::io::{SharedBuf, Sink};
use hivekeep_ctl::output::{OutputFormat, Printer};
use hivekeep_ctl::print;
use hivekeep_ctl::verbosity::{Logger, BASIC, FLOW};
use hivekeep_proto::api::v1 as pb;

const POOL_LABEL: &str = "hivekeep.io/worker-pool";

/// 2026-01-01T12:00:00Z.
fn fixed_now() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_767_268_800)
}

async fn connect(fake: &Fake) -> Api {
    let addr = serve(fake.clone()).await;
    let cfg = ConnectionConfig {
        endpoint: Some(addr.to_string()),
        plaintext: true,
        ..Default::default()
    };
    let ch = cfg.connect(&Logger::silent()).await.unwrap();
    Api::new(ch, RequestDecorator::default(), Logger::silent())
}

fn resources(actors: i32, cpu: i64, mem: i64) -> Option<pb::WorkerResources> {
    Some(pb::WorkerResources {
        actors,
        cpu_millicores: cpu,
        memory_bytes: mem,
    })
}

#[allow(clippy::too_many_arguments)]
fn listed_worker(
    name: &str,
    created: Option<prost_types::Timestamp>,
    ns: &str,
    pool: &str,
    pod: &str,
    labels: &[(&str, &str)],
    status: Option<pb::WorkerStatus>,
) -> pb::Worker {
    pb::Worker {
        metadata: (!name.is_empty() || created.is_some()).then(|| pb::ResourceMetadata {
            name: name.into(),
            create_time: created,
            ..Default::default()
        }),
        worker_namespace: ns.into(),
        worker_pool: pool.into(),
        worker_pod: pod.into(),
        labels: labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        status,
        ..Default::default()
    }
}

fn status(
    state: pb::WorkerState,
    hosted: Option<(i32, i32)>,
    class: &str,
) -> Option<pb::WorkerStatus> {
    Some(pb::WorkerStatus {
        state: state as i32,
        capacity: hosted.and_then(|(_, cap)| resources(cap, 0, 0)),
        allocated: hosted.and_then(|(n, _)| resources(n, 0, 0)),
        sandbox_classes: if class.is_empty() {
            vec![]
        } else {
            vec![pb::WorkerSandboxClass {
                sandbox_class: class.into(),
                ..Default::default()
            }]
        },
        ..Default::default()
    })
}

/// An Actor of `hive` placed on `worker`.
fn actor_on(hive: &str, name: &str, worker: &str) -> pb::Actor {
    let mut a = actor(hive, name, pb::ActorState::Running);
    a.status.as_mut().unwrap().worker_assignment = Some(pb::WorkerAssignment {
        worker: Some(pb::ObjectRef {
            hive: String::new(),
            name: worker.into(),
        }),
        ..Default::default()
    });
    a
}

fn seed(fake: &Fake, workers: Vec<pb::Worker>, actors: Vec<pb::Actor>) {
    let mut s = fake.state();
    for (i, w) in workers.into_iter().enumerate() {
        s.workers.insert(format!("{i:03}"), w);
    }
    for a in actors {
        let m = a.metadata.clone().unwrap();
        s.actors.insert((m.hive, m.name), a);
    }
}

fn filter(ns: &str, hive: &str, selector: &str, class: &str) -> WorkerFilter {
    let opt = |s: &str| (!s.is_empty()).then(|| s.to_string());
    WorkerFilter {
        namespace: opt(ns),
        hive: opt(hive),
        selector: opt(selector),
        sandbox_class: opt(class),
    }
}

#[tokio::test]
async fn get_workers_filters() {
    let before = |secs| Some(timestamp_before(fixed_now(), Duration::from_secs(secs)));
    use pb::WorkerState::Active;
    let workers = vec![
        listed_worker(
            "worker-1",
            before(5 * 60),
            "ns-1",
            "counter",
            "pod-1",
            &[(POOL_LABEL, "counter")],
            status(Active, Some((1, 1)), "microvm"),
        ),
        listed_worker(
            "worker-2",
            before(3 * 3600),
            "ns-1",
            "other",
            "pod-2",
            &[(POOL_LABEL, "other")],
            status(Active, None, "gvisor"),
        ),
        listed_worker(
            "worker-3",
            before(72 * 3600),
            "ns-2",
            "counter",
            "pod-3",
            &[(POOL_LABEL, "counter")],
            status(Active, Some((1, 1)), "gvisor"),
        ),
    ];
    let fake = Fake::default();
    seed(
        &fake,
        workers,
        vec![
            actor_on("space-a", "actor-a", "worker-1"),
            actor_on("space-b", "actor-b", "worker-3"),
        ],
    );
    let api = connect(&fake).await;

    let header = "NAME       POOL      STATE                 ACTORS   CPU(MILLICORES)   MEMORY   POD          AGE\n";
    let row1 = "worker-1   counter   WORKER_STATE_ACTIVE   1/1      -                 -        ns-1/pod-1   5m\n";
    let row2 = "worker-2   other     WORKER_STATE_ACTIVE   0/0      -                 -        ns-1/pod-2   3h\n";
    let row3 = "worker-3   counter   WORKER_STATE_ACTIVE   1/1      -                 -        ns-2/pod-3   3d\n";
    let selector = &format!("{POOL_LABEL}=counter");
    for (name, f, want) in [
        (
            "no filter",
            filter("", "", "", ""),
            format!("{header}{row1}{row2}{row3}"),
        ),
        (
            "namespace",
            filter("ns-1", "", "", ""),
            format!("{header}{row1}{row2}"),
        ),
        (
            "hive",
            filter("", "space-a", "", ""),
            format!("{header}{row1}"),
        ),
        // With no rows the columns are as wide as the header alone.
        (
            "hive excludes free workers",
            filter("", "no-such-hive", "", ""),
            "NAME   POOL   STATE   ACTORS   CPU(MILLICORES)   MEMORY   POD   AGE\n".to_string(),
        ),
        (
            "selector",
            filter("", "", selector, ""),
            format!("{header}{row1}{row3}"),
        ),
        (
            "sandbox class",
            filter("", "", "", "microvm"),
            format!("{header}{row1}"),
        ),
        (
            "sandbox class gvisor",
            filter("", "", "", "gvisor"),
            format!("{header}{row2}{row3}"),
        ),
        (
            "combined",
            filter("ns-1", "", selector, ""),
            format!("{header}{row1}"),
        ),
    ] {
        let sel = validate_filter(&[], &f).unwrap();
        let workers = list_filtered(&api, &f, sel.as_ref()).await.unwrap();
        let mut buf = SharedBuf::default();
        print::worker::print_list(
            &Printer::at(OutputFormat::Table, fixed_now()),
            &mut buf,
            workers,
        )
        .unwrap();
        assert_eq!(buf.contents(), want, "{name}");
    }
}

#[test]
fn get_workers_invalid_selector() {
    let err = validate_filter(&[], &filter("", "", "invalid==selector==", "")).unwrap_err();
    assert!(err.is_usage(), "{err}");
}

#[tokio::test]
async fn get_workers_by_name() {
    // Ages are measured against the real clock here, as the command does.
    let before = |secs| {
        Some(timestamp_before(
            SystemTime::now(),
            Duration::from_secs(secs),
        ))
    };
    let fake = Fake::default();
    {
        let mut s = fake.state();
        s.workers.insert(
            "worker-1".into(),
            listed_worker(
                "worker-1",
                before(5 * 60),
                "ns-1",
                "counter",
                "pod-1",
                &[],
                Some(pb::WorkerStatus {
                    state: pb::WorkerState::Active as i32,
                    capacity: resources(4, 2000, 4 << 30),
                    allocated: resources(1, 500, 1 << 30),
                    ..Default::default()
                }),
            ),
        );
        s.workers.insert(
            "worker-2".into(),
            listed_worker(
                "worker-2",
                before(72 * 3600),
                "ns-1",
                "counter",
                "pod-2",
                &[],
                status(pb::WorkerState::Draining, None, ""),
            ),
        );
    }
    let addr = serve(fake.clone()).await;
    let out = hivectl(addr, &["get", "workers", "worker-1", "worker-2"])
        .await
        .ok()
        .to_string();
    assert_eq!(
        out,
        "NAME       POOL      STATE                   ACTORS   CPU(MILLICORES)   MEMORY    POD          AGE\n\
         worker-1   counter   WORKER_STATE_ACTIVE     1/4      500/2000          1Gi/4Gi   ns-1/pod-1   5m\n\
         worker-2   counter   WORKER_STATE_DRAINING   0/0      -                 -         ns-1/pod-2   3d\n"
    );
    assert!(fake.state().calls_to("ListWorkers").is_empty());
    assert_eq!(fake.state().calls_to("GetWorker").len(), 2);
}

#[tokio::test]
async fn get_worker_by_name_single() {
    let fake = Fake::default();
    fake.state().workers.insert(
        "worker-1".into(),
        listed_worker("worker-1", None, "", "", "", &[], None),
    );
    let addr = serve(fake.clone()).await;
    let out = hivectl(addr, &["get", "worker", "worker-1", "-o", "json"])
        .await
        .ok()
        .to_string();
    assert_eq!(
        out,
        "{\n  \"metadata\": {\n    \"name\": \"worker-1\"\n  }\n}\n"
    );
}

#[tokio::test]
async fn get_worker_by_name_error() {
    let fake = Fake::default();
    let addr = serve(fake.clone()).await;
    let run = hivectl(addr, &["get", "worker", "missing-worker"]).await;
    assert!(
        run.error()
            .message()
            .starts_with("failed to get worker \"missing-worker\": rpc error: code = NotFound"),
        "{}",
        run.error()
    );
}

// `top worker`.

struct FakeMetrics(std::result::Result<Vec<PodMetrics>, String>);

impl MetricsSource for FakeMetrics {
    async fn list(&self, _namespace: &str) -> std::result::Result<Vec<PodMetrics>, String> {
        self.0.clone()
    }
}

fn pod_metrics(ns: &str, pod: &str, containers: &[(&str, &str, &str)]) -> PodMetrics {
    PodMetrics {
        namespace: ns.into(),
        name: pod.into(),
        containers: containers
            .iter()
            .map(|(name, cpu, mem)| ContainerMetrics {
                name: name.to_string(),
                usage: [
                    ("cpu".to_string(), cpu.to_string()),
                    ("memory".to_string(), mem.to_string()),
                ]
                .into(),
            })
            .collect(),
    }
}

/// Runs `top worker` and returns (stdout, stderr).
async fn top(
    workers: Vec<pb::Worker>,
    actors: Vec<pb::Actor>,
    metrics: FakeMetrics,
    f: WorkerFilter,
    level: u8,
) -> (Result<()>, String, String) {
    let fake = Fake::default();
    seed(&fake, workers, actors);
    let api = connect(&fake).await;
    let (out, out_buf) = Sink::buffer();
    let (err, err_buf) = Sink::buffer();
    let r = run_with(
        &api,
        &metrics,
        &f,
        &Printer::new(OutputFormat::Table),
        &out,
        &Logger::new(level, err),
    )
    .await;
    (r, out_buf.contents(), err_buf.contents())
}

#[tokio::test]
async fn top_workers_success() {
    let workers = vec![
        listed_worker(
            "",
            None,
            "demo-counter",
            "counter",
            "counter-worker-pool-7b9f8-x123",
            &[(POOL_LABEL, "counter")],
            status(pb::WorkerState::Unspecified, Some((1, 1)), "gvisor"),
        ),
        listed_worker(
            "",
            None,
            "demo-counter",
            "counter",
            "counter-worker-pool-7b9f8-y456",
            &[(POOL_LABEL, "counter")],
            status(pb::WorkerState::Unspecified, None, "microvm"),
        ),
    ];
    let metrics = FakeMetrics(Ok(vec![
        pod_metrics(
            "demo-counter",
            "counter-worker-pool-7b9f8-x123",
            &[("worker", "342m", "412Mi")],
        ),
        pod_metrics(
            "demo-counter",
            "counter-worker-pool-7b9f8-y456",
            &[("worker", "2m", "64Mi")],
        ),
    ]));
    let (r, out, _) = top(workers, vec![], metrics, WorkerFilter::default(), 0).await;
    r.unwrap();
    assert_eq!(
        out,
        "NAME                             POOL      CLASS     STATUS          CPU(CORES)   MEMORY(bytes)\n\
         counter-worker-pool-7b9f8-x123   counter   gvisor    ASSIGNED(1/1)   342m         412Mi\n\
         counter-worker-pool-7b9f8-y456   counter   microvm   FREE            2m           64Mi\n"
    );
}

#[tokio::test]
async fn top_workers_filter_namespace() {
    let workers = vec![
        listed_worker(
            "",
            None,
            "ns-1",
            "pool-1",
            "pod-1",
            &[],
            status(pb::WorkerState::Unspecified, None, "gvisor"),
        ),
        listed_worker("", None, "ns-2", "pool-2", "pod-2", &[], None),
    ];
    let (r, out, _) = top(
        workers,
        vec![],
        FakeMetrics(Ok(vec![])),
        filter("ns-1", "", "", ""),
        0,
    )
    .await;
    r.unwrap();
    assert_eq!(
        out,
        "NAME    POOL     CLASS    STATUS   CPU(CORES)            MEMORY(bytes)\n\
         pod-1   pool-1   gvisor   FREE     metrics unavailable   metrics unavailable\n"
    );
}

#[tokio::test]
async fn top_workers_filter_hive() {
    let workers = vec![
        listed_worker(
            "worker-1",
            None,
            "ns-1",
            "pool-1",
            "pod-1",
            &[],
            status(pb::WorkerState::Unspecified, Some((1, 1)), "microvm"),
        ),
        listed_worker(
            "worker-2",
            None,
            "ns-1",
            "pool-1",
            "pod-2",
            &[],
            status(pb::WorkerState::Unspecified, Some((1, 1)), ""),
        ),
    ];
    let actors = vec![
        actor_on("space-a", "actor-a", "worker-1"),
        actor_on("space-b", "actor-b", "worker-2"),
    ];
    let (r, out, _) = top(
        workers,
        actors,
        FakeMetrics(Ok(vec![])),
        filter("", "space-a", "", ""),
        0,
    )
    .await;
    r.unwrap();
    assert_eq!(
        out,
        "NAME    POOL     CLASS     STATUS          CPU(CORES)            MEMORY(bytes)\n\
         pod-1   pool-1   microvm   ASSIGNED(1/1)   metrics unavailable   metrics unavailable\n"
    );
}

#[tokio::test]
async fn top_workers_filter_selector() {
    let workers = vec![
        listed_worker(
            "",
            None,
            "ns-1",
            "counter",
            "pod-1",
            &[(POOL_LABEL, "counter")],
            status(pb::WorkerState::Unspecified, None, "gvisor"),
        ),
        listed_worker(
            "",
            None,
            "ns-1",
            "other",
            "pod-2",
            &[(POOL_LABEL, "other")],
            None,
        ),
    ];
    let (r, out, _) = top(
        workers,
        vec![],
        FakeMetrics(Ok(vec![])),
        filter("", "", &format!("{POOL_LABEL}=counter"), ""),
        0,
    )
    .await;
    r.unwrap();
    assert_eq!(
        out,
        "NAME    POOL      CLASS    STATUS   CPU(CORES)            MEMORY(bytes)\n\
         pod-1   counter   gvisor   FREE     metrics unavailable   metrics unavailable\n"
    );
}

#[tokio::test]
async fn top_workers_metrics_unavailable() {
    let workers = vec![listed_worker(
        "",
        None,
        "ns-1",
        "pool-1",
        "pod-1",
        &[],
        None,
    )];
    let (r, out, _) = top(
        workers,
        vec![],
        FakeMetrics(Err("metrics-server unavailable".into())),
        WorkerFilter::default(),
        0,
    )
    .await;
    r.unwrap();
    assert_eq!(
        out,
        "NAME    POOL     CLASS   STATUS   CPU(CORES)            MEMORY(bytes)\n\
         pod-1   pool-1           FREE     metrics unavailable   metrics unavailable\n"
    );
}

#[tokio::test]
async fn top_workers_invalid_selector() {
    let (r, out, _) = top(
        vec![],
        vec![],
        FakeMetrics(Ok(vec![])),
        filter("", "", "invalid==selector==", ""),
        0,
    )
    .await;
    assert!(r.unwrap_err().is_usage());
    assert_eq!(out, "");
}

#[tokio::test]
async fn top_workers_verbose_surfaces_metrics_issues() {
    let workers = || {
        vec![
            listed_worker("w-1", None, "ns-1", "pool-1", "pod-1", &[], None),
            listed_worker("w-2", None, "ns-1", "pool-1", "pod-2", &[], None),
        ]
    };

    // -v: a failed metrics listing.
    let (r, _, err) = top(
        workers(),
        vec![],
        FakeMetrics(Err("metrics API down".into())),
        WorkerFilter::default(),
        BASIC,
    )
    .await;
    r.unwrap();
    assert!(
        err.contains(
            "Failed to list pod metrics (CPU/memory will be unavailable): metrics API down"
        ),
        "{err}"
    );

    // -vv: a missing pod and a pod without the worker container.
    let (r, _, err) = top(
        workers(),
        vec![],
        FakeMetrics(Ok(vec![pod_metrics(
            "ns-1",
            "pod-1",
            &[("sidecar", "100m", "32Mi")],
        )])),
        WorkerFilter::default(),
        FLOW,
    )
    .await;
    r.unwrap();
    for want in [
        "Container \"worker\" not found in pod metrics for ns-1/pod-1; summing all 1 container(s)",
        "No pod metrics returned for worker pod ns-1/pod-2",
    ] {
        assert!(err.contains(want), "{err} lacks {want}");
    }
}
