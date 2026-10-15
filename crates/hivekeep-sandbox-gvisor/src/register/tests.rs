#![allow(clippy::type_complexity)]

use super::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

use crate::keeper_dial::{DEFAULT_KEEPER_SPIFFE_ID, DEFAULT_POD_IDENTITY_OID};
use crate::runtimes_file::Runtime;
use hivekeep_core::hardware::{ATTR_ARCHITECTURE, ATTR_CPU_FEATURES, ATTR_SANDBOX_BUNDLE_HASH};

const ACTORS: i32 = 7;

fn attrs(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
    kv.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn gvisor_file(
    def: BTreeMap<String, String>,
    restorable: &[BTreeMap<String, String>],
) -> RuntimesFile {
    RuntimesFile {
        sandbox_classes: vec![RuntimesClass {
            sandbox_class: "gvisor".into(),
            default_runtime: Runtime { version: def },
            restorable_runtimes: restorable
                .iter()
                .map(|v| Runtime { version: v.clone() })
                .collect(),
        }],
    }
}

#[test]
fn capacity_from_files() {
    for (name, cpu, mem, want) in [
        (
            "limits set",
            "2000",
            "4294967296",
            kpb::WorkerResources {
                actors: ACTORS,
                cpu_millicores: 2000,
                memory_bytes: 4294967296,
            },
        ),
        (
            "unparseable is none",
            "2Gi",
            "",
            kpb::WorkerResources {
                actors: ACTORS,
                ..Default::default()
            },
        ),
        (
            "negative is none",
            "-1",
            "-1",
            kpb::WorkerResources {
                actors: ACTORS,
                ..Default::default()
            },
        ),
    ] {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join(CPU_LIMIT_FILE), cpu).unwrap();
        std::fs::write(d.path().join(MEMORY_LIMIT_FILE), mem).unwrap();
        assert_eq!(
            capacity_from_dir(d.path(), ACTORS, 0).unwrap(),
            want,
            "{name}"
        );
    }
}

#[test]
fn capacity_with_no_files() {
    let d = tempfile::tempdir().unwrap();
    assert_eq!(
        capacity_from_dir(d.path(), ACTORS, 1 << 20).unwrap(),
        kpb::WorkerResources {
            actors: ACTORS,
            ..Default::default()
        }
    );
}

#[test]
fn capacity_keeps_the_reserved_memory() {
    let d = tempfile::tempdir().unwrap();
    std::fs::write(d.path().join(MEMORY_LIMIT_FILE), "1073741824").unwrap();
    assert_eq!(
        capacity_from_dir(d.path(), ACTORS, 64 << 20)
            .unwrap()
            .memory_bytes,
        960 << 20
    );
    assert!(
        capacity_from_dir(d.path(), ACTORS, 1 << 30).is_err(),
        "all of it"
    );
    assert!(
        capacity_from_dir(d.path(), ACTORS, 2 << 30).is_err(),
        "more than all of it"
    );
}

#[test]
fn compat_is_sorted_by_key() {
    let host = hivekeep_core::cpu_features::host_arch();
    let got = build_compat(&attrs(&[
        (ATTR_SANDBOX_BUNDLE_HASH, "sha256:abc"),
        (ATTR_CPU_FEATURES, "3f"),
        (ATTR_ARCHITECTURE, host),
    ]));
    let e = |k: &str, v: &str| kpb::AttributeEntry {
        key: k.into(),
        value: v.into(),
    };
    assert_eq!(
        got,
        kpb::VersionedSandboxCompat {
            schema_version: SCHEMA_VERSION_V1.into(),
            attributes: vec![
                e(ATTR_ARCHITECTURE, host),
                e(ATTR_CPU_FEATURES, "3f"),
                e(ATTR_SANDBOX_BUNDLE_HASH, "sha256:abc")
            ],
        }
    );
}

#[test]
fn pod_ips() {
    assert!(parse_pod_ips("").is_empty());
    assert_eq!(parse_pod_ips("10.0.0.1"), ["10.0.0.1"]);
    assert_eq!(parse_pod_ips("10.0.0.1,fd00::1"), ["10.0.0.1", "fd00::1"]);
    assert_eq!(
        parse_pod_ips(" 10.0.0.1 , ,fd00::1,"),
        ["10.0.0.1", "fd00::1"]
    );
}

#[test]
fn epoch_require_and_registered() {
    let e = Epoch::default();
    assert_eq!(e.require().unwrap_err().code(), tonic::Code::Unavailable);
    assert!(!e.registered());
    e.store(3);
    assert_eq!(e.require().unwrap(), 3);
    assert!(e.registered());
}

#[derive(Default)]
struct FakeSupport {
    acquire_fails: usize,
    acquires: AtomicUsize,
    register_errs: Mutex<Vec<tonic::Status>>,
    registered: Mutex<Vec<kpb::RegisterWorkerRequest>>,
    reported: Mutex<Vec<kpb::SetWorkerSandboxRuntimesRequest>>,
    on_report: Option<Box<dyn Fn(&kpb::SetWorkerSandboxRuntimesRequest) + Send + Sync>>,
}

#[async_trait]
impl SupportClient for FakeSupport {
    async fn acquire_worker_epoch(&self) -> Result<i64, tonic::Status> {
        let n = self.acquires.fetch_add(1, Ordering::SeqCst) + 1;
        if n <= self.acquire_fails {
            return Err(tonic::Status::not_found("worker record does not exist yet"));
        }
        Ok(n as i64)
    }

    async fn register_worker(&self, req: kpb::RegisterWorkerRequest) -> Result<(), tonic::Status> {
        self.registered.lock().push(req);
        let mut errs = self.register_errs.lock();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(errs.remove(0))
        }
    }

    async fn set_worker_sandbox_runtimes(
        &self,
        req: kpb::SetWorkerSandboxRuntimesRequest,
    ) -> Result<(), tonic::Status> {
        if let Some(f) = &self.on_report {
            f(&req);
        }
        self.reported.lock().push(req);
        Ok(())
    }
}

/// A timer that fires at once, recording each duration.
fn immediately(waits: Arc<Mutex<Vec<Duration>>>) -> After {
    Arc::new(move |d| {
        waits.lock().push(d);
        Box::pin(async {})
    })
}

#[tokio::test]
async fn register_once_retries_and_resends_the_same_epoch() {
    let capacity = kpb::WorkerResources {
        actors: ACTORS,
        ..Default::default()
    };
    let c = FakeSupport {
        acquire_fails: 2,
        register_errs: Mutex::new(vec![
            tonic::Status::deadline_exceeded("timed out"),
            tonic::Status::aborted("conflict"),
        ]),
        ..Default::default()
    };
    let waits = Arc::new(Mutex::new(Vec::new()));
    let req = kpb::RegisterWorkerRequest {
        epoch: 0,
        ips: vec!["10.0.0.1".into()],
        capacity: Some(capacity),
    };
    let epoch = register_once(
        &c,
        req.clone(),
        Duration::from_millis(10),
        Duration::from_millis(25),
        &immediately(waits.clone()),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(epoch, 3, "the first acquire that succeeded");
    assert_eq!(c.acquires.load(Ordering::SeqCst), 3);
    let want = kpb::RegisterWorkerRequest { epoch: 3, ..req };
    assert_eq!(*c.registered.lock(), vec![want.clone(), want.clone(), want]);
    let ms = Duration::from_millis;
    assert_eq!(*waits.lock(), vec![ms(10), ms(20), ms(25), ms(25)]);
}

#[tokio::test]
async fn register_once_stops_on_refusals() {
    for status in [
        tonic::Status::failed_precondition("refused"),
        tonic::Status::invalid_argument("refused"),
    ] {
        let code = status.code();
        let c = FakeSupport {
            register_errs: Mutex::new(vec![status]),
            ..Default::default()
        };
        let waits = Arc::new(Mutex::new(Vec::new()));
        let err = register_once(
            &c,
            kpb::RegisterWorkerRequest::default(),
            Duration::from_millis(1),
            Duration::from_millis(1),
            &immediately(waits),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, RegisterError::Refused(ref s) if s.code() == code),
            "{err}"
        );
        assert_eq!(c.registered.lock().len(), 1);
    }
}

fn dial_cfg(dir: &Path) -> KeeperDialConfig {
    KeeperDialConfig {
        socket: dir.join("keeper.sock"),
        credential_bundle: dir.join("does-not-exist.pem"),
        trust_bundle: dir.join("also-missing.pem"),
        keeper_spiffe_id: DEFAULT_KEEPER_SPIFFE_ID.into(),
        pod_identity_oid: DEFAULT_POD_IDENTITY_OID.into(),
    }
}

fn report_cfg(dir: &Path, runtimes: &Path) -> ReportConfig {
    ReportConfig {
        dial: dial_cfg(dir),
        actors: ACTORS,
        pod_ips: vec!["10.0.0.1".into()],
        memory_reserved_bytes: 0,
        actor_overhead: None,
        sandbox_class: "gvisor".into(),
        runtimes_file: runtimes.into(),
        capacity_dir: dir.join("capacity"),
    }
}

#[tokio::test]
async fn run_stores_the_epoch_before_reporting_runtimes() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("sandbox-runtimes.json");
    runtimes_file::write(
        &path,
        &gvisor_file(attrs(&[(ATTR_SANDBOX_BUNDLE_HASH, "sha256:v1")]), &[]),
    )
    .unwrap();
    let epoch = Arc::new(Epoch::default());
    let cancel = CancellationToken::new();
    let (e, c) = (epoch.clone(), cancel.clone());
    let client = Arc::new(FakeSupport {
        on_report: Some(Box::new(move |req| {
            assert_eq!(
                e.require().unwrap(),
                req.epoch,
                "epoch not stored when runtimes were reported"
            );
            c.cancel();
        })),
        ..Default::default()
    });
    let err = run(
        &report_cfg(d.path(), &path),
        kpb::WorkerResources::default(),
        client,
        &epoch,
        None,
        &cancel,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, RegisterError::Cancelled));
    assert_eq!(epoch.require().unwrap(), 1);
}

fn reconciler(path: &Path, epoch: i64, report: ReportFn) -> Reconciler {
    let p = path.to_path_buf();
    Reconciler::new(
        "gvisor",
        path,
        None,
        epoch,
        Arc::new(move || runtimes_file::read(&p)),
        report,
    )
}

#[tokio::test]
async fn reconciler_sync() {
    let epoch = 4;
    let host = hivekeep_core::cpu_features::host_arch();
    let v1 = gvisor_file(
        attrs(&[
            (ATTR_ARCHITECTURE, host),
            (ATTR_CPU_FEATURES, "3"),
            (ATTR_SANDBOX_BUNDLE_HASH, "sha256:v1"),
        ]),
        &[],
    );
    let v2 = gvisor_file(
        attrs(&[
            (ATTR_ARCHITECTURE, host),
            (ATTR_CPU_FEATURES, "7"),
            (ATTR_SANDBOX_BUNDLE_HASH, "sha256:v2"),
        ]),
        &[v1.sandbox_classes[0].default_runtime.version.clone()],
    );
    let req_v1 = build_runtimes_request(epoch, &v1.sandbox_classes[0]);
    let req_v2 = build_runtimes_request(epoch, &v2.sandbox_classes[0]);
    type Setup = Box<dyn Fn(&Path)>;
    let write =
        |f: RuntimesFile| -> Setup { Box::new(move |p| runtimes_file::write(p, &f).unwrap()) };
    let cases: Vec<(
        &str,
        Setup,
        Option<kpb::SetWorkerSandboxRuntimesRequest>,
        Option<tonic::Status>,
        bool,
        bool,
        Vec<kpb::SetWorkerSandboxRuntimesRequest>,
        Option<kpb::SetWorkerSandboxRuntimesRequest>,
    )> = vec![
        (
            "file missing",
            Box::new(|_| {}),
            None,
            None,
            false,
            false,
            vec![],
            None,
        ),
        (
            "corrupt JSON",
            Box::new(|p| std::fs::write(p, "{not json").unwrap()),
            None,
            None,
            false,
            false,
            vec![],
            None,
        ),
        (
            "no classes",
            write(RuntimesFile::default()),
            None,
            None,
            false,
            false,
            vec![],
            None,
        ),
        (
            "other class only",
            write(RuntimesFile {
                sandbox_classes: vec![RuntimesClass {
                    sandbox_class: "microvm".into(),
                    default_runtime: v1.sandbox_classes[0].default_runtime.clone(),
                    restorable_runtimes: vec![],
                }],
            }),
            None,
            None,
            false,
            false,
            vec![],
            None,
        ),
        (
            "empty default runtime",
            write(gvisor_file(BTreeMap::new(), &[])),
            None,
            None,
            false,
            false,
            vec![],
            None,
        ),
        (
            "register error asks for retry and keeps prior state",
            write(v1.clone()),
            None,
            Some(tonic::Status::unknown("worker record does not exist yet")),
            true,
            false,
            vec![],
            None,
        ),
        (
            "InvalidArgument is not retried",
            write(v1.clone()),
            None,
            Some(tonic::Status::invalid_argument("invalid attribute key")),
            false,
            false,
            vec![],
            None,
        ),
        (
            "FailedPrecondition is an error",
            write(v1.clone()),
            None,
            Some(tonic::Status::failed_precondition("epoch superseded")),
            false,
            true,
            vec![],
            None,
        ),
        (
            "first success registers and records the request",
            write(v1.clone()),
            None,
            None,
            false,
            false,
            vec![req_v1.clone()],
            Some(req_v1.clone()),
        ),
        (
            "unchanged file is a no-op",
            write(v1.clone()),
            Some(req_v1.clone()),
            None,
            false,
            false,
            vec![],
            Some(req_v1.clone()),
        ),
        (
            "updated file registers new default and restorable runtimes",
            write(v2.clone()),
            Some(req_v1.clone()),
            None,
            false,
            false,
            vec![req_v2.clone()],
            Some(req_v2.clone()),
        ),
    ];
    for (name, setup, reported, err, want_retry, want_err, want_calls, want_reported) in cases {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("sandbox-runtimes.json");
        setup(&path);
        let calls: Arc<Mutex<Vec<kpb::SetWorkerSandboxRuntimesRequest>>> = Arc::default();
        let c = calls.clone();
        let mut r = reconciler(
            &path,
            epoch,
            Arc::new(move |req| {
                let (c, err) = (c.clone(), err.clone());
                Box::pin(async move {
                    if let Some(e) = err {
                        return Err(e);
                    }
                    c.lock().push(req);
                    Ok(())
                })
            }),
        );
        r.reported = reported;
        let got = r.sync().await;
        assert_eq!(got.is_err(), want_err, "{name}: {got:?}");
        if !want_err {
            assert_eq!(got.unwrap(), want_retry, "{name}");
        }
        assert_eq!(*calls.lock(), want_calls, "{name}");
        assert_eq!(r.reported, want_reported, "{name}");
    }
}

#[tokio::test]
async fn reconciler_backs_off_and_resets() {
    let file = Arc::new(Mutex::new(gvisor_file(
        attrs(&[(ATTR_SANDBOX_BUNDLE_HASH, "sha256:v1")]),
        &[],
    )));
    let (changes_tx, changes_rx) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let attempts = Arc::new(AtomicUsize::new(0));
    let (initial, max, resync) = (
        Duration::from_millis(10),
        Duration::from_millis(35),
        Duration::from_secs(300),
    );
    let f = file.clone();
    let (a, c, tx) = (attempts.clone(), cancel.clone(), changes_tx.clone());
    let mut r = Reconciler::new(
        "gvisor",
        Path::new("sandbox-runtimes.json"),
        None,
        1,
        Arc::new(move || Ok(f.lock().clone())),
        Arc::new({
            let file = file.clone();
            move |_| {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                let r = match n {
                    1..=3 => Err(tonic::Status::unknown("transient failure")),
                    4 => {
                        *file.lock() =
                            gvisor_file(attrs(&[(ATTR_SANDBOX_BUNDLE_HASH, "sha256:v2")]), &[]);
                        let _ = tx.try_send(());
                        Ok(())
                    }
                    5 => Err(tonic::Status::unknown("transient failure after reset")),
                    _ => {
                        c.cancel();
                        Ok(())
                    }
                };
                Box::pin(async move { r })
            }
        }),
    );
    r.initial_backoff = initial;
    r.max_backoff = max;
    r.resync = resync;
    let waits = Arc::new(Mutex::new(Vec::new()));
    let w = waits.clone();
    r.after = Arc::new(move |d| {
        w.lock().push(d);
        if d == resync {
            Box::pin(futures::future::pending())
        } else {
            Box::pin(async {})
        }
    });
    let err = r.run(Some(changes_rx), &cancel).await.unwrap_err();
    assert!(matches!(err, RegisterError::Cancelled));
    assert_eq!(
        *waits.lock(),
        vec![initial, 2 * initial, max, resync, initial, resync]
    );
}

#[tokio::test]
async fn reconciler_ignores_changes_while_backing_off() {
    let file = gvisor_file(attrs(&[(ATTR_SANDBOX_BUNDLE_HASH, "sha256:v1")]), &[]);
    let (changes_tx, changes_rx) = mpsc::channel(1);
    let (started_tx, mut started) = mpsc::unbounded_channel::<usize>();
    let release = Arc::new(tokio::sync::Notify::new());
    let attempts = Arc::new(AtomicUsize::new(0));
    let (a, rel) = (attempts.clone(), release.clone());
    let mut r = Reconciler::new(
        "gvisor",
        Path::new("sandbox-runtimes.json"),
        None,
        1,
        Arc::new(move || Ok(file.clone())),
        Arc::new(move |_| {
            let n = a.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = started_tx.send(n);
            let (rel, tx) = (rel.clone(), changes_tx.clone());
            Box::pin(async move {
                if n == 1 {
                    let _ = tx.try_send(());
                    return Err(tonic::Status::unknown("transient failure"));
                }
                rel.notified().await;
                Ok(())
            })
        }),
    );
    let (backoff_tx, backoff_rx) = tokio::sync::oneshot::channel::<()>();
    let backoff_rx = Arc::new(Mutex::new(Some(backoff_rx)));
    let (armed_tx, mut armed) = mpsc::unbounded_channel::<Duration>();
    let initial = r.initial_backoff;
    r.after = Arc::new(move |d| {
        let _ = armed_tx.send(d);
        if d == initial {
            if let Some(rx) = backoff_rx.lock().take() {
                return Box::pin(async move {
                    let _ = rx.await;
                });
            }
        }
        Box::pin(futures::future::pending())
    });
    let resync = r.resync;
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { r.run(Some(changes_rx), &c).await });
    assert_eq!(started.recv().await, Some(1));
    assert_eq!(armed.recv().await, Some(initial));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        started.try_recv().is_err(),
        "the change signal bypassed the backoff"
    );
    backoff_tx.send(()).unwrap();
    assert_eq!(started.recv().await, Some(2));
    release.notify_one();
    // The change that arrived during the backoff is taken once the sync
    // succeeds: a second, no-op sync and its resync timer.
    assert_eq!(armed.recv().await, Some(resync));
    assert_eq!(armed.recv().await, Some(resync));
    cancel.cancel();
    assert!(matches!(task.await.unwrap(), Err(RegisterError::Cancelled)));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reconciler_resyncs_periodically() {
    let reads = Arc::new(AtomicUsize::new(0));
    let cancel = CancellationToken::new();
    let (rd, c) = (reads.clone(), cancel.clone());
    let mut r = Reconciler::new(
        "gvisor",
        Path::new("sandbox-runtimes.json"),
        None,
        1,
        Arc::new(move || {
            if rd.fetch_add(1, Ordering::SeqCst) + 1 >= 2 {
                c.cancel();
            }
            Ok(gvisor_file(
                attrs(&[(ATTR_SANDBOX_BUNDLE_HASH, "sha256:v1")]),
                &[],
            ))
        }),
        Arc::new(|_| Box::pin(async { Ok(()) })),
    );
    let armed = Arc::new(Mutex::new(Vec::new()));
    let (a, c2) = (armed.clone(), cancel.clone());
    r.after = Arc::new(move |d| {
        a.lock().push(d);
        if c2.is_cancelled() {
            return Box::pin(futures::future::pending());
        }
        Box::pin(async {})
    });
    assert!(matches!(
        r.run(None, &cancel).await,
        Err(RegisterError::Cancelled)
    ));
    assert_eq!(
        reads.load(Ordering::SeqCst),
        2,
        "initial sync and one resync"
    );
    assert_eq!(armed.lock()[0], RESYNC_INTERVAL);
}

#[tokio::test]
async fn reconciler_follows_the_file() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("sandbox-runtimes.json");
    let (tx, mut got) = mpsc::unbounded_channel();
    let mut r = reconciler(
        &path,
        1,
        Arc::new(move |req| {
            let _ = tx.send(req);
            Box::pin(async { Ok(()) })
        }),
    );
    let cancel = CancellationToken::new();
    let changes =
        crate::file_watch::watch_file_path(&path, Duration::from_millis(20), cancel.clone());
    let c = cancel.clone();
    let task = tokio::spawn(async move { r.run(Some(changes), &c).await });
    let host = hivekeep_core::cpu_features::host_arch();
    let v1 = gvisor_file(
        attrs(&[
            (ATTR_ARCHITECTURE, host),
            (ATTR_CPU_FEATURES, "3"),
            (ATTR_SANDBOX_BUNDLE_HASH, "sha256:v1"),
        ]),
        &[],
    );
    runtimes_file::write(&path, &v1).unwrap();
    let first = tokio::time::timeout(Duration::from_secs(10), got.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, build_runtimes_request(1, &v1.sandbox_classes[0]));
    let v2 = gvisor_file(
        attrs(&[
            (ATTR_ARCHITECTURE, host),
            (ATTR_CPU_FEATURES, "7"),
            (ATTR_SANDBOX_BUNDLE_HASH, "sha256:v2"),
        ]),
        &[v1.sandbox_classes[0].default_runtime.version.clone()],
    );
    runtimes_file::write(&path, &v2).unwrap();
    let second = tokio::time::timeout(Duration::from_secs(10), got.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second, build_runtimes_request(1, &v2.sandbox_classes[0]));
    cancel.cancel();
    assert!(matches!(task.await.unwrap(), Err(RegisterError::Cancelled)));
}

#[tokio::test]
async fn report_fails_fast_on_bad_credentials() {
    let d = tempfile::tempdir().unwrap();
    let cfg = report_cfg(d.path(), &d.path().join("sandbox-runtimes.json"));
    let r = tokio::time::timeout(
        Duration::from_secs(10),
        report(cfg, Arc::default(), CancellationToken::new()),
    )
    .await;
    assert!(matches!(r, Ok(Err(RegisterError::Config(_)))), "{r:?}");
}

#[tokio::test]
async fn report_fails_fast_on_missing_config() {
    let d = tempfile::tempdir().unwrap();
    let base = report_cfg(d.path(), &d.path().join("sandbox-runtimes.json"));
    for (name, cfg) in [
        (
            "missing SandboxClass",
            ReportConfig {
                sandbox_class: String::new(),
                ..base.clone()
            },
        ),
        (
            "missing RuntimesFile",
            ReportConfig {
                runtimes_file: PathBuf::new(),
                ..base.clone()
            },
        ),
        (
            "missing PodIPs",
            ReportConfig {
                pod_ips: vec![],
                ..base.clone()
            },
        ),
    ] {
        let r = tokio::time::timeout(
            Duration::from_secs(10),
            report(cfg, Arc::default(), CancellationToken::new()),
        )
        .await;
        assert!(
            matches!(r, Ok(Err(RegisterError::Config(_)))),
            "{name}: {r:?}"
        );
    }
}

#[tokio::test]
async fn sync_reports_the_actor_overhead() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("sandbox-runtimes.json");
    runtimes_file::write(
        &path,
        &gvisor_file(
            attrs(&[(ATTR_SANDBOX_BUNDLE_HASH, "sha256:v2")]),
            &[attrs(&[(ATTR_SANDBOX_BUNDLE_HASH, "sha256:v1")])],
        ),
    )
    .unwrap();
    let overhead = kpb::RuntimeOverhead {
        memory_bytes: 4 << 20,
        memory_bytes_per_gib: 8 << 20,
    };
    let got: Arc<Mutex<Option<kpb::SetWorkerSandboxRuntimesRequest>>> = Arc::default();
    let g = got.clone();
    let mut r = reconciler(
        &path,
        3,
        Arc::new(move |req| {
            *g.lock() = Some(req);
            Box::pin(async { Ok(()) })
        }),
    );
    r.actor_overhead = Some(overhead);
    assert!(!r.sync().await.unwrap());
    let req = got.lock().clone().unwrap();
    let c = &req.sandbox_classes[0];
    assert_eq!(c.restorable_runtimes.len(), 1);
    for rt in c.default_runtime.iter().chain(&c.restorable_runtimes) {
        assert_eq!(rt.actor_overhead, Some(overhead));
    }
}
