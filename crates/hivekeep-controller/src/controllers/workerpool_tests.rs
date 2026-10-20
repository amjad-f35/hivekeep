// Table-driven cases spell out their tuple types.
#![allow(clippy::type_complexity)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use hivekeep_k8s::crd::workerpool::{
    REASON_RESOLVED, REASON_SANDBOX_CLASS_MISMATCH, REASON_SANDBOX_CONFIG_DELETING,
    REASON_SANDBOX_CONFIG_NOT_FOUND,
};
use hivekeep_k8s::crd::{
    SandboxClass, SandboxConfigReference, WorkerPoolSandboxClass, WorkerPoolStatus,
};
use hivekeep_k8s::fake::{Action as FakeAction, FakeApiServer, Reaction};
use k8s_openapi::api::apps::v1::{DeploymentSpec, DeploymentStatus};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, LabelSelector, Time};
use k8s_openapi::jiff::Timestamp;
use parking_lot::Mutex;

use super::super::events::RecordingEventRecorder;
use super::super::sandboxconfig_protection::SandboxConfigProtectionReconciler;
use super::super::test_support::*;
use super::super::workerpool_sandboxconfig::sandbox_class_status;
use super::*;

const GV: SandboxClass = SandboxClass::Gvisor;

fn reconciler(fake: &FakeApiServer) -> (WorkerPoolReconciler, Arc<RecordingEventRecorder>) {
    let rec = Arc::new(RecordingEventRecorder::new());
    (
        WorkerPoolReconciler::live(fake.client(), rec.clone(), pod_settings()),
        rec,
    )
}

fn pool_key() -> ObjectKey {
    ObjectKey::new("default", "pool")
}

fn stored_pool(fake: &FakeApiServer) -> WorkerPool {
    fake.get::<WorkerPool>("default", "pool")
        .expect("pool stored")
}

fn is_status_update(a: &FakeAction) -> bool {
    a.subresource.as_deref() == Some("status")
}

/// The config the status in a WorkerPool request body uses for gvisor.
fn body_status_config(a: &FakeAction) -> String {
    a.body
        .clone()
        .and_then(|b| serde_json::from_value::<WorkerPool>(b).ok())
        .map(|wp| status_config(&wp, GV))
        .unwrap_or_default()
}

fn t(secs: i64) -> Time {
    Time(Timestamp::from_second(secs).unwrap())
}

const T0: i64 = 1_790_812_800;

fn assert_no_pending(wp: &WorkerPool, when: &str) {
    for c in super::super::workerpool_sandboxconfig::status_classes(wp) {
        assert!(
            c.pending_config_ref.is_none(),
            "{when}: class {} still claims {:?}",
            c.name,
            c.pending_config_ref
        );
    }
}

#[tokio::test]
async fn reconcile_protects_before_recording() {
    let protected = |sc| with_finalizer(sc, WORKER_POOL_PROTECTION_FINALIZER);
    let cases = vec![
        (
            "unprotected class default",
            make_sandbox_config("fresh-default", GV, true),
            "",
            true,
            true,
        ),
        (
            "unprotected configRef target",
            make_sandbox_config("fresh-ref", GV, false),
            "fresh-ref",
            true,
            true,
        ),
        (
            "already protected config",
            protected(make_sandbox_config("held", GV, true)),
            "",
            true,
            true,
        ),
        (
            "unresolved pool protects nothing",
            make_sandbox_config("other-class", SandboxClass::Microvm, false),
            "other-class",
            false,
            false,
        ),
    ];
    for (name, config, r, want_resolved, want_protected) in cases {
        let fake = FakeApiServer::new();
        let mut wp = make_worker_pool("pool", "default", 1, "worker:v1");
        if !r.is_empty() {
            wp.spec.sandbox_classes[0].config_ref = Some(SandboxConfigReference::new(r));
        }
        fake.insert(&wp);
        fake.insert(&config);
        let config_name = config.name_any();
        // Whether the status was written, and whether the config carried
        // the finalizer at that moment.
        let seen = Arc::new(Mutex::new((false, false)));
        {
            let (seen, f, n) = (seen.clone(), fake.clone(), config_name.clone());
            fake.prepend_reactor("update", "workerpools", move |a| {
                if is_status_update(a) {
                    let sc = f.get::<SandboxConfig>("", &n).unwrap();
                    *seen.lock() = (
                        true,
                        has_finalizer(&sc.metadata, WORKER_POOL_PROTECTION_FINALIZER),
                    );
                }
                Reaction::Pass
            });
        }
        let (r, _) = reconciler(&fake);
        r.reconcile(&pool_key())
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));

        let sc = fake.get::<SandboxConfig>("", &config_name).unwrap();
        assert_eq!(
            has_finalizer(&sc.metadata, WORKER_POOL_PROTECTION_FINALIZER),
            want_protected,
            "{name}: protected"
        );
        let got = stored_pool(&fake);
        assert_eq!(
            status_config(&got, GV) == config_name,
            want_resolved,
            "{name}: status {:?}",
            got.status
        );
        let (wrote, protected_at_write) = *seen.lock();
        assert!(wrote, "{name}: status was never written");
        if want_resolved {
            assert!(
                protected_at_write,
                "{name}: status named the config before it carried the protection finalizer"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconcile_config_deleted_before_claim() {
    let config = |name: &str, created: i64, finalizers: &[&str]| {
        let mut sc = make_sandbox_config(name, GV, true);
        sc.metadata.creation_timestamp = Some(t(created));
        for f in finalizers {
            sc = with_finalizer(sc, f);
        }
        sc
    };
    let using = |name: &str| vec![class_status(GV, Some(name), None)];
    struct Case {
        name: &'static str,
        new_default: SandboxConfig,
        pinned: bool,
        status: Vec<WorkerPoolSandboxClassStatus>,
        deleted_before: bool,
        delete_during_write: bool,
        release: bool,
        want_first: &'static str,
        want: &'static str,
    }
    let base = |name, new_default| Case {
        name,
        new_default,
        pinned: false,
        status: vec![],
        deleted_before: false,
        delete_during_write: false,
        release: false,
        want_first: "",
        want: "",
    };
    let new = |f: &[&str]| config("new-default", T0 + 60, f);
    let cases = vec![
        Case { want_first: "new-default", want: "new-default", ..base("not deleted: switched", new(&[])) },
        Case {
            delete_during_write: true,
            release: true,
            want: "old-default",
            ..base("deleted and released: never named, falls back on the next reconcile", new(&[]))
        },
        Case {
            delete_during_write: true,
            release: true,
            want: "old-default",
            ..base(
                "deleted, released, held by another finalizer: never named, falls back on the next reconcile",
                new(&[TEST_HOLD_FINALIZER]),
            )
        },
        Case {
            delete_during_write: true,
            want: "old-default",
            ..base("deleted, not yet released: never named, falls back on the next reconcile", new(&[]))
        },
        Case {
            status: using("old-default"),
            delete_during_write: true,
            release: true,
            want_first: "old-default",
            want: "old-default",
            ..base("deleted and released while switching away from a config: keeps it", new(&[]))
        },
        Case {
            status: using("old-default"),
            delete_during_write: true,
            want_first: "old-default",
            want: "old-default",
            ..base("deleted, not yet released, while switching away from a config: keeps it", new(&[]))
        },
        Case {
            pinned: true,
            status: using("new-default"),
            deleted_before: true,
            ..base("pinned, named in status but released: dropped", new(&[TEST_HOLD_FINALIZER]))
        },
    ];
    for tc in cases {
        let fake = FakeApiServer::new();
        let mut wp = make_worker_pool("pool", "default", 1, "worker:v1");
        if !tc.status.is_empty() {
            wp.status = Some(WorkerPoolStatus {
                sandbox_classes: tc.status.clone(),
                ..Default::default()
            });
        }
        if tc.pinned {
            wp.spec.sandbox_classes[0].config_ref =
                Some(SandboxConfigReference::new("new-default"));
        }
        let mut new_default = tc.new_default.clone();
        if tc.deleted_before {
            new_default.metadata.deletion_timestamp = Some(t(T0));
        }
        fake.insert(&wp);
        fake.insert(&new_default);
        fake.insert(&config("old-default", T0, &[]));
        let deleted = Arc::new(AtomicBool::new(false));
        {
            let (f, deleted, during, release) = (
                fake.clone(),
                deleted.clone(),
                tc.delete_during_write,
                tc.release,
            );
            fake.prepend_reactor("update", "workerpools", move |a| {
                if is_status_update(a) && during && !deleted.swap(true, Ordering::SeqCst) {
                    let client = f.client();
                    block_on(async move {
                        Api::<SandboxConfig>::all(client.clone())
                            .delete("new-default", &Default::default())
                            .await
                            .expect("delete new-default");
                        if release {
                            SandboxConfigProtectionReconciler::live(client)
                                .reconcile(&ObjectKey::cluster("new-default"))
                                .await
                                .expect("protection reconcile");
                        }
                    });
                }
                Reaction::Pass
            });
        }
        let (r, _) = reconciler(&fake);
        for (i, want) in [tc.want_first, tc.want].into_iter().enumerate() {
            r.reconcile(&pool_key())
                .await
                .unwrap_or_else(|e| panic!("{}: reconcile {}: {e}", tc.name, i + 1));
            if tc.delete_during_write {
                assert!(
                    deleted.load(Ordering::SeqCst),
                    "{}: status was never written, so nothing was deleted",
                    tc.name
                );
            }
            let got = stored_pool(&fake);
            assert_eq!(
                status_config(&got, GV),
                want,
                "{}: after reconcile {}",
                tc.name,
                i + 1
            );
            assert_no_pending(&got, tc.name);
        }
    }
}

#[tokio::test]
async fn worker_pools_for_sandbox_config_cases() {
    let gvisor = vec![WorkerPoolSandboxClass {
        name: GV,
        config_ref: None,
    }];
    let microvm = vec![WorkerPoolSandboxClass {
        name: SandboxClass::Microvm,
        config_ref: None,
    }];
    let microvm_ref = vec![WorkerPoolSandboxClass {
        name: SandboxClass::Microvm,
        config_ref: Some(SandboxConfigReference::new("sc")),
    }];
    let using_sc = vec![class_status(SandboxClass::Microvm, Some("sc"), None)];
    let cases = vec![
        (
            "gvisor config: its class and the pools naming it",
            make_sandbox_config("sc", GV, false),
            false,
            vec![
                ObjectKey::new("a", "gv"),
                ObjectKey::new("b", "vm-ref"),
                ObjectKey::new("b", "vm-status"),
            ],
        ),
        (
            "microvm config: every microvm pool",
            make_sandbox_config("other", SandboxClass::Microvm, false),
            false,
            vec![
                ObjectKey::new("b", "vm"),
                ObjectKey::new("b", "vm-ref"),
                ObjectKey::new("b", "vm-status"),
            ],
        ),
        (
            "list fails: nothing",
            make_sandbox_config("sc", GV, false),
            true,
            vec![],
        ),
    ];
    for (name, sc, list_fails, want) in cases {
        let fake = FakeApiServer::new();
        fake.insert(&make_worker_pool_classes("a", "gv", gvisor.clone(), vec![]));
        fake.insert(&make_worker_pool_classes(
            "b",
            "vm",
            microvm.clone(),
            vec![],
        ));
        fake.insert(&make_worker_pool_classes(
            "b",
            "vm-ref",
            microvm_ref.clone(),
            vec![],
        ));
        fake.insert(&make_worker_pool_classes(
            "b",
            "vm-status",
            microvm.clone(),
            using_sc.clone(),
        ));
        if list_fails {
            fake.prepend_reactor("list", "workerpools", |_| {
                Reaction::fail(500, "InternalError", "list failed")
            });
        }
        let (r, _) = reconciler(&fake);
        let mut got = r.worker_pools_for_sandbox_config(&sc).await;
        got.sort();
        assert_eq!(got, want, "{name}");
    }
}

fn resolved_cond(config: &str) -> Condition {
    Condition {
        type_: CONDITION_RESOLVED.into(),
        status: "True".into(),
        reason: REASON_RESOLVED.into(),
        message: format!("resolved to SandboxConfig {config:?}"),
        observed_generation: None,
        last_transition_time: t(T0),
    }
}

fn failed_cond(reason: &str, message: &str) -> Condition {
    Condition {
        type_: CONDITION_RESOLVED.into(),
        status: "False".into(),
        reason: reason.into(),
        message: message.into(),
        observed_generation: None,
        last_transition_time: t(T0),
    }
}

#[tokio::test]
async fn record_sandbox_config_events_cases() {
    let failed = failed_cond(
        REASON_SANDBOX_CONFIG_NOT_FOUND,
        r#"SandboxConfig "x" not found"#,
    );
    let class = |config: &str, cond: Condition| {
        vec![WorkerPoolSandboxClassStatus {
            conditions: vec![cond],
            ..class_status(GV, (!config.is_empty()).then_some(config), None)
        }]
    };
    let warning = format!("Warning {} {}", failed.reason, failed.message);
    let uses_x =
        r#"Normal SandboxConfigSwitched sandbox class "gvisor" uses SandboxConfig "x""#.to_string();
    let cases: Vec<(&str, Vec<WorkerPoolSandboxClassStatus>, Vec<WorkerPoolSandboxClassStatus>, Vec<String>)> = vec![
        ("first resolve", vec![], class("x", resolved_cond("x")), vec![uses_x.clone()]),
        ("first resolve after a failure", class("", failed.clone()), class("x", resolved_cond("x")), vec![uses_x.clone()]),
        (
            "switch",
            class("x", resolved_cond("x")),
            class("y", resolved_cond("y")),
            vec![r#"Normal SandboxConfigSwitched sandbox class "gvisor" switched from SandboxConfig "x" to "y""#.into()],
        ),
        ("same config: nothing", class("x", resolved_cond("x")), class("x", resolved_cond("x")), vec![]),
        ("resolves again to the kept config: nothing", class("x", failed.clone()), class("x", resolved_cond("x")), vec![]),
        ("first failure", vec![], class("", failed.clone()), vec![warning.clone()]),
        ("failure keeping the config: warning only", class("x", resolved_cond("x")), class("x", failed.clone()), vec![warning.clone()]),
        ("same failure already reported", class("", failed.clone()), class("", failed.clone()), vec![]),
        (
            "another reason",
            class("", failed_cond(REASON_SANDBOX_CLASS_MISMATCH, &failed.message)),
            class("", failed.clone()),
            vec![warning.clone()],
        ),
        (
            "another message",
            class("", failed_cond(&failed.reason, r#"SandboxConfig "y" not found"#)),
            class("", failed.clone()),
            vec![warning.clone()],
        ),
        ("resolved in between", class("", resolved_cond("x")), class("", failed.clone()), vec![warning.clone()]),
    ];
    let fake = FakeApiServer::new();
    for (name, prev, classes, want) in cases {
        let wp = make_worker_pool("pool", "default", 1, "worker:v1");
        let (r, rec) = reconciler(&fake);
        r.record_sandbox_config_events(&wp, &prev, &classes);
        assert_eq!(rec.drain(), want, "{name}");
    }
}

#[tokio::test]
async fn reconcile_reports_switch_once_written() {
    for (name, rejected) in [
        ("written at once", 0),
        ("written on the second try", 1),
        ("written on the third try", 2),
    ] {
        let fake = FakeApiServer::new();
        let wp = make_worker_pool_classes(
            "default",
            "pool",
            vec![WorkerPoolSandboxClass {
                name: GV,
                config_ref: Some(SandboxConfigReference::new("y")),
            }],
            vec![class_status(GV, Some("x"), None)],
        );
        fake.insert(&wp);
        fake.insert(&make_sandbox_config("x", GV, false));
        fake.insert(&make_sandbox_config("y", GV, false));
        let count = Arc::new(AtomicUsize::new(0));
        {
            let count = count.clone();
            fake.prepend_reactor("update", "workerpools", move |a| {
                if is_status_update(a)
                    && body_status_config(a) == "y"
                    && count.load(Ordering::SeqCst) < rejected
                {
                    count.fetch_add(1, Ordering::SeqCst);
                    return Reaction::fail(
                        409,
                        "Conflict",
                        "Operation cannot be fulfilled on workerpools \"pool\": stale",
                    );
                }
                Reaction::Pass
            });
        }
        let (r, rec) = reconciler(&fake);
        for i in 0..rejected {
            let err = r.reconcile(&pool_key()).await.expect_err("conflict");
            assert!(
                err.is_conflict(),
                "{name}: reconcile {} error {err}, want a conflict",
                i + 1
            );
            let evs = rec.drain();
            assert!(
                evs.is_empty(),
                "{name}: reconcile {} emitted {evs:?} before status was written",
                i + 1
            );
        }
        r.reconcile(&pool_key())
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(
            rec.drain(),
            vec![
                r#"Normal SandboxConfigSwitched sandbox class "gvisor" switched from SandboxConfig "x" to "y""#
            ],
            "{name}"
        );
        assert_eq!(status_config(&stored_pool(&fake), GV), "y", "{name}");
    }
}

#[tokio::test]
async fn reconcile_confirms_switch() {
    let x = || {
        with_finalizer(
            make_sandbox_config("x", GV, false),
            WORKER_POOL_PROTECTION_FINALIZER,
        )
    };
    let deleting = |mut sc: SandboxConfig| {
        sc.metadata.deletion_timestamp = Some(super::super::meta::now());
        sc
    };
    struct Case {
        name: &'static str,
        in_use: &'static str,
        live_x: Option<SandboxConfig>,
        want_read: bool,
        want_config: &'static str,
        want_reason: &'static str,
        want_deployment: bool,
    }
    let cases = vec![
        Case {
            name: "live: switches",
            in_use: "y",
            live_x: Some(x()),
            want_read: true,
            want_config: "x",
            want_reason: REASON_RESOLVED,
            want_deployment: true,
        },
        Case {
            name: "being deleted: keeps the config in use",
            in_use: "y",
            live_x: Some(deleting(x())),
            want_read: true,
            want_config: "y",
            want_reason: REASON_SANDBOX_CONFIG_DELETING,
            want_deployment: true,
        },
        Case {
            name: "gone: keeps the config in use",
            in_use: "y",
            live_x: None,
            want_read: true,
            want_config: "y",
            want_reason: REASON_SANDBOX_CONFIG_NOT_FOUND,
            want_deployment: true,
        },
        Case {
            name: "live, new pool: switches and gets a Deployment",
            in_use: "",
            live_x: Some(x()),
            want_read: true,
            want_config: "x",
            want_reason: REASON_RESOLVED,
            want_deployment: true,
        },
        Case {
            name: "being deleted, new pool: no config, no Deployment",
            in_use: "",
            live_x: Some(deleting(x())),
            want_read: true,
            want_config: "",
            want_reason: REASON_SANDBOX_CONFIG_DELETING,
            want_deployment: false,
        },
        Case {
            name: "already in use: not read again",
            in_use: "x",
            live_x: Some(deleting(x())),
            want_read: false,
            want_config: "x",
            want_reason: REASON_RESOLVED,
            want_deployment: true,
        },
    ];
    for tc in cases {
        let cache = FakeApiServer::new();
        let mut wp = make_worker_pool("pool", "default", 1, "worker:v1");
        wp.spec.sandbox_classes[0].config_ref = Some(SandboxConfigReference::new("x"));
        if !tc.in_use.is_empty() {
            wp.status = Some(WorkerPoolStatus {
                sandbox_classes: vec![class_status(GV, Some(tc.in_use), None)],
                ..Default::default()
            });
            let mut dep = Deployment::default();
            dep.metadata.name = Some("pool".into());
            dep.metadata.namespace = Some("default".into());
            cache.insert(&dep);
        }
        cache.insert(&wp);
        cache.insert(&x());
        cache.insert(&with_finalizer(
            make_sandbox_config("y", GV, false),
            WORKER_POOL_PROTECTION_FINALIZER,
        ));

        let live = FakeApiServer::new();
        if let Some(lx) = &tc.live_x {
            live.insert(lx);
        }
        // Whether x was read live, and whether the pool's status claimed
        // it at that moment.
        let seen = Arc::new(Mutex::new((false, false)));
        {
            let (seen, c) = (seen.clone(), cache.clone());
            live.prepend_reactor("get", "sandboxconfigs", move |a| {
                let pool = c.get::<WorkerPool>("default", "pool").unwrap();
                let claimed = pool
                    .sandbox_configs_in_use()
                    .contains(&a.name.as_deref().unwrap_or_default());
                *seen.lock() = (true, claimed);
                Reaction::Pass
            });
        }
        let (r, _) = reconciler(&cache);
        let r = r.with_api_reader(live.client());
        r.reconcile(&pool_key())
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", tc.name));

        let (read, claimed_at_read) = *seen.lock();
        assert_eq!(
            read, tc.want_read,
            "{}: read x from the API server",
            tc.name
        );
        if read {
            assert!(
                claimed_at_read,
                "{}: x was read before the pool's status claimed it",
                tc.name
            );
        }
        let got = stored_pool(&cache);
        assert_eq!(status_config(&got, GV), tc.want_config, "{}", tc.name);
        let class = sandbox_class_status(&got, GV)
            .unwrap_or_else(|| panic!("{}: no gvisor status entry", tc.name));
        assert!(
            class.pending_config_ref.is_none(),
            "{}: pendingConfigRef left",
            tc.name
        );
        assert_eq!(
            class
                .condition(CONDITION_RESOLVED)
                .map(|c| c.reason.as_str()),
            Some(tc.want_reason),
            "{}",
            tc.name
        );
        assert_eq!(
            cache.get::<Deployment>("default", "pool").is_some(),
            tc.want_deployment,
            "{}: has Deployment",
            tc.name
        );
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Point {
    BeforeClaim,
    AfterClaim,
    AfterRead,
    AfterFinal,
}

/// Mutable state of one interleaving case, shared with the reactors.
#[derive(Default)]
struct Interleave {
    armed: bool,
    hit: bool,
    inside: bool,
    status_writes: usize,
    failures: Vec<String>,
}

/// Deletes `deleted`, runs the protection reconciler over both configs,
/// and checks what survived, if `at` is the point the case interleaves at.
fn interleave(
    fake: &FakeApiServer,
    st: &Mutex<Interleave>,
    at: Point,
    want_at: Point,
    deleted: &str,
    want_held: bool,
) {
    {
        let mut s = st.lock();
        if !s.armed || at != want_at || s.inside {
            return;
        }
        s.hit = true;
        s.inside = true;
    }
    let client = fake.client();
    let deleted_name = deleted.to_string();
    block_on(async move {
        Api::<SandboxConfig>::all(client.clone())
            .delete(&deleted_name, &Default::default())
            .await
            .expect("delete");
        protect(&client).await;
    });
    let held = fake.get::<SandboxConfig>("", deleted).is_some();
    let mut s = st.lock();
    if held != want_held {
        s.failures.push(format!(
            "{at:?}: {deleted:?} exists after the protection reconcile = {held}, want {want_held}"
        ));
    }
    if let Some(msg) = dangling_config_ref(fake) {
        s.failures.push(format!("{at:?}: {msg}"));
    }
    s.inside = false;
}

async fn protect(client: &Client) {
    let p = SandboxConfigProtectionReconciler::live(client.clone());
    for name in ["x", "y"] {
        p.reconcile(&ObjectKey::cluster(name))
            .await
            .expect("protection reconcile");
    }
}

/// A message if the pool's status names a config that does not exist.
fn dangling_config_ref(fake: &FakeApiServer) -> Option<String> {
    let pool = fake.get::<WorkerPool>("default", "pool")?;
    for c in super::super::workerpool_sandboxconfig::status_classes(&pool) {
        if let Some(r) = &c.config_ref {
            if fake.get::<SandboxConfig>("", &r.name).is_none() {
                return Some(format!("status names {:?}, which no longer exists", r.name));
            }
        }
    }
    None
}

/// A switch from y to x with a deletion and a protection reconcile
/// interleaved at each point: before the claim is written, after it, after
/// x is read from the API server, and after the final write. Throughout,
/// the config `configRef` names must exist, and the deleted config must
/// outlive its last claim.
///
/// The reactors of the fake API server run before a request is served, so
/// "after the claim" is the moment the confirming read arrives, and "after
/// the read" the moment the final status write arrives; nothing reaches the
/// API server in between. "After the final write" runs once the reconcile
/// returned: what it still does (applying the Deployment) touches no
/// config.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switch_interleavings() {
    use Point::*;
    struct Case {
        name: &'static str,
        deleted: &'static str,
        at: Point,
        unprotected: bool,
        want_held_at_point: bool,
        want_first: &'static str,
        want_settled: &'static str,
        want_gone: bool,
    }
    let cases = vec![
        Case {
            name: "x deleted and released before the claim: switch rejected",
            deleted: "x",
            at: BeforeClaim,
            unprotected: false,
            want_held_at_point: false,
            want_first: "y",
            want_settled: "y",
            want_gone: true,
        },
        Case {
            name: "unused x deleted and released before the claim: switch rejected",
            deleted: "x",
            at: BeforeClaim,
            unprotected: true,
            want_held_at_point: false,
            want_first: "y",
            want_settled: "y",
            want_gone: true,
        },
        Case {
            name: "x deleted after the claim: held by the claim, switch rejected, then released",
            deleted: "x",
            at: AfterClaim,
            unprotected: false,
            want_held_at_point: true,
            want_first: "y",
            want_settled: "y",
            want_gone: true,
        },
        Case {
            name: "x deleted after the read: held by the claim, switched, kept while in use",
            deleted: "x",
            at: AfterRead,
            unprotected: false,
            want_held_at_point: true,
            want_first: "x",
            want_settled: "x",
            want_gone: false,
        },
        Case {
            name: "x deleted after the final write: held, kept while in use",
            deleted: "x",
            at: AfterFinal,
            unprotected: false,
            want_held_at_point: true,
            want_first: "x",
            want_settled: "x",
            want_gone: false,
        },
        Case {
            name: "y deleted before the claim: held while named, released after the switch",
            deleted: "y",
            at: BeforeClaim,
            unprotected: false,
            want_held_at_point: true,
            want_first: "x",
            want_settled: "x",
            want_gone: true,
        },
        Case {
            name: "y deleted after the claim: held while named, released after the switch",
            deleted: "y",
            at: AfterClaim,
            unprotected: false,
            want_held_at_point: true,
            want_first: "x",
            want_settled: "x",
            want_gone: true,
        },
        Case {
            name: "y deleted after the read: held while named, released after the switch",
            deleted: "y",
            at: AfterRead,
            unprotected: false,
            want_held_at_point: true,
            want_first: "x",
            want_settled: "x",
            want_gone: true,
        },
    ];
    for tc in cases {
        let fake = FakeApiServer::new();
        let mut wp = make_worker_pool("pool", "default", 1, "worker:v1");
        wp.spec.sandbox_classes[0].config_ref = Some(SandboxConfigReference::new("x"));
        wp.status = Some(WorkerPoolStatus {
            sandbox_classes: vec![class_status(GV, Some("y"), None)],
            ..Default::default()
        });
        let mut x = make_sandbox_config("x", GV, false);
        if !tc.unprotected {
            x = with_finalizer(x, WORKER_POOL_PROTECTION_FINALIZER);
        }
        fake.insert(&wp);
        fake.insert(&x);
        fake.insert(&with_finalizer(
            make_sandbox_config("y", GV, false),
            WORKER_POOL_PROTECTION_FINALIZER,
        ));

        let st = Arc::new(Mutex::new(Interleave {
            armed: true,
            ..Default::default()
        }));
        {
            let (f, st, at, deleted, held) = (
                fake.clone(),
                st.clone(),
                tc.at,
                tc.deleted,
                tc.want_held_at_point,
            );
            fake.prepend_reactor("update", "workerpools", move |a| {
                if !is_status_update(a) || st.lock().inside {
                    return Reaction::Pass;
                }
                let n = {
                    let mut s = st.lock();
                    s.status_writes += 1;
                    s.status_writes
                };
                let point = if n == 1 { BeforeClaim } else { AfterRead };
                interleave(&f, &st, point, at, deleted, held);
                Reaction::Pass
            });
        }
        {
            let (f, st, at, deleted, held) = (
                fake.clone(),
                st.clone(),
                tc.at,
                tc.deleted,
                tc.want_held_at_point,
            );
            fake.prepend_reactor("get", "sandboxconfigs", move |_| {
                interleave(&f, &st, AfterClaim, at, deleted, held);
                Reaction::Pass
            });
        }

        let (r, _) = reconciler(&fake);
        r.reconcile(&pool_key())
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", tc.name));
        interleave(
            &fake,
            &st,
            AfterFinal,
            tc.at,
            tc.deleted,
            tc.want_held_at_point,
        );
        {
            let mut s = st.lock();
            s.armed = false;
            assert!(
                s.hit,
                "{}: the reconcile never reached {:?}",
                tc.name, tc.at
            );
            assert_eq!(
                s.status_writes, 2,
                "{}: status writes; want the claim and the final write",
                tc.name
            );
            assert!(s.failures.is_empty(), "{}: {:?}", tc.name, s.failures);
        }
        let got = stored_pool(&fake);
        assert_eq!(
            status_config(&got, GV),
            tc.want_first,
            "{}: after the reconcile",
            tc.name
        );
        assert_eq!(
            dangling_config_ref(&fake),
            None,
            "{}: after the reconcile",
            tc.name
        );

        // Settle: the deletion and the status writes trigger the protection
        // reconciler and another pool reconcile.
        let client = fake.client();
        protect(&client).await;
        r.reconcile(&pool_key())
            .await
            .unwrap_or_else(|e| panic!("{}: second reconcile: {e}", tc.name));
        protect(&client).await;
        let got = stored_pool(&fake);
        assert_eq!(
            status_config(&got, GV),
            tc.want_settled,
            "{}: once settled",
            tc.name
        );
        assert_eq!(
            dangling_config_ref(&fake),
            None,
            "{}: once settled",
            tc.name
        );
        let gone = fake.get::<SandboxConfig>("", tc.deleted).is_none();
        assert_eq!(
            gone, tc.want_gone,
            "{}: once settled, {:?} gone",
            tc.name, tc.deleted
        );
    }
}

#[tokio::test]
async fn reconcile_applies_existing_deployment_first() {
    #[derive(Clone, Copy)]
    enum Fail {
        List,
        Protect,
        Status,
    }
    let cases = [
        ("list fails, Deployment exists", Fail::List, true, 3),
        (
            "protection fails, Deployment exists",
            Fail::Protect,
            true,
            3,
        ),
        (
            "status write fails, Deployment exists",
            Fail::Status,
            true,
            3,
        ),
        ("list fails, no Deployment", Fail::List, false, 0),
        ("protection fails, no Deployment", Fail::Protect, false, 0),
        ("status write fails, no Deployment", Fail::Status, false, 0),
    ];
    for (name, fail, has_deployment, want_replicas) in cases {
        let fake = FakeApiServer::new();
        fake.insert(&make_worker_pool("pool", "default", 3, "worker:v1"));
        fake.insert(&make_sandbox_config("def", GV, true));
        if has_deployment {
            let mut dep = Deployment {
                spec: Some(DeploymentSpec {
                    replicas: Some(1),
                    ..Default::default()
                }),
                ..Default::default()
            };
            dep.metadata.name = Some("pool".into());
            dep.metadata.namespace = Some("default".into());
            fake.insert(&dep);
        }
        let injected = || Reaction::fail(500, "InternalError", "injected");
        match fail {
            Fail::List => fake.prepend_reactor("list", "sandboxconfigs", move |_| injected()),
            Fail::Protect => fake.prepend_reactor("patch", "sandboxconfigs", move |_| injected()),
            Fail::Status => fake.prepend_reactor("update", "workerpools", move |a| {
                if is_status_update(a) {
                    injected()
                } else {
                    Reaction::Pass
                }
            }),
        }
        let (r, _) = reconciler(&fake);
        let err = r.reconcile(&pool_key()).await.expect_err(name);
        assert!(
            err.to_string().contains("injected"),
            "{name}: error {err}, want the injected one"
        );
        let dep = fake.get::<Deployment>("default", "pool");
        if want_replicas == 0 {
            assert!(dep.is_none(), "{name}: a Deployment was created");
            continue;
        }
        let replicas = dep.and_then(|d| d.spec?.replicas);
        assert_eq!(replicas, Some(want_replicas), "{name}");
    }
}

#[tokio::test]
async fn sync_status_cases() {
    let classes = |config: &str| {
        vec![WorkerPoolSandboxClassStatus {
            conditions: vec![Condition {
                type_: CONDITION_RESOLVED.into(),
                status: "True".into(),
                reason: REASON_RESOLVED.into(),
                message: format!("resolved to SandboxConfig {config}"),
                observed_generation: Some(2),
                last_transition_time: t(T0),
            }],
            ..class_status(GV, Some(config), None)
        }]
    };
    let before = WorkerPoolStatus {
        replicas: 3,
        ready_replicas: 2,
        selector: "app=old".into(),
        sandbox_classes: classes("old"),
    };
    let dep = Deployment {
        spec: Some(DeploymentSpec {
            selector: LabelSelector {
                match_labels: Some([("app".to_string(), "new".to_string())].into()),
                match_expressions: None,
            },
            ..Default::default()
        }),
        status: Some(DeploymentStatus {
            replicas: Some(5),
            ready_replicas: Some(4),
            ..Default::default()
        }),
        ..Default::default()
    };
    let cases = vec![
        (
            "nil Deployment keeps the replica fields",
            None,
            classes("new"),
            WorkerPoolStatus {
                sandbox_classes: classes("new"),
                ..before.clone()
            },
            true,
        ),
        (
            "Deployment replaces the replica fields",
            Some(&dep),
            classes("new"),
            WorkerPoolStatus {
                replicas: 5,
                ready_replicas: 4,
                selector: "app=new".into(),
                sandbox_classes: classes("new"),
            },
            true,
        ),
        (
            "nothing changed writes nothing",
            None,
            classes("old"),
            before.clone(),
            false,
        ),
    ];
    for (name, dep, classes, want, want_write) in cases {
        let fake = FakeApiServer::new();
        let mut wp = make_worker_pool("pool", "default", 1, "worker:v1");
        wp.status = Some(before.clone());
        fake.insert(&wp);
        let mut stored = stored_pool(&fake);
        let rv = stored.metadata.resource_version.clone();
        let (r, _) = reconciler(&fake);
        r.sync_status(&mut stored, dep, classes)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let got = stored_pool(&fake);
        assert_eq!(got.status.as_ref(), Some(&want), "{name}");
        assert_eq!(
            got.metadata.resource_version != rv,
            want_write,
            "{name}: wrote status"
        );
    }
}

#[tokio::test]
async fn add_protection_finalizer_cases() {
    run_protection_finalizer_cases(vec![
        ProtectionFinalizerCase {
            name: "add to unprotected config",
            op: FinalizerOp::Add,
            sc: protected_sandbox_config(false, false),
            stale: false,
            want_patches: 1,
            want: true,
            want_conflict: false,
        },
        ProtectionFinalizerCase {
            name: "add to protected config: no patch",
            op: FinalizerOp::Add,
            sc: protected_sandbox_config(true, false),
            stale: false,
            want_patches: 0,
            want: true,
            want_conflict: false,
        },
        ProtectionFinalizerCase {
            name: "add to deleting config: no patch",
            op: FinalizerOp::Add,
            sc: protected_sandbox_config(false, true),
            stale: false,
            want_patches: 0,
            want: false,
            want_conflict: false,
        },
        ProtectionFinalizerCase {
            name: "add to stale config: conflict",
            op: FinalizerOp::Add,
            sc: protected_sandbox_config(false, false),
            stale: true,
            want_patches: 1,
            want: false,
            want_conflict: true,
        },
    ])
    .await;
}
