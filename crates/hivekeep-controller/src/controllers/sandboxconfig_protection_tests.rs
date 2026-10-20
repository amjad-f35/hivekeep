use hivekeep_k8s::crd::{SandboxClass, WorkerPoolStatus};
use hivekeep_k8s::fake::{FakeApiServer, Reaction};

use super::super::test_support::*;
use super::*;

const GV: SandboxClass = SandboxClass::Gvisor;

/// A pool with one status class per name, using that config ("" for none).
fn using(names: &[&str]) -> WorkerPool {
    let mut wp = make_worker_pool("pool", "default", 1, "worker:v1");
    wp.status = Some(WorkerPoolStatus {
        sandbox_classes: names
            .iter()
            .map(|n| class_status(GV, (!n.is_empty()).then_some(*n), None))
            .collect(),
        ..Default::default()
    });
    wp
}

/// A pool using "other" and switching to `name`.
fn claiming(name: &str) -> WorkerPool {
    let mut wp = using(&["other"]);
    wp.status.as_mut().unwrap().sandbox_classes[0].pending_config_ref =
        Some(hivekeep_k8s::crd::SandboxConfigReference::new(name));
    wp
}

/// A config with the protection finalizer if `protected`; a deleting one
/// also carries a hold finalizer, as the API server keeps no deleting
/// object without one.
fn config(protected: bool, deleting: bool) -> SandboxConfig {
    let mut sc = make_sandbox_config("sc", GV, true);
    if protected {
        sc = with_finalizer(sc, WORKER_POOL_PROTECTION_FINALIZER);
    }
    if deleting {
        sc = with_finalizer(sc, TEST_HOLD_FINALIZER);
        sc.metadata.deletion_timestamp = Some(super::super::meta::now());
    }
    sc
}

fn fail_list(fake: &FakeApiServer) {
    fake.prepend_reactor("list", "workerpools", |_| {
        Reaction::fail(500, "InternalError", "list failed")
    });
}

#[tokio::test]
async fn sandbox_config_protection_reconcile() {
    struct Case {
        name: &'static str,
        sc: SandboxConfig,
        cached: Vec<WorkerPool>,
        live: Vec<WorkerPool>,
        cached_err: bool,
        live_err: bool,
        want: bool,
        want_err: bool,
    }
    let c = |name, sc, cached: Vec<WorkerPool>, live: Vec<WorkerPool>, want| Case {
        name,
        sc,
        cached,
        live,
        cached_err: false,
        live_err: false,
        want,
        want_err: false,
    };
    let cases = vec![
        c(
            "live and unused: left alone",
            config(false, false),
            vec![],
            vec![],
            false,
        ),
        c(
            "live and used but unprotected: left alone, adding is the WorkerPool controller's job",
            config(false, false),
            vec![using(&["sc"])],
            vec![],
            false,
        ),
        c(
            "live and protected: keep",
            config(true, false),
            vec![],
            vec![],
            true,
        ),
        c(
            "deleting, used in cache: keep",
            config(true, true),
            vec![using(&["sc"])],
            vec![],
            true,
        ),
        c(
            "deleting, unused in cache but used live: keep",
            config(true, true),
            vec![],
            vec![using(&["sc"])],
            true,
        ),
        c(
            "deleting, used by a later sandbox class in cache: keep",
            config(true, true),
            vec![using(&["other", "sc"])],
            vec![],
            true,
        ),
        c(
            "deleting, used by a later sandbox class live: keep",
            config(true, true),
            vec![],
            vec![using(&["other", "sc"])],
            true,
        ),
        c(
            "deleting, claimed in cache: keep",
            config(true, true),
            vec![claiming("sc")],
            vec![],
            true,
        ),
        c(
            "deleting, unclaimed in cache but claimed live: keep",
            config(true, true),
            vec![],
            vec![claiming("sc")],
            true,
        ),
        c(
            "deleting and unused: remove",
            config(true, true),
            vec![using(&["other"])],
            vec![using(&["other"])],
            false,
        ),
        c(
            "deleting, only a pool that has no config in use: remove",
            config(true, true),
            vec![using(&[""])],
            vec![using(&[""])],
            false,
        ),
        c(
            "deleting without the finalizer: nothing to do",
            config(false, true),
            vec![],
            vec![],
            false,
        ),
        Case {
            cached_err: true,
            want_err: true,
            ..c(
                "deleting, cached list fails: keep and return the error",
                config(true, true),
                vec![],
                vec![],
                true,
            )
        },
        Case {
            live_err: true,
            want_err: true,
            ..c(
                "deleting, unused in cache, live list fails: keep and return the error",
                config(true, true),
                vec![using(&["other"])],
                vec![],
                true,
            )
        },
    ];
    for tc in cases {
        let cache = FakeApiServer::new();
        cache.insert(&tc.sc);
        for wp in &tc.cached {
            cache.insert(wp);
        }
        if tc.cached_err {
            fail_list(&cache);
        }
        let live = FakeApiServer::new();
        for wp in &tc.live {
            live.insert(wp);
        }
        if tc.live_err {
            fail_list(&live);
        }
        let r = SandboxConfigProtectionReconciler::new(
            cache.client(),
            live.client(),
            Reader::live_cluster(cache.client()),
            Reader::live_namespaced(cache.client()),
        );
        let res = r.reconcile(&ObjectKey::cluster("sc")).await;
        match (&res, tc.want_err) {
            (Ok(_), false) => {}
            (Err(e), true) => assert!(e.to_string().contains("list failed"), "{}: {e}", tc.name),
            (r, _) => panic!("{}: unexpected result {r:?}", tc.name),
        }
        let got = cache
            .get::<SandboxConfig>("", "sc")
            .unwrap_or_else(|| panic!("{}: config gone", tc.name));
        assert_eq!(
            has_finalizer(&got.metadata, WORKER_POOL_PROTECTION_FINALIZER),
            tc.want,
            "{}",
            tc.name
        );
        if has_finalizer(&tc.sc.metadata, TEST_HOLD_FINALIZER) {
            assert!(
                has_finalizer(&got.metadata, TEST_HOLD_FINALIZER),
                "{}: lost the hold finalizer",
                tc.name
            );
        }
    }
}

#[tokio::test]
async fn sandbox_config_protection_reconcile_get() {
    for (name, get_fails) in [
        ("not found: nothing to release", false),
        ("get fails: return the error", true),
    ] {
        let fake = FakeApiServer::new();
        if get_fails {
            fake.prepend_reactor("get", "sandboxconfigs", |_| {
                Reaction::fail(500, "InternalError", "get failed")
            });
        }
        let r = SandboxConfigProtectionReconciler::live(fake.client());
        match r.reconcile(&ObjectKey::cluster("missing")).await {
            Ok(action) => {
                assert!(!get_fails, "{name}: no error");
                assert_eq!(action, Action::Done, "{name}");
            }
            Err(e) => {
                assert!(get_fails, "{name}: {e}");
                assert!(e.to_string().contains("get failed"), "{name}: {e}");
            }
        }
    }
}

#[tokio::test]
async fn remove_protection_finalizer_cases() {
    run_protection_finalizer_cases(vec![
        ProtectionFinalizerCase {
            name: "remove from protected config",
            op: FinalizerOp::Remove,
            sc: protected_sandbox_config(true, false),
            stale: false,
            want_patches: 1,
            want: false,
            want_conflict: false,
        },
        ProtectionFinalizerCase {
            name: "remove from deleting config",
            op: FinalizerOp::Remove,
            sc: protected_sandbox_config(true, true),
            stale: false,
            want_patches: 1,
            want: false,
            want_conflict: false,
        },
        ProtectionFinalizerCase {
            name: "remove from unprotected config: no patch",
            op: FinalizerOp::Remove,
            sc: protected_sandbox_config(false, false),
            stale: false,
            want_patches: 0,
            want: false,
            want_conflict: false,
        },
        ProtectionFinalizerCase {
            name: "remove from stale config: conflict",
            op: FinalizerOp::Remove,
            sc: protected_sandbox_config(true, false),
            stale: true,
            want_patches: 1,
            want: true,
            want_conflict: true,
        },
    ])
    .await;
}

#[test]
fn worker_pool_events_map_to_configs_in_use() {
    let mut keys = sandbox_configs_for_worker_pool(&claiming("next"));
    keys.sort();
    assert_eq!(
        keys,
        vec![ObjectKey::cluster("next"), ObjectKey::cluster("other")]
    );
    assert!(sandbox_configs_for_worker_pool(&make_worker_pool("p", "ns", 1, "w")).is_empty());
}
