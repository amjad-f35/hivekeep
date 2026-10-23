//! How a WorkerPool resolves its SandboxConfig: which config the
//! controller picks, how the pick shows in the pool's status and events,
//! and how the protection finalizer holds a config until no pool names it.
//!
//! The pools run no workers (zero replicas): worker pods take nothing from
//! the config, so resolution is all there is to test. The class defaults
//! created here are cluster-scoped, so every pool of the class without a
//! configRef follows them too, and back; each is a copy of the installed
//! default's spec under another name, so nothing those pools run changes.

mod ops_support;

use std::time::{Duration, Instant};

use hivekeep_e2e::crd::{
    new_worker_pool, sandbox_config_resource, worker_pool_resource, SandboxClassEntry,
    WorkerPoolSpec,
};
use hivekeep_e2e::harness::T;
use hivekeep_e2e::kube_util::is_not_found;
use hivekeep_e2e::namespace::create_namespace;
use hivekeep_e2e::sandbox::{is_microvm, SANDBOX_CLASS_GVISOR, SANDBOX_CLASS_MICROVM};
use hivekeep_e2e::suite::{clients, e2e_test};
use k8s_openapi::api::events::v1::Event;
use kube::api::{Api, DeleteParams, DynamicObject, ListParams, Patch, PatchParams, PostParams};
use ops_support::*;
use serde_json::{json, Value};

/// Marks a SandboxConfig as its class's default.
const CLASS_DEFAULT_ANNOTATION: &str = "sandboxconfig.hivekeep.io/is-class-default";
/// Reason of the Normal event emitted each time a pool's config changes.
const EVENT_REASON_SWITCHED: &str = "SandboxConfigSwitched";
/// The pool status condition reporting a resolved class.
const CONDITION_RESOLVED: &str = "Resolved";
/// Bounds each wait on the controller: a switch takes a few reconciles, and
/// deleting a config in use also waits for the protection reconciler.
const STEP_TIMEOUT: Duration = Duration::from_secs(60);

fn configs() -> Api<DynamicObject> {
    Api::all_with(clients().kube(), &sandbox_config_resource())
}

fn annotation(obj: &DynamicObject, key: &str) -> Option<String> {
    obj.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(key).cloned())
}

/// The config a pool of `class` with no configRef resolves to now: the
/// newest of the class annotated as default (by creation time, then name),
/// the rule the controller applies.
async fn newest_class_default(t: &T, class: &str) -> DynamicObject {
    let list = configs()
        .list(&ListParams::default())
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to list SandboxConfigs: {e}")));
    let mut best: Option<DynamicObject> = None;
    for sc in list.items {
        let sc_class = sc.data["spec"]["sandboxClass"].as_str().unwrap_or_default();
        if sc_class != class
            || annotation(&sc, CLASS_DEFAULT_ANNOTATION).as_deref() != Some("true")
            || sc.metadata.deletion_timestamp.is_some()
        {
            continue;
        }
        let newer = match &best {
            None => true,
            Some(b) => {
                let (bt, st) = (
                    &b.metadata.creation_timestamp,
                    &sc.metadata.creation_timestamp,
                );
                bt < st || (bt == st && sc.metadata.name < b.metadata.name)
            }
        };
        if newer {
            best = Some(sc);
        }
    }
    best.unwrap_or_else(|| t.fatal(format!("no SandboxConfig of class {class:?} is annotated {CLASS_DEFAULT_ANNOTATION}=true; the install ships one")))
}

/// Sets or removes the class-default annotation.
async fn set_class_default(t: &T, name: &str, is_default: bool) {
    let value = if is_default {
        json!("true")
    } else {
        Value::Null
    };
    let patch = json!({ "metadata": { "annotations": { CLASS_DEFAULT_ANNOTATION: value } } });
    configs()
        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to patch SandboxConfig {name}: {e}")));
    t.log(format!(
        "SandboxConfig {name}: {CLASS_DEFAULT_ANNOTATION}={value}"
    ));
}

/// Whether a pool status entry reports `want`, resolved, nothing pending.
fn uses_config(entry: &Value, want: &str) -> bool {
    let resolved = entry["conditions"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|c| c["type"] == CONDITION_RESOLVED && c["status"] == "True");
    entry["configRef"]["name"].as_str() == Some(want)
        && entry.get("pendingConfigRef").is_none_or(Value::is_null)
        && resolved
}

/// Blocks until the pool's status reports its class resolved to `want`.
async fn wait_for_sandbox_config(t: &T, ns: &str, pool: &str, class: &str, want: &str) {
    let pools: Api<DynamicObject> =
        Api::namespaced_with(clients().kube(), ns, &worker_pool_resource());
    let deadline = Instant::now() + STEP_TIMEOUT;
    let mut last = Value::Null;
    let mut last_err = None;
    loop {
        match pools.get(pool).await {
            Ok(wp) => {
                last = wp.data["status"]["sandboxClasses"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .rfind(|e| e["name"] == class)
                    .cloned()
                    .unwrap_or(Value::Null);
                if uses_config(&last, want) {
                    t.log(format!("WorkerPool {ns}/{pool} uses SandboxConfig {want}"));
                    return;
                }
            }
            Err(e) => last_err = Some(e.to_string()),
        }
        if Instant::now() > deadline {
            t.fatal(format!("timed out after {STEP_TIMEOUT:?} waiting for WorkerPool {ns}/{pool} to use SandboxConfig {want:?} (last status {last}, err {last_err:?})"));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Blocks until the config is gone, which for one a pool used means its
/// protection finalizer was released.
async fn wait_for_sandbox_config_gone(t: &T, name: &str) {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let state = match configs().get(name).await {
            Err(e) if is_not_found(&e) => {
                t.log(format!("SandboxConfig {name} is gone"));
                return;
            }
            Err(e) => e.to_string(),
            Ok(sc) => format!(
                "deletionTimestamp {:?}, finalizers {:?}",
                sc.metadata.deletion_timestamp, sc.metadata.finalizers
            ),
        };
        if Instant::now() > deadline {
            t.fatal(format!("timed out after {STEP_TIMEOUT:?} waiting for SandboxConfig {name} to be deleted ({state})"));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// When an event was emitted: its eventTime, else its creation time.
fn event_time(ev: &Event) -> String {
    ev.event_time
        .as_ref()
        .map(|m| m.0.to_string())
        .or_else(|| {
            ev.metadata
                .creation_timestamp
                .as_ref()
                .map(|c| c.0.to_string())
        })
        .unwrap_or_default()
}

/// The events regarding the pool with UID `uid`, oldest first.
async fn pool_events(ns: &str, uid: &str) -> Result<Vec<Event>, String> {
    let list = Api::<Event>::namespaced(clients().kube(), ns)
        .list(&ListParams::default())
        .await
        .map_err(|e| e.to_string())?;
    let mut events: Vec<Event> = list
        .items
        .into_iter()
        .filter(|e| e.regarding.as_ref().and_then(|r| r.uid.as_deref()) == Some(uid))
        .collect();
    events.sort_by_key(|e| {
        let ts = e
            .event_time
            .as_ref()
            .map(|m| m.0)
            .or_else(|| e.metadata.creation_timestamp.as_ref().map(|c| c.0));
        ts.map(|t| t.as_nanosecond()).unwrap_or_default()
    });
    Ok(events)
}

/// Blocks until an event regarding the pool carries `note`; events are
/// recorded asynchronously and can trail the status write they report.
async fn wait_for_event(t: &T, ns: &str, pool: &str, uid: &str, note: &str) {
    let deadline = Instant::now() + STEP_TIMEOUT;
    let mut seen = Vec::new();
    let mut last_err = None;
    loop {
        match pool_events(ns, uid).await {
            Ok(events) => {
                seen.clear();
                for ev in &events {
                    let (ty, reason, n) = (
                        ev.type_.clone().unwrap_or_default(),
                        ev.reason.clone().unwrap_or_default(),
                        ev.note.clone().unwrap_or_default(),
                    );
                    if n == note {
                        t.log(format!("Event: {ty} {reason} {n}"));
                        return;
                    }
                    seen.push(format!("{ty} {reason} {n} ({})", event_time(ev)));
                }
            }
            Err(e) => last_err = Some(e),
        }
        if Instant::now() > deadline {
            t.fatal(format!("timed out after {STEP_TIMEOUT:?} waiting for event {note:?} on WorkerPool {ns}/{pool} (seen: {seen:?}, err {last_err:?})"));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn uses_note(class: &str, name: &str) -> String {
    format!("sandbox class {class:?} uses SandboxConfig {name:?}")
}

fn switched_note(class: &str, from: &str, to: &str) -> String {
    format!("sandbox class {class:?} switched from SandboxConfig {from:?} to {to:?}")
}

/// A pool with no configRef follows its class default, the newest config
/// of its class annotated as default. From the installed default, a newer
/// default is created, un-annotated, re-annotated, and deleted while in use;
/// the pool's status follows each step, and its events record every move
/// exactly once.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn worker_pool_follows_class_default() {
    e2e_test("TestWorkerPoolFollowsClassDefault", |t| async move {
        let class = if is_microvm() {
            SANDBOX_CLASS_MICROVM
        } else {
            SANDBOX_CLASS_GVISOR
        };
        let base = newest_class_default(&t, class).await;
        let base_name = base.metadata.name.clone().unwrap_or_default();
        t.log(format!("Class {class:?} default as installed: {base_name}"));

        // 1. A pool with no configRef resolves to the installed default.
        let ns = create_namespace(&t).await.name;
        let pool_name = "follows-default";
        let spec = WorkerPoolSpec {
            replicas: 0,
            // Never pulled: the pool runs no workers.
            worker_image: "worker:unused".into(),
            template: None,
            sandbox_classes: vec![SandboxClassEntry {
                name: class.into(),
                config_ref: None,
            }],
        };
        let pool =
            Api::<DynamicObject>::namespaced_with(clients().kube(), &ns, &worker_pool_resource())
                .create(
                    &PostParams::default(),
                    &new_worker_pool(&ns, pool_name, Default::default(), &spec),
                )
                .await
                .unwrap_or_else(|e| t.fatal(format!("failed to create WorkerPool: {e}")));
        let uid = pool.metadata.uid.clone().unwrap_or_default();
        wait_for_sandbox_config(&t, &ns, pool_name, class, &base_name).await;
        wait_for_event(&t, &ns, pool_name, &uid, &uses_note(class, &base_name)).await;

        // 2. A newer default appears and the pool moves to it. Its name
        // sorts before the installed one's, the tie-break should both carry
        // the same creation second.
        let v2 = format!("e2e-{class}-default-{ns}");
        let mut obj = DynamicObject::new(&v2, &sandbox_config_resource());
        obj.metadata.annotations =
            Some([(CLASS_DEFAULT_ANNOTATION.to_string(), "true".to_string())].into());
        obj.data = json!({ "spec": base.data["spec"].clone() });
        configs()
            .create(&PostParams::default(), &obj)
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to create SandboxConfig {v2}: {e}")));
        // Always, pass or fail: a class default left behind moves every pool
        // of the class in the cluster.
        let v2c = v2.clone();
        t.cleanup(move |t| async move {
            match tokio::time::timeout(
                Duration::from_secs(60),
                configs().delete(&v2c, &DeleteParams::default()),
            )
            .await
            {
                Ok(Err(e)) if !is_not_found(&e) => {
                    t.error(format!("failed to delete SandboxConfig {v2c}: {e}"))
                }
                Err(_) => t.error(format!("failed to delete SandboxConfig {v2c}: timed out")),
                _ => {}
            }
        });
        wait_for_sandbox_config(&t, &ns, pool_name, class, &v2).await;
        wait_for_event(
            &t,
            &ns,
            pool_name,
            &uid,
            &switched_note(class, &base_name, &v2),
        )
        .await;

        // 3. The newer config stops being a default: the pool moves back.
        set_class_default(&t, &v2, false).await;
        wait_for_sandbox_config(&t, &ns, pool_name, class, &base_name).await;
        wait_for_event(
            &t,
            &ns,
            pool_name,
            &uid,
            &switched_note(class, &v2, &base_name),
        )
        .await;

        // 4. It becomes a default again and is deleted while in use. A
        // default being deleted is never picked, so the pool moves back, and
        // only then does the protection finalizer let it go.
        set_class_default(&t, &v2, true).await;
        wait_for_sandbox_config(&t, &ns, pool_name, class, &v2).await;
        wait_for_event(
            &t,
            &ns,
            pool_name,
            &uid,
            &switched_note(class, &base_name, &v2),
        )
        .await;
        configs()
            .delete(&v2, &DeleteParams::default())
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to delete SandboxConfig {v2}: {e}")));
        wait_for_sandbox_config(&t, &ns, pool_name, class, &base_name).await;
        wait_for_sandbox_config_gone(&t, &v2).await;
        wait_for_event(
            &t,
            &ns,
            pool_name,
            &uid,
            &switched_note(class, &v2, &base_name),
        )
        .await;

        // 5. The events are the pool's config history, each move once; a
        // move reported twice in one reconcile would show as a series.
        let want = vec![
            uses_note(class, &base_name),
            switched_note(class, &base_name, &v2),
            switched_note(class, &v2, &base_name),
            switched_note(class, &base_name, &v2),
            switched_note(class, &v2, &base_name),
        ];
        let events = pool_events(&ns, &uid)
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to list events: {e}")));
        let mut got = Vec::new();
        for ev in &events {
            let note = ev.note.clone().unwrap_or_default();
            if ev.type_.as_deref() != Some("Normal")
                || ev.reason.as_deref() != Some(EVENT_REASON_SWITCHED)
            {
                t.error(format!(
                    "unexpected {:?} {:?} event: {note}",
                    ev.type_, ev.reason
                ));
            }
            if let Some(series) = &ev.series {
                t.error(format!(
                    "event recorded {} times, want once: {note}",
                    series.count
                ));
            }
            got.push(note);
        }
        if got != want {
            t.error(format!(
                "events regarding the pool, oldest first:\n  got:\n    {}\n  want:\n    {}",
                got.join("\n    "),
                want.join("\n    ")
            ));
        }
    });
}

#[test]
fn notes_and_resolution() {
    assert_eq!(
        uses_note("gvisor", "a"),
        r#"sandbox class "gvisor" uses SandboxConfig "a""#
    );
    assert_eq!(
        switched_note("gvisor", "a", "b"),
        r#"sandbox class "gvisor" switched from SandboxConfig "a" to "b""#
    );
    let entry = json!({"name": "gvisor", "configRef": {"name": "a"}, "conditions": [{"type": "Resolved", "status": "True"}]});
    assert!(uses_config(&entry, "a"));
    assert!(!uses_config(&entry, "b"));
    let pending = json!({"name": "gvisor", "configRef": {"name": "a"}, "pendingConfigRef": {"name": "b"}, "conditions": [{"type": "Resolved", "status": "True"}]});
    assert!(!uses_config(&pending, "a"));
    let _ = NEEDS_CLUSTER;
}
