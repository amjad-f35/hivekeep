//! Per-test namespaces.
//!
//! [`create_namespace`] makes a randomly named, labeled namespace and
//! deletes it as soon as its test passes: each one usually holds a
//! WorkerPool, and keeping every pool alive until the run ends starves the
//! tests still to come of node capacity. A failed test keeps its namespace,
//! because the worker pods in it hold the logs (and, for micro-VM workers,
//! the guest console) that explain the failure. Leftovers carry
//! [`NAMESPACE_LABEL`], so `kubectl delete ns -l hivekeep.io/e2e` finds them.

use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Namespace as K8sNamespace;
use kube::api::{Api, DeleteParams, PostParams};
use parking_lot::Mutex;
use rand::Rng;

use crate::harness::T;
use crate::kube_util::is_not_found;

/// Marks the namespaces the suites create.
pub const NAMESPACE_LABEL: &str = "hivekeep.io/e2e";

static REGISTERED: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// A random name of the form `aaaa-ddd`.
pub fn random_namespace_name() -> String {
    const LETTERS: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    const DIGITS: &[u8] = b"0123456789";
    let mut rng = rand::rng();
    let mut s = String::with_capacity(8);
    for _ in 0..4 {
        s.push(LETTERS[rng.random_range(0..LETTERS.len())] as char);
    }
    s.push('-');
    for _ in 0..3 {
        s.push(DIGITS[rng.random_range(0..DIGITS.len())] as char);
    }
    s
}

/// A namespace created for one test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    /// Its name.
    pub name: String,
}

fn reserve_name() -> Option<String> {
    let mut registered = REGISTERED.lock();
    for _ in 0..1000 {
        let name = random_namespace_name();
        if !registered.contains(&name) {
            registered.push(name.clone());
            return Some(name);
        }
    }
    None
}

fn unregister(name: &str) {
    REGISTERED.lock().retain(|n| n != name);
}

/// The namespaces created and not yet released.
pub fn registered() -> Vec<String> {
    REGISTERED.lock().clone()
}

/// Creates a fresh labeled namespace and waits for it to be active. It is
/// deleted when `t` passes and kept when it fails.
pub async fn create_namespace(t: &T) -> Namespace {
    let name = reserve_name().unwrap_or_else(|| t.fatal("Failed to create unique namespace name."));
    t.log(format!("Creating namespace: {name}"));
    let api: Api<K8sNamespace> = Api::all(crate::suite::clients().kube());
    let mut ns = K8sNamespace::default();
    ns.metadata.name = Some(name.clone());
    ns.metadata.labels = Some([(NAMESPACE_LABEL.to_string(), "true".to_string())].into());
    if let Err(e) = api.create(&PostParams::default(), &ns).await {
        t.fatal(format!("Failed to create namespace {name}: {e}"));
    }
    let n = name.clone();
    t.cleanup(move |t| async move {
        if !t.failed() {
            delete_namespace(&n).await;
        }
    });

    let timeout = Duration::from_secs(60);
    let deadline = Instant::now() + timeout;
    loop {
        let got = api.get(&name).await;
        if let Ok(ns) = &got {
            if ns.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Active") {
                break;
            }
        }
        if Instant::now() > deadline {
            t.fatal(format!(
                "Timed out waiting for namespace {name:?} to be active after {timeout:?}: {:?}",
                got.err()
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Namespace { name }
}

/// Requests deletion of `name` and unregisters it, without waiting for it
/// to finish terminating: the point is to hand the node back, and blocking
/// here would serialize parallel subtests behind each other's teardown.
pub async fn delete_namespace(name: &str) {
    unregister(name);
    let api: Api<K8sNamespace> = Api::all(crate::suite::clients().kube());
    match tokio::time::timeout(
        Duration::from_secs(30),
        api.delete(name, &DeleteParams::default()),
    )
    .await
    {
        Ok(Err(e)) if !is_not_found(&e) => eprintln!("Failed to delete namespace {name}: {e}"),
        Err(_) => eprintln!("Failed to delete namespace {name}: timed out"),
        _ => {}
    }
}

/// Reports the namespaces still registered and leaves them in place, for a
/// run that failed: what is left is exactly the failures' namespaces.
pub fn retain_namespaces() {
    let mut registered = REGISTERED.lock();
    if registered.is_empty() {
        return;
    }
    println!(
        "Tests failed: keeping {} namespace(s) for diagnosis: {}",
        registered.len(),
        registered.join(" ")
    );
    println!("Delete them (and any from earlier failed runs) with: kubectl delete ns -l {NAMESPACE_LABEL}");
    registered.clear();
}

/// Deletes every namespace still registered: a backstop for tests whose
/// own cleanup never ran.
pub async fn cleanup_namespaces() {
    let names = std::mem::take(&mut *REGISTERED.lock());
    if names.is_empty() {
        return;
    }
    let api: Api<K8sNamespace> = Api::all(crate::suite::clients().kube());
    println!("Cleaning up {} namespaces...", names.len());
    for ns in names {
        println!("Deleting namespace {ns}...");
        if let Err(e) = api.delete(&ns, &DeleteParams::default()).await {
            eprintln!("Failed to delete namespace {ns}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_have_the_documented_shape() {
        let re = regex::Regex::new("^[a-z]{4}-[0-9]{3}$").unwrap();
        for _ in 0..100 {
            let n = random_namespace_name();
            assert!(re.is_match(&n), "{n}");
        }
        let a = reserve_name().unwrap();
        assert!(registered().contains(&a));
        unregister(&a);
        assert!(!registered().contains(&a));
    }
}
