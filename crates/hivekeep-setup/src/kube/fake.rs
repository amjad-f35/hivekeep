//! An in-memory [`Cluster`] for tests: an object store, a discovery table,
//! a log of every request, and reactors that can intercept requests to
//! inject failures or canned answers.

use super::cluster::{Cluster, ExecOutput, Patch, Propagation, ResourceInfo};
use super::{Gvk, KubeError, Object, ObjectExt};
use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

/// One request the fake received.
#[derive(Clone, Debug)]
pub struct Action {
    /// `apply`, `get`, `list`, `create`, `delete`, `patch`, `resolve`.
    pub verb: String,
    pub gvk: Gvk,
    pub namespace: String,
    pub name: String,
    pub propagation: Option<Propagation>,
    pub patch: Option<Patch>,
    pub object: Option<Object>,
}

type ReactorFn = dyn Fn(&Action) -> Option<Result<Object, KubeError>> + Send + Sync;
type ExecFn = dyn Fn(&str, &str, &str, &[String], Option<&[u8]>) -> Result<ExecOutput, KubeError>
    + Send
    + Sync;

struct Reactor {
    verb: String,
    kind: String,
    f: Box<ReactorFn>,
}

type Key = (String, String, String, String);

/// Kinds that are cluster-scoped when the fake infers a resource.
const CLUSTER_SCOPED: &[&str] = &[
    "Namespace",
    "CustomResourceDefinition",
    "ClusterRole",
    "ClusterRoleBinding",
    "ClusterTrustBundle",
    "StorageClass",
    "CSIDriver",
    "PriorityClass",
    "ValidatingWebhookConfiguration",
    "MutatingWebhookConfiguration",
    "ValidatingAdmissionPolicy",
    "ValidatingAdmissionPolicyBinding",
    "Node",
    "PersistentVolume",
    "APIService",
    "RuntimeClass",
    "SandboxConfig",
    "CSIDriverConfig",
];

#[derive(Default)]
struct State {
    /// `None` means any gvk resolves, with an inferred plural and scope.
    resources: Option<Vec<ResourceInfo>>,
    objects: BTreeMap<Key, Object>,
    actions: Vec<Action>,
    reactors: Vec<Arc<Reactor>>,
    raw: BTreeMap<String, Vec<u8>>,
    server_version: String,
}

/// The in-memory cluster. Cheap to clone; clones share state.
#[derive(Clone, Default)]
pub struct FakeCluster {
    state: Arc<Mutex<State>>,
    exec: Arc<Mutex<Option<Box<ExecFn>>>>,
}

fn key(gvk: &Gvk, ns: &str, name: &str) -> Key {
    (
        gvk.group.clone(),
        gvk.kind.clone(),
        ns.to_string(),
        name.to_string(),
    )
}

/// Infers the plural resource name the way the API server's defaults do.
pub fn plural_of(kind: &str) -> String {
    let lower = kind.to_lowercase();
    if lower.ends_with('y') && !lower.ends_with("ey") {
        format!("{}ies", &lower[..lower.len() - 1])
    } else if lower.ends_with('s') || lower.ends_with("ch") || lower.ends_with('x') {
        format!("{lower}es")
    } else {
        format!("{lower}s")
    }
}

/// A resource entry with the inferred plural and scope.
pub fn inferred(gvk: &Gvk) -> ResourceInfo {
    ResourceInfo {
        gvk: gvk.clone(),
        plural: plural_of(&gvk.kind),
        namespaced: !CLUSTER_SCOPED.contains(&gvk.kind.as_str()),
    }
}

impl FakeCluster {
    pub fn new() -> Self {
        let f = Self::default();
        f.state.lock().server_version = "v1.36.0".into();
        f
    }

    /// A fake holding `objs`.
    pub fn with_objects(objs: impl IntoIterator<Item = Object>) -> Self {
        let f = Self::new();
        for o in objs {
            f.insert(o);
        }
        f
    }

    /// Restricts discovery to exactly these resources.
    pub fn set_resources(&self, resources: Vec<ResourceInfo>) {
        self.state.lock().resources = Some(resources);
    }

    pub fn set_server_version(&self, v: &str) {
        self.state.lock().server_version = v.to_string();
    }

    /// Serves `body` for a raw GET of `path`.
    pub fn set_raw(&self, path: &str, body: &[u8]) {
        self.state
            .lock()
            .raw
            .insert(path.to_string(), body.to_vec());
    }

    /// Answers pod execs with `f(namespace, pod, container, command, stdin)`.
    pub fn on_exec(
        &self,
        f: impl Fn(&str, &str, &str, &[String], Option<&[u8]>) -> Result<ExecOutput, KubeError>
            + Send
            + Sync
            + 'static,
    ) {
        *self.exec.lock() = Some(Box::new(f));
    }

    /// Stores an object directly, without recording an action.
    pub fn insert(&self, obj: Object) {
        let gvk = obj.gvk();
        let res = inferred(&gvk);
        let ns = res.effective_namespace(obj.obj_namespace()).to_string();
        self.state
            .lock()
            .objects
            .insert(key(&gvk, &ns, obj.name()), obj);
    }

    /// The stored object, if any.
    pub fn object(&self, gvk: &Gvk, namespace: &str, name: &str) -> Option<Object> {
        self.state
            .lock()
            .objects
            .get(&key(gvk, namespace, name))
            .cloned()
    }

    /// Every stored object of a kind, in key order.
    pub fn objects_of(&self, kind: &str) -> Vec<Object> {
        self.state
            .lock()
            .objects
            .iter()
            .filter(|(k, _)| k.1 == kind)
            .map(|(_, v)| v.clone())
            .collect()
    }

    pub fn actions(&self) -> Vec<Action> {
        self.state.lock().actions.clone()
    }

    pub fn clear_actions(&self) {
        self.state.lock().actions.clear();
    }

    /// Intercepts requests matching `verb` and `kind` (`"*"` matches any).
    /// Reactors added later run first; one returning `None` passes the
    /// request on.
    pub fn prepend_reactor(
        &self,
        verb: &str,
        kind: &str,
        f: impl Fn(&Action) -> Option<Result<Object, KubeError>> + Send + Sync + 'static,
    ) {
        self.state.lock().reactors.insert(
            0,
            Arc::new(Reactor {
                verb: verb.into(),
                kind: kind.into(),
                f: Box::new(f),
            }),
        );
    }

    fn record(&self, action: Action) -> Option<Result<Object, KubeError>> {
        // Reactors run without the lock held so they may inspect the fake.
        let matching: Vec<Arc<Reactor>> = {
            let mut st = self.state.lock();
            st.actions.push(action.clone());
            st.reactors
                .iter()
                .filter(|r| {
                    (r.verb == "*" || r.verb == action.verb)
                        && (r.kind == "*" || r.kind == action.gvk.kind)
                })
                .cloned()
                .collect()
        };
        for r in matching {
            if let Some(out) = (r.f)(&action) {
                return Some(out);
            }
        }
        None
    }

    fn action(verb: &str, res: &ResourceInfo, ns: &str, name: &str) -> Action {
        Action {
            verb: verb.into(),
            gvk: res.gvk.clone(),
            namespace: ns.into(),
            name: name.into(),
            propagation: None,
            patch: None,
            object: None,
        }
    }
}

/// Applies an RFC 7386 merge patch in place.
pub fn merge_patch(target: &mut Value, patch: &Value) {
    match patch {
        Value::Object(p) => {
            if !target.is_object() {
                *target = Value::Object(Default::default());
            }
            let t = target.as_object_mut().expect("object");
            for (k, v) in p {
                if v.is_null() {
                    t.remove(k);
                } else {
                    merge_patch(t.entry(k.clone()).or_insert(Value::Null), v);
                }
            }
        }
        other => *target = other.clone(),
    }
}

fn label_matches(obj: &Object, selector: Option<&str>) -> bool {
    let Some(sel) = selector else { return true };
    let labels = obj.labels();
    sel.split(',')
        .filter(|s| !s.is_empty())
        .all(|term| match term.split_once('=') {
            Some((k, v)) => labels.get(k.trim()).and_then(|x| x.as_str()) == Some(v.trim()),
            None => labels.contains_key(term.trim()),
        })
}

#[async_trait]
impl Cluster for FakeCluster {
    async fn resolve(&self, gvk: &Gvk) -> Result<ResourceInfo, KubeError> {
        let st = self.state.lock();
        match &st.resources {
            None => Ok(inferred(gvk)),
            Some(list) => list
                .iter()
                .find(|r| r.gvk == *gvk)
                .cloned()
                .ok_or_else(|| KubeError::no_match(gvk)),
        }
    }

    async fn group_versions(&self, group: &str) -> Result<Vec<String>, KubeError> {
        let st = self.state.lock();
        let mut out: Vec<String> = match &st.resources {
            None => vec![],
            Some(list) => list
                .iter()
                .filter(|r| r.gvk.group == group)
                .map(|r| r.gvk.version.clone())
                .collect(),
        };
        out.dedup();
        Ok(out)
    }

    async fn server_version(&self) -> Result<String, KubeError> {
        Ok(self.state.lock().server_version.clone())
    }

    async fn apply(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        obj: &Object,
    ) -> Result<Object, KubeError> {
        let ns = res.effective_namespace(namespace);
        let mut a = Self::action("apply", res, ns, obj.name());
        a.object = Some(obj.clone());
        if let Some(out) = self.record(a) {
            return out;
        }
        let mut stored = obj.clone();
        if !ns.is_empty() {
            stored.set_obj_namespace(ns);
        }
        let mut st = self.state.lock();
        let k = key(&res.gvk, ns, obj.name());
        if let Some(prev) = st.objects.get(&k) {
            if stored.get("status").is_none() {
                if let Some(status) = prev.get("status") {
                    stored["status"] = status.clone();
                }
            }
        }
        st.objects.insert(k, stored.clone());
        Ok(stored)
    }

    async fn get(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        name: &str,
    ) -> Result<Object, KubeError> {
        let ns = res.effective_namespace(namespace);
        if let Some(out) = self.record(Self::action("get", res, ns, name)) {
            return out;
        }
        self.state
            .lock()
            .objects
            .get(&key(&res.gvk, ns, name))
            .cloned()
            .ok_or_else(|| KubeError::not_found(&res.plural, name))
    }

    async fn list(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        label_selector: Option<&str>,
    ) -> Result<Vec<Object>, KubeError> {
        let ns = if res.namespaced { namespace } else { "" };
        if let Some(out) = self.record(Self::action("list", res, ns, "")) {
            return out.map(|v| match v {
                Value::Array(items) => items,
                other => other
                    .get("items")
                    .and_then(|i| i.as_array())
                    .cloned()
                    .unwrap_or_default(),
            });
        }
        let st = self.state.lock();
        Ok(st
            .objects
            .iter()
            .filter(|(k, _)| {
                k.0 == res.gvk.group && k.1 == res.gvk.kind && (ns.is_empty() || k.2 == ns)
            })
            .filter(|(_, v)| label_matches(v, label_selector))
            .map(|(_, v)| v.clone())
            .collect())
    }

    async fn create(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        obj: &Object,
    ) -> Result<Object, KubeError> {
        let ns = res.effective_namespace(namespace);
        let mut a = Self::action("create", res, ns, obj.name());
        a.object = Some(obj.clone());
        if let Some(out) = self.record(a) {
            return out;
        }
        let mut st = self.state.lock();
        let k = key(&res.gvk, ns, obj.name());
        if st.objects.contains_key(&k) {
            return Err(KubeError::api(
                409,
                "AlreadyExists",
                format!("{} {:?} already exists", res.plural, obj.name()),
            ));
        }
        let mut stored = obj.clone();
        if !ns.is_empty() {
            stored.set_obj_namespace(ns);
        }
        st.objects.insert(k, stored.clone());
        Ok(stored)
    }

    async fn delete(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        name: &str,
        propagation: Option<Propagation>,
    ) -> Result<(), KubeError> {
        let ns = res.effective_namespace(namespace);
        let mut a = Self::action("delete", res, ns, name);
        a.propagation = propagation;
        if let Some(out) = self.record(a) {
            return out.map(|_| ());
        }
        match self.state.lock().objects.remove(&key(&res.gvk, ns, name)) {
            Some(_) => Ok(()),
            None => Err(KubeError::not_found(&res.plural, name)),
        }
    }

    async fn patch(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        name: &str,
        patch: &Patch,
    ) -> Result<Object, KubeError> {
        let ns = res.effective_namespace(namespace);
        let mut a = Self::action("patch", res, ns, name);
        a.patch = Some(patch.clone());
        if let Some(out) = self.record(a) {
            return out;
        }
        let mut st = self.state.lock();
        let Some(obj) = st.objects.get_mut(&key(&res.gvk, ns, name)) else {
            return Err(KubeError::not_found(&res.plural, name));
        };
        match patch {
            Patch::Merge(p) | Patch::Strategic(p) => merge_patch(obj, p),
        }
        Ok(obj.clone())
    }

    async fn raw_get(&self, path: &str) -> Result<Vec<u8>, KubeError> {
        self.state
            .lock()
            .raw
            .get(path)
            .cloned()
            .ok_or_else(|| KubeError::not_found("path", path))
    }

    async fn exec(
        &self,
        namespace: &str,
        pod: &str,
        container: &str,
        command: &[String],
        stdin: Option<Vec<u8>>,
    ) -> Result<ExecOutput, KubeError> {
        match &*self.exec.lock() {
            Some(f) => f(namespace, pod, container, command, stdin.as_deref()),
            None => Err(KubeError::Other(format!(
                "exec in pod {namespace}/{pod}: no exec handler"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plurals() {
        assert_eq!(plural_of("Deployment"), "deployments");
        assert_eq!(plural_of("NetworkPolicy"), "networkpolicies");
        assert_eq!(plural_of("Ingress"), "ingresses");
        assert_eq!(plural_of("ClusterTrustBundle"), "clustertrustbundles");
    }

    #[test]
    fn merge_patch_removes_nulls() {
        let mut v = serde_json::json!({"a": {"b": 1, "c": 2}});
        merge_patch(&mut v, &serde_json::json!({"a": {"b": null, "d": 3}}));
        assert_eq!(v, serde_json::json!({"a": {"c": 2, "d": 3}}));
    }
}
