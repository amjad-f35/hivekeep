//! An in-memory Kubernetes API server for tests.
//!
//! [`FakeApiServer`] is a `tower::Service` that a real [`kube::Client`]
//! talks to, so code under test exercises the same request paths, typed
//! (de)serialization and watch streams it uses against a cluster. It keeps
//! objects as JSON keyed by API group, resource, namespace and name, and
//! implements the parts of API server behaviour controllers depend on:
//!
//! - get, list, watch (with label and field selectors), create (with
//!   `generateName`), update, delete and delete-collection;
//! - JSON merge, JSON, strategic merge (approximated) and server-side apply
//!   patches, the latter tracking which fields each manager applied;
//! - the `status` subresource: main-resource writes keep status, status
//!   writes change nothing else;
//! - resourceVersion optimistic concurrency, UID/resourceVersion delete
//!   preconditions, finalizers and deletionTimestamp, and the rule that a
//!   deleting object takes no new finalizers;
//! - discovery documents for the group versions a test declares served.
//!
//! Like a recording fake clientset, it records every request as an
//! [`Action`] and lets a test prepend reactors that inject errors or
//! canned responses.

pub mod patch;
pub mod selector;

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::Frame;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full, StreamBody};
use kube::Resource;
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc};

use selector::{labels_of, FieldSelector, LabelSelector};

/// The response body type of the fake server.
pub type FakeBody = UnsyncBoxBody<Bytes, std::io::Error>;

/// Identifies a stored object. Versions of one group share storage, as on
/// a real API server.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey {
    /// API group ("" for core).
    pub group: String,
    /// Plural resource name.
    pub resource: String,
    /// Namespace ("" for cluster-scoped).
    pub namespace: String,
    /// Object name.
    pub name: String,
}

/// A request the server received.
#[derive(Debug, Clone)]
pub struct Action {
    /// get, list, watch, create, update, patch, delete, deletecollection,
    /// or discover (for a discovery document; `resource` is then the
    /// group version).
    pub verb: String,
    /// API group.
    pub group: String,
    /// API version.
    pub version: String,
    /// Plural resource name.
    pub resource: String,
    /// Subresource, e.g. `status`.
    pub subresource: Option<String>,
    /// Namespace from the path.
    pub namespace: Option<String>,
    /// Name from the path.
    pub name: Option<String>,
    /// Decoded query parameters.
    pub query: HashMap<String, String>,
    /// The request body, when it was JSON.
    pub body: Option<Value>,
    /// The request content type.
    pub content_type: Option<String>,
}

impl Action {
    /// Whether this action is `verb` on `resource`.
    pub fn is(&self, verb: &str, resource: &str) -> bool {
        self.verb == verb && self.resource == resource
    }
}

/// What a reactor decides about a request.
#[derive(Debug, Clone)]
pub enum Reaction {
    /// Let the next reactor, then the default behaviour, handle it.
    Pass,
    /// Replace the request body and continue with the default behaviour.
    Mutate(Value),
    /// Answer 200 with this body.
    Respond(Value),
    /// Fail with an API status error.
    Fail {
        /// HTTP status code.
        code: u16,
        /// Machine-readable reason, e.g. `Forbidden`.
        reason: String,
        /// Human-readable message.
        message: String,
    },
}

impl Reaction {
    /// A failure with `code` and `reason`.
    pub fn fail(code: u16, reason: &str, message: impl Into<String>) -> Self {
        Reaction::Fail {
            code,
            reason: reason.into(),
            message: message.into(),
        }
    }
}

type ReactorFn = dyn Fn(&Action) -> Reaction + Send + Sync;

/// A served resource, for discovery.
#[derive(Debug, Clone)]
pub struct ServedResource {
    /// Plural name.
    pub name: String,
    /// Kind.
    pub kind: String,
    /// Whether it is namespaced.
    pub namespaced: bool,
}

#[derive(Debug, Clone)]
struct Event {
    rv: u64,
    key: ObjectKey,
    kind: &'static str,
    object: Value,
}

#[derive(Default)]
struct State {
    objects: BTreeMap<ObjectKey, Value>,
    applied: HashMap<(ObjectKey, String, bool), Value>,
    rv: u64,
    log: Vec<Event>,
    discovery: BTreeMap<String, Vec<ServedResource>>,
    reactors: Vec<(String, String, Arc<ReactorFn>)>,
    actions: Vec<Action>,
    names: u64,
}

struct Inner {
    state: Mutex<State>,
    events: broadcast::Sender<Event>,
}

/// The in-memory API server. Cloning shares the server.
#[derive(Clone)]
pub struct FakeApiServer {
    inner: Arc<Inner>,
}

impl Default for FakeApiServer {
    fn default() -> Self {
        Self::new()
    }
}

/// A server error.
#[derive(Debug, Clone)]
struct ApiError {
    code: u16,
    reason: String,
    message: String,
}

impl ApiError {
    fn new(code: u16, reason: &str, message: impl Into<String>) -> Self {
        ApiError {
            code,
            reason: reason.into(),
            message: message.into(),
        }
    }
    fn not_found(key: &ObjectKey) -> Self {
        ApiError::new(
            404,
            "NotFound",
            format!("{} \"{}\" not found", key.resource, key.name),
        )
    }
    fn conflict(key: &ObjectKey, why: &str) -> Self {
        ApiError::new(
            409,
            "Conflict",
            format!(
                "Operation cannot be fulfilled on {} \"{}\": {why}",
                key.resource, key.name
            ),
        )
    }
}

type Result<T> = std::result::Result<T, ApiError>;

struct Request {
    method: http::Method,
    path: String,
    query: HashMap<String, String>,
    content_type: Option<String>,
    body: Option<Value>,
}

enum Route {
    ApiVersions,
    GroupList,
    Discovery(String),
    Resource {
        group: String,
        version: String,
        namespace: Option<String>,
        resource: String,
        name: Option<String>,
        sub: Option<String>,
    },
}

fn route(path: &str) -> Option<Route> {
    let segs: Vec<&str> = path.trim_matches('/').split('/').collect();
    let (group, version, rest) = match segs.as_slice() {
        ["api"] => return Some(Route::ApiVersions),
        ["apis"] => return Some(Route::GroupList),
        ["api", v] => return Some(Route::Discovery(v.to_string())),
        ["apis", g, v] => return Some(Route::Discovery(format!("{g}/{v}"))),
        ["api", v, rest @ ..] => (String::new(), v.to_string(), rest),
        ["apis", g, v, rest @ ..] => (g.to_string(), v.to_string(), rest),
        _ => return None,
    };
    let (namespace, rest) = match rest {
        ["namespaces", ns, r, tail @ ..]
            if !(group.is_empty() && matches!(*r, "status" | "finalize")) =>
        {
            (Some(ns.to_string()), {
                let mut v = vec![*r];
                v.extend_from_slice(tail);
                v
            })
        }
        other => (None, other.to_vec()),
    };
    let resource = rest.first()?.to_string();
    Some(Route::Resource {
        group,
        version,
        namespace,
        resource,
        name: rest.get(1).map(|s| s.to_string()),
        sub: rest.get(2).map(|s| s.to_string()),
    })
}

fn api_version(group: &str, version: &str) -> String {
    if group.is_empty() {
        version.to_string()
    } else {
        format!("{group}/{version}")
    }
}

fn meta_str<'a>(obj: &'a Value, field: &str) -> Option<&'a str> {
    obj.get("metadata")?.get(field)?.as_str()
}

fn finalizers(obj: &Value) -> Vec<String> {
    obj.pointer("/metadata/finalizers")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn set_meta(obj: &mut Value, field: &str, value: Value) {
    if !obj.get("metadata").is_some_and(Value::is_object) {
        obj["metadata"] = json!({});
    }
    obj["metadata"][field] = value;
}

fn remove_meta(obj: &mut Value, field: &str) {
    if let Some(m) = obj.get_mut("metadata").and_then(Value::as_object_mut) {
        m.remove(field);
    }
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// The parts of an object other than metadata and status.
fn spec_of(obj: &Value) -> Value {
    let mut v = obj.clone();
    if let Some(m) = v.as_object_mut() {
        m.remove("metadata");
        m.remove("status");
        m.remove("apiVersion");
        m.remove("kind");
    }
    v
}

/// The object without fields the server owns, for no-op detection.
fn comparable(obj: &Value) -> Value {
    let mut v = obj.clone();
    if let Some(m) = v.get_mut("metadata").and_then(Value::as_object_mut) {
        m.remove("resourceVersion");
        m.remove("generation");
        m.remove("managedFields");
    }
    if let Some(m) = v.as_object_mut() {
        m.remove("apiVersion");
    }
    v
}

impl FakeApiServer {
    /// An empty server that serves no discovery documents.
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(4096);
        FakeApiServer {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                events,
            }),
        }
    }

    /// A client talking to this server, with default namespace `default`.
    pub fn client(&self) -> kube::Client {
        kube::Client::new(self.clone(), "default")
    }

    /// Declares `group_version` served with `resources` (plural name, kind,
    /// namespaced) in discovery. Adds to what the group version already
    /// serves.
    pub fn serve(&self, group_version: &str, resources: &[(&str, &str, bool)]) {
        let mut st = self.inner.state.lock();
        let list = st.discovery.entry(group_version.to_string()).or_default();
        for (name, kind, namespaced) in resources {
            list.push(ServedResource {
                name: name.to_string(),
                kind: kind.to_string(),
                namespaced: *namespaced,
            });
        }
    }

    /// Puts `reactor` in front of the reactors for `verb` on `resource`
    /// (either may be `*`).
    pub fn prepend_reactor(
        &self,
        verb: &str,
        resource: &str,
        reactor: impl Fn(&Action) -> Reaction + Send + Sync + 'static,
    ) {
        self.inner
            .state
            .lock()
            .reactors
            .insert(0, (verb.into(), resource.into(), Arc::new(reactor)));
    }

    /// Every request received so far.
    pub fn actions(&self) -> Vec<Action> {
        self.inner.state.lock().actions.clone()
    }

    /// Forgets the recorded requests.
    pub fn clear_actions(&self) {
        self.inner.state.lock().actions.clear();
    }

    /// Stores `obj` as a create would (assigning UID, resourceVersion and
    /// creation time when unset) and returns the stored object.
    pub fn insert<K>(&self, obj: &K) -> K
    where
        K: Resource<DynamicType = ()> + Serialize + DeserializeOwned,
    {
        let mut v = serde_json::to_value(obj).expect("serializable object");
        v["apiVersion"] = json!(K::api_version(&()));
        v["kind"] = json!(K::kind(&()));
        let ns = meta_str(&v, "namespace").unwrap_or("").to_string();
        let stored = self
            .insert_json(&K::group(&()), &K::plural(&()), &ns, v)
            .expect("insert");
        serde_json::from_value(stored).expect("stored object decodes")
    }

    /// Stores a JSON object under `group`/`resource`/`namespace`.
    pub fn insert_json(
        &self,
        group: &str,
        resource: &str,
        namespace: &str,
        obj: Value,
    ) -> std::result::Result<Value, String> {
        let mut st = self.inner.state.lock();
        self.create_locked(&mut st, group, resource, namespace, obj)
            .map_err(|e| e.message)
    }

    /// The stored object, typed.
    pub fn get<K>(&self, namespace: &str, name: &str) -> Option<K>
    where
        K: Resource<DynamicType = ()> + DeserializeOwned,
    {
        let key = ObjectKey {
            group: K::group(&()).into_owned(),
            resource: K::plural(&()).into_owned(),
            namespace: namespace.into(),
            name: name.into(),
        };
        let st = self.inner.state.lock();
        let mut v = st.objects.get(&key)?.clone();
        v["apiVersion"] = json!(K::api_version(&()));
        Some(serde_json::from_value(v).expect("stored object decodes"))
    }

    /// Every stored object of `K`, in key order.
    pub fn list<K>(&self) -> Vec<K>
    where
        K: Resource<DynamicType = ()> + DeserializeOwned,
    {
        let (group, resource) = (K::group(&()), K::plural(&()));
        let st = self.inner.state.lock();
        st.objects
            .iter()
            .filter(|(k, _)| k.group == group && k.resource == resource)
            .map(|(_, v)| {
                let mut v = v.clone();
                v["apiVersion"] = json!(K::api_version(&()));
                serde_json::from_value(v).expect("stored object decodes")
            })
            .collect()
    }

    /// The stored JSON object.
    pub fn get_json(&self, key: &ObjectKey) -> Option<Value> {
        self.inner.state.lock().objects.get(key).cloned()
    }

    /// Removes an object outright, ignoring finalizers, and emits a delete
    /// event.
    pub fn remove(&self, key: &ObjectKey) -> Option<Value> {
        let mut st = self.inner.state.lock();
        let obj = st.objects.remove(key)?;
        self.emit(&mut st, key.clone(), "DELETED", obj.clone());
        Some(obj)
    }

    fn emit(&self, st: &mut State, key: ObjectKey, kind: &'static str, mut object: Value) {
        st.rv += 1;
        let rv = st.rv;
        if kind == "DELETED" {
            set_meta(&mut object, "resourceVersion", json!(rv.to_string()));
        }
        let ev = Event {
            rv,
            key,
            kind,
            object,
        };
        st.log.push(ev.clone());
        let _ = self.inner.events.send(ev);
    }

    fn create_locked(
        &self,
        st: &mut State,
        group: &str,
        resource: &str,
        namespace: &str,
        mut obj: Value,
    ) -> Result<Value> {
        if !obj.is_object() {
            return Err(ApiError::new(400, "BadRequest", "body is not an object"));
        }
        if let Some(ns) = meta_str(&obj, "namespace") {
            if !namespace.is_empty() && ns != namespace {
                return Err(ApiError::new(
                    400,
                    "BadRequest",
                    "the namespace of the object does not match the namespace on the request",
                ));
            }
        }
        let name = match meta_str(&obj, "name").filter(|n| !n.is_empty()) {
            Some(n) => n.to_string(),
            None => match meta_str(&obj, "generateName").filter(|n| !n.is_empty()) {
                Some(prefix) => {
                    st.names += 1;
                    format!("{prefix}{:05x}", st.names)
                }
                None => {
                    return Err(ApiError::new(
                        422,
                        "Invalid",
                        "metadata.name: Required value: name or generateName is required",
                    ))
                }
            },
        };
        let key = ObjectKey {
            group: group.into(),
            resource: resource.into(),
            namespace: namespace.into(),
            name: name.clone(),
        };
        if st.objects.contains_key(&key) {
            return Err(ApiError::new(
                409,
                "AlreadyExists",
                format!("{resource} \"{name}\" already exists"),
            ));
        }
        set_meta(&mut obj, "name", json!(name));
        if !namespace.is_empty() {
            set_meta(&mut obj, "namespace", json!(namespace));
        }
        if meta_str(&obj, "uid").is_none_or(str::is_empty) {
            set_meta(&mut obj, "uid", json!(uuid::Uuid::new_v4().to_string()));
        }
        if meta_str(&obj, "creationTimestamp").is_none_or(str::is_empty) {
            set_meta(&mut obj, "creationTimestamp", json!(now_rfc3339()));
        }
        set_meta(&mut obj, "generation", json!(1));
        let rv = st.rv + 1;
        set_meta(&mut obj, "resourceVersion", json!(rv.to_string()));
        st.objects.insert(key.clone(), obj.clone());
        self.emit(st, key, "ADDED", obj.clone());
        Ok(obj)
    }

    /// Stores `new` in place of `old` for `key`, applying the server's
    /// rules for generations, finalizers and deletion.
    fn commit_locked(&self, st: &mut State, key: &ObjectKey, mut new: Value) -> Result<Value> {
        let old = st
            .objects
            .get(key)
            .cloned()
            .ok_or_else(|| ApiError::not_found(key))?;
        // Server-owned metadata.
        for f in [
            "uid",
            "creationTimestamp",
            "deletionTimestamp",
            "name",
            "namespace",
        ] {
            match old.get("metadata").and_then(|m| m.get(f)) {
                Some(v) => set_meta(&mut new, f, v.clone()),
                None => remove_meta(&mut new, f),
            }
        }
        if let Some(k) = old.get("kind") {
            new["kind"] = k.clone();
        }
        if meta_str(&old, "deletionTimestamp").is_some() {
            let before = finalizers(&old);
            if let Some(f) = finalizers(&new).iter().find(|f| !before.contains(f)) {
                return Err(ApiError::new(
                    422,
                    "Invalid",
                    format!(
                        "{} \"{}\" is invalid: metadata.finalizers: Forbidden: no new finalizers can be added if the object is being deleted, found new finalizers [\"{f}\"]",
                        key.resource, key.name
                    ),
                ));
            }
        }
        let generation = old
            .pointer("/metadata/generation")
            .and_then(Value::as_i64)
            .unwrap_or(1);
        let generation = if spec_of(&old) != spec_of(&new) {
            generation + 1
        } else {
            generation
        };
        set_meta(&mut new, "generation", json!(generation));
        if comparable(&old) == comparable(&new) {
            return Ok(old);
        }
        let deleting = meta_str(&new, "deletionTimestamp").is_some();
        if deleting && finalizers(&new).is_empty() {
            st.objects.remove(key);
            st.applied.retain(|(k, _, _), _| k != key);
            self.emit(st, key.clone(), "DELETED", new.clone());
            return Ok(new);
        }
        let rv = st.rv + 1;
        set_meta(&mut new, "resourceVersion", json!(rv.to_string()));
        st.objects.insert(key.clone(), new.clone());
        self.emit(st, key.clone(), "MODIFIED", new.clone());
        Ok(new)
    }

    fn check_rv(key: &ObjectKey, old: &Value, body: &Value) -> Result<()> {
        if let Some(want) = meta_str(body, "resourceVersion").filter(|s| !s.is_empty()) {
            if Some(want) != meta_str(old, "resourceVersion") {
                return Err(ApiError::conflict(
                    key,
                    "the object has been modified; please apply your changes to the latest version and try again",
                ));
            }
        }
        Ok(())
    }

    fn handle(&self, req: Request) -> Result<(u16, ResponseBody)> {
        let route =
            route(&req.path).ok_or_else(|| ApiError::new(404, "NotFound", "unknown path"))?;
        let (group, version, namespace, resource, name, sub) = match route {
            Route::ApiVersions => {
                return Ok((
                    200,
                    ResponseBody::Json(json!({"kind": "APIVersions", "versions": ["v1"]})),
                ))
            }
            Route::GroupList => return Ok((200, ResponseBody::Json(self.group_list()))),
            Route::Discovery(gv) => return self.discovery(gv, &req),
            Route::Resource {
                group,
                version,
                namespace,
                resource,
                name,
                sub,
            } => (group, version, namespace, resource, name, sub),
        };
        let watch = req
            .query
            .get("watch")
            .is_some_and(|w| w == "true" || w == "1");
        let verb = match (&req.method, name.is_some()) {
            (&http::Method::GET, true) => "get",
            (&http::Method::GET, false) if watch => "watch",
            (&http::Method::GET, false) => "list",
            (&http::Method::POST, _) => "create",
            (&http::Method::PUT, _) => "update",
            (&http::Method::PATCH, _) => "patch",
            (&http::Method::DELETE, true) => "delete",
            (&http::Method::DELETE, false) => "deletecollection",
            _ => return Err(ApiError::new(405, "MethodNotAllowed", "method not allowed")),
        };
        let mut action = Action {
            verb: verb.into(),
            group: group.clone(),
            version: version.clone(),
            resource: resource.clone(),
            subresource: sub.clone(),
            namespace: namespace.clone(),
            name: name.clone(),
            query: req.query.clone(),
            body: req.body.clone(),
            content_type: req.content_type.clone(),
        };
        let reactors: Vec<_> = {
            let mut st = self.inner.state.lock();
            st.actions.push(action.clone());
            st.reactors
                .iter()
                .filter(|(v, r, _)| (v == "*" || v == verb) && (r == "*" || *r == resource))
                .map(|(_, _, f)| f.clone())
                .collect()
        };
        let mut body = req.body.clone();
        for reactor in reactors {
            match reactor(&action) {
                Reaction::Pass => {}
                Reaction::Mutate(v) => {
                    body = Some(v.clone());
                    action.body = Some(v);
                }
                Reaction::Respond(v) => return Ok((200, ResponseBody::Json(v))),
                Reaction::Fail {
                    code,
                    reason,
                    message,
                } => {
                    return Err(ApiError {
                        code,
                        reason,
                        message,
                    })
                }
            }
        }
        let av = api_version(&group, &version);
        let ns = namespace.clone().unwrap_or_default();
        let key = |name: &str| ObjectKey {
            group: group.clone(),
            resource: resource.clone(),
            namespace: ns.clone(),
            name: name.to_string(),
        };
        let out = |mut v: Value| {
            v["apiVersion"] = json!(av);
            v
        };
        let body = body.unwrap_or(Value::Null);
        match verb {
            "get" => {
                let k = key(name.as_deref().unwrap_or(""));
                let st = self.inner.state.lock();
                let obj = st
                    .objects
                    .get(&k)
                    .cloned()
                    .ok_or_else(|| ApiError::not_found(&k))?;
                Ok((200, ResponseBody::Json(out(obj))))
            }
            "list" => {
                let items = self.matching(&group, &resource, namespace.as_deref(), &req.query)?;
                let st = self.inner.state.lock();
                let kind = items
                    .first()
                    .and_then(|i| i.get("kind"))
                    .and_then(Value::as_str)
                    .map(|k| format!("{k}List"))
                    .unwrap_or_else(|| "List".into());
                let items: Vec<Value> = items.into_iter().map(out).collect();
                Ok((
                    200,
                    ResponseBody::Json(json!({
                        "apiVersion": av,
                        "kind": kind,
                        "metadata": {"resourceVersion": st.rv.to_string()},
                        "items": items,
                    })),
                ))
            }
            "watch" => self.watch(
                group.clone(),
                resource.clone(),
                namespace.clone(),
                av.clone(),
                &req.query,
            ),
            "create" => {
                let mut st = self.inner.state.lock();
                let created = self.create_locked(&mut st, &group, &resource, &ns, body)?;
                Ok((201, ResponseBody::Json(out(created))))
            }
            "update" => {
                let k = key(name.as_deref().unwrap_or(""));
                let mut st = self.inner.state.lock();
                let old = st
                    .objects
                    .get(&k)
                    .cloned()
                    .ok_or_else(|| ApiError::not_found(&k))?;
                Self::check_rv(&k, &old, &body)?;
                let new = if sub.as_deref() == Some("status") {
                    let mut new = old.clone();
                    new["status"] = body.get("status").cloned().unwrap_or(Value::Null);
                    new
                } else {
                    let mut new = body;
                    match old.get("status") {
                        Some(s) => new["status"] = s.clone(),
                        None => {
                            if let Some(m) = new.as_object_mut() {
                                m.remove("status");
                            }
                        }
                    }
                    new
                };
                let stored = self.commit_locked(&mut st, &k, new)?;
                Ok((200, ResponseBody::Json(out(stored))))
            }
            "patch" => {
                let k = key(name.as_deref().unwrap_or(""));
                let ct = req.content_type.clone().unwrap_or_default();
                let is_status = sub.as_deref() == Some("status");
                let mut st = self.inner.state.lock();
                let old = st.objects.get(&k).cloned();
                if ct.starts_with("application/apply-patch") {
                    let manager = req.query.get("fieldManager").cloned().unwrap_or_default();
                    let akey = (k.clone(), manager, is_status);
                    let prev = st.applied.get(&akey).cloned();
                    let Some(old) = old else {
                        if is_status {
                            return Err(ApiError::not_found(&k));
                        }
                        let mut obj = Value::Null;
                        patch::apply(&mut obj, &body, None);
                        set_meta(&mut obj, "name", json!(k.name));
                        let created = self.create_locked(&mut st, &group, &resource, &ns, obj)?;
                        st.applied.insert(akey, body);
                        return Ok((201, ResponseBody::Json(out(created))));
                    };
                    let mut new = old.clone();
                    let mut applied = body.clone();
                    let mut prev_v = prev.clone();
                    for v in [&mut applied, prev_v.get_or_insert(Value::Null)] {
                        if let Some(m) = v.as_object_mut() {
                            m.remove("apiVersion");
                            m.remove("kind");
                        }
                        if let Some(m) = v.get_mut("metadata").and_then(Value::as_object_mut) {
                            m.remove("name");
                            m.remove("namespace");
                        }
                    }
                    if is_status {
                        let mut status = old.get("status").cloned().unwrap_or(Value::Null);
                        patch::apply(
                            &mut status,
                            applied.get("status").unwrap_or(&Value::Null),
                            prev_v.as_ref().and_then(|p| p.get("status")),
                        );
                        new["status"] = status;
                    } else {
                        if let Some(m) = applied.as_object_mut() {
                            m.remove("status");
                        }
                        patch::apply(&mut new, &applied, prev_v.as_ref().filter(|p| !p.is_null()));
                    }
                    let stored = self.commit_locked(&mut st, &k, new)?;
                    st.applied.insert(akey, body);
                    return Ok((200, ResponseBody::Json(out(stored))));
                }
                let old = old.ok_or_else(|| ApiError::not_found(&k))?;
                let mut patched = old.clone();
                if ct.starts_with("application/json-patch") {
                    patch::json_patch(&mut patched, &body)
                        .map_err(|e| ApiError::new(422, "Invalid", e))?;
                } else if ct.starts_with("application/strategic-merge-patch") {
                    Self::check_rv(&k, &old, &body)?;
                    patch::strategic_merge_patch(&mut patched, &body);
                } else {
                    Self::check_rv(&k, &old, &body)?;
                    patch::merge_patch(&mut patched, &body);
                }
                let new = if is_status {
                    let mut new = old.clone();
                    new["status"] = patched.get("status").cloned().unwrap_or(Value::Null);
                    new
                } else {
                    match old.get("status") {
                        Some(s) => patched["status"] = s.clone(),
                        None => {
                            if let Some(m) = patched.as_object_mut() {
                                m.remove("status");
                            }
                        }
                    }
                    patched
                };
                let stored = self.commit_locked(&mut st, &k, new)?;
                Ok((200, ResponseBody::Json(out(stored))))
            }
            "delete" => {
                let k = key(name.as_deref().unwrap_or(""));
                let mut st = self.inner.state.lock();
                let deleted = self.delete_locked(&mut st, &k, &body)?;
                Ok((200, ResponseBody::Json(out(deleted))))
            }
            "deletecollection" => {
                let items = self.matching(&group, &resource, namespace.as_deref(), &req.query)?;
                let mut st = self.inner.state.lock();
                let mut deleted = Vec::new();
                for item in items {
                    let k = key(meta_str(&item, "name").unwrap_or(""));
                    let k = ObjectKey {
                        namespace: meta_str(&item, "namespace").unwrap_or("").into(),
                        ..k
                    };
                    deleted.push(out(self.delete_locked(&mut st, &k, &Value::Null)?));
                }
                Ok((
                    200,
                    ResponseBody::Json(
                        json!({"apiVersion": av, "kind": "List", "metadata": {}, "items": deleted}),
                    ),
                ))
            }
            _ => unreachable!(),
        }
    }

    fn delete_locked(&self, st: &mut State, key: &ObjectKey, opts: &Value) -> Result<Value> {
        let old = st
            .objects
            .get(key)
            .cloned()
            .ok_or_else(|| ApiError::not_found(key))?;
        if let Some(pre) = opts.get("preconditions") {
            if let Some(uid) = pre.get("uid").and_then(Value::as_str) {
                if Some(uid) != meta_str(&old, "uid") {
                    return Err(ApiError::conflict(
                        key,
                        &format!("Precondition failed: UID in precondition: {uid}, UID in object meta: {}", meta_str(&old, "uid").unwrap_or("")),
                    ));
                }
            }
            if let Some(rv) = pre.get("resourceVersion").and_then(Value::as_str) {
                if Some(rv) != meta_str(&old, "resourceVersion") {
                    return Err(ApiError::conflict(
                        key,
                        "Precondition failed: resourceVersion",
                    ));
                }
            }
        }
        if !finalizers(&old).is_empty() {
            if meta_str(&old, "deletionTimestamp").is_some() {
                return Ok(old);
            }
            let mut new = old.clone();
            set_meta(&mut new, "deletionTimestamp", json!(now_rfc3339()));
            set_meta(&mut new, "deletionGracePeriodSeconds", json!(0));
            let rv = st.rv + 1;
            set_meta(&mut new, "resourceVersion", json!(rv.to_string()));
            st.objects.insert(key.clone(), new.clone());
            self.emit(st, key.clone(), "MODIFIED", new.clone());
            return Ok(new);
        }
        st.objects.remove(key);
        st.applied.retain(|(k, _, _), _| k != key);
        self.emit(st, key.clone(), "DELETED", old.clone());
        Ok(old)
    }

    fn matching(
        &self,
        group: &str,
        resource: &str,
        namespace: Option<&str>,
        query: &HashMap<String, String>,
    ) -> Result<Vec<Value>> {
        let labels =
            LabelSelector::parse(query.get("labelSelector").map(String::as_str).unwrap_or(""))
                .map_err(|e| ApiError::new(400, "BadRequest", e))?;
        let fields =
            FieldSelector::parse(query.get("fieldSelector").map(String::as_str).unwrap_or(""))
                .map_err(|e| ApiError::new(400, "BadRequest", e))?;
        let st = self.inner.state.lock();
        Ok(st
            .objects
            .iter()
            .filter(|(k, v)| {
                k.group == group
                    && k.resource == resource
                    && namespace.is_none_or(|ns| k.namespace == ns)
                    && labels.matches(&labels_of(v))
                    && fields.matches(v)
            })
            .map(|(_, v)| v.clone())
            .collect())
    }

    fn watch(
        &self,
        group: String,
        resource: String,
        namespace: Option<String>,
        av: String,
        query: &HashMap<String, String>,
    ) -> Result<(u16, ResponseBody)> {
        let labels =
            LabelSelector::parse(query.get("labelSelector").map(String::as_str).unwrap_or(""))
                .map_err(|e| ApiError::new(400, "BadRequest", e))?;
        let fields =
            FieldSelector::parse(query.get("fieldSelector").map(String::as_str).unwrap_or(""))
                .map_err(|e| ApiError::new(400, "BadRequest", e))?;
        let since: Option<u64> = query
            .get("resourceVersion")
            .and_then(|rv| rv.parse().ok())
            .filter(|rv| *rv > 0);
        let filter = move |k: &ObjectKey, v: &Value| {
            k.group == group
                && k.resource == resource
                && namespace.as_deref().is_none_or(|ns| k.namespace == ns)
                && labels.matches(&labels_of(v))
                && fields.matches(v)
        };
        let (backlog, mut sub, start) = {
            let st = self.inner.state.lock();
            let sub = self.inner.events.subscribe();
            let backlog: Vec<(&'static str, Value)> = match since {
                Some(rv) => st
                    .log
                    .iter()
                    .filter(|e| e.rv > rv && filter(&e.key, &e.object))
                    .map(|e| (e.kind, e.object.clone()))
                    .collect(),
                None => st
                    .objects
                    .iter()
                    .filter(|(k, v)| filter(k, v))
                    .map(|(_, v)| ("ADDED", v.clone()))
                    .collect(),
            };
            (backlog, sub, st.rv)
        };
        let (tx, rx) = mpsc::channel::<std::result::Result<Frame<Bytes>, std::io::Error>>(1024);
        let line = move |kind: &str, mut obj: Value| {
            obj["apiVersion"] = json!(av);
            let mut s = serde_json::to_vec(&json!({"type": kind, "object": obj})).expect("json");
            s.push(b'\n');
            Ok(Frame::data(Bytes::from(s)))
        };
        tokio::spawn(async move {
            for (kind, obj) in backlog {
                if tx.send(line(kind, obj)).await.is_err() {
                    return;
                }
            }
            loop {
                tokio::select! {
                    _ = tx.closed() => return,
                    ev = sub.recv() => match ev {
                        Ok(ev) => {
                            if ev.rv <= start || !filter(&ev.key, &ev.object) {
                                continue;
                            }
                            if tx.send(line(ev.kind, ev.object)).await.is_err() {
                                return;
                            }
                        }
                        Err(_) => return,
                    },
                }
            }
        });
        Ok((200, ResponseBody::Stream(rx)))
    }

    fn group_list(&self) -> Value {
        let st = self.inner.state.lock();
        let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for gv in st.discovery.keys() {
            if let Some((g, v)) = gv.split_once('/') {
                groups.entry(g.into()).or_default().push(v.into());
            }
        }
        let groups: Vec<Value> = groups
            .into_iter()
            .map(|(g, vs)| {
                let versions: Vec<Value> = vs
                    .iter()
                    .map(|v| json!({"groupVersion": format!("{g}/{v}"), "version": v}))
                    .collect();
                json!({"name": g, "versions": versions, "preferredVersion": versions[0]})
            })
            .collect();
        json!({"kind": "APIGroupList", "apiVersion": "v1", "groups": groups})
    }

    fn discovery(&self, gv: String, req: &Request) -> Result<(u16, ResponseBody)> {
        let action = Action {
            verb: "discover".into(),
            group: gv
                .split_once('/')
                .map(|(g, _)| g.to_string())
                .unwrap_or_default(),
            version: gv.rsplit('/').next().unwrap_or("").into(),
            resource: gv.clone(),
            subresource: None,
            namespace: None,
            name: None,
            query: req.query.clone(),
            body: None,
            content_type: None,
        };
        let reactors: Vec<_> = {
            let mut st = self.inner.state.lock();
            st.actions.push(action.clone());
            st.reactors
                .iter()
                .filter(|(v, r, _)| (v == "*" || v == "discover") && (r == "*" || *r == gv))
                .map(|(_, _, f)| f.clone())
                .collect()
        };
        for reactor in reactors {
            match reactor(&action) {
                Reaction::Pass | Reaction::Mutate(_) => {}
                Reaction::Respond(v) => return Ok((200, ResponseBody::Json(v))),
                Reaction::Fail {
                    code,
                    reason,
                    message,
                } => {
                    return Err(ApiError {
                        code,
                        reason,
                        message,
                    })
                }
            }
        }
        let st = self.inner.state.lock();
        let resources = st.discovery.get(&gv).ok_or_else(|| {
            ApiError::new(
                404,
                "NotFound",
                format!("the server could not find the requested resource ({gv})"),
            )
        })?;
        let resources: Vec<Value> = resources
            .iter()
            .map(|r| {
                json!({
                    "name": r.name,
                    "singularName": r.kind.to_lowercase(),
                    "namespaced": r.namespaced,
                    "kind": r.kind,
                    "verbs": ["create", "delete", "get", "list", "patch", "update", "watch"],
                })
            })
            .collect();
        Ok((
            200,
            ResponseBody::Json(
                json!({"kind": "APIResourceList", "apiVersion": "v1", "groupVersion": gv, "resources": resources}),
            ),
        ))
    }
}

enum ResponseBody {
    Json(Value),
    Stream(mpsc::Receiver<std::result::Result<Frame<Bytes>, std::io::Error>>),
}

fn respond(result: Result<(u16, ResponseBody)>) -> http::Response<FakeBody> {
    let (code, body) = match result {
        Ok(ok) => ok,
        Err(e) => (
            e.code,
            ResponseBody::Json(json!({
                "kind": "Status",
                "apiVersion": "v1",
                "metadata": {},
                "status": "Failure",
                "message": e.message,
                "reason": e.reason,
                "code": e.code,
            })),
        ),
    };
    let body: FakeBody = match body {
        ResponseBody::Json(v) => Full::new(Bytes::from(serde_json::to_vec(&v).expect("json")))
            .map_err(|never: Infallible| match never {})
            .boxed_unsync(),
        ResponseBody::Stream(rx) => {
            StreamBody::new(tokio_stream::wrappers::ReceiverStream::new(rx)).boxed_unsync()
        }
    };
    http::Response::builder()
        .status(code)
        .header("content-type", "application/json")
        .body(body)
        .expect("response")
}

fn parse_query(q: Option<&str>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for pair in q.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        out.insert(decode(k), decode(v));
    }
    out
}

fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => match u8::from_str_radix(&s[i + 1..i + 3], 16) {
                Ok(b) => {
                    out.push(b);
                    i += 2;
                }
                Err(_) => out.push(b'%'),
            },
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

impl tower::Service<http::Request<kube::client::Body>> for FakeApiServer {
    type Response = http::Response<FakeBody>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<kube::client::Body>) -> Self::Future {
        let this = self.clone();
        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let bytes = body
                .collect()
                .await
                .map(|c| c.to_bytes())
                .unwrap_or_default();
            let body = if bytes.is_empty() {
                None
            } else {
                serde_json::from_slice(&bytes)
                    .ok()
                    .or_else(|| serde_yaml::from_slice(&bytes).ok())
            };
            let request = Request {
                method: parts.method.clone(),
                path: parts.uri.path().to_string(),
                query: parse_query(parts.uri.query()),
                content_type: parts
                    .headers
                    .get(http::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from),
                body,
            };
            Ok(respond(this.handle(request)))
        })
    }
}

#[cfg(test)]
mod tests;
