//! Group/version/kind identifiers and helpers for untyped objects.

use serde_json::Value;
use std::fmt;

/// A Kubernetes group, version and kind. The core group is `""`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Gvk {
    pub group: String,
    pub version: String,
    pub kind: String,
}

impl Gvk {
    pub fn new(
        group: impl Into<String>,
        version: impl Into<String>,
        kind: impl Into<String>,
    ) -> Self {
        Self {
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
        }
    }

    /// Parses an `apiVersion` (`v1` or `group/v1`) and a kind.
    pub fn from_api_version(api_version: &str, kind: &str) -> Self {
        match api_version.split_once('/') {
            Some((group, version)) => Self::new(group, version, kind),
            None => Self::new("", api_version, kind),
        }
    }

    /// The gvk of a typed k8s-openapi resource.
    pub fn of<K: k8s_openapi::Resource>() -> Self {
        Self::from_api_version(K::API_VERSION, K::KIND)
    }

    /// The `apiVersion` field this gvk serialises as.
    pub fn api_version(&self) -> String {
        if self.group.is_empty() {
            self.version.clone()
        } else {
            format!("{}/{}", self.group, self.version)
        }
    }

    pub fn namespace() -> Self {
        Self::new("", "v1", "Namespace")
    }
}

impl fmt::Display for Gvk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.group.is_empty() {
            write!(f, "/{}, Kind={}", self.version, self.kind)
        } else {
            write!(f, "{}/{}, Kind={}", self.group, self.version, self.kind)
        }
    }
}

/// An untyped Kubernetes object, as decoded from a manifest.
pub type Object = Value;

/// Accessors for the identifying fields of an untyped object.
pub trait ObjectExt {
    fn kind(&self) -> &str;
    fn api_version(&self) -> &str;
    fn name(&self) -> &str;
    fn obj_namespace(&self) -> &str;
    fn gvk(&self) -> Gvk;
    fn set_obj_namespace(&mut self, ns: &str);
    fn set_name(&mut self, name: &str);
    /// The value at a field path, if every step exists.
    fn at(&self, path: &[&str]) -> Option<&Value>;
    /// The value at a field path, creating intermediate maps as needed.
    fn at_mut(&mut self, path: &[&str]) -> &mut Value;
    fn labels(&self) -> serde_json::Map<String, Value>;
    fn annotations(&self) -> serde_json::Map<String, Value>;
}

fn str_at<'a>(v: &'a Value, path: &[&str]) -> &'a str {
    let mut cur = v;
    for p in path {
        match cur.get(p) {
            Some(next) => cur = next,
            None => return "",
        }
    }
    cur.as_str().unwrap_or("")
}

impl ObjectExt for Value {
    fn kind(&self) -> &str {
        str_at(self, &["kind"])
    }
    fn api_version(&self) -> &str {
        str_at(self, &["apiVersion"])
    }
    fn name(&self) -> &str {
        str_at(self, &["metadata", "name"])
    }
    fn obj_namespace(&self) -> &str {
        str_at(self, &["metadata", "namespace"])
    }
    fn gvk(&self) -> Gvk {
        Gvk::from_api_version(self.api_version(), self.kind())
    }
    fn set_obj_namespace(&mut self, ns: &str) {
        *self.at_mut(&["metadata", "namespace"]) = Value::String(ns.to_string());
    }
    fn set_name(&mut self, name: &str) {
        *self.at_mut(&["metadata", "name"]) = Value::String(name.to_string());
    }
    fn at(&self, path: &[&str]) -> Option<&Value> {
        let mut cur = self;
        for p in path {
            cur = cur.get(p)?;
        }
        Some(cur)
    }
    fn at_mut(&mut self, path: &[&str]) -> &mut Value {
        let mut cur = self;
        for p in path {
            if !cur.is_object() {
                *cur = Value::Object(Default::default());
            }
            cur = cur
                .as_object_mut()
                .expect("just made an object")
                .entry(p.to_string())
                .or_insert(Value::Null);
        }
        cur
    }
    fn labels(&self) -> serde_json::Map<String, Value> {
        self.at(&["metadata", "labels"])
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default()
    }
    fn annotations(&self) -> serde_json::Map<String, Value> {
        self.at(&["metadata", "annotations"])
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default()
    }
}

/// A short human description: `kind/name -n namespace`.
pub fn describe(obj: &Value) -> String {
    let kind = obj.kind().to_lowercase();
    match obj.obj_namespace() {
        "" => format!("{kind}/{}", obj.name()),
        ns => format!("{kind}/{} -n {ns}", obj.name()),
    }
}

/// A minimal object carrying only identifying fields.
pub fn stub(gvk: &Gvk, namespace: &str, name: &str) -> Value {
    let mut obj = serde_json::json!({
        "apiVersion": gvk.api_version(),
        "kind": gvk.kind,
        "metadata": {"name": name},
    });
    if !namespace.is_empty() {
        obj.set_obj_namespace(namespace);
    }
    obj
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_version_round_trips() {
        let g = Gvk::from_api_version("apps/v1", "Deployment");
        assert_eq!(g.group, "apps");
        assert_eq!(g.api_version(), "apps/v1");
        let core = Gvk::from_api_version("v1", "ConfigMap");
        assert_eq!(core.group, "");
        assert_eq!(core.api_version(), "v1");
    }

    #[test]
    fn describe_names_namespace() {
        let obj = stub(&Gvk::new("", "v1", "ConfigMap"), "hivekeep-system", "x");
        assert_eq!(describe(&obj), "configmap/x -n hivekeep-system");
        let ns = stub(&Gvk::namespace(), "", "x");
        assert_eq!(describe(&ns), "namespace/x");
    }
}
