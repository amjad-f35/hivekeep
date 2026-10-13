//! Label and field selector matching for the in-memory API server.

use std::collections::BTreeMap;

use serde_json::Value;

/// One requirement of a label selector.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Requirement {
    Eq(String, String),
    NotEq(String, String),
    In(String, Vec<String>),
    NotIn(String, Vec<String>),
    Exists(String),
    NotExists(String),
}

/// A parsed label selector, as sent in `labelSelector=`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LabelSelector(Vec<Requirement>);

impl LabelSelector {
    /// Parses the string form of a label selector. An empty string selects
    /// everything.
    pub fn parse(s: &str) -> Result<Self, String> {
        let mut reqs = Vec::new();
        for term in split_terms(s) {
            let term = term.trim();
            if term.is_empty() {
                continue;
            }
            if let Some(key) = term.strip_prefix('!') {
                reqs.push(Requirement::NotExists(key.trim().to_string()));
            } else if let Some((k, v)) = term.split_once("!=") {
                reqs.push(Requirement::NotEq(k.trim().into(), v.trim().into()));
            } else if let Some((k, v)) = term.split_once("==") {
                reqs.push(Requirement::Eq(k.trim().into(), v.trim().into()));
            } else if let Some((k, v)) = term.split_once('=') {
                reqs.push(Requirement::Eq(k.trim().into(), v.trim().into()));
            } else if let Some((k, rest)) = term.split_once(" notin ") {
                reqs.push(Requirement::NotIn(k.trim().into(), set_values(rest)?));
            } else if let Some((k, rest)) = term.split_once(" in ") {
                reqs.push(Requirement::In(k.trim().into(), set_values(rest)?));
            } else {
                reqs.push(Requirement::Exists(term.to_string()));
            }
        }
        Ok(LabelSelector(reqs))
    }

    /// Whether `labels` satisfies every requirement.
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        self.0.iter().all(|r| match r {
            Requirement::Eq(k, v) => labels.get(k) == Some(v),
            Requirement::NotEq(k, v) => labels.get(k) != Some(v),
            Requirement::In(k, vs) => labels.get(k).is_some_and(|l| vs.contains(l)),
            Requirement::NotIn(k, vs) => !labels.get(k).is_some_and(|l| vs.contains(l)),
            Requirement::Exists(k) => labels.contains_key(k),
            Requirement::NotExists(k) => !labels.contains_key(k),
        })
    }
}

/// Splits on commas outside parentheses.
fn split_terms(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&s[start..]);
    out
}

fn set_values(s: &str) -> Result<Vec<String>, String> {
    let s = s.trim();
    let inner = s
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| format!("invalid set {s:?}"))?;
    Ok(inner
        .split(',')
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect())
}

/// A parsed field selector: `path=value` / `path!=value` terms over the
/// object's JSON, e.g. `metadata.name=foo,spec.nodeName=node-a`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldSelector(Vec<(String, String, bool)>);

impl FieldSelector {
    /// Parses a field selector.
    pub fn parse(s: &str) -> Result<Self, String> {
        let mut terms = Vec::new();
        for term in s.split(',') {
            let term = term.trim();
            if term.is_empty() {
                continue;
            }
            if let Some((k, v)) = term.split_once("!=") {
                terms.push((k.into(), v.into(), false));
            } else if let Some((k, v)) = term.split_once("==") {
                terms.push((k.into(), v.into(), true));
            } else if let Some((k, v)) = term.split_once('=') {
                terms.push((k.into(), v.into(), true));
            } else {
                return Err(format!("invalid field selector term {term:?}"));
            }
        }
        Ok(FieldSelector(terms))
    }

    /// Whether `obj` satisfies every term. A missing field reads as "".
    pub fn matches(&self, obj: &Value) -> bool {
        self.0.iter().all(|(path, want, eq)| {
            let mut v = obj;
            for seg in path.split('.') {
                v = v.get(seg).unwrap_or(&Value::Null);
            }
            let got = match v {
                Value::String(s) => s.clone(),
                Value::Null => String::new(),
                other => other.to_string(),
            };
            (got == *want) == *eq
        })
    }
}

/// The labels of a JSON object.
pub fn labels_of(obj: &Value) -> BTreeMap<String, String> {
    obj.pointer("/metadata/labels")
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn label_selectors() {
        let l = labels(&[("app", "web"), ("tier", "front")]);
        for (sel, want) in [
            ("", true),
            ("app=web", true),
            ("app==web", true),
            ("app=db", false),
            ("app!=db", true),
            ("app", true),
            ("!app", false),
            ("!missing", true),
            ("tier in (front,back)", true),
            ("tier notin (front)", false),
            ("app=web,tier in (back)", false),
        ] {
            assert_eq!(
                LabelSelector::parse(sel).unwrap().matches(&l),
                want,
                "{sel}"
            );
        }
    }

    #[test]
    fn field_selectors() {
        let obj = serde_json::json!({"metadata": {"name": "a"}, "spec": {"nodeName": "n1"}});
        assert!(FieldSelector::parse("metadata.name=a")
            .unwrap()
            .matches(&obj));
        assert!(FieldSelector::parse("spec.nodeName=n1,metadata.name!=b")
            .unwrap()
            .matches(&obj));
        assert!(!FieldSelector::parse("spec.nodeName=n2")
            .unwrap()
            .matches(&obj));
        assert!(FieldSelector::parse("spec.missing=").unwrap().matches(&obj));
    }
}
