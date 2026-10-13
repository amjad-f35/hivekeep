//! Patch semantics for the in-memory API server: JSON merge patch, JSON
//! patch, strategic merge (approximated by merging named list items), and
//! server-side apply with per-manager field ownership.

use serde_json::{Map, Value};

/// Applies an RFC 7386 JSON merge patch.
pub fn merge_patch(target: &mut Value, patch: &Value) {
    let Value::Object(patch) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let t = target.as_object_mut().expect("object");
    for (k, v) in patch {
        if v.is_null() {
            t.remove(k);
        } else {
            merge_patch(t.entry(k.clone()).or_insert(Value::Null), v);
        }
    }
}

/// Applies a strategic merge patch: a merge patch in which lists of
/// objects keyed by `name` merge item by item instead of being replaced.
pub fn strategic_merge_patch(target: &mut Value, patch: &Value) {
    match patch {
        Value::Object(p) => {
            if !target.is_object() {
                *target = Value::Object(Map::new());
            }
            let t = target.as_object_mut().expect("object");
            for (k, v) in p {
                if k.starts_with('$') {
                    continue;
                }
                if v.is_null() {
                    t.remove(k);
                } else {
                    strategic_merge_patch(t.entry(k.clone()).or_insert(Value::Null), v);
                }
            }
        }
        Value::Array(items)
            if is_named_list(items) && target.as_array().is_some_and(|t| is_named_list(t)) =>
        {
            let t = target.as_array_mut().expect("array");
            for item in items {
                let name = item.get("name").cloned();
                match t.iter_mut().find(|x| x.get("name") == name.as_ref()) {
                    Some(existing) => strategic_merge_patch(existing, item),
                    None => t.push(item.clone()),
                }
            }
        }
        other => *target = other.clone(),
    }
}

fn is_named_list(items: &[Value]) -> bool {
    !items.is_empty()
        && items
            .iter()
            .all(|i| i.get("name").is_some_and(Value::is_string))
}

/// Applies an RFC 6902 JSON patch (add, remove, replace, test, copy, move).
pub fn json_patch(target: &mut Value, ops: &Value) -> Result<(), String> {
    let ops = ops.as_array().ok_or("json patch must be an array")?;
    for op in ops {
        let kind = op.get("op").and_then(Value::as_str).ok_or("op missing")?;
        let path = op
            .get("path")
            .and_then(Value::as_str)
            .ok_or("path missing")?;
        match kind {
            "add" => add(
                target,
                path,
                op.get("value").cloned().unwrap_or(Value::Null),
            )?,
            "remove" => {
                remove(target, path)?;
            }
            "replace" => {
                remove(target, path)?;
                add(
                    target,
                    path,
                    op.get("value").cloned().unwrap_or(Value::Null),
                )?;
            }
            "test" => {
                if target.pointer(path) != op.get("value") {
                    return Err(format!("test failed at {path}"));
                }
            }
            "copy" | "move" => {
                let from = op
                    .get("from")
                    .and_then(Value::as_str)
                    .ok_or("from missing")?;
                let v = target.pointer(from).cloned().ok_or("from not found")?;
                if kind == "move" {
                    remove(target, from)?;
                }
                add(target, path, v)?;
            }
            other => return Err(format!("unsupported op {other}")),
        }
    }
    Ok(())
}

fn split_pointer(path: &str) -> Result<(String, String), String> {
    let idx = path
        .rfind('/')
        .ok_or_else(|| format!("bad pointer {path}"))?;
    let last = path[idx + 1..].replace("~1", "/").replace("~0", "~");
    Ok((path[..idx].to_string(), last))
}

fn add(target: &mut Value, path: &str, value: Value) -> Result<(), String> {
    if path.is_empty() {
        *target = value;
        return Ok(());
    }
    let (parent, last) = split_pointer(path)?;
    let p = target
        .pointer_mut(&parent)
        .ok_or_else(|| format!("parent of {path} not found"))?;
    match p {
        Value::Object(m) => {
            m.insert(last, value);
        }
        Value::Array(a) => {
            if last == "-" {
                a.push(value);
            } else {
                let i: usize = last.parse().map_err(|_| format!("bad index {last}"))?;
                if i > a.len() {
                    return Err(format!("index {i} out of range"));
                }
                a.insert(i, value);
            }
        }
        _ => return Err(format!("cannot add under {parent}")),
    }
    Ok(())
}

fn remove(target: &mut Value, path: &str) -> Result<Value, String> {
    let (parent, last) = split_pointer(path)?;
    let p = target
        .pointer_mut(&parent)
        .ok_or_else(|| format!("parent of {path} not found"))?;
    match p {
        Value::Object(m) => m.remove(&last).ok_or_else(|| format!("{path} not found")),
        Value::Array(a) => {
            let i: usize = last.parse().map_err(|_| format!("bad index {last}"))?;
            if i >= a.len() {
                return Err(format!("index {i} out of range"));
            }
            Ok(a.remove(i))
        }
        _ => Err(format!("cannot remove under {parent}")),
    }
}

/// Merges a server-side apply configuration into `target`. `prev` is what
/// the same field manager applied last time: fields it set then and omits
/// now are removed, while fields it never set (another manager's) are kept.
/// Lists of objects keyed by `name` merge by key; other lists are atomic.
pub fn apply(target: &mut Value, applied: &Value, prev: Option<&Value>) {
    match applied {
        Value::Object(a) => {
            if !target.is_object() {
                *target = Value::Object(Map::new());
            }
            let t = target.as_object_mut().expect("object");
            if let Some(Value::Object(p)) = prev {
                for (k, pv) in p {
                    if a.contains_key(k) {
                        continue;
                    }
                    // Drop only what this manager set under k.
                    match (pv, t.get_mut(k)) {
                        (Value::Object(_), Some(tv @ Value::Object(_))) => {
                            apply(tv, &Value::Object(Map::new()), Some(pv));
                            if tv.as_object().is_some_and(Map::is_empty) {
                                t.remove(k);
                            }
                        }
                        _ => {
                            t.remove(k);
                        }
                    }
                }
            }
            for (k, v) in a {
                if v.is_null() {
                    t.remove(k);
                    continue;
                }
                let prev_k = prev.and_then(|p| p.get(k));
                apply(t.entry(k.clone()).or_insert(Value::Null), v, prev_k);
            }
        }
        Value::Array(items)
            if is_named_list(items)
                && target
                    .as_array()
                    .is_some_and(|t| t.is_empty() || is_named_list(t)) =>
        {
            let t = target.as_array_mut().expect("array");
            if let Some(Value::Array(p)) = prev {
                for old in p {
                    let name = old.get("name");
                    if !items.iter().any(|i| i.get("name") == name) {
                        t.retain(|x| x.get("name") != name);
                    }
                }
            }
            for item in items {
                let name = item.get("name");
                let prev_item = match prev {
                    Some(Value::Array(p)) => p.iter().find(|x| x.get("name") == name),
                    _ => None,
                };
                match t.iter_mut().find(|x| x.get("name") == name) {
                    Some(existing) => apply(existing, item, prev_item),
                    None => {
                        let mut fresh = Value::Null;
                        apply(&mut fresh, item, None);
                        t.push(fresh);
                    }
                }
            }
        }
        other => *target = other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merge_patch_removes_nulls() {
        let mut v = json!({"a": 1, "b": {"c": 2, "d": 3}});
        merge_patch(&mut v, &json!({"a": null, "b": {"c": 5}}));
        assert_eq!(v, json!({"b": {"c": 5, "d": 3}}));
    }

    #[test]
    fn json_patch_ops() {
        let mut v = json!({"a": [1, 2], "b": "x"});
        json_patch(
            &mut v,
            &json!([
                {"op": "add", "path": "/a/-", "value": 3},
                {"op": "replace", "path": "/b", "value": "y"},
                {"op": "remove", "path": "/a/0"},
                {"op": "test", "path": "/b", "value": "y"}
            ]),
        )
        .unwrap();
        assert_eq!(v, json!({"a": [2, 3], "b": "y"}));
        assert!(json_patch(&mut v, &json!([{"op": "test", "path": "/b", "value": "z"}])).is_err());
    }

    #[test]
    fn apply_tracks_ownership() {
        let mut obj = json!({"spec": {"replicas": 1, "other": "kept", "list": [{"name": "a", "v": 1}, {"name": "x", "v": 9}]}});
        let prev = json!({"spec": {"replicas": 1, "gone": true, "list": [{"name": "a", "v": 1}, {"name": "b"}]}});
        obj["spec"]["gone"] = json!(true);
        obj["spec"]["list"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name": "b"}));
        let applied = json!({"spec": {"replicas": 2, "list": [{"name": "a", "v": 2}]}});
        apply(&mut obj, &applied, Some(&prev));
        assert_eq!(
            obj,
            json!({"spec": {"replicas": 2, "other": "kept", "list": [{"name": "a", "v": 2}, {"name": "x", "v": 9}]}})
        );
    }

    #[test]
    fn strategic_merges_named_lists() {
        let mut v = json!({"containers": [{"name": "a", "image": "1"}]});
        strategic_merge_patch(
            &mut v,
            &json!({"containers": [{"name": "a", "image": "2"}, {"name": "b"}]}),
        );
        assert_eq!(
            v,
            json!({"containers": [{"name": "a", "image": "2"}, {"name": "b"}]})
        );
    }
}
