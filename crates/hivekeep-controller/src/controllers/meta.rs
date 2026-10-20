//! Object metadata helpers: status conditions, finalizers, controller
//! references and label selector strings.

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
    Condition, LabelSelector, ObjectMeta, OwnerReference, Time,
};
use k8s_openapi::jiff::Timestamp;

/// The current time at the one-second precision objects store.
pub fn now() -> Time {
    let s = Timestamp::now().as_second();
    Time(Timestamp::from_second(s).unwrap_or(Timestamp::UNIX_EPOCH))
}

/// The condition of `type_` in `conditions`.
pub fn find_condition<'a>(conditions: &'a [Condition], type_: &str) -> Option<&'a Condition> {
    conditions.iter().find(|c| c.type_ == type_)
}

/// Sets `new` in `conditions`, replacing the condition of the same type.
/// `last_transition_time` moves (to `now`) only when the status changes,
/// or when the condition is new. Returns whether anything changed.
pub fn set_condition(conditions: &mut Vec<Condition>, mut new: Condition, now: Time) -> bool {
    let Some(existing) = conditions.iter_mut().find(|c| c.type_ == new.type_) else {
        new.last_transition_time = now;
        conditions.push(new);
        return true;
    };
    let mut changed = false;
    if existing.status != new.status {
        existing.status = new.status;
        existing.last_transition_time = now;
        changed = true;
    }
    if existing.reason != new.reason {
        existing.reason = new.reason;
        changed = true;
    }
    if existing.message != new.message {
        existing.message = new.message;
        changed = true;
    }
    if existing.observed_generation != new.observed_generation {
        existing.observed_generation = new.observed_generation;
        changed = true;
    }
    changed
}

/// Whether `meta` carries `finalizer`.
pub fn has_finalizer(meta: &ObjectMeta, finalizer: &str) -> bool {
    meta.finalizers.iter().flatten().any(|f| f == finalizer)
}

/// The owner reference marked as the object's controller.
pub fn controller_of(meta: &ObjectMeta) -> Option<&OwnerReference> {
    meta.owner_references
        .iter()
        .flatten()
        .find(|o| o.controller == Some(true))
}

/// Whether `api_version` is a version of `group`.
pub fn in_group(api_version: &str, group: &str) -> bool {
    api_version.split_once('/').map(|(g, _)| g) == Some(group)
}

/// `selector` in the label selector syntax the API server parses, with
/// requirements sorted by key. An absent or empty selector renders "".
pub fn selector_string(selector: Option<&LabelSelector>) -> String {
    let Some(sel) = selector else {
        return String::new();
    };
    let mut reqs: Vec<(String, String)> = Vec::new();
    for (k, v) in sel.match_labels.iter().flatten() {
        reqs.push((k.clone(), format!("{k}={v}")));
    }
    for e in sel.match_expressions.iter().flatten() {
        let mut values: Vec<String> = e.values.clone().unwrap_or_default();
        values.sort();
        let s = match e.operator.as_str() {
            "In" => format!("{} in ({})", e.key, values.join(",")),
            "NotIn" => format!("{} notin ({})", e.key, values.join(",")),
            "Exists" => e.key.clone(),
            "DoesNotExist" => format!("!{}", e.key),
            other => format!("{} {other} ({})", e.key, values.join(",")),
        };
        reqs.push((e.key.clone(), s));
    }
    reqs.sort_by(|a, b| a.0.cmp(&b.0));
    reqs.into_iter()
        .map(|(_, s)| s)
        .collect::<Vec<_>>()
        .join(",")
}

/// A one-entry string map.
pub fn label(key: &str, value: &str) -> BTreeMap<String, String> {
    BTreeMap::from([(key.to_string(), value.to_string())])
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement;

    fn cond(status: &str, reason: &str, gen: i64) -> Condition {
        Condition {
            type_: "Resolved".into(),
            status: status.into(),
            reason: reason.into(),
            message: "m".into(),
            observed_generation: Some(gen),
            last_transition_time: Time(Timestamp::UNIX_EPOCH),
        }
    }

    #[test]
    fn set_condition_moves_time_only_on_status_change() {
        let t1 = Time(Timestamp::from_second(100).unwrap());
        let t2 = Time(Timestamp::from_second(200).unwrap());
        let mut conds = Vec::new();
        assert!(set_condition(&mut conds, cond("True", "A", 1), t1.clone()));
        assert_eq!(conds[0].last_transition_time, t1);
        assert!(set_condition(&mut conds, cond("True", "B", 2), t2.clone()));
        assert_eq!(
            conds[0].last_transition_time, t1,
            "same status keeps the time"
        );
        assert_eq!(conds[0].reason, "B");
        assert!(!set_condition(&mut conds, cond("True", "B", 2), t2.clone()));
        assert!(set_condition(&mut conds, cond("False", "B", 2), t2.clone()));
        assert_eq!(conds[0].last_transition_time, t2);
        assert_eq!(conds.len(), 1);
    }

    #[test]
    fn selector_strings() {
        assert_eq!(selector_string(None), "");
        assert_eq!(selector_string(Some(&LabelSelector::default())), "");
        let sel = LabelSelector {
            match_labels: Some(BTreeMap::from([
                ("b".into(), "2".into()),
                ("a".into(), "1".into()),
            ])),
            match_expressions: Some(vec![
                LabelSelectorRequirement {
                    key: "c".into(),
                    operator: "In".into(),
                    values: Some(vec!["y".into(), "x".into()]),
                },
                LabelSelectorRequirement {
                    key: "d".into(),
                    operator: "DoesNotExist".into(),
                    values: None,
                },
            ]),
        };
        assert_eq!(selector_string(Some(&sel)), "a=1,b=2,c in (x,y),!d");
    }

    #[test]
    fn group_of_api_version() {
        assert!(in_group("hivekeep.io/v1", "hivekeep.io"));
        assert!(!in_group("v1", "hivekeep.io"));
        assert!(!in_group("apps/v1", "hivekeep.io"));
    }
}
