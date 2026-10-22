//! Walking the API: per-entity rule drivers and standard-method parsing.

use crate::apitool::model::{self, append_name, short_name, Api, Message};

use super::{CheckResult, Finding, LintError};

/// A standard method with everything a rule needs resolved.
pub(super) struct StdMethod<'a> {
    pub verb: Verb,
    /// The parent named after the verb for a subresource method (e.g. `Actor`
    /// in `GetActorEgressPolicy`, `Global` in `GetGlobalAccessPolicy`), or empty
    /// for a top-level resource.
    pub parent: String,
    pub rpc: &'a model::Method,
    pub resource: &'a Message,
    pub request: Message,
    pub response: Message,
}

impl StdMethod<'_> {
    pub fn is_subresource(&self) -> bool {
        !self.parent.is_empty()
    }

    pub fn finding(&self, message: String) -> Finding {
        Finding::new(subject_of(self.rpc), message)
    }

    pub fn annotation(&self) -> &model::ResourceAnnotation {
        self.resource
            .resource_annotation
            .as_ref()
            .expect("standard methods belong to resources")
    }
}

/// Names a method in findings, e.g. `hivekeep.api.v1.Control.GetActor`.
pub(super) fn subject_of(m: &model::Method) -> String {
    append_name(&m.service_full_name, &m.name)
}

/// A standard method's verb, which its name starts with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Verb {
    Get,
    List,
    Create,
    Update,
    Delete,
}

impl Verb {
    pub const ALL: [Verb; 5] = [
        Verb::Get,
        Verb::List,
        Verb::Create,
        Verb::Update,
        Verb::Delete,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Verb::Get => "Get",
            Verb::List => "List",
            Verb::Create => "Create",
            Verb::Update => "Update",
            Verb::Delete => "Delete",
        }
    }
}

impl std::fmt::Display for Verb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The standard verb `method_name` starts with, if another word follows it:
/// `Get` for `GetActorEgressPolicy`, nothing for `Getaway`.
pub(super) fn standard_verb_prefix(method_name: &str) -> Option<Verb> {
    Verb::ALL.into_iter().find(|v| {
        method_name
            .strip_prefix(v.as_str())
            .and_then(|rest| rest.chars().next())
            .is_some_and(char::is_uppercase)
    })
}

/// How a method with verb `v` names `resource`: by its plural for List, by its
/// name otherwise.
pub(super) fn resource_name_for(v: Verb, resource: &Message) -> &str {
    let a = resource.resource_annotation.as_ref().expect("resource");
    if v == Verb::List {
        &a.plural
    } else {
        &resource.name
    }
}

/// Parses `method_name` as a standard method on `resource`: the verb, then one
/// of the resource's parents (none for a top-level resource), then the resource
/// as [`resource_name_for`] names it. Returns the verb and the parent.
pub(super) fn parse_standard_method(
    method_name: &str,
    resource: &Message,
) -> Option<(Verb, String)> {
    let a = resource.resource_annotation.as_ref()?;
    let parents: Vec<&str> = if a.parents.is_empty() {
        vec![""]
    } else {
        a.parents.iter().map(String::as_str).collect()
    };
    for p in parents {
        for v in Verb::ALL {
            if method_name == format!("{v}{p}{}", resource_name_for(v, resource)) {
                return Some((v, p.to_string()));
            }
        }
    }
    None
}

/// Describes how a standard method with verb `v` on `resource` is named.
pub(super) fn want_standard_name(v: Verb, resource: &Message) -> String {
    let parents = &resource
        .resource_annotation
        .as_ref()
        .expect("resource")
        .parents;
    if parents.is_empty() {
        format!("want {v}, then {}", resource_name_for(v, resource))
    } else {
        format!(
            "want {v}, then one of the parents {}, then {}",
            parents.join("/"),
            resource_name_for(v, resource)
        )
    }
}

fn message_or_placeholder(
    by_name: &std::collections::HashMap<&str, &Message>,
    full: &str,
) -> Message {
    by_name
        .get(full)
        .map(|m| (*m).clone())
        .unwrap_or_else(|| Message {
            full_name: full.to_string(),
            name: short_name(full).to_string(),
            ..Message::default()
        })
}

/// Runs `check` on every standard method whose verb is one of `verbs`.
pub(super) fn for_each_standard_method(
    api: &Api,
    verbs: &[Verb],
    check: impl Fn(&StdMethod<'_>) -> Vec<Finding>,
) -> CheckResult {
    let resources = model::resources(api)?;
    let by_name = api.messages_by_full_name();
    let mut findings = Vec::new();
    for r in &resources {
        for rpc in &r.methods {
            let Some(prefix) = standard_verb_prefix(&rpc.name) else {
                continue;
            };
            let Some((verb, parent)) = parse_standard_method(&rpc.name, &r.message) else {
                return Err(LintError::Method(format!(
                    "{}: a method named with {prefix} is a standard method - {}, or name it with a custom verb",
                    subject_of(rpc),
                    want_standard_name(prefix, &r.message)
                )));
            };
            if !verbs.contains(&verb) {
                continue;
            }
            let m = StdMethod {
                verb,
                parent,
                rpc,
                resource: &r.message,
                request: message_or_placeholder(&by_name, &rpc.input_name),
                response: message_or_placeholder(&by_name, &rpc.output_name),
            };
            findings.extend(check(&m));
        }
    }
    Ok(findings)
}

/// Runs `check` on every resource.
pub(super) fn for_each_resource(
    api: &Api,
    check: impl Fn(&model::Resource) -> Vec<Finding>,
) -> CheckResult {
    Ok(model::resources(api)?.iter().flat_map(check).collect())
}

/// Runs `check` on every message.
pub(super) fn for_each_message(api: &Api, check: impl Fn(&Message) -> Vec<Finding>) -> CheckResult {
    Ok(api.messages.iter().flat_map(check).collect())
}

/// Runs `check` on every field of every message.
pub(super) fn for_each_field(
    api: &Api,
    check: impl Fn(&Message, &model::Field) -> Vec<Finding>,
) -> CheckResult {
    Ok(api
        .messages
        .iter()
        .flat_map(|m| {
            m.fields
                .iter()
                .flat_map(|f| check(m, f))
                .collect::<Vec<_>>()
        })
        .collect())
}

/// Runs `check` on every method of every service.
pub(super) fn for_each_method(
    api: &Api,
    check: impl Fn(&model::Method) -> Vec<Finding>,
) -> CheckResult {
    Ok(api
        .services
        .iter()
        .flat_map(|s| s.methods.iter().flat_map(&check).collect::<Vec<_>>())
        .collect())
}

/// Runs `check` on every enum.
pub(super) fn for_each_enum(
    api: &Api,
    check: impl Fn(&model::Enum) -> Vec<Finding>,
) -> CheckResult {
    Ok(api.enums.iter().flat_map(check).collect())
}

/// Runs every check in order and joins their findings; any error drops them all.
pub(super) fn all_of(api: &Api, checks: &[&dyn Fn(&Api) -> CheckResult]) -> CheckResult {
    let mut findings = Vec::new();
    for check in checks {
        findings.extend(check(api)?);
    }
    Ok(findings)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_of_joins_findings_in_order() {
        let a = |_: &Api| -> CheckResult { Ok(vec![Finding::new("a", "m")]) };
        let b = |_: &Api| -> CheckResult { Ok(vec![Finding::new("b", "m")]) };
        let got = all_of(&Api::default(), &[&a, &b]).unwrap();
        assert_eq!(got, vec![Finding::new("a", "m"), Finding::new("b", "m")]);
    }

    #[test]
    fn all_of_error_drops_every_finding() {
        let a = |_: &Api| -> CheckResult { Ok(vec![Finding::new("a", "m")]) };
        let fails = |_: &Api| -> CheckResult { Err(LintError::Method("boom".into())) };
        assert!(all_of(&Api::default(), &[&a, &fails]).is_err());
    }
}
