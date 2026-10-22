//! Checking that a message has exactly an expected set of fields.

use crate::apitool::model::{Field, Kind, Message};

use super::Finding;

/// Whether a field has the wanted type (including repeatedness), and how that
/// type is named in findings.
#[derive(Debug, Clone)]
pub(super) enum TypeMatcher {
    Scalar(Kind),
    Message(String),
    RepeatedMessage(String),
}

impl TypeMatcher {
    pub fn label(&self) -> String {
        match self {
            TypeMatcher::Scalar(k) => k.to_string(),
            TypeMatcher::Message(t) => t.clone(),
            TypeMatcher::RepeatedMessage(t) => format!("repeated {t}"),
        }
    }

    pub fn matches(&self, f: &Field) -> bool {
        match self {
            TypeMatcher::Scalar(k) => !f.repeated && f.kind == *k,
            TypeMatcher::Message(t) => is_message_of(f, t),
            TypeMatcher::RepeatedMessage(t) => {
                f.repeated && f.kind == Kind::Message && f.type_full_name == *t
            }
        }
    }
}

pub(super) fn scalar(kind: Kind) -> TypeMatcher {
    TypeMatcher::Scalar(kind)
}

pub(super) fn message(type_full_name: &str) -> TypeMatcher {
    TypeMatcher::Message(type_full_name.to_string())
}

pub(super) fn repeated_message(type_full_name: &str) -> TypeMatcher {
    TypeMatcher::RepeatedMessage(type_full_name.to_string())
}

/// One field a message should have.
#[derive(Debug, Clone)]
pub(super) struct WantField {
    pub name: String,
    pub typ: TypeMatcher,
    /// The field may be absent.
    pub optional: bool,
}

impl WantField {
    fn describe(&self, with_optional: bool) -> String {
        let prefix = if self.optional && with_optional {
            "optional "
        } else {
            ""
        };
        format!("{prefix}{:?} {}", self.name, self.typ.label())
    }
}

/// Some of the fields a message should have.
pub(super) type WantFields = Vec<WantField>;

/// Exactly one field named `name`, of type `t`.
pub(super) fn required(name: &str, t: TypeMatcher) -> WantFields {
    vec![WantField {
        name: name.into(),
        typ: t,
        optional: false,
    }]
}

/// At most one field named `name`, of type `t`.
pub(super) fn optional(name: &str, t: TypeMatcher) -> WantFields {
    vec![WantField {
        name: name.into(),
        typ: t,
        optional: true,
    }]
}

/// Checks that `msg` has exactly the fields `want` lists and nothing else.
/// `role` names the message in findings, such as "request".
pub(super) fn check_fields(
    subject: &str,
    role: &str,
    msg: &Message,
    want: &[WantFields],
) -> Vec<Finding> {
    let all: Vec<&WantField> = want.iter().flatten().collect();
    let mut findings = Vec::new();
    let mut counts = vec![0usize; all.len()];
    let mut assigned = vec![false; msg.fields.len()];
    let finding = |m: String| Finding::new(subject, m);

    // A field with a wanted name belongs to that wanted field, whatever its type.
    for (fi, f) in msg.fields.iter().enumerate() {
        if let Some(wi) = all.iter().position(|w| w.name == f.name) {
            assigned[fi] = true;
            counts[wi] += 1;
            if !all[wi].typ.matches(f) {
                findings.push(finding(format!(
                    "{role} field {:?} is {}, want {}",
                    f.name,
                    type_of(f),
                    all[wi].typ.label()
                )));
            }
        }
    }
    for (fi, f) in msg.fields.iter().enumerate() {
        if assigned[fi] {
            continue;
        }
        match match_by_type(&all, &counts, f) {
            None => {
                let wanted: Vec<String> = all.iter().map(|w| w.describe(true)).collect();
                findings.push(finding(format!(
                    "{role} field {:?} is not allowed - want only {}",
                    f.name,
                    join_and(&wanted)
                )));
            }
            Some(wi) => {
                counts[wi] += 1;
                findings.push(finding(format!(
                    "{role} field {:?} is {}, want it named {:?}",
                    f.name,
                    type_of(f),
                    all[wi].name
                )));
            }
        }
    }

    for (wi, w) in all.iter().enumerate() {
        if counts[wi] == 0 && !w.optional {
            findings.push(finding(format!("{role} has no {} field", w.describe(true))));
        } else if counts[wi] > 1 {
            findings.push(finding(format!(
                "{role} has {} {} fields, want at most 1",
                counts[wi],
                w.describe(false)
            )));
        }
    }
    findings
}

/// The wanted field `f` is misnamed for: one of `f`'s type not yet filled.
fn match_by_type(want: &[&WantField], counts: &[usize], f: &Field) -> Option<usize> {
    want.iter()
        .enumerate()
        .find(|(wi, w)| counts[*wi] == 0 && w.typ.matches(f))
        .map(|(wi, _)| wi)
}

/// Describes `f`'s type in findings: `string`, `hivekeep.api.v1.ObjectRef`, or
/// `repeated hivekeep.api.v1.Actor`.
pub(super) fn type_of(f: &Field) -> String {
    if f.type_full_name.is_empty() {
        f.type_display.clone()
    } else if f.repeated {
        format!("repeated {}", f.type_full_name)
    } else {
        f.type_full_name.clone()
    }
}

/// Joins names as an English list: "a and b", or "a, b, and c".
fn join_and(names: &[String]) -> String {
    match names {
        [] => String::new(),
        [a] => a.clone(),
        [a, b] => format!("{a} and {b}"),
        [rest @ .., last] => format!("{}, and {last}", rest.join(", ")),
    }
}

/// Whether `f` is a singular field of message type `type_full_name`.
pub(super) fn is_message_of(f: &Field, type_full_name: &str) -> bool {
    !f.repeated && f.kind == Kind::Message && f.type_full_name == type_full_name
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fld(name: &str, kind: Kind, type_full_name: &str, display: &str, repeated: bool) -> Field {
        Field {
            name: name.into(),
            kind,
            type_full_name: type_full_name.into(),
            type_display: display.into(),
            repeated,
            ..Field::default()
        }
    }

    #[test]
    fn check_fields_cases() {
        let reference = fld(
            "gadget",
            Kind::Message,
            "hivekeep.api.v1.ObjectRef",
            "",
            false,
        );
        let name = fld("name", Kind::String, "", "string", false);
        let opts = fld(
            "options",
            Kind::Message,
            "hivekeep.api.v1.DeleteOptions",
            "",
            false,
        );
        let want = vec![
            required("gadget", message("hivekeep.api.v1.ObjectRef")),
            required("name", scalar(Kind::String)),
            optional("options", message("hivekeep.api.v1.DeleteOptions")),
        ];
        let stray_msg = r#"want only "gadget" hivekeep.api.v1.ObjectRef, "name" string, and optional "options" hivekeep.api.v1.DeleteOptions"#;

        let cases: Vec<(&str, Vec<Field>, Vec<String>)> = vec![
            ("exactly the wanted fields", vec![reference.clone(), name.clone(), opts.clone()], vec![]),
            ("optional field absent", vec![reference.clone(), name.clone()], vec![]),
            (
                "required field missing",
                vec![reference.clone()],
                vec![r#"request has no "name" string field"#.into()],
            ),
            (
                "named field of the wrong type",
                vec![reference.clone(), fld("name", Kind::Int64, "", "int64", false)],
                vec![r#"request field "name" is int64, want string"#.into()],
            ),
            (
                "field of a required type, misnamed",
                vec![fld("parent", Kind::Message, "hivekeep.api.v1.ObjectRef", "", false), name.clone()],
                vec![r#"request field "parent" is hivekeep.api.v1.ObjectRef, want it named "gadget""#.into()],
            ),
            (
                "field of an already filled wanted field's type is stray",
                vec![
                    reference.clone(),
                    name.clone(),
                    fld("other", Kind::Message, "hivekeep.api.v1.ObjectRef", "", false),
                ],
                vec![format!(r#"request field "other" is not allowed - {stray_msg}"#)],
            ),
            (
                "stray field",
                vec![reference.clone(), name.clone(), fld("dry_run", Kind::Bool, "", "bool", false)],
                vec![format!(r#"request field "dry_run" is not allowed - {stray_msg}"#)],
            ),
            (
                "optional field twice",
                vec![reference.clone(), name.clone(), opts.clone(), opts.clone()],
                vec![r#"request has 2 "options" hivekeep.api.v1.DeleteOptions fields, want at most 1"#.into()],
            ),
            (
                "named field twice",
                vec![reference.clone(), reference.clone(), name.clone()],
                vec![r#"request has 2 "gadget" hivekeep.api.v1.ObjectRef fields, want at most 1"#.into()],
            ),
            (
                "repeated field where a singular message is wanted",
                vec![fld("gadget", Kind::Message, "hivekeep.api.v1.ObjectRef", "", true), name.clone()],
                vec![r#"request field "gadget" is repeated hivekeep.api.v1.ObjectRef, want hivekeep.api.v1.ObjectRef"#.into()],
            ),
            (
                "repeated field where a singular scalar is wanted",
                vec![reference.clone(), fld("name", Kind::String, "", "repeated string", true)],
                vec![r#"request field "name" is repeated string, want string"#.into()],
            ),
        ];
        for (case, fields, want_msgs) in cases {
            let msg = Message {
                fields,
                ..Message::default()
            };
            let got = check_fields("s", "request", &msg, &want);
            let want_findings: Vec<Finding> = want_msgs
                .into_iter()
                .map(|m| Finding::new("s", m))
                .collect();
            assert_eq!(got, want_findings, "{case}");
        }
    }
}
