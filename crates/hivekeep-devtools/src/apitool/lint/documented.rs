//! Every API element carries a doc comment.

use crate::apitool::model::{append_name, Api};

use super::method::{
    all_of, for_each_enum, for_each_field, for_each_message, for_each_method, subject_of,
};
use super::{CheckResult, Finding, Rule};

pub const DOCUMENTED: Rule = Rule {
    name: "documented",
    description: "Every message, enum, message field, and RPC method has a doc comment.",
    check: check_documented,
};

fn check_documented(api: &Api) -> CheckResult {
    all_of(
        api,
        &[
            &|api| {
                for_each_message(api, |m| {
                    undocumented(&m.comment, &m.full_name, "message has no doc comment")
                })
            },
            &|api| {
                for_each_field(api, |m, f| {
                    undocumented(
                        &f.comment,
                        &append_name(&m.full_name, &f.name),
                        "field has no doc comment",
                    )
                })
            },
            &|api| {
                for_each_enum(api, |e| {
                    undocumented(&e.comment, &e.full_name, "enum has no doc comment")
                })
            },
            &|api| {
                for_each_method(api, |rpc| {
                    undocumented(&rpc.comment, &subject_of(rpc), "method has no doc comment")
                })
            },
        ],
    )
}

fn undocumented(comment: &str, subject: &str, message: &str) -> Vec<Finding> {
    if doc_text(comment).is_empty() {
        vec![Finding::new(subject, message)]
    } else {
        vec![]
    }
}

/// The prose of a comment, without machine-readable tag lines.
fn doc_text(comment: &str) -> String {
    comment
        .split('\n')
        .filter(|line| !is_tag_line(line))
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

/// Whether `line` is a machine-readable annotation rather than prose: a
/// declarative-validation tag (`+k8s:required`), one of our own metadata tags
/// (`+hk:preview`), or a field-rules summary (`Rules: required; dns-label.`).
fn is_tag_line(line: &str) -> bool {
    let line = line.trim();
    line.starts_with("+k8s:") || line.starts_with("+hk:") || line.starts_with("Rules:")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apitool::model::{Enum, Field, Message, Method, Service};

    fn api(message: &str, enumeration: &str, field: &str, method: &str) -> Api {
        Api {
            services: vec![Service {
                name: "Control".into(),
                methods: vec![Method {
                    name: "DoThing".into(),
                    service_full_name: "test.Control".into(),
                    service_name: "Control".into(),
                    comment: method.into(),
                    input_name: "test.DoThingRequest".into(),
                    output_name: "test.DoThingResponse".into(),
                    ..Method::default()
                }],
                ..Service::default()
            }],
            messages: vec![Message {
                full_name: "test.DoThingRequest".into(),
                name: "DoThingRequest".into(),
                comment: message.into(),
                fields: vec![Field {
                    name: "widget".into(),
                    comment: field.into(),
                    ..Field::default()
                }],
                ..Message::default()
            }],
            enums: vec![Enum {
                full_name: "test.Widget.State".into(),
                name: "Widget.State".into(),
                parent_full_name: "test.Widget".into(),
                comment: enumeration.into(),
                values: vec![],
            }],
        }
    }

    const MSG: (&str, &str) = ("test.DoThingRequest", "message has no doc comment");
    const FIELD: (&str, &str) = ("test.DoThingRequest.widget", "field has no doc comment");
    const ENUM: (&str, &str) = ("test.Widget.State", "enum has no doc comment");
    const METHOD: (&str, &str) = ("test.Control.DoThing", "method has no doc comment");

    type Case = (
        &'static str,
        [&'static str; 4],
        Vec<(&'static str, &'static str)>,
    );

    #[test]
    fn documented() {
        let cases: Vec<Case> = vec![
            (
                "all documented",
                ["a request", "a state", "does a thing", "does another thing"],
                vec![],
            ),
            (
                "message undocumented",
                ["", "a state", "does a thing", "does another thing"],
                vec![MSG],
            ),
            (
                "enum undocumented",
                ["a request", "", "does a thing", "does another thing"],
                vec![ENUM],
            ),
            (
                "field undocumented",
                ["a request", "a state", "", "does another thing"],
                vec![FIELD],
            ),
            (
                "method undocumented",
                ["a request", "a state", "does a thing", ""],
                vec![METHOD],
            ),
            (
                "nothing documented",
                ["", "", "", ""],
                vec![MSG, FIELD, ENUM, METHOD],
            ),
            (
                "whitespace-only comment counts as undocumented",
                ["\t", "\t", "   ", "\n"],
                vec![MSG, FIELD, ENUM, METHOD],
            ),
            (
                "empty comment lines",
                ["a request", "a state", "\n\n", "does another thing"],
                vec![FIELD],
            ),
            (
                "tags only",
                [
                    "+k8s:required",
                    "+k8s:optional",
                    "+k8s:required\n+k8s:format=k8s-short-name",
                    "does another thing",
                ],
                vec![MSG, FIELD, ENUM],
            ),
            (
                "rules summaries only",
                [
                    "Rules: required.",
                    "+hk:preview",
                    "Rules: dns-label.",
                    "does another thing",
                ],
                vec![MSG, FIELD, ENUM],
            ),
            (
                "prose and tags",
                [
                    "a request\n\n+k8s:required",
                    "a state",
                    "does a thing\n\n+k8s:immutable",
                    "does another thing",
                ],
                vec![],
            ),
        ];
        for (name, [m, e, f, rpc], want) in cases {
            let want: Vec<Finding> = want
                .into_iter()
                .map(|(s, msg)| Finding::new(s, msg))
                .collect();
            assert_eq!(
                (DOCUMENTED.check)(&api(m, e, f, rpc)).unwrap(),
                want,
                "{name}"
            );
        }
    }
}
