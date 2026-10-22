//! Naming rules: request/response message names, subresource names, enums.

use std::sync::LazyLock;

use regex::Regex;

use crate::apitool::model::{self, append_name, short_name, Api};

use super::method::{
    for_each_enum, for_each_method, for_each_resource, standard_verb_prefix, subject_of,
};
use super::{CheckResult, Finding, Rule};

pub const REQUEST_NAME_MATCHES_METHOD: Rule = Rule {
    name: "request-name-matches-method",
    description: r#"Every method's request message is named "{MethodName}Request"."#,
    check: check_request_name_matches_method,
};

fn check_request_name_matches_method(api: &Api) -> CheckResult {
    for_each_method(api, |rpc| {
        let got = short_name(&rpc.input_name);
        let want = format!("{}Request", rpc.name);
        if got == want {
            vec![]
        } else {
            vec![Finding::new(
                subject_of(rpc),
                format!("request message is named {got:?}, want {want:?}"),
            )]
        }
    })
}

pub const CUSTOM_RESPONSE_NAME_MATCHES_METHOD: Rule = Rule {
    name: "custom-response-name-matches-method",
    description: r#"Every custom method's response message is named "{MethodName}Response", not the resource itself."#,
    check: check_custom_response_name,
};

fn check_custom_response_name(api: &Api) -> CheckResult {
    for_each_resource(api, |r| {
        r.methods
            .iter()
            .filter(|rpc| standard_verb_prefix(&rpc.name).is_none())
            .filter_map(|rpc| {
                let got = short_name(&rpc.output_name);
                let want = format!("{}Response", rpc.name);
                (got != want).then(|| {
                    Finding::new(
                        subject_of(rpc),
                        format!("response message is named {got:?}, want {want:?}"),
                    )
                })
            })
            .collect()
    })
}

pub const SUBRESOURCE_NAMING: Rule = Rule {
    name: "subresource-naming",
    description: "A subresource's message is named without its parent type.",
    check: check_subresource_naming,
};

fn check_subresource_naming(api: &Api) -> CheckResult {
    for_each_resource(api, |r| {
        let parents = r
            .message
            .resource_annotation
            .as_ref()
            .map(|a| a.parents.clone())
            .unwrap_or_default();
        parents
            .iter()
            .filter(|p| r.message.name.starts_with(p.as_str()))
            .map(|p| {
                Finding::new(
                    r.message.full_name.clone(),
                    format!("message is named with its parent type {p} - name it for the subresource alone"),
                )
            })
            .collect()
    })
}

pub const ENUM_ZERO_VALUE_UNSPECIFIED: Rule = Rule {
    name: "enum-zero-value-unspecified",
    description: r#"Every enum's zero value is named "{EnumName}_UNSPECIFIED"."#,
    check: check_enum_zero_value_unspecified,
};

pub const ENUM_VALUES_PREFIXED: Rule = Rule {
    name: "enum-values-prefixed",
    description: r#"Every enum's values are prefixed with "{ENUM_NAME}_"."#,
    check: check_enum_values_prefixed,
};

fn enum_prefix(e: &model::Enum) -> String {
    format!("{}_", snake_case(short_name(&e.full_name)).to_uppercase())
}

fn check_enum_zero_value_unspecified(api: &Api) -> CheckResult {
    for_each_enum(api, |e| {
        let want = format!("{}UNSPECIFIED", enum_prefix(e));
        match e.value_by_number(0) {
            None => vec![Finding::new(e.full_name.clone(), "has no value numbered 0")],
            Some(zero) if zero.name != want => vec![Finding::new(
                e.full_name.clone(),
                format!("zero value is named {:?}, want {want:?}", zero.name),
            )],
            Some(_) => vec![],
        }
    })
}

fn check_enum_values_prefixed(api: &Api) -> CheckResult {
    for_each_enum(api, |e| {
        let prefix = enum_prefix(e);
        e.values
            .iter()
            .filter(|v| !v.name.starts_with(&prefix))
            .map(|v| {
                Finding::new(
                    append_name(&e.full_name, &v.name),
                    format!("not prefixed with {prefix:?}"),
                )
            })
            .collect()
    })
}

static PASCAL_BOUNDARY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"([a-z0-9])([A-Z])").expect("valid regex"));

/// Converts PascalCase to snake_case: `actor_assignment` for `ActorAssignment`.
pub(super) fn snake_case(s: &str) -> String {
    PASCAL_BOUNDARY.replace_all(s, "${1}_${2}").to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::*;
    use super::super::STANDARD_METHOD_RETURNS_RESOURCE;
    use super::*;
    use crate::apitool::model::{Enum, EnumValue};

    #[test]
    fn snake_case_examples() {
        assert_eq!(snake_case("ActorAssignment"), "actor_assignment");
        assert_eq!(snake_case("Hive"), "hive");
        assert_eq!(snake_case("GadgetBox"), "gadget_box");
    }

    #[test]
    fn request_name_matches_method() {
        let cases = vec![
            (
                "compliant",
                method_api(
                    raw_rpc("DoThing", "test.DoThingRequest", "test.DoThingResponse"),
                    vec![
                        msg("DoThingRequest", vec![]),
                        msg("DoThingResponse", vec![]),
                    ],
                ),
                vec![],
            ),
            (
                "wrong suffix",
                method_api(
                    raw_rpc("DoThing", "test.DoThingReq", "test.DoThingResponse"),
                    vec![msg("DoThingReq", vec![]), msg("DoThingResponse", vec![])],
                ),
                findings(
                    "test.Control.DoThing",
                    &[r#"request message is named "DoThingReq", want "DoThingRequest""#],
                ),
            ),
            (
                "wrong verb",
                method_api(
                    raw_rpc("DoThing", "test.GetThingRequest", "test.DoThingResponse"),
                    vec![
                        msg("GetThingRequest", vec![]),
                        msg("DoThingResponse", vec![]),
                    ],
                ),
                findings(
                    "test.Control.DoThing",
                    &[r#"request message is named "GetThingRequest", want "DoThingRequest""#],
                ),
            ),
            (
                "request declared outside this API",
                method_api(
                    raw_rpc("DoThing", "google.protobuf.Empty", "test.DoThingResponse"),
                    vec![msg("DoThingResponse", vec![])],
                ),
                findings(
                    "test.Control.DoThing",
                    &[r#"request message is named "Empty", want "DoThingRequest""#],
                ),
            ),
        ];
        for (name, api, want) in cases {
            assert_eq!(
                (REQUEST_NAME_MATCHES_METHOD.check)(&api).unwrap(),
                want,
                "{name}"
            );
        }
    }

    #[test]
    fn custom_response_name_matches_method() {
        let cases = vec![
            (
                "compliant",
                rpc("DrainWidget", "Widget", "DrainWidgetResponse"),
                vec![],
            ),
            (
                "returns the resource",
                rpc("DrainWidget", "Widget", "Widget"),
                findings(
                    "test.Control.DrainWidget",
                    &[r#"response message is named "Widget", want "DrainWidgetResponse""#],
                ),
            ),
            (
                "wrong name",
                rpc("DrainWidget", "Widget", "DrainResponse"),
                findings(
                    "test.Control.DrainWidget",
                    &[r#"response message is named "DrainResponse", want "DrainWidgetResponse""#],
                ),
            ),
            (
                "a standard method is not checked",
                rpc("GetWidget", "Widget", "Widget"),
                vec![],
            ),
        ];
        for (name, method, want) in cases {
            let api = method_api(method, vec![resource("Widget", &[], vec![])]);
            assert_eq!(
                (CUSTOM_RESPONSE_NAME_MATCHES_METHOD.check)(&api).unwrap(),
                want,
                "{name}"
            );
        }
    }

    #[test]
    fn standard_method_naming() {
        let widget_of_gadget = || {
            vec![
                resource("Widget", &["Gadget"], vec![]),
                resource("Gadget", &[], vec![]),
            ]
        };
        let err = |m: &str, verb: &str, how: &str| {
            format!("test.Control.{m}: a method named with {verb} is a standard method - {how}, or name it with a custom verb")
        };
        let cases: Vec<(&str, Method, Vec<Message>, String)> = vec![
            (
                "Get naming a parent",
                rpc("GetGadgetWidget", "Widget", "Widget"),
                widget_of_gadget(),
                String::new(),
            ),
            (
                "Get naming Global",
                rpc("GetGlobalWidget", "Widget", "Widget"),
                vec![resource("Widget", &["Global"], vec![])],
                String::new(),
            ),
            (
                "Get naming the second of two parents",
                rpc("GetGadgetWidget", "Widget", "Widget"),
                vec![
                    resource("Widget", &["Global", "Gadget"], vec![]),
                    resource("Gadget", &[], vec![]),
                ],
                String::new(),
            ),
            (
                "Get naming no parent",
                rpc("GetWidget", "Widget", "Widget"),
                widget_of_gadget(),
                err(
                    "GetWidget",
                    "Get",
                    "want Get, then one of the parents Gadget, then Widget",
                ),
            ),
            (
                "Get naming a resource that isn't a parent",
                rpc("GetGadgetBoxWidget", "Widget", "Widget"),
                vec![
                    resource("Widget", &["Gadget"], vec![]),
                    resource("GadgetBox", &[], vec![]),
                    resource("Gadget", &[], vec![]),
                ],
                err(
                    "GetGadgetBoxWidget",
                    "Get",
                    "want Get, then one of the parents Gadget, then Widget",
                ),
            ),
            (
                "Get naming neither of two parents",
                rpc("GetWidget", "Widget", "Widget"),
                vec![
                    resource("Widget", &["Global", "Gadget"], vec![]),
                    resource("Gadget", &[], vec![]),
                ],
                err(
                    "GetWidget",
                    "Get",
                    "want Get, then one of the parents Global/Gadget, then Widget",
                ),
            ),
            (
                "List naming a parent and the plural",
                rpc("ListGadgetWidgets", "Widget", "Widget"),
                widget_of_gadget(),
                String::new(),
            ),
            (
                "List with the singular",
                rpc("ListGadgetWidget", "Widget", "Widget"),
                widget_of_gadget(),
                err(
                    "ListGadgetWidget",
                    "List",
                    "want List, then one of the parents Gadget, then Widgets",
                ),
            ),
            (
                "List naming no parent",
                rpc("ListWidgets", "Widget", "Widget"),
                widget_of_gadget(),
                err(
                    "ListWidgets",
                    "List",
                    "want List, then one of the parents Gadget, then Widgets",
                ),
            ),
            (
                "List with the declared plural",
                rpc("ListGadgetPeople", "Person", "Person"),
                vec![
                    plural_resource("Person", "People", &["Gadget"]),
                    resource("Gadget", &[], vec![]),
                ],
                String::new(),
            ),
            (
                "List with a plural other than the declared one",
                rpc("ListGadgetPersons", "Person", "Person"),
                vec![
                    plural_resource("Person", "People", &["Gadget"]),
                    resource("Gadget", &[], vec![]),
                ],
                err(
                    "ListGadgetPersons",
                    "List",
                    "want List, then one of the parents Gadget, then People",
                ),
            ),
            (
                "custom method",
                rpc("DrainWidget", "Widget", "Widget"),
                widget_of_gadget(),
                String::new(),
            ),
            (
                "word starting with a verb is not a verb",
                rpc("Getaway", "Widget", "Widget"),
                widget_of_gadget(),
                String::new(),
            ),
            (
                "top-level Get",
                rpc("GetWidget", "Widget", "Widget"),
                vec![resource("Widget", &[], vec![])],
                String::new(),
            ),
            (
                "top-level List",
                rpc("ListWidgets", "Widget", "Widget"),
                vec![resource("Widget", &[], vec![])],
                String::new(),
            ),
            (
                "top-level Get with the plural",
                rpc("GetWidgets", "Widget", "Widget"),
                vec![resource("Widget", &[], vec![])],
                err("GetWidgets", "Get", "want Get, then Widget"),
            ),
            (
                "top-level List with the singular",
                rpc("ListWidget", "Widget", "Widget"),
                vec![resource("Widget", &[], vec![])],
                err("ListWidget", "List", "want List, then Widgets"),
            ),
            (
                "top-level Get naming a parent it doesn't have",
                rpc("GetGadgetWidget", "Widget", "Widget"),
                vec![
                    resource("Widget", &[], vec![]),
                    resource("Gadget", &[], vec![]),
                ],
                err("GetGadgetWidget", "Get", "want Get, then Widget"),
            ),
            (
                "message named with its parent type",
                rpc("GetGadgetGadgetWidget", "GadgetWidget", "GadgetWidget"),
                vec![
                    resource("GadgetWidget", &["Gadget"], vec![]),
                    resource("Gadget", &[], vec![]),
                ],
                String::new(),
            ),
        ];
        for (name, method, messages, want_err) in cases {
            // Every rule over standard methods parses them the same way; this
            // one stands in for all of them.
            let got = (STANDARD_METHOD_RETURNS_RESOURCE.check)(&method_api(method, messages))
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default();
            assert_eq!(got, want_err, "{name}");
        }
    }

    use crate::apitool::model::{Message, Method};

    #[test]
    fn subresource_naming() {
        let cases = vec![
            (
                "message named with its parent type",
                method_api(
                    rpc("GetGadgetGadgetWidget", "GadgetWidget", "GadgetWidget"),
                    vec![resource("GadgetWidget", &["Gadget"], vec![]), resource("Gadget", &[], vec![])],
                ),
                findings("test.GadgetWidget", &["message is named with its parent type Gadget - name it for the subresource alone"]),
            ),
            (
                "message named with Global",
                method_api(
                    rpc("GetGlobalGlobalWidget", "GlobalWidget", "GlobalWidget"),
                    vec![resource("GlobalWidget", &["Global", "Gadget"], vec![]), resource("Gadget", &[], vec![])],
                ),
                findings("test.GlobalWidget", &["message is named with its parent type Global - name it for the subresource alone"]),
            ),
            (
                "top-level resource named after another resource",
                method_api(
                    rpc("GetGadgetWidget", "GadgetWidget", "GadgetWidget"),
                    vec![resource("GadgetWidget", &[], vec![]), resource("Gadget", &[], vec![])],
                ),
                vec![],
            ),
        ];
        for (name, api, want) in cases {
            assert_eq!((SUBRESOURCE_NAMING.check)(&api).unwrap(), want, "{name}");
        }
    }

    fn enumeration(full: &str, parent: &str, values: &[(&str, i32)]) -> Enum {
        Enum {
            full_name: full.into(),
            name: full.trim_start_matches("test.").into(),
            parent_full_name: parent.into(),
            comment: String::new(),
            values: values
                .iter()
                .map(|(n, v)| EnumValue {
                    name: n.to_string(),
                    number: *v,
                    comment: String::new(),
                })
                .collect(),
        }
    }

    fn enum_api(e: Enum) -> Api {
        Api {
            enums: vec![e],
            ..Api::default()
        }
    }

    #[test]
    fn enum_zero_value_unspecified() {
        let cases = vec![
            (
                "compliant top-level",
                enumeration(
                    "test.ActorState",
                    "",
                    &[("ACTOR_STATE_UNSPECIFIED", 0), ("ACTOR_STATE_RUNNING", 1)],
                ),
                vec![],
            ),
            (
                "compliant nested",
                enumeration(
                    "test.Worker.State",
                    "test.Worker",
                    &[("STATE_UNSPECIFIED", 0), ("STATE_ACTIVE", 1)],
                ),
                vec![],
            ),
            (
                "wrong zero-value name",
                enumeration("test.ActorState", "", &[("UNKNOWN", 0)]),
                findings(
                    "test.ActorState",
                    &[r#"zero value is named "UNKNOWN", want "ACTOR_STATE_UNSPECIFIED""#],
                ),
            ),
            (
                "no zero value at all",
                enumeration("test.ActorState", "", &[("ACTOR_STATE_RUNNING", 1)]),
                findings("test.ActorState", &["has no value numbered 0"]),
            ),
        ];
        for (name, e, want) in cases {
            assert_eq!(
                (ENUM_ZERO_VALUE_UNSPECIFIED.check)(&enum_api(e)).unwrap(),
                want,
                "{name}"
            );
        }
    }

    #[test]
    fn enum_values_prefixed() {
        let cases = vec![
            (
                "compliant top-level",
                enumeration(
                    "test.SandboxClass",
                    "",
                    &[
                        ("SANDBOX_CLASS_UNSPECIFIED", 0),
                        ("SANDBOX_CLASS_GVISOR", 1),
                    ],
                ),
                vec![],
            ),
            (
                "unprefixed top-level value",
                enumeration(
                    "test.SandboxClass",
                    "",
                    &[("UNSPECIFIED", 0), ("GVISOR", 1)],
                ),
                vec![
                    Finding::new(
                        "test.SandboxClass.UNSPECIFIED",
                        r#"not prefixed with "SANDBOX_CLASS_""#,
                    ),
                    Finding::new(
                        "test.SandboxClass.GVISOR",
                        r#"not prefixed with "SANDBOX_CLASS_""#,
                    ),
                ],
            ),
            (
                "compliant nested",
                enumeration(
                    "test.Worker.State",
                    "test.Worker",
                    &[("STATE_UNSPECIFIED", 0), ("STATE_ACTIVE", 1)],
                ),
                vec![],
            ),
            (
                "unprefixed nested value",
                enumeration(
                    "test.Worker.State",
                    "test.Worker",
                    &[("UNSPECIFIED", 0), ("ACTIVE", 1)],
                ),
                vec![
                    Finding::new(
                        "test.Worker.State.UNSPECIFIED",
                        r#"not prefixed with "STATE_""#,
                    ),
                    Finding::new("test.Worker.State.ACTIVE", r#"not prefixed with "STATE_""#),
                ],
            ),
        ];
        for (name, e, want) in cases {
            assert_eq!(
                (ENUM_VALUES_PREFIXED.check)(&enum_api(e)).unwrap(),
                want,
                "{name}"
            );
        }
    }
}
