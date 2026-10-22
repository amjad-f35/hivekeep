//! Shape rules for resource messages themselves.

use crate::apitool::model::{Api, Field, Message};

use super::fields::{is_message_of, type_of};
use super::method::for_each_resource;
use super::{CheckResult, Finding, Rule, RESOURCE_METADATA_TYPE};

pub const RESOURCE_METADATA: Rule = Rule {
    name: "resource-metadata",
    description: r#"Every resource message has a "ResourceMetadata metadata = 1" field."#,
    check: check_resource_metadata,
};

fn check_resource_metadata(api: &Api) -> CheckResult {
    for_each_resource(api, |r| {
        let subject = r.message.full_name.clone();
        match find_field_by_name(&r.message, "metadata") {
            None => vec![Finding::new(subject, r#"has no "metadata" field"#)],
            Some(f) if !is_message_of(f, RESOURCE_METADATA_TYPE) => vec![Finding::new(
                subject,
                format!(
                    r#""metadata" field is {}, want {RESOURCE_METADATA_TYPE}"#,
                    type_of(f)
                ),
            )],
            Some(f) if f.number != 1 => vec![Finding::new(
                subject,
                format!(r#""metadata" field is number {}, want 1"#, f.number),
            )],
            Some(_) => vec![],
        }
    })
}

pub const RESOURCE_STATUS_FIELD_SHAPE: Rule = Rule {
    name: "resource-status-field-shape",
    description: r#"A resource's "status" field, if present, is typed "{ResourceName}Status"."#,
    check: check_resource_status_field,
};

fn check_resource_status_field(api: &Api) -> CheckResult {
    for_each_resource(api, |r| {
        let Some(f) = find_field_by_name(&r.message, "status") else {
            return vec![];
        };
        let want = format!("{}Status", r.message.full_name);
        if is_message_of(f, &want) {
            vec![]
        } else {
            vec![Finding::new(
                r.message.full_name.clone(),
                format!(r#""status" field is {}, want {want}"#, type_of(f)),
            )]
        }
    })
}

fn find_field_by_name<'a>(m: &'a Message, name: &str) -> Option<&'a Field> {
    m.fields.iter().find(|f| f.name == name)
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::*;
    use super::*;
    use crate::apitool::model::Kind;

    fn metadata(
        number: i32,
        kind: Kind,
        type_full_name: &str,
        display: &str,
        repeated: bool,
    ) -> Field {
        Field {
            name: "metadata".into(),
            number,
            kind,
            type_full_name: type_full_name.into(),
            type_display: display.into(),
            repeated,
            ..Field::default()
        }
    }

    #[test]
    fn resource_metadata() {
        let rm = "hivekeep.api.v1.ResourceMetadata";
        let cases: Vec<(&str, Api, Option<Vec<Finding>>)> = vec![
            (
                "compliant",
                messages_api(vec![resource(
                    "Widget",
                    &[],
                    vec![metadata(1, Kind::Message, rm, "", false)],
                )]),
                Some(vec![]),
            ),
            (
                "missing entirely",
                messages_api(vec![resource(
                    "Widget",
                    &[],
                    vec![Field {
                        number: 1,
                        ..named("name")
                    }],
                )]),
                Some(findings("test.Widget", &[r#"has no "metadata" field"#])),
            ),
            (
                "wrong type",
                messages_api(vec![resource(
                    "Widget",
                    &[],
                    vec![metadata(
                        1,
                        Kind::Message,
                        "hivekeep.api.v1.ObjectRef",
                        "",
                        false,
                    )],
                )]),
                Some(findings(
                    "test.Widget",
                    &[
                        r#""metadata" field is hivekeep.api.v1.ObjectRef, want hivekeep.api.v1.ResourceMetadata"#,
                    ],
                )),
            ),
            (
                "scalar",
                messages_api(vec![resource(
                    "Widget",
                    &[],
                    vec![metadata(1, Kind::String, "", "string", false)],
                )]),
                Some(findings(
                    "test.Widget",
                    &[r#""metadata" field is string, want hivekeep.api.v1.ResourceMetadata"#],
                )),
            ),
            (
                "repeated",
                messages_api(vec![resource(
                    "Widget",
                    &[],
                    vec![metadata(1, Kind::Message, rm, "", true)],
                )]),
                Some(findings(
                    "test.Widget",
                    &[
                        r#""metadata" field is repeated hivekeep.api.v1.ResourceMetadata, want hivekeep.api.v1.ResourceMetadata"#,
                    ],
                )),
            ),
            (
                "wrong field number",
                messages_api(vec![resource(
                    "Widget",
                    &[],
                    vec![metadata(2, Kind::Message, rm, "", false)],
                )]),
                Some(findings(
                    "test.Widget",
                    &[r#""metadata" field is number 2, want 1"#],
                )),
            ),
            (
                "a message that isn't a resource is not checked",
                messages_api(vec![msg("Widget", vec![])]),
                Some(vec![]),
            ),
            (
                "parent that isn't a resource",
                messages_api(vec![resource(
                    "Widget",
                    &["Gadget"],
                    vec![metadata(1, Kind::Message, rm, "", false)],
                )]),
                None,
            ),
        ];
        for (name, api, want) in cases {
            let got = (RESOURCE_METADATA.check)(&api);
            match want {
                Some(want) => assert_eq!(got.unwrap(), want, "{name}"),
                None => assert!(got.is_err(), "{name}"),
            }
        }
    }

    #[test]
    fn resource_status_field_shape() {
        let cases = vec![
            (
                "no status field at all",
                messages_api(vec![resource("Widget", &[], vec![])]),
                vec![],
            ),
            (
                "correctly typed status field",
                messages_api(vec![resource(
                    "Widget",
                    &[],
                    vec![msg_field("status", "test.WidgetStatus")],
                )]),
                vec![],
            ),
            (
                "wrongly typed status field",
                messages_api(vec![resource(
                    "Widget",
                    &[],
                    vec![msg_field("status", "test.WrongStatus")],
                )]),
                findings(
                    "test.Widget",
                    &[r#""status" field is test.WrongStatus, want test.WidgetStatus"#],
                ),
            ),
            (
                "status field is repeated",
                messages_api(vec![resource(
                    "Widget",
                    &[],
                    vec![repeated_field("status", "test.WidgetStatus")],
                )]),
                findings(
                    "test.Widget",
                    &[r#""status" field is repeated test.WidgetStatus, want test.WidgetStatus"#],
                ),
            ),
            (
                "status field is a scalar",
                messages_api(vec![resource(
                    "Widget",
                    &[],
                    vec![scalar_field("status", Kind::String)],
                )]),
                findings(
                    "test.Widget",
                    &[r#""status" field is string, want test.WidgetStatus"#],
                ),
            ),
            (
                "a message that isn't a resource is not checked",
                messages_api(vec![msg(
                    "Widget",
                    vec![scalar_field("status", Kind::String)],
                )]),
                vec![],
            ),
        ];
        for (name, api, want) in cases {
            assert_eq!(
                (RESOURCE_STATUS_FIELD_SHAPE.check)(&api).unwrap(),
                want,
                "{name}"
            );
        }
    }
}
