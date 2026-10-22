use std::collections::BTreeMap;

use super::*;
use crate::apitool::validate::API_SOURCE;

fn build_api(body: &str) -> Api {
    let text = format!("syntax = \"proto3\";\npackage fixture;\n\n{body}");
    build(&text).expect("compiling fixture")
}

fn field(name: &str, number: i32, kind: Kind, display: &str) -> Field {
    Field {
        name: name.into(),
        number,
        kind,
        type_display: display.into(),
        ..Field::default()
    }
}

fn message(full: &str, name: &str, fields: Vec<Field>) -> Message {
    Message {
        full_name: full.into(),
        name: name.into(),
        fields,
        ..Message::default()
    }
}

#[test]
fn build_map_field() {
    let got = build_api("message Widget {\n  map<string, string> labels = 1;\n}\n");
    let mut labels = field("labels", 1, Kind::Message, "map<string, string>");
    labels.map_value_kind = Kind::String;
    let want = Api {
        messages: vec![message("fixture.Widget", "Widget", vec![labels])],
        ..Api::default()
    };
    assert_eq!(got, want);
}

#[test]
fn build_repeated_message_field() {
    let got = build_api(
        "message Widget {\n  repeated Gadget gadgets = 1;\n}\n\nmessage Gadget {\n  string id = 1;\n}\n",
    );
    let mut gadgets = field("gadgets", 1, Kind::Message, "repeated Gadget");
    gadgets.repeated = true;
    gadgets.type_full_name = "fixture.Gadget".into();
    let want = Api {
        messages: vec![
            message("fixture.Widget", "Widget", vec![gadgets]),
            message(
                "fixture.Gadget",
                "Gadget",
                vec![field("id", 1, Kind::String, "string")],
            ),
        ],
        ..Api::default()
    };
    assert_eq!(got, want);
}

#[test]
fn build_map_value_message_field() {
    let got = build_api(
        "message Widget {\n  map<string, Gadget> gadgets_by_id = 1;\n}\n\nmessage Gadget {\n  string id = 1;\n}\n",
    );
    let mut f = field("gadgets_by_id", 1, Kind::Message, "map<string, Gadget>");
    f.map_value_full_name = "fixture.Gadget".into();
    f.map_value_kind = Kind::Message;
    let want = Api {
        messages: vec![
            message("fixture.Widget", "Widget", vec![f]),
            message(
                "fixture.Gadget",
                "Gadget",
                vec![field("id", 1, Kind::String, "string")],
            ),
        ],
        ..Api::default()
    };
    assert_eq!(got, want);
}

#[test]
fn build_oneof() {
    let got = build_api(
        "message Ref {\n  oneof reference {\n    string snapshot = 1;\n    string tag = 2;\n  }\n}\n",
    );
    let mut snapshot = field("snapshot", 1, Kind::String, "string");
    snapshot.oneof_name = "reference".into();
    let mut tag = field("tag", 2, Kind::String, "string");
    tag.oneof_name = "reference".into();
    let want = Api {
        messages: vec![message("fixture.Ref", "Ref", vec![snapshot, tag])],
        ..Api::default()
    };
    assert_eq!(got, want);
}

#[test]
fn build_nested_enums() {
    let got = build_api(
        "message Widget {\n  Status status = 1;\n\n  enum Status {\n    STATUS_UNSPECIFIED = 0;\n    STATUS_ACTIVE = 1;\n  }\n}\n",
    );
    let mut status = field("status", 1, Kind::Enum, "Widget.Status");
    status.type_full_name = "fixture.Widget.Status".into();
    let want = Api {
        messages: vec![message("fixture.Widget", "Widget", vec![status])],
        enums: vec![Enum {
            full_name: "fixture.Widget.Status".into(),
            name: "Widget.Status".into(),
            parent_full_name: "fixture.Widget".into(),
            comment: String::new(),
            values: vec![
                EnumValue {
                    name: "STATUS_UNSPECIFIED".into(),
                    number: 0,
                    comment: String::new(),
                },
                EnumValue {
                    name: "STATUS_ACTIVE".into(),
                    number: 1,
                    comment: String::new(),
                },
            ],
        }],
        ..Api::default()
    };
    assert_eq!(got, want);
}

#[test]
fn build_comments() {
    let got = build_api(
        "// Widget's doc comment.\nmessage Widget {\n  // name's doc comment.\n  string name = 1;\n}\n",
    );
    let mut name = field("name", 1, Kind::String, "string");
    name.comment = "name's doc comment.".into();
    let mut widget = message("fixture.Widget", "Widget", vec![name]);
    widget.comment = "Widget's doc comment.".into();
    let want = Api {
        messages: vec![widget],
        ..Api::default()
    };
    assert_eq!(got, want);
}

#[test]
fn build_proto3_optional() {
    let got = build_api("message Widget {\n  optional string nickname = 1;\n}\n");
    let mut f = field("nickname", 1, Kind::String, "string");
    f.proto3_optional = true;
    let want = Api {
        messages: vec![message("fixture.Widget", "Widget", vec![f])],
        ..Api::default()
    };
    assert_eq!(got, want);
}

#[test]
fn build_rejects_invalid_source() {
    let err = build("syntax = \"proto3\";\nmessage {").unwrap_err();
    assert!(
        err.to_string().starts_with("while compiling proto source:"),
        "{err}"
    );
}

fn annotation(parents: &[&str], singleton: bool, scope: Scope, plural: &str) -> ResourceAnnotation {
    ResourceAnnotation {
        parents: parents.iter().map(|s| s.to_string()).collect(),
        singleton,
        scope,
        plural: plural.into(),
    }
}

#[test]
fn build_annotations() {
    let api = build(API_SOURCE).expect("building the API source");

    let got: BTreeMap<String, ResourceAnnotation> = api
        .messages
        .iter()
        .filter_map(|m| Some((m.name.clone(), m.resource_annotation.clone()?)))
        .collect();
    let want: BTreeMap<String, ResourceAnnotation> = [
        ("Actor", annotation(&[], false, Scope::Hive, "Actors")),
        (
            "EgressPolicy",
            annotation(&["Actor"], true, Scope::Unspecified, "EgressPolicies"),
        ),
        ("Tag", annotation(&[], false, Scope::Hive, "Tags")),
        ("Hive", annotation(&[], false, Scope::Global, "Hives")),
        (
            "ActorTemplate",
            annotation(&[], false, Scope::Hive, "ActorTemplates"),
        ),
        ("Worker", annotation(&[], false, Scope::Global, "Workers")),
        (
            "ActorAssignment",
            annotation(&["Worker"], false, Scope::Unspecified, "ActorAssignments"),
        ),
        (
            "AccessPolicy",
            annotation(
                &["Global", "Hive"],
                true,
                Scope::Unspecified,
                "AccessPolicies",
            ),
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    assert_eq!(got, want);

    let want_methods: BTreeMap<&str, &str> = [
        ("GetActor", "Actor"),
        ("SuspendActor", "Actor"),
        ("GetActorEgressPolicy", "EgressPolicy"),
        ("GetGlobalAccessPolicy", "AccessPolicy"),
        ("GetHiveAccessPolicy", "AccessPolicy"),
        ("ListWorkerActorAssignments", "ActorAssignment"),
    ]
    .into_iter()
    .collect();
    let got_methods: BTreeMap<&str, &str> = api
        .services
        .iter()
        .flat_map(|s| &s.methods)
        .filter(|m| want_methods.contains_key(m.name.as_str()))
        .map(|m| (m.name.as_str(), m.resource.as_str()))
        .collect();
    assert_eq!(got_methods, want_methods);
}

fn method(name: &str, resource: &str) -> Method {
    Method {
        name: name.into(),
        service_name: "Control".into(),
        resource: resource.into(),
        ..Method::default()
    }
}

fn resource_msg(full: &str, name: &str, a: ResourceAnnotation) -> Message {
    Message {
        full_name: full.into(),
        name: name.into(),
        resource_annotation: Some(a),
        ..Message::default()
    }
}

#[test]
fn resources_groups_methods() {
    let api = Api {
        services: vec![Service {
            name: "Control".into(),
            methods: vec![
                method("GetActor", "Actor"),
                method("GetWorker", "Worker"),
                method("SuspendActor", "Actor"),
            ],
            ..Service::default()
        }],
        messages: vec![
            message("test.GetActorRequest", "GetActorRequest", vec![]),
            resource_msg(
                "test.Worker",
                "Worker",
                annotation(&[], false, Scope::Global, "Workers"),
            ),
            resource_msg(
                "test.Actor",
                "Actor",
                annotation(&[], false, Scope::Hive, "Actors"),
            ),
            resource_msg(
                "test.EgressPolicy",
                "EgressPolicy",
                annotation(
                    &["Global", "Actor"],
                    false,
                    Scope::Unspecified,
                    "EgressPolicies",
                ),
            ),
        ],
        ..Api::default()
    };
    let groups = resources(&api).expect("resources");
    let order: Vec<_> = groups.iter().map(|g| g.message.name.as_str()).collect();
    assert_eq!(order, ["Worker", "Actor", "EgressPolicy"]);
    let methods: Vec<Vec<&str>> = groups
        .iter()
        .map(|g| g.methods.iter().map(|m| m.name.as_str()).collect())
        .collect();
    assert_eq!(
        methods,
        vec![vec!["GetWorker"], vec!["GetActor", "SuspendActor"], vec![]]
    );
}

#[test]
fn resources_invalid() {
    let control = |methods: Vec<Method>| {
        vec![Service {
            name: "Control".into(),
            methods,
            ..Service::default()
        }]
    };
    let actor = resource_msg(
        "test.Actor",
        "Actor",
        annotation(&[], false, Scope::Hive, "Actors"),
    );
    let cases: Vec<(&str, Api, &str)> = vec![
        (
            "method without annotation",
            Api {
                services: control(vec![method("GetActor", "")]),
                messages: vec![actor.clone()],
                ..Api::default()
            },
            "Control.GetActor: no resource annotation",
        ),
        (
            "method annotated with a message that isn't a resource",
            Api {
                services: control(vec![method("GetActor", "Actor")]),
                messages: vec![message("test.Actor", "Actor", vec![])],
                ..Api::default()
            },
            r#"annotated resource "Actor" is not a resource in this API"#,
        ),
        (
            "method annotated with an unknown resource",
            Api {
                services: control(vec![method("GetActor", "Actor")]),
                ..Api::default()
            },
            r#"annotated resource "Actor" is not a resource in this API"#,
        ),
        (
            "unknown parent",
            Api {
                messages: vec![resource_msg(
                    "test.EgressPolicy",
                    "EgressPolicy",
                    annotation(&["Actor"], false, Scope::Unspecified, "EgressPolicies"),
                )],
                ..Api::default()
            },
            r#"test.EgressPolicy: parent "Actor" is neither "Global" nor a resource in this API"#,
        ),
        (
            "resource without plural",
            Api {
                messages: vec![resource_msg(
                    "test.Actor",
                    "Actor",
                    annotation(&[], false, Scope::Hive, ""),
                )],
                ..Api::default()
            },
            "test.Actor: resource has no plural",
        ),
        (
            "top-level resource without scope",
            Api {
                messages: vec![resource_msg(
                    "test.Actor",
                    "Actor",
                    annotation(&[], false, Scope::Unspecified, "Actors"),
                )],
                ..Api::default()
            },
            "test.Actor: top-level resource has no scope",
        ),
        (
            "subresource with scope",
            Api {
                messages: vec![
                    actor.clone(),
                    resource_msg(
                        "test.EgressPolicy",
                        "EgressPolicy",
                        annotation(&["Actor"], false, Scope::Hive, "EgressPolicies"),
                    ),
                ],
                ..Api::default()
            },
            "test.EgressPolicy: subresource has scope SCOPE_HIVE",
        ),
    ];
    for (name, api, want) in cases {
        let err = resources(&api).expect_err(name);
        assert!(
            err.to_string().contains(want),
            "{name}: {err} does not contain {want:?}"
        );
    }
}

#[test]
fn scope_to_service_keeps_reachable_types() {
    let control = Service {
        name: "Control".into(),
        methods: vec![Method {
            name: "GetActorEgressPolicy".into(),
            service_name: "Control".into(),
            input_name: "test.GetActorEgressPolicyRequest".into(),
            output_name: "test.EgressPolicy".into(),
            resource: "EgressPolicy".into(),
            ..Method::default()
        }],
        ..Service::default()
    };
    let actor = resource_msg(
        "test.Actor",
        "Actor",
        annotation(&[], false, Scope::Hive, "Actors"),
    );
    let egress = resource_msg(
        "test.EgressPolicy",
        "EgressPolicy",
        annotation(
            &["Global", "Actor"],
            false,
            Scope::Unspecified,
            "EgressPolicies",
        ),
    );
    let request = message(
        "test.GetActorEgressPolicyRequest",
        "GetActorEgressPolicyRequest",
        vec![],
    );
    let unused = message("test.Unused", "Unused", vec![]);
    let api = Api {
        services: vec![
            control.clone(),
            Service {
                name: "Other".into(),
                ..Service::default()
            },
        ],
        messages: vec![actor.clone(), unused, egress.clone(), request.clone()],
        ..Api::default()
    };
    let got = scope_to_service(&api, "Control");
    let want = Api {
        services: vec![control],
        messages: vec![actor, egress, request],
        ..Api::default()
    };
    assert_eq!(got, want);
    assert_eq!(scope_to_service(&api, "Missing"), Api::default());
}
