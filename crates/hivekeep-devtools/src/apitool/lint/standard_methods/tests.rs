use super::super::fixtures::*;
use super::*;
use crate::apitool::model::{Field, Message};

const REF: &str = "hivekeep.api.v1.ObjectRef";
const LIST_OPTS: &str = "hivekeep.api.v1.ListOptions";
const CREATE_OPTS: &str = "hivekeep.api.v1.CreateOptions";
const DELETE_OPTS: &str = "hivekeep.api.v1.DeleteOptions";

fn f(method: &str, message: &str) -> Finding {
    Finding::new(format!("test.Control.{method}"), message)
}

type Case = (&'static str, Api, Vec<Finding>);

fn run(rule: Rule, cases: Vec<Case>) {
    for (name, api, want) in cases {
        let got = (rule.check)(&api).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(got, want, "{name}");
    }
}

fn widget() -> Vec<Message> {
    vec![resource("Widget", &[], vec![])]
}

fn widget_of_gadget() -> Vec<Message> {
    vec![
        resource("Widget", &["Gadget"], vec![]),
        resource("Gadget", &[], vec![]),
    ]
}

fn with(mut base: Vec<Message>, extra: Vec<Message>) -> Vec<Message> {
    base.extend(extra);
    base
}

#[test]
fn standard_method_returns_resource() {
    run(
        STANDARD_METHOD_RETURNS_RESOURCE,
        vec![
            (
                "Get returns the resource directly",
                method_api(rpc("GetWidget", "Widget", "Widget"), with(widget(), vec![msg("GetWidgetRequest", vec![])])),
                vec![],
            ),
            (
                "Get returns a wrapper instead",
                method_api(
                    rpc("GetWidget", "Widget", "GetWidgetResponse"),
                    with(widget(), vec![msg("GetWidgetRequest", vec![]), msg("GetWidgetResponse", vec![])]),
                ),
                vec![f("GetWidget", "Get method returns test.GetWidgetResponse, want the resource itself (test.Widget)")],
            ),
            (
                "Create returns a wrapper instead",
                method_api(
                    rpc("CreateWidget", "Widget", "CreateWidgetResponse"),
                    with(widget(), vec![msg("CreateWidgetRequest", vec![]), msg("CreateWidgetResponse", vec![])]),
                ),
                vec![f("CreateWidget", "Create method returns test.CreateWidgetResponse, want the resource itself (test.Widget)")],
            ),
            (
                "Update returns a wrapper instead",
                method_api(
                    rpc("UpdateWidget", "Widget", "UpdateWidgetResponse"),
                    with(widget(), vec![msg("UpdateWidgetRequest", vec![]), msg("UpdateWidgetResponse", vec![])]),
                ),
                vec![f("UpdateWidget", "Update method returns test.UpdateWidgetResponse, want the resource itself (test.Widget)")],
            ),
            (
                "Delete returns a wrapper instead",
                method_api(
                    rpc("DeleteWidget", "Widget", "DeleteWidgetResponse"),
                    with(widget(), vec![msg("DeleteWidgetRequest", vec![]), msg("DeleteWidgetResponse", vec![])]),
                ),
                vec![f("DeleteWidget", "Delete method returns test.DeleteWidgetResponse, want the resource itself (test.Widget)")],
            ),
            (
                "a List method is not checked",
                method_api(
                    rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
                    with(widget(), vec![msg("ListWidgetsRequest", vec![]), msg("ListWidgetsResponse", vec![])]),
                ),
                vec![],
            ),
            (
                "returns a type not declared in this API",
                method_api(
                    crate::apitool::model::Method {
                        resource: "Widget".into(),
                        ..raw_rpc("GetWidget", "test.GetWidgetRequest", "google.protobuf.Empty")
                    },
                    with(widget(), vec![msg("GetWidgetRequest", vec![])]),
                ),
                vec![f("GetWidget", "Get method returns google.protobuf.Empty, want the resource itself (test.Widget)")],
            ),
            (
                "subresource: returns the resource directly",
                method_api(
                    rpc("GetGlobalWidget", "Widget", "Widget"),
                    vec![resource("Widget", &["Global"], vec![]), msg("GetGlobalWidgetRequest", vec![])],
                ),
                vec![],
            ),
            (
                "subresource: returns a wrapper instead",
                method_api(
                    rpc("GetGlobalWidget", "Widget", "GetGlobalWidgetResponse"),
                    vec![
                        resource("Widget", &["Global"], vec![]),
                        msg("GetGlobalWidgetRequest", vec![]),
                        msg("GetGlobalWidgetResponse", vec![]),
                    ],
                ),
                vec![f(
                    "GetGlobalWidget",
                    "Get method returns test.GetGlobalWidgetResponse, want the resource itself (test.Widget)",
                )],
            ),
        ],
    );
}

#[test]
fn get_request_shape() {
    let widget_ref = msg_field("widget", REF);
    let misnamed = msg_field("thing", REF);
    let gadget_ref = msg_field("gadget", REF);
    let name = scalar_field("name", Kind::String);
    let get = |fields: Vec<Field>| {
        method_api(
            rpc("GetWidget", "Widget", "Widget"),
            with(widget(), vec![msg("GetWidgetRequest", fields)]),
        )
    };
    let sub = |fields: Vec<Field>| {
        method_api(
            rpc("GetGadgetWidget", "Widget", "Widget"),
            with(
                widget_of_gadget(),
                vec![msg("GetGadgetWidgetRequest", fields)],
            ),
        )
    };
    let global = |fields: Vec<Field>| {
        method_api(
            rpc("GetGlobalWidget", "Widget", "Widget"),
            vec![
                resource("Widget", &["Global"], vec![]),
                msg("GetGlobalWidgetRequest", fields),
            ],
        )
    };
    run(
        GET_REQUEST_SHAPE,
        vec![
            (
                "single ObjectRef field",
                get(vec![widget_ref.clone()]),
                vec![],
            ),
            (
                "no fields",
                get(vec![]),
                vec![f(
                    "GetWidget",
                    r#"request has no "widget" hivekeep.api.v1.ObjectRef field"#,
                )],
            ),
            (
                "stray field",
                get(vec![widget_ref.clone(), named("extra")]),
                vec![f(
                    "GetWidget",
                    r#"request field "extra" is not allowed - want only "widget" hivekeep.api.v1.ObjectRef"#,
                )],
            ),
            (
                "single field, wrong type",
                get(vec![name.clone()]),
                vec![
                    f(
                        "GetWidget",
                        r#"request field "name" is not allowed - want only "widget" hivekeep.api.v1.ObjectRef"#,
                    ),
                    f(
                        "GetWidget",
                        r#"request has no "widget" hivekeep.api.v1.ObjectRef field"#,
                    ),
                ],
            ),
            (
                "single field, wrong name",
                get(vec![misnamed]),
                vec![f(
                    "GetWidget",
                    r#"request field "thing" is hivekeep.api.v1.ObjectRef, want it named "widget""#,
                )],
            ),
            (
                "a Create method is not checked",
                method_api(
                    rpc("CreateWidget", "Widget", "Widget"),
                    with(
                        widget(),
                        vec![msg(
                            "CreateWidgetRequest",
                            vec![msg_field("widget", "test.Widget")],
                        )],
                    ),
                ),
                vec![],
            ),
            (
                "subresource: parent ObjectRef and name",
                sub(vec![gadget_ref.clone(), name.clone()]),
                vec![],
            ),
            (
                "subresource: no name",
                sub(vec![gadget_ref.clone()]),
                vec![f(
                    "GetGadgetWidget",
                    r#"request has no "name" string field"#,
                )],
            ),
            (
                "subresource: name not a string",
                sub(vec![gadget_ref.clone(), scalar_field("name", Kind::Int64)]),
                vec![f(
                    "GetGadgetWidget",
                    r#"request field "name" is int64, want string"#,
                )],
            ),
            (
                "subresource: no parent ObjectRef",
                sub(vec![name.clone()]),
                vec![f(
                    "GetGadgetWidget",
                    r#"request has no "gadget" hivekeep.api.v1.ObjectRef field"#,
                )],
            ),
            (
                "subresource: parent field not an ObjectRef",
                sub(vec![scalar_field("gadget", Kind::String), name.clone()]),
                vec![f(
                    "GetGadgetWidget",
                    r#"request field "gadget" is string, want hivekeep.api.v1.ObjectRef"#,
                )],
            ),
            (
                "subresource: the resource's own ObjectRef instead",
                sub(vec![widget_ref.clone()]),
                vec![
                    f(
                        "GetGadgetWidget",
                        r#"request field "widget" is hivekeep.api.v1.ObjectRef, want it named "gadget""#,
                    ),
                    f("GetGadgetWidget", r#"request has no "name" string field"#),
                ],
            ),
            (
                "subresource: stray field",
                sub(vec![gadget_ref.clone(), name.clone(), named("extra")]),
                vec![f(
                    "GetGadgetWidget",
                    r#"request field "extra" is not allowed - want only "gadget" hivekeep.api.v1.ObjectRef and "name" string"#,
                )],
            ),
            (
                "subresource of Global: name alone",
                global(vec![name.clone()]),
                vec![],
            ),
            (
                "subresource of Global: no name",
                global(vec![]),
                vec![f(
                    "GetGlobalWidget",
                    r#"request has no "name" string field"#,
                )],
            ),
            (
                "subresource of Global: a parent ObjectRef is a stray field",
                global(vec![msg_field("global", REF), name.clone()]),
                vec![f(
                    "GetGlobalWidget",
                    r#"request field "global" is not allowed - want only "name" string"#,
                )],
            ),
        ],
    );

    // A method with no resource annotation makes the rule fail outright.
    let api = method_api(
        rpc("GetWidget", "", "Widget"),
        with(widget(), vec![msg("GetWidgetRequest", vec![widget_ref])]),
    );
    assert!((GET_REQUEST_SHAPE.check)(&api).is_err());
}

#[test]
fn list_method_shape() {
    let options = msg_field("options", LIST_OPTS);
    let hive = scalar_field("hive", Kind::String);
    let gadget_ref = msg_field("gadget", REF);
    let widgets = repeated_field("widgets", "test.Widget");
    let token = scalar_field("next_page_token", Kind::String);
    let list = |base: Vec<Message>, req: Option<Vec<Field>>, resp: Option<Vec<Field>>| {
        let mut msgs = base;
        if let Some(r) = req {
            msgs.push(msg("ListWidgetsRequest", r));
        }
        if let Some(r) = resp {
            msgs.push(msg("ListWidgetsResponse", r));
        }
        method_api(rpc("ListWidgets", "Widget", "ListWidgetsResponse"), msgs)
    };
    let sub = |req: Vec<Field>, resp: Vec<Field>| {
        method_api(
            rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
            with(
                widget_of_gadget(),
                vec![
                    msg("ListGadgetWidgetsRequest", req),
                    msg("ListGadgetWidgetsResponse", resp),
                ],
            ),
        )
    };
    let only_hive_opts = r#"want only "hive" string and "options" hivekeep.api.v1.ListOptions"#;
    let only_page = r#"want only "widgets" repeated test.Widget and "next_page_token" string"#;
    run(
        LIST_METHOD_SHAPE,
        vec![
            (
                "compliant",
                list(
                    widget(),
                    Some(vec![hive.clone(), options.clone()]),
                    Some(vec![widgets.clone(), token.clone()]),
                ),
                vec![],
            ),
            (
                "missing options",
                list(
                    widget(),
                    Some(vec![hive.clone()]),
                    Some(vec![widgets.clone(), token.clone()]),
                ),
                vec![f(
                    "ListWidgets",
                    r#"request has no "options" hivekeep.api.v1.ListOptions field"#,
                )],
            ),
            (
                "loose page_size and page_token outside ListOptions",
                list(
                    widget(),
                    Some(vec![
                        hive.clone(),
                        scalar_field("page_size", Kind::Int32),
                        scalar_field("page_token", Kind::String),
                    ]),
                    Some(vec![widgets.clone(), token.clone()]),
                ),
                vec![
                    f(
                        "ListWidgets",
                        &format!(r#"request field "page_size" is not allowed - {only_hive_opts}"#),
                    ),
                    f(
                        "ListWidgets",
                        &format!(r#"request field "page_token" is not allowed - {only_hive_opts}"#),
                    ),
                    f(
                        "ListWidgets",
                        r#"request has no "options" hivekeep.api.v1.ListOptions field"#,
                    ),
                ],
            ),
            (
                "missing repeated field and next_page_token",
                list(
                    widget(),
                    Some(vec![hive.clone(), options.clone()]),
                    Some(vec![msg_field("widget", "test.Widget")]),
                ),
                vec![
                    f(
                        "ListWidgets",
                        &format!(r#"response field "widget" is not allowed - {only_page}"#),
                    ),
                    f(
                        "ListWidgets",
                        r#"response has no "widgets" repeated test.Widget field"#,
                    ),
                    f(
                        "ListWidgets",
                        r#"response has no "next_page_token" string field"#,
                    ),
                ],
            ),
            (
                "repeated scalar field doesn't satisfy the rule",
                list(
                    widget(),
                    Some(vec![hive.clone(), options.clone()]),
                    Some(vec![
                        Field {
                            repeated: true,
                            ..named("ids")
                        },
                        token.clone(),
                    ]),
                ),
                vec![
                    f(
                        "ListWidgets",
                        &format!(r#"response field "ids" is not allowed - {only_page}"#),
                    ),
                    f(
                        "ListWidgets",
                        r#"response has no "widgets" repeated test.Widget field"#,
                    ),
                ],
            ),
            (
                "repeated field misnamed - doesn't match the resource's plural",
                list(
                    widget(),
                    Some(vec![hive.clone(), options.clone()]),
                    Some(vec![repeated_field("items", "test.Widget"), token.clone()]),
                ),
                vec![f(
                    "ListWidgets",
                    r#"response field "items" is repeated test.Widget, want it named "widgets""#,
                )],
            ),
            (
                "repeated field of another message",
                list(
                    widget(),
                    Some(vec![hive.clone(), options.clone()]),
                    Some(vec![
                        repeated_field("widgets", "test.Gadget"),
                        token.clone(),
                    ]),
                ),
                vec![f(
                    "ListWidgets",
                    r#"response field "widgets" is repeated test.Gadget, want repeated test.Widget"#,
                )],
            ),
            (
                "hive-scoped resource without hive",
                list(
                    widget(),
                    Some(vec![options.clone()]),
                    Some(vec![widgets.clone(), token.clone()]),
                ),
                vec![f("ListWidgets", r#"request has no "hive" string field"#)],
            ),
            (
                "global resource",
                list(
                    vec![global_resource("Widget", vec![])],
                    Some(vec![options.clone()]),
                    Some(vec![widgets.clone(), token.clone()]),
                ),
                vec![],
            ),
            (
                "global resource with hive",
                list(
                    vec![global_resource("Widget", vec![])],
                    Some(vec![hive.clone(), options.clone()]),
                    Some(vec![widgets.clone(), token.clone()]),
                ),
                vec![f(
                    "ListWidgets",
                    r#"request field "hive" is not allowed - want only "options" hivekeep.api.v1.ListOptions"#,
                )],
            ),
            (
                "stray field on the request - sorting/filtering is not supported",
                list(
                    widget(),
                    Some(vec![hive.clone(), options.clone(), named("filter")]),
                    Some(vec![widgets.clone(), token.clone()]),
                ),
                vec![f(
                    "ListWidgets",
                    &format!(r#"request field "filter" is not allowed - {only_hive_opts}"#),
                )],
            ),
            (
                "stray field on the response",
                list(
                    widget(),
                    Some(vec![hive.clone(), options.clone()]),
                    Some(vec![widgets.clone(), token.clone(), named("total_count")]),
                ),
                vec![f(
                    "ListWidgets",
                    &format!(r#"response field "total_count" is not allowed - {only_page}"#),
                )],
            ),
            (
                "a Get method is not checked",
                method_api(
                    rpc("GetWidget", "Widget", "Widget"),
                    with(widget(), vec![msg("GetWidgetRequest", vec![])]),
                ),
                vec![],
            ),
            (
                "request not declared in this API",
                list(widget(), None, Some(vec![widgets.clone(), token.clone()])),
                vec![
                    f("ListWidgets", r#"request has no "hive" string field"#),
                    f(
                        "ListWidgets",
                        r#"request has no "options" hivekeep.api.v1.ListOptions field"#,
                    ),
                ],
            ),
            (
                "response not declared in this API",
                list(widget(), Some(vec![hive.clone(), options.clone()]), None),
                vec![
                    f(
                        "ListWidgets",
                        r#"response has no "widgets" repeated test.Widget field"#,
                    ),
                    f(
                        "ListWidgets",
                        r#"response has no "next_page_token" string field"#,
                    ),
                ],
            ),
            (
                "subresource: compliant",
                sub(
                    vec![gadget_ref.clone(), options.clone()],
                    vec![widgets.clone(), token.clone()],
                ),
                vec![],
            ),
            (
                "subresource: missing parent field",
                sub(vec![options.clone()], vec![widgets.clone(), token.clone()]),
                vec![f(
                    "ListGadgetWidgets",
                    r#"request has no "gadget" hivekeep.api.v1.ObjectRef field"#,
                )],
            ),
            (
                "subresource: parent field is not an ObjectRef",
                sub(
                    vec![scalar_field("gadget", Kind::String), options.clone()],
                    vec![widgets.clone(), token.clone()],
                ),
                vec![f(
                    "ListGadgetWidgets",
                    r#"request field "gadget" is string, want hivekeep.api.v1.ObjectRef"#,
                )],
            ),
            (
                "subresource: hive field is not allowed",
                sub(
                    vec![gadget_ref.clone(), hive.clone(), options.clone()],
                    vec![widgets.clone(), token.clone()],
                ),
                vec![f(
                    "ListGadgetWidgets",
                    r#"request field "hive" is not allowed - want only "gadget" hivekeep.api.v1.ObjectRef and "options" hivekeep.api.v1.ListOptions"#,
                )],
            ),
            (
                "subresource: repeated field named after the parent too",
                sub(
                    vec![gadget_ref.clone(), options.clone()],
                    vec![
                        repeated_field("gadget_widgets", "test.Widget"),
                        token.clone(),
                    ],
                ),
                vec![f(
                    "ListGadgetWidgets",
                    r#"response field "gadget_widgets" is repeated test.Widget, want it named "widgets""#,
                )],
            ),
            (
                "subresource of Global: compliant",
                method_api(
                    rpc("ListGlobalWidgets", "Widget", "ListGlobalWidgetsResponse"),
                    vec![
                        resource("Widget", &["Global"], vec![]),
                        msg("ListGlobalWidgetsRequest", vec![options.clone()]),
                        msg(
                            "ListGlobalWidgetsResponse",
                            vec![widgets.clone(), token.clone()],
                        ),
                    ],
                ),
                vec![],
            ),
            (
                "subresource of Global: parent field is not allowed",
                method_api(
                    rpc("ListGlobalWidgets", "Widget", "ListGlobalWidgetsResponse"),
                    vec![
                        resource("Widget", &["Global"], vec![]),
                        msg(
                            "ListGlobalWidgetsRequest",
                            vec![msg_field("global", REF), options.clone()],
                        ),
                        msg(
                            "ListGlobalWidgetsResponse",
                            vec![widgets.clone(), token.clone()],
                        ),
                    ],
                ),
                vec![f(
                    "ListGlobalWidgets",
                    r#"request field "global" is not allowed - want only "options" hivekeep.api.v1.ListOptions"#,
                )],
            ),
            (
                "subresource: a parent whose name starts with another parent's",
                method_api(
                    rpc(
                        "ListGadgetBoxWidgets",
                        "Widget",
                        "ListGadgetBoxWidgetsResponse",
                    ),
                    vec![
                        resource("Widget", &["Gadget", "GadgetBox"], vec![]),
                        resource("Gadget", &[], vec![]),
                        resource("GadgetBox", &[], vec![]),
                        msg(
                            "ListGadgetBoxWidgetsRequest",
                            vec![msg_field("gadget_box", REF), options.clone()],
                        ),
                        msg(
                            "ListGadgetBoxWidgetsResponse",
                            vec![widgets.clone(), token.clone()],
                        ),
                    ],
                ),
                vec![],
            ),
        ],
    );
}

#[test]
fn list_response_name_matches_method() {
    run(
        LIST_RESPONSE_NAME_MATCHES_METHOD,
        vec![
            (
                "compliant",
                method_api(
                    rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
                    with(
                        widget(),
                        vec![
                            msg("ListWidgetsRequest", vec![]),
                            msg("ListWidgetsResponse", vec![]),
                        ],
                    ),
                ),
                vec![],
            ),
            (
                "wrong name",
                method_api(
                    rpc("ListWidgets", "Widget", "WidgetsResponse"),
                    with(
                        widget(),
                        vec![
                            msg("ListWidgetsRequest", vec![]),
                            msg("WidgetsResponse", vec![]),
                        ],
                    ),
                ),
                vec![f(
                    "ListWidgets",
                    r#"response message is named "WidgetsResponse", want "ListWidgetsResponse""#,
                )],
            ),
            (
                "a Get method is not checked",
                method_api(
                    rpc("GetWidget", "Widget", "GetWidgetResponse"),
                    with(
                        widget(),
                        vec![
                            msg("GetWidgetRequest", vec![]),
                            msg("GetWidgetResponse", vec![]),
                        ],
                    ),
                ),
                vec![],
            ),
        ],
    );
}

#[test]
fn create_request_shape() {
    let w = msg_field("widget", "test.Widget");
    let options = msg_field("options", CREATE_OPTS);
    let gadget_ref = msg_field("gadget", REF);
    let create = |fields: Vec<Field>| {
        method_api(
            rpc("CreateWidget", "Widget", "Widget"),
            with(widget(), vec![msg("CreateWidgetRequest", fields)]),
        )
    };
    let sub = |fields: Vec<Field>| {
        method_api(
            rpc("CreateGadgetWidget", "Widget", "Widget"),
            with(
                widget_of_gadget(),
                vec![msg("CreateGadgetWidgetRequest", fields)],
            ),
        )
    };
    run(
        CREATE_REQUEST_SHAPE,
        vec![
            ("resource field alone", create(vec![w.clone()]), vec![]),
            (
                "resource field plus CreateOptions",
                create(vec![w.clone(), options.clone()]),
                vec![],
            ),
            (
                "no fields",
                create(vec![]),
                vec![f(
                    "CreateWidget",
                    r#"request has no "widget" test.Widget field"#,
                )],
            ),
            (
                "two resource fields",
                create(vec![w.clone(), w.clone()]),
                vec![f(
                    "CreateWidget",
                    r#"request has 2 "widget" test.Widget fields, want at most 1"#,
                )],
            ),
            (
                "two CreateOptions fields",
                create(vec![w.clone(), options.clone(), options.clone()]),
                vec![f(
                    "CreateWidget",
                    r#"request has 2 "options" hivekeep.api.v1.CreateOptions fields, want at most 1"#,
                )],
            ),
            (
                "CreateOptions field misnamed",
                create(vec![w.clone(), msg_field("opts", CREATE_OPTS)]),
                vec![f(
                    "CreateWidget",
                    r#"request field "opts" is hivekeep.api.v1.CreateOptions, want it named "options""#,
                )],
            ),
            (
                "loose control field outside CreateOptions",
                create(vec![w.clone(), named("dry_run")]),
                vec![f(
                    "CreateWidget",
                    r#"request field "dry_run" is not allowed - want only "widget" test.Widget and optional "options" hivekeep.api.v1.CreateOptions"#,
                )],
            ),
            (
                "resource field, wrong name",
                create(vec![msg_field("thing", "test.Widget")]),
                vec![f(
                    "CreateWidget",
                    r#"request field "thing" is test.Widget, want it named "widget""#,
                )],
            ),
            (
                "a Delete method is not checked",
                method_api(
                    rpc("DeleteWidget", "Widget", "Widget"),
                    with(
                        widget(),
                        vec![msg("DeleteWidgetRequest", vec![msg_field("widget", REF)])],
                    ),
                ),
                vec![],
            ),
            (
                "subresource: parent ObjectRef and resource field",
                sub(vec![gadget_ref.clone(), w.clone()]),
                vec![],
            ),
            (
                "subresource: parent ObjectRef, resource field, and CreateOptions",
                sub(vec![gadget_ref.clone(), w.clone(), options.clone()]),
                vec![],
            ),
            (
                "subresource: no parent ObjectRef",
                sub(vec![w.clone()]),
                vec![f(
                    "CreateGadgetWidget",
                    r#"request has no "gadget" hivekeep.api.v1.ObjectRef field"#,
                )],
            ),
            (
                "subresource: parent field not an ObjectRef",
                sub(vec![scalar_field("gadget", Kind::String), w.clone()]),
                vec![f(
                    "CreateGadgetWidget",
                    r#"request field "gadget" is string, want hivekeep.api.v1.ObjectRef"#,
                )],
            ),
            (
                "subresource: no resource field",
                sub(vec![gadget_ref.clone()]),
                vec![f(
                    "CreateGadgetWidget",
                    r#"request has no "widget" test.Widget field"#,
                )],
            ),
            (
                "subresource of Global: resource field alone",
                method_api(
                    rpc("CreateGlobalWidget", "Widget", "Widget"),
                    vec![
                        resource("Widget", &["Global"], vec![]),
                        msg("CreateGlobalWidgetRequest", vec![w.clone()]),
                    ],
                ),
                vec![],
            ),
        ],
    );
}

#[test]
fn update_request_shape() {
    let w = msg_field("widget", "test.Widget");
    let gadget_ref = msg_field("gadget", REF);
    let update = |fields: Vec<Field>| {
        method_api(
            rpc("UpdateWidget", "Widget", "Widget"),
            with(widget(), vec![msg("UpdateWidgetRequest", fields)]),
        )
    };
    let sub = |fields: Vec<Field>| {
        method_api(
            rpc("UpdateGadgetWidget", "Widget", "Widget"),
            with(
                widget_of_gadget(),
                vec![msg("UpdateGadgetWidgetRequest", fields)],
            ),
        )
    };
    run(
        UPDATE_REQUEST_SHAPE,
        vec![
            ("resource field alone", update(vec![w.clone()]), vec![]),
            (
                "no fields",
                update(vec![]),
                vec![f(
                    "UpdateWidget",
                    r#"request has no "widget" test.Widget field"#,
                )],
            ),
            (
                "two resource fields",
                update(vec![w.clone(), w.clone()]),
                vec![f(
                    "UpdateWidget",
                    r#"request has 2 "widget" test.Widget fields, want at most 1"#,
                )],
            ),
            (
                "loose control field outside the resource",
                update(vec![w.clone(), named("dry_run")]),
                vec![f(
                    "UpdateWidget",
                    r#"request field "dry_run" is not allowed - want only "widget" test.Widget"#,
                )],
            ),
            (
                "resource field, wrong name",
                update(vec![msg_field("thing", "test.Widget")]),
                vec![f(
                    "UpdateWidget",
                    r#"request field "thing" is test.Widget, want it named "widget""#,
                )],
            ),
            (
                "a Delete method is not checked",
                method_api(
                    rpc("DeleteWidget", "Widget", "Widget"),
                    with(
                        widget(),
                        vec![msg("DeleteWidgetRequest", vec![msg_field("widget", REF)])],
                    ),
                ),
                vec![],
            ),
            (
                "subresource: parent ObjectRef and resource field",
                sub(vec![gadget_ref.clone(), w.clone()]),
                vec![],
            ),
            (
                "subresource: no parent ObjectRef",
                sub(vec![w.clone()]),
                vec![f(
                    "UpdateGadgetWidget",
                    r#"request has no "gadget" hivekeep.api.v1.ObjectRef field"#,
                )],
            ),
            (
                "subresource: parent field not an ObjectRef",
                sub(vec![scalar_field("gadget", Kind::String), w.clone()]),
                vec![f(
                    "UpdateGadgetWidget",
                    r#"request field "gadget" is string, want hivekeep.api.v1.ObjectRef"#,
                )],
            ),
            (
                "subresource: no resource field",
                sub(vec![gadget_ref.clone()]),
                vec![f(
                    "UpdateGadgetWidget",
                    r#"request has no "widget" test.Widget field"#,
                )],
            ),
            (
                "subresource of Global: resource field alone",
                method_api(
                    rpc("UpdateGlobalWidget", "Widget", "Widget"),
                    vec![
                        resource("Widget", &["Global"], vec![]),
                        msg("UpdateGlobalWidgetRequest", vec![w.clone()]),
                    ],
                ),
                vec![],
            ),
        ],
    );
}

#[test]
fn delete_request_shape() {
    let widget_ref = msg_field("widget", REF);
    let options = msg_field("options", DELETE_OPTS);
    let gadget_ref = msg_field("gadget", REF);
    let name = scalar_field("name", Kind::String);
    let delete = |fields: Vec<Field>| {
        method_api(
            rpc("DeleteWidget", "Widget", "Widget"),
            with(widget(), vec![msg("DeleteWidgetRequest", fields)]),
        )
    };
    let sub = |fields: Vec<Field>| {
        method_api(
            rpc("DeleteGadgetWidget", "Widget", "Widget"),
            with(
                widget_of_gadget(),
                vec![msg("DeleteGadgetWidgetRequest", fields)],
            ),
        )
    };
    run(
        DELETE_REQUEST_SHAPE,
        vec![
            ("ObjectRef alone", delete(vec![widget_ref.clone()]), vec![]),
            (
                "ObjectRef plus DeleteOptions",
                delete(vec![widget_ref.clone(), options.clone()]),
                vec![],
            ),
            (
                "no fields",
                delete(vec![]),
                vec![f(
                    "DeleteWidget",
                    r#"request has no "widget" hivekeep.api.v1.ObjectRef field"#,
                )],
            ),
            (
                "two ObjectRef fields",
                delete(vec![widget_ref.clone(), widget_ref.clone()]),
                vec![f(
                    "DeleteWidget",
                    r#"request has 2 "widget" hivekeep.api.v1.ObjectRef fields, want at most 1"#,
                )],
            ),
            (
                "two DeleteOptions fields",
                delete(vec![widget_ref.clone(), options.clone(), options.clone()]),
                vec![f(
                    "DeleteWidget",
                    r#"request has 2 "options" hivekeep.api.v1.DeleteOptions fields, want at most 1"#,
                )],
            ),
            (
                "DeleteOptions field misnamed",
                delete(vec![widget_ref.clone(), msg_field("opts", DELETE_OPTS)]),
                vec![f(
                    "DeleteWidget",
                    r#"request field "opts" is hivekeep.api.v1.DeleteOptions, want it named "options""#,
                )],
            ),
            (
                "loose control field outside DeleteOptions",
                delete(vec![widget_ref.clone(), named("dry_run")]),
                vec![f(
                    "DeleteWidget",
                    r#"request field "dry_run" is not allowed - want only "widget" hivekeep.api.v1.ObjectRef and optional "options" hivekeep.api.v1.DeleteOptions"#,
                )],
            ),
            (
                "ObjectRef field, wrong name",
                delete(vec![msg_field("thing", REF)]),
                vec![f(
                    "DeleteWidget",
                    r#"request field "thing" is hivekeep.api.v1.ObjectRef, want it named "widget""#,
                )],
            ),
            (
                "a Create method is not checked",
                method_api(
                    rpc("CreateWidget", "Widget", "Widget"),
                    with(
                        widget(),
                        vec![msg(
                            "CreateWidgetRequest",
                            vec![msg_field("widget", "test.Widget")],
                        )],
                    ),
                ),
                vec![],
            ),
            (
                "subresource: parent ObjectRef and name",
                sub(vec![gadget_ref.clone(), name.clone()]),
                vec![],
            ),
            (
                "subresource: parent ObjectRef, name, and DeleteOptions",
                sub(vec![gadget_ref.clone(), name.clone(), options.clone()]),
                vec![],
            ),
            (
                "subresource: no name",
                sub(vec![gadget_ref.clone(), options.clone()]),
                vec![f(
                    "DeleteGadgetWidget",
                    r#"request has no "name" string field"#,
                )],
            ),
            (
                "subresource: no parent ObjectRef",
                sub(vec![name.clone(), options.clone()]),
                vec![f(
                    "DeleteGadgetWidget",
                    r#"request has no "gadget" hivekeep.api.v1.ObjectRef field"#,
                )],
            ),
            (
                "subresource: the resource's own ObjectRef instead",
                sub(vec![widget_ref.clone(), options.clone()]),
                vec![
                    f(
                        "DeleteGadgetWidget",
                        r#"request field "widget" is hivekeep.api.v1.ObjectRef, want it named "gadget""#,
                    ),
                    f(
                        "DeleteGadgetWidget",
                        r#"request has no "name" string field"#,
                    ),
                ],
            ),
            (
                "subresource: two DeleteOptions fields",
                sub(vec![
                    gadget_ref.clone(),
                    name.clone(),
                    options.clone(),
                    options.clone(),
                ]),
                vec![f(
                    "DeleteGadgetWidget",
                    r#"request has 2 "options" hivekeep.api.v1.DeleteOptions fields, want at most 1"#,
                )],
            ),
            (
                "subresource: loose control field outside DeleteOptions",
                sub(vec![gadget_ref.clone(), name.clone(), named("dry_run")]),
                vec![f(
                    "DeleteGadgetWidget",
                    r#"request field "dry_run" is not allowed - want only "gadget" hivekeep.api.v1.ObjectRef, "name" string, and optional "options" hivekeep.api.v1.DeleteOptions"#,
                )],
            ),
            (
                "subresource of Global: name and DeleteOptions",
                method_api(
                    rpc("DeleteGlobalWidget", "Widget", "Widget"),
                    vec![
                        resource("Widget", &["Global"], vec![]),
                        msg(
                            "DeleteGlobalWidgetRequest",
                            vec![name.clone(), options.clone()],
                        ),
                    ],
                ),
                vec![],
            ),
        ],
    );
}

#[test]
fn delete_options_shape() {
    let version = scalar_field("version", Kind::Int64);
    let uid = scalar_field("uid", Kind::String);
    let opts = |fields: Vec<Field>| {
        messages_api(vec![Message {
            full_name: DELETE_OPTS.into(),
            name: "DeleteOptions".into(),
            fields,
            ..Message::default()
        }])
    };
    let sf = |m: &str| Finding::new(DELETE_OPTS, m);
    run(
        DELETE_OPTIONS_SHAPE,
        vec![
            (
                "version and uid",
                opts(vec![version.clone(), uid.clone()]),
                vec![],
            ),
            (
                "version alone - presence not required",
                opts(vec![version.clone()]),
                vec![],
            ),
            (
                "uid alone - presence not required",
                opts(vec![uid.clone()]),
                vec![],
            ),
            ("no fields at all", opts(vec![]), vec![]),
            (
                "version wrong type",
                opts(vec![scalar_field("version", Kind::String), uid.clone()]),
                vec![sf(r#"DeleteOptions field "version" is string, want int64"#)],
            ),
            (
                "uid wrong type",
                opts(vec![version.clone(), scalar_field("uid", Kind::Int64)]),
                vec![sf(r#"DeleteOptions field "uid" is int64, want string"#)],
            ),
            (
                "extra field beyond version/uid",
                opts(vec![
                    version.clone(),
                    uid.clone(),
                    scalar_field("dry_run", Kind::Bool),
                ]),
                vec![sf(
                    r#"DeleteOptions field "dry_run" is not allowed - want only optional "version" int64 and optional "uid" string"#,
                )],
            ),
            (
                "DeleteOptions not declared in this API",
                Api::default(),
                vec![],
            ),
        ],
    );
}

#[test]
fn singleton_no_list() {
    run(
        SINGLETON_NO_LIST,
        vec![
            (
                "singleton with a List method",
                method_api(
                    rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
                    vec![
                        singleton_resource("Widget", &["Gadget"]),
                        resource("Gadget", &[], vec![]),
                        msg("ListGadgetWidgetsRequest", vec![]),
                        msg("ListGadgetWidgetsResponse", vec![]),
                    ],
                ),
                vec![f(
                    "ListGadgetWidgets",
                    "lists test.Widget, which is a singleton - a singleton has no List method",
                )],
            ),
            (
                "singleton with a Get method",
                method_api(
                    rpc("GetGadgetWidget", "Widget", "Widget"),
                    vec![
                        singleton_resource("Widget", &["Gadget"]),
                        resource("Gadget", &[], vec![]),
                        msg("GetGadgetWidgetRequest", vec![]),
                    ],
                ),
                vec![],
            ),
            (
                "non-singleton with a List method",
                method_api(
                    rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
                    with(
                        widget_of_gadget(),
                        vec![
                            msg("ListGadgetWidgetsRequest", vec![]),
                            msg("ListGadgetWidgetsResponse", vec![]),
                        ],
                    ),
                ),
                vec![],
            ),
        ],
    );
}
