//! Shape rules for standard methods (Get, List, Create, Update, Delete).

use crate::apitool::model::{Api, Kind, Scope, GLOBAL_PARENT};

use super::fields::{
    check_fields, message, optional, repeated_message, required, scalar, WantFields,
};
use super::method::{for_each_standard_method, StdMethod, Verb};
use super::naming::snake_case;
use super::{
    CheckResult, Finding, Rule, CREATE_OPTIONS_TYPE, DELETE_OPTIONS_TYPE, LIST_OPTIONS_TYPE,
    OBJECT_REF_TYPE,
};

pub const STANDARD_METHOD_RETURNS_RESOURCE: Rule = Rule {
    name: "standard-method-returns-resource",
    description: "A Get/Create/Update/Delete method's response is the resource itself, not a wrapper message.",
    check: |api| {
        for_each_standard_method(
            api,
            &[Verb::Get, Verb::Create, Verb::Update, Verb::Delete],
            check_returns_resource,
        )
    },
};

fn check_returns_resource(m: &StdMethod<'_>) -> Vec<Finding> {
    if m.response.full_name == m.resource.full_name {
        return vec![];
    }
    vec![m.finding(format!(
        "{} method returns {}, want the resource itself ({})",
        m.verb, m.response.full_name, m.resource.full_name
    ))]
}

pub const GET_REQUEST_SHAPE: Rule = Rule {
    name: "get-request-shape",
    description: concat!(
        "A Get method's request has exactly one field, of type ObjectRef and named after the resource's snake_case type name, identifying the resource. ",
        r#"For a subresource, it has the parent's ObjectRef, named after the parent (none for Global), plus a string "name" field - nothing else."#
    ),
    check: |api| for_each_standard_method(api, &[Verb::Get], |m| m.request_has(&[m.resource_ref_fields()])),
};

pub const LIST_METHOD_SHAPE: Rule = Rule {
    name: "list-method-shape",
    description: concat!(
        "A List method's request has a ListOptions options field, plus a hive field for a hive-scoped top-level resource, ",
        "or for a subresource an ObjectRef field named after the parent (none for Global). Its response has a repeated field of the resource, ",
        "named after the resource's plural, plus a next_page_token field - nothing else."
    ),
    check: |api| for_each_standard_method(api, &[Verb::List], check_list_method),
};

fn check_list_method(m: &StdMethod<'_>) -> Vec<Finding> {
    let mut out = m.request_has(&[m.list_scope_field(), list_options()]);
    out.extend(m.response_has(&[m.resource_page_field(), next_page_token()]));
    out
}

pub const LIST_RESPONSE_NAME_MATCHES_METHOD: Rule = Rule {
    name: "list-response-name-matches-method",
    description: r#"Every "List*" method's response message is named "{MethodName}Response"."#,
    check: |api| {
        for_each_standard_method(api, &[Verb::List], |m| {
            let got = crate::apitool::model::short_name(&m.response.full_name);
            let want = format!("{}Response", m.rpc.name);
            if got == want {
                vec![]
            } else {
                vec![m.finding(format!("response message is named {got:?}, want {want:?}"))]
            }
        })
    },
};

pub const CREATE_REQUEST_SHAPE: Rule = Rule {
    name: "create-request-shape",
    description: concat!(
        "A Create method's request has exactly one field, of the resource's own type and named after its snake_case type name, plus an optional CreateOptions field named \"options\" - nothing else. ",
        "For a subresource, it also has the parent's ObjectRef, named after the parent (none for Global)."
    ),
    check: |api| {
        for_each_standard_method(api, &[Verb::Create], |m| {
            m.request_has(&[m.parent_ref_field(), m.embedded_resource_field(), optional_create_options()])
        })
    },
};

pub const UPDATE_REQUEST_SHAPE: Rule = Rule {
    name: "update-request-shape",
    description: concat!(
        "An Update method's request has exactly one field, of the resource's own type and named after its snake_case type name - nothing else. ",
        "For a subresource, it also has the parent's ObjectRef, named after the parent (none for Global)."
    ),
    check: |api| {
        for_each_standard_method(api, &[Verb::Update], |m| {
            m.request_has(&[m.parent_ref_field(), m.embedded_resource_field()])
        })
    },
};

pub const DELETE_REQUEST_SHAPE: Rule = Rule {
    name: "delete-request-shape",
    description: concat!(
        "A Delete method's request has exactly one ObjectRef field, named after the resource's snake_case type name, plus an optional DeleteOptions field named \"options\" - nothing else. ",
        r#"For a subresource, the parent's ObjectRef, named after the parent (none for Global), plus a string "name" field replace the resource's ObjectRef."#
    ),
    check: |api| {
        for_each_standard_method(api, &[Verb::Delete], |m| {
            m.request_has(&[m.resource_ref_fields(), optional_delete_options()])
        })
    },
};

pub const DELETE_OPTIONS_SHAPE: Rule = Rule {
    name: "delete-options-shape",
    description: "The shared hivekeep.api.v1.DeleteOptions message has only version (int64) and uid (string) fields - nothing else.",
    check: check_delete_options_shape,
};

fn check_delete_options_shape(api: &Api) -> CheckResult {
    let by_name = api.messages_by_full_name();
    let Some(opts) = by_name.get(DELETE_OPTIONS_TYPE) else {
        return Ok(vec![]); // not declared in this API - nothing to check.
    };
    Ok(check_fields(
        &opts.full_name,
        "DeleteOptions",
        opts,
        &[
            optional("version", scalar(Kind::Int64)),
            optional("uid", scalar(Kind::String)),
        ],
    ))
}

pub const SINGLETON_NO_LIST: Rule = Rule {
    name: "singleton-no-list",
    description: "A singleton resource has no List method.",
    check: |api| {
        for_each_standard_method(api, &[Verb::List], |m| {
            if !m.annotation().singleton {
                return vec![];
            }
            vec![m.finding(format!(
                "lists {}, which is a singleton - a singleton has no List method",
                m.resource.full_name
            ))]
        })
    },
};

// Fields every request or response of their kind has the same way.
fn list_options() -> WantFields {
    required("options", message(LIST_OPTIONS_TYPE))
}
fn next_page_token() -> WantFields {
    required("next_page_token", scalar(Kind::String))
}
fn optional_create_options() -> WantFields {
    optional("options", message(CREATE_OPTIONS_TYPE))
}
fn optional_delete_options() -> WantFields {
    optional("options", message(DELETE_OPTIONS_TYPE))
}

impl StdMethod<'_> {
    fn subject(&self) -> String {
        super::method::subject_of(self.rpc)
    }

    /// Checks that the request has exactly the fields `want` lists.
    fn request_has(&self, want: &[WantFields]) -> Vec<Finding> {
        check_fields(&self.subject(), "request", &self.request, want)
    }

    /// Checks that the response has exactly the fields `want` lists.
    fn response_has(&self, want: &[WantFields]) -> Vec<Finding> {
        check_fields(&self.subject(), "response", &self.response, want)
    }

    /// How the request references the resource: its own ObjectRef, or for a
    /// subresource, the parent's plus a name.
    fn resource_ref_fields(&self) -> WantFields {
        if !self.is_subresource() {
            return required(&snake_case(&self.resource.name), message(OBJECT_REF_TYPE));
        }
        let mut out = self.parent_ref_field();
        out.extend(required("name", scalar(Kind::String)));
        out
    }

    /// The parent's ObjectRef, named after it. None for a top-level resource
    /// or a subresource of Global, which has no identity.
    fn parent_ref_field(&self) -> WantFields {
        if !self.is_subresource() || self.parent == GLOBAL_PARENT {
            return vec![];
        }
        required(&snake_case(&self.parent), message(OBJECT_REF_TYPE))
    }

    /// The resource, embedded whole in a Create or Update request.
    fn embedded_resource_field(&self) -> WantFields {
        required(
            &snake_case(&self.resource.name),
            message(&self.resource.full_name),
        )
    }

    /// How a List request scopes the listing: a hive for a hive-scoped
    /// top-level resource, nothing for a global one, or the parent's ObjectRef
    /// for a subresource. Sorting and filtering are not supported.
    fn list_scope_field(&self) -> WantFields {
        if self.is_subresource() {
            self.parent_ref_field()
        } else if self.annotation().scope == Scope::Hive {
            required("hive", scalar(Kind::String))
        } else {
            vec![]
        }
    }

    /// The repeated field of a List response holding a page of resources.
    fn resource_page_field(&self) -> WantFields {
        required(
            &snake_case(&self.annotation().plural),
            repeated_message(&self.resource.full_name),
        )
    }
}

#[cfg(test)]
mod tests;
