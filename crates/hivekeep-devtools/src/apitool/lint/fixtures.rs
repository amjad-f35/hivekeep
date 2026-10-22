//! Builders for the APIs the rule tests check. Every message lives in package
//! `test`, and every method on service `test.Control`.

use crate::apitool::model::{
    Api, Field, Kind, Message, Method, ResourceAnnotation, Scope, Service,
};

use super::Finding;

/// An API whose `test.Control` service has the one method `rpc`, plus messages.
pub fn method_api(mut rpc: Method, messages: Vec<Message>) -> Api {
    rpc.service_full_name = "test.Control".into();
    rpc.service_name = "Control".into();
    Api {
        services: vec![Service {
            name: "Control".into(),
            methods: vec![rpc],
            ..Service::default()
        }],
        messages,
        ..Api::default()
    }
}

/// An API with just messages.
pub fn messages_api(messages: Vec<Message>) -> Api {
    Api {
        messages,
        ..Api::default()
    }
}

/// A method `name`, annotated with `resource`, taking `test.{name}Request` and
/// returning `test.{output}`.
pub fn rpc(name: &str, resource: &str, output: &str) -> Method {
    Method {
        name: name.into(),
        resource: resource.into(),
        input_name: format!("test.{name}Request"),
        output_name: format!("test.{output}"),
        ..Method::default()
    }
}

/// A method with explicit input/output full names and no annotation.
pub fn raw_rpc(name: &str, input: &str, output: &str) -> Method {
    Method {
        name: name.into(),
        input_name: input.into(),
        output_name: output.into(),
        ..Method::default()
    }
}

/// A resource `test.{name}` with plural `{name}s`; a top-level one (no parents)
/// is hive-scoped.
pub fn resource(name: &str, parents: &[&str], fields: Vec<Field>) -> Message {
    Message {
        full_name: format!("test.{name}"),
        name: name.into(),
        fields,
        resource_annotation: Some(ResourceAnnotation {
            parents: parents.iter().map(|p| p.to_string()).collect(),
            plural: format!("{name}s"),
            scope: if parents.is_empty() {
                Scope::Hive
            } else {
                Scope::Unspecified
            },
            singleton: false,
        }),
        ..Message::default()
    }
}

/// A global-scoped, top-level resource `test.{name}`.
pub fn global_resource(name: &str, fields: Vec<Field>) -> Message {
    let mut m = resource(name, &[], fields);
    m.resource_annotation.as_mut().unwrap().scope = Scope::Global;
    m
}

/// A singleton resource `test.{name}`.
pub fn singleton_resource(name: &str, parents: &[&str]) -> Message {
    let mut m = resource(name, parents, vec![]);
    m.resource_annotation.as_mut().unwrap().singleton = true;
    m
}

/// A resource `test.{name}` with an explicit plural.
pub fn plural_resource(name: &str, plural: &str, parents: &[&str]) -> Message {
    let mut m = resource(name, parents, vec![]);
    m.resource_annotation.as_mut().unwrap().plural = plural.into();
    m
}

/// A plain message `test.{name}`.
pub fn msg(name: &str, fields: Vec<Field>) -> Message {
    Message {
        full_name: format!("test.{name}"),
        name: name.into(),
        fields,
        ..Message::default()
    }
}

/// A field with just a name.
pub fn named(name: &str) -> Field {
    Field {
        name: name.into(),
        ..Field::default()
    }
}

/// A singular message-typed field.
pub fn msg_field(name: &str, type_full_name: &str) -> Field {
    Field {
        name: name.into(),
        kind: Kind::Message,
        type_full_name: type_full_name.into(),
        ..Field::default()
    }
}

/// A repeated message-typed field.
pub fn repeated_field(name: &str, type_full_name: &str) -> Field {
    Field {
        repeated: true,
        ..msg_field(name, type_full_name)
    }
}

/// A singular scalar field, displayed as its kind.
pub fn scalar_field(name: &str, kind: Kind) -> Field {
    Field {
        name: name.into(),
        kind,
        type_display: kind.to_string(),
        ..Field::default()
    }
}

/// Findings all about `subject`.
pub fn findings(subject: &str, messages: &[&str]) -> Vec<Finding> {
    messages.iter().map(|m| Finding::new(subject, *m)).collect()
}
