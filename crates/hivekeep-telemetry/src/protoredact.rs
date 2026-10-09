//! Masking of protobuf fields labeled `[debug_redact = true]`.
//!
//! The label sits next to the field in the `.proto` file; this module reads
//! it from the message descriptors and produces a copy of a message with the
//! labeled fields masked, so the message can be logged without exposing the
//! secrets it carries. Names and structure are kept, so a log line still
//! shows which fields were set.
//!
//! Generated prost types carry no reflection, so messages are handled as
//! [`DynamicMessage`]s resolved against a descriptor pool. The pool starts
//! with every Hivekeep proto ([`hivekeep_proto::FILE_DESCRIPTOR_SET`]); crates
//! with protos of their own add them with [`register_file_descriptor_set`].

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, RwLock};

use prost_reflect::{
    DescriptorError, DescriptorPool, DynamicMessage, ExtensionDescriptor, FieldDescriptor, Kind,
    MessageDescriptor, MethodDescriptor, ReflectMessage, SerializeOptions, Value,
};

/// Replaces the value of a singular string field marked `debug_redact`.
pub const PLACEHOLDER: &str = "[REDACTED]";

static POOL: LazyLock<RwLock<DescriptorPool>> = LazyLock::new(|| {
    let pool = DescriptorPool::decode(hivekeep_proto::FILE_DESCRIPTOR_SET)
        .expect("the embedded Hivekeep descriptor set is valid");
    RwLock::new(pool)
});

/// The descriptor pool messages are resolved against.
pub fn descriptor_pool() -> DescriptorPool {
    POOL.read().expect("descriptor pool lock").clone()
}

/// Adds an encoded `FileDescriptorSet` to the pool, so its messages can be
/// redacted and its methods logged by the interceptors.
pub fn register_file_descriptor_set(bytes: &[u8]) -> Result<(), DescriptorError> {
    let mut pool = POOL.write().expect("descriptor pool lock");
    let mut next = pool.clone();
    next.decode_file_descriptor_set(bytes)?;
    *pool = next;
    Ok(())
}

/// The descriptor of the message named `full_name` (`hivekeep.api.v1.Actor`).
pub fn message_descriptor(full_name: &str) -> Option<MessageDescriptor> {
    descriptor_pool().get_message_by_name(full_name)
}

/// The descriptor of a gRPC method given its path (`/pkg.Service/Method`).
pub fn method_descriptor(path: &str) -> Option<MethodDescriptor> {
    let (service, method) = path.trim_start_matches('/').rsplit_once('/')?;
    descriptor_pool()
        .get_service_by_name(service)?
        .methods()
        .find(|m| m.name() == method)
}

/// Converts a generated message into a [`DynamicMessage`] of type `desc`.
///
/// The conversion goes through the wire encoding, so `desc` must describe
/// the same schema as `M`. A message that does not decode as `desc` yields an
/// empty message rather than an error: callers use this to log, and a log
/// line must never fail.
pub fn to_dynamic<M: prost::Message>(desc: &MessageDescriptor, msg: &M) -> DynamicMessage {
    DynamicMessage::decode(desc.clone(), msg.encode_to_vec().as_slice())
        .unwrap_or_else(|_| DynamicMessage::new(desc.clone()))
}

/// Renders `msg` as JSON with its proto field names, the shape log lines
/// carry. Default-valued fields are left out, and 64-bit integers stay
/// numbers.
pub fn to_json(msg: &DynamicMessage) -> serde_json::Value {
    let opts = SerializeOptions::new()
        .use_proto_field_name(true)
        .stringify_64_bit_integers(false);
    msg.serialize_with_options(serde_json::value::Serializer, &opts)
        .unwrap_or(serde_json::Value::Null)
}

/// Reports whether logging `msg` as it is would expose a `debug_redact`
/// field: its type can reach such a field and at least one is populated.
/// A type that cannot reach a labeled field and a message whose labeled
/// fields are all unset both answer false. The type check is memoized; the
/// populated walk stops at the first hit and visits only sub-messages whose
/// type can reach a labeled field.
pub fn needs_redaction(msg: &DynamicMessage) -> bool {
    reaches_debug_redact(&msg.descriptor()) && has_populated_debug_redact(msg)
}

/// `msg` with every `debug_redact` field masked, without modifying `msg`.
///
/// When [`needs_redaction`] is false the message is returned borrowed:
/// there is nothing to mask, so nothing to copy. Otherwise the result is an
/// owned, masked copy.
pub fn redacted(msg: &DynamicMessage) -> Cow<'_, DynamicMessage> {
    if !needs_redaction(msg) {
        return Cow::Borrowed(msg);
    }
    let mut copy = msg.clone();
    redact(&mut copy);
    Cow::Owned(copy)
}

/// Memoized answer of [`reaches_debug_redact`] per message type. The answer
/// depends only on the schema, which is fixed for the life of the pool, so
/// it is computed once per type. Keyed by file and full name, since a test
/// may build a type of the same name in a pool of its own.
static REACHABLE: LazyLock<RwLock<HashMap<(String, String), bool>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

fn cache_key(md: &MessageDescriptor) -> (String, String) {
    (
        md.parent_file().name().to_string(),
        md.full_name().to_string(),
    )
}

/// Whether the answer for `md` has been memoized.
#[cfg(test)]
pub(crate) fn reachability_cached(md: &MessageDescriptor) -> bool {
    REACHABLE
        .read()
        .expect("reachability lock")
        .contains_key(&cache_key(md))
}

/// Reports whether a message of type `md` can carry a `debug_redact` field,
/// directly or through any nested message, list element or map value,
/// however deep.
pub fn reaches_debug_redact(md: &MessageDescriptor) -> bool {
    let key = cache_key(md);
    if let Some(v) = REACHABLE.read().expect("reachability lock").get(&key) {
        return *v;
    }
    let r = walk_reaches_debug_redact(md, &mut HashSet::new());
    REACHABLE.write().expect("reachability lock").insert(key, r);
    r
}

/// The uncached walk. `seen` guards recursive schemas (a message type that
/// contains itself).
fn walk_reaches_debug_redact(md: &MessageDescriptor, seen: &mut HashSet<String>) -> bool {
    if !seen.insert(md.full_name().to_string()) {
        return false;
    }
    // Extension fields are not listed among the fields. A type that admits
    // extensions may carry a labeled one, so it counts as reaching a secret
    // and the populated walk, which does visit extensions, decides.
    if md.extension_ranges().len() > 0 {
        return true;
    }
    for fd in md.fields() {
        if field_has_debug_redact(&fd) {
            return true;
        }
        // The nested type for message fields, the entry type for maps,
        // whose value field is checked in turn.
        if let Kind::Message(sub) = fd.kind() {
            if walk_reaches_debug_redact(&sub, seen) {
                return true;
            }
        }
    }
    false
}

fn options_have_debug_redact(opts: &DynamicMessage) -> bool {
    opts.get_field_by_name("debug_redact")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

/// Whether `fd` carries `[debug_redact = true]`.
pub fn field_has_debug_redact(fd: &FieldDescriptor) -> bool {
    options_have_debug_redact(&fd.options())
}

fn extension_has_debug_redact(ed: &ExtensionDescriptor) -> bool {
    options_have_debug_redact(&ed.options())
}

fn kind_reaches(kind: &Kind) -> bool {
    matches!(kind, Kind::Message(sub) if reaches_debug_redact(sub))
}

/// Whether `msg` has a populated `debug_redact` field anywhere below it,
/// stopping at the first one found.
pub(crate) fn has_populated_debug_redact(msg: &DynamicMessage) -> bool {
    let md = msg.descriptor();
    if md.extension_ranges().len() > 0
        && msg.extensions().any(|(ed, value)| {
            extension_has_debug_redact(&ed)
                || (kind_reaches(&ed.kind()) && value_has_populated_debug_redact(value))
        })
    {
        return true;
    }
    for fd in md.fields() {
        if field_has_debug_redact(&fd) {
            if msg.has_field(&fd) {
                return true;
            }
            continue;
        }
        if kind_reaches(&fd.kind())
            && msg.has_field(&fd)
            && value_has_populated_debug_redact(&msg.get_field(&fd))
        {
            return true;
        }
    }
    false
}

/// [`has_populated_debug_redact`] for the value of one unlabeled field
/// whose type can reach a labeled one.
fn value_has_populated_debug_redact(value: &Value) -> bool {
    match value {
        Value::Message(m) => has_populated_debug_redact(m),
        Value::List(items) => items.iter().any(value_has_populated_debug_redact),
        Value::Map(entries) => entries.values().any(value_has_populated_debug_redact),
        _ => false,
    }
}

/// Masks, in place, every populated field of `msg` that carries the
/// `debug_redact` option, recursing through nested messages, lists and map
/// values. Singular string fields are replaced with [`PLACEHOLDER`]; any
/// other kind (bytes, repeated, map, message, ...) is cleared, because no
/// honest placeholder exists for them. Sub-messages whose type cannot carry
/// a labeled field are not visited.
fn redact(msg: &mut DynamicMessage) {
    let mut clear = Vec::new();
    for (fd, value) in msg.fields_mut() {
        if field_has_debug_redact(&fd) {
            if matches!(fd.kind(), Kind::String) && !fd.is_list() {
                *value = Value::String(PLACEHOLDER.to_string());
            } else {
                clear.push(fd);
            }
        } else if kind_reaches(&fd.kind()) {
            redact_value(value);
        }
    }
    for fd in clear {
        msg.clear_field(&fd);
    }

    let mut clear_ext = Vec::new();
    for (ed, value) in msg.extensions_mut() {
        if extension_has_debug_redact(&ed) {
            if matches!(ed.kind(), Kind::String) && !ed.is_list() {
                *value = Value::String(PLACEHOLDER.to_string());
            } else {
                clear_ext.push(ed);
            }
        } else if kind_reaches(&ed.kind()) {
            redact_value(value);
        }
    }
    for ed in clear_ext {
        msg.clear_extension(&ed);
    }
}

fn redact_value(value: &mut Value) {
    match value {
        Value::Message(m) => redact(m),
        Value::List(items) => items.iter_mut().for_each(redact_value),
        Value::Map(entries) => entries.values_mut().for_each(redact_value),
        _ => {}
    }
}

#[cfg(test)]
pub(crate) mod testschema {
    //! Test-time schemas exercising shapes the production protos do not
    //! have yet.

    use prost_reflect::DescriptorPool;
    use prost_types::field_descriptor_proto::{Label, Type};
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FieldOptions, FileDescriptorProto,
        FileDescriptorSet, MessageOptions, OneofDescriptorProto,
    };

    /// `FieldOptions` with `debug_redact = true`. prost-types does not
    /// model the option, so it is appended as an unknown field (number 16),
    /// which is how it travels on the wire anyway.
    pub(crate) fn redact_opts() -> FieldOptions {
        FieldOptions::default()
    }

    pub(crate) fn field(
        name: &str,
        num: i32,
        typ: Type,
        label: Label,
        type_name: &str,
        redact: bool,
    ) -> FieldDescriptorProto {
        FieldDescriptorProto {
            name: Some(name.into()),
            number: Some(num),
            r#type: Some(typ as i32),
            label: Some(label as i32),
            type_name: (!type_name.is_empty()).then(|| type_name.to_string()),
            options: redact.then(redact_opts),
            ..Default::default()
        }
    }

    fn map_entry(name: &str, value_type: &str, value_kind: Type) -> DescriptorProto {
        DescriptorProto {
            name: Some(name.into()),
            options: Some(MessageOptions {
                map_entry: Some(true),
                ..Default::default()
            }),
            field: vec![
                field("key", 1, Type::String, Label::Optional, "", false),
                field("value", 2, value_kind, Label::Optional, value_type, false),
            ],
            ..Default::default()
        }
    }

    /// Builds a pool from `files`, marking the fields named in `redacted`
    /// (full names) with `debug_redact`.
    pub(crate) fn pool(files: Vec<FileDescriptorProto>) -> DescriptorPool {
        let set = FileDescriptorSet { file: files };
        let bytes = super::tests::with_debug_redact(prost::Message::encode_to_vec(&set));
        DescriptorPool::decode(bytes.as_slice()).expect("test schema")
    }

    /// ```text
    /// message Inner { string secret = 1 [debug_redact]; string name = 2; }
    /// message Outer {
    ///   map<string, Inner> by_name = 1;
    ///   map<string, string> labels = 2 [debug_redact];
    ///   repeated string tokens = 3 [debug_redact];
    ///   int64 count = 4 [debug_redact];
    ///   bytes raw = 5 [debug_redact];
    ///   Inner one = 6;
    ///   repeated Inner many = 7;
    ///   string plain = 8;
    ///   Inner secret_one = 9 [debug_redact];
    ///   repeated Inner secret_many = 10 [debug_redact];
    ///   oneof choice { string secret_choice = 11 [debug_redact]; string other_choice = 12; }
    /// }
    /// ```
    pub(crate) fn redact_test_pool() -> DescriptorPool {
        let mut secret_choice = field("secret_choice", 11, Type::String, Label::Optional, "", true);
        secret_choice.oneof_index = Some(0);
        let mut other_choice = field("other_choice", 12, Type::String, Label::Optional, "", false);
        other_choice.oneof_index = Some(0);
        let fdp = FileDescriptorProto {
            name: Some("redacttest.proto".into()),
            package: Some("redacttest".into()),
            syntax: Some("proto3".into()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Inner".into()),
                    field: vec![
                        field("secret", 1, Type::String, Label::Optional, "", true),
                        field("name", 2, Type::String, Label::Optional, "", false),
                    ],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Outer".into()),
                    nested_type: vec![
                        map_entry("ByNameEntry", ".redacttest.Inner", Type::Message),
                        map_entry("LabelsEntry", "", Type::String),
                    ],
                    field: vec![
                        field(
                            "by_name",
                            1,
                            Type::Message,
                            Label::Repeated,
                            ".redacttest.Outer.ByNameEntry",
                            false,
                        ),
                        field(
                            "labels",
                            2,
                            Type::Message,
                            Label::Repeated,
                            ".redacttest.Outer.LabelsEntry",
                            true,
                        ),
                        field("tokens", 3, Type::String, Label::Repeated, "", true),
                        field("count", 4, Type::Int64, Label::Optional, "", true),
                        field("raw", 5, Type::Bytes, Label::Optional, "", true),
                        field(
                            "one",
                            6,
                            Type::Message,
                            Label::Optional,
                            ".redacttest.Inner",
                            false,
                        ),
                        field(
                            "many",
                            7,
                            Type::Message,
                            Label::Repeated,
                            ".redacttest.Inner",
                            false,
                        ),
                        field("plain", 8, Type::String, Label::Optional, "", false),
                        field(
                            "secret_one",
                            9,
                            Type::Message,
                            Label::Optional,
                            ".redacttest.Inner",
                            true,
                        ),
                        field(
                            "secret_many",
                            10,
                            Type::Message,
                            Label::Repeated,
                            ".redacttest.Inner",
                            true,
                        ),
                        secret_choice,
                        other_choice,
                    ],
                    oneof_decl: vec![OneofDescriptorProto {
                        name: Some("choice".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        pool(vec![fdp])
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::testschema::*;
    use super::*;
    use hivekeep_proto::{api::v1 as api, keeper::v1 as keeper};
    use prost_reflect::MapKey;
    use prost_types::field_descriptor_proto::{Label, Type};
    use prost_types::{DescriptorProto, FileDescriptorProto};

    /// Rewrites an encoded `FileDescriptorSet` so that every `FieldOptions`
    /// present carries `debug_redact = true` (field 16, varint 1). The test
    /// schemas only attach options to the fields meant to be labeled.
    pub(crate) fn with_debug_redact(set: Vec<u8>) -> Vec<u8> {
        let pool = DescriptorPool::global();
        let set_desc = pool
            .get_message_by_name("google.protobuf.FileDescriptorSet")
            .expect("descriptor.proto in the global pool");
        let mut msg = DynamicMessage::decode(set_desc, set.as_slice()).expect("set decodes");
        mark(&mut msg);
        prost::Message::encode_to_vec(&msg)
    }

    fn mark(msg: &mut DynamicMessage) {
        if msg.descriptor().full_name() == "google.protobuf.FieldOptions" {
            msg.set_field_by_name("debug_redact", Value::Bool(true));
            return;
        }
        for (_, v) in msg.fields_mut() {
            mark_value(v);
        }
    }

    fn mark_value(v: &mut Value) {
        match v {
            Value::Message(m) => mark(m),
            Value::List(items) => items.iter_mut().for_each(mark_value),
            _ => {}
        }
    }

    fn dyn_of<M: prost::Message>(full_name: &str, m: &M) -> DynamicMessage {
        to_dynamic(&message_descriptor(full_name).expect(full_name), m)
    }

    fn get_str(m: &DynamicMessage, name: &str) -> String {
        m.get_field_by_name(name)
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default()
    }

    fn sub(m: &DynamicMessage, name: &str) -> DynamicMessage {
        m.get_field_by_name(name)
            .and_then(|v| v.as_message().cloned())
            .expect(name)
    }

    fn list(m: &DynamicMessage, name: &str) -> Vec<Value> {
        m.get_field_by_name(name)
            .and_then(|v| v.as_list().map(<[Value]>::to_vec))
            .unwrap_or_default()
    }

    fn run_request(env_value: &str) -> keeper::RunRequest {
        keeper::RunRequest {
            spec: Some(keeper::WorkloadSpec {
                containers: vec![keeper::Container {
                    name: "main".into(),
                    env: vec![keeper::EnvVar {
                        name: "API_KEY".into(),
                        value: env_value.into(),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn redacted_skips_unlabeled_scalar_maps() {
        let mut run = run_request("sk-secret");
        let spec = run.spec.as_mut().unwrap();
        spec.volumes = vec![keeper::Volume {
            name: "data".into(),
            external: Some(keeper::ExternalVolumeSource {
                volume_context: [("fsType".to_string(), "ext4".to_string())].into(),
                ..Default::default()
            }),
            ..Default::default()
        }];
        let msg = dyn_of("hivekeep.keeper.v1.RunRequest", &run);
        let got = redacted(&msg);
        let spec = sub(&got, "spec");
        let env = list(list(&spec, "containers")[0].as_message().unwrap(), "env");
        assert_eq!(get_str(env[0].as_message().unwrap(), "value"), PLACEHOLDER);
        let vol = list(&spec, "volumes")[0].as_message().unwrap().clone();
        let ext = sub(&vol, "external");
        let fs_type = ext
            .get_field_by_name("volume_context")
            .unwrap()
            .as_map()
            .unwrap()
            .get(&MapKey::String("fsType".into()))
            .and_then(|v| v.as_str().map(str::to_string));
        assert_eq!(fs_type.as_deref(), Some("ext4"));
        // The original is untouched.
        assert_eq!(run.spec.unwrap().containers[0].env[0].value, "sk-secret");
    }

    fn inner(pool: &DescriptorPool, secret: &str, name: &str) -> DynamicMessage {
        let mut m = DynamicMessage::new(pool.get_message_by_name("redacttest.Inner").unwrap());
        if !secret.is_empty() {
            m.set_field_by_name("secret", Value::String(secret.into()));
        }
        m.set_field_by_name("name", Value::String(name.into()));
        m
    }

    #[test]
    fn redacted_covers_every_field_shape() {
        let pool = redact_test_pool();
        let mut outer = DynamicMessage::new(pool.get_message_by_name("redacttest.Outer").unwrap());
        outer.set_field_by_name(
            "by_name",
            Value::Map(
                [(
                    MapKey::String("a".into()),
                    Value::Message(inner(&pool, "s1", "n1")),
                )]
                .into(),
            ),
        );
        outer.set_field_by_name(
            "labels",
            Value::Map([(MapKey::String("k".into()), Value::String("v".into()))].into()),
        );
        outer.set_field_by_name("tokens", Value::List(vec![Value::String("tok".into())]));
        outer.set_field_by_name("count", Value::I64(42));
        outer.set_field_by_name(
            "raw",
            Value::Bytes(prost::bytes::Bytes::from_static(b"bytes")),
        );
        outer.set_field_by_name("one", Value::Message(inner(&pool, "s2", "n2")));
        outer.set_field_by_name(
            "many",
            Value::List(vec![Value::Message(inner(&pool, "s3", "n3"))]),
        );
        outer.set_field_by_name("plain", Value::String("keep".into()));
        outer.set_field_by_name("secret_one", Value::Message(inner(&pool, "s4", "n4")));
        outer.set_field_by_name(
            "secret_many",
            Value::List(vec![Value::Message(inner(&pool, "s5", "n5"))]),
        );
        outer.set_field_by_name("secret_choice", Value::String("chosen-secret".into()));

        let got = redacted(&outer).into_owned();

        // Labeled field inside a map value: masked, sibling and entry kept.
        let by_name = got.get_field_by_name("by_name").unwrap();
        let in_map = by_name.as_map().unwrap()[&MapKey::String("a".into())]
            .as_message()
            .unwrap()
            .clone();
        assert_eq!(get_str(&in_map, "secret"), PLACEHOLDER);
        assert_eq!(get_str(&in_map, "name"), "n1");
        // Labeled map, repeated string, scalar and bytes: cleared.
        for name in ["labels", "tokens", "count", "raw"] {
            assert!(!got.has_field_by_name(name), "{name} should be cleared");
        }
        // Nested singular and repeated messages: masked, siblings kept.
        let one = sub(&got, "one");
        assert_eq!(
            (
                get_str(&one, "secret").as_str(),
                get_str(&one, "name").as_str()
            ),
            (PLACEHOLDER, "n2")
        );
        let many = list(&got, "many")[0].as_message().unwrap().clone();
        assert_eq!(
            (
                get_str(&many, "secret").as_str(),
                get_str(&many, "name").as_str()
            ),
            (PLACEHOLDER, "n3")
        );
        // Unlabeled field untouched.
        assert_eq!(get_str(&got, "plain"), "keep");
        // A labeled message or repeated message is dropped whole.
        for name in ["secret_one", "secret_many"] {
            assert!(!got.has_field_by_name(name), "{name} should be cleared");
        }
        // A labeled oneof member is masked and stays the selected member.
        assert_eq!(get_str(&got, "secret_choice"), PLACEHOLDER);
        assert!(got.has_field_by_name("secret_choice"));
        assert!(!got.has_field_by_name("other_choice"));
        // The original is untouched.
        assert_eq!(get_str(&outer, "secret_choice"), "chosen-secret");
    }

    #[test]
    fn redacted_leaves_unset_fields_unset() {
        let pool = redact_test_pool();
        let mut outer = DynamicMessage::new(pool.get_message_by_name("redacttest.Outer").unwrap());
        outer.set_field_by_name("one", Value::Message(inner(&pool, "", "only-name")));
        let got = redacted(&outer).into_owned();
        let one = sub(&got, "one");
        assert!(
            !one.has_field_by_name("secret"),
            "unset secret gained a value"
        );
        assert_eq!(get_str(&one, "name"), "only-name");
        assert!(!got.has_field_by_name("secret_choice") && !got.has_field_by_name("other_choice"));
        assert!(!got.has_field_by_name("count") && !got.has_field_by_name("raw"));
    }

    #[test]
    fn redacted_returns_messages_with_nothing_to_mask_without_copying() {
        let clean = [
            dyn_of(
                "hivekeep.api.v1.ListActorsResponse",
                &api::ListActorsResponse {
                    actors: vec![api::Actor {
                        metadata: Some(api::ResourceMetadata {
                            name: "a".into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ),
            dyn_of(
                "hivekeep.api.v1.ObjectRef",
                &api::ObjectRef {
                    hive: "s".into(),
                    name: "n".into(),
                },
            ),
            dyn_of("hivekeep.api.v1.Worker", &api::Worker::default()),
            dyn_of(
                "hivekeep.api.v1.ActorTemplate",
                &api::ActorTemplate {
                    containers: vec![api::Container {
                        name: "c".into(),
                        image: "img".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ),
            dyn_of(
                "hivekeep.api.v1.MintActorJwtResponse",
                &api::MintActorJwtResponse::default(),
            ),
            dyn_of("hivekeep.keeper.v1.RunRequest", &run_request("")),
        ];
        for m in &clean {
            assert!(
                !needs_redaction(m),
                "{}: NeedsRedaction should be false",
                m.descriptor().full_name()
            );
            assert!(
                matches!(redacted(m), Cow::Borrowed(_)),
                "{}: copied",
                m.descriptor().full_name()
            );
        }
        let env_tpl = api::ActorTemplate {
            containers: vec![api::Container {
                env: vec![api::EnvVar {
                    name: "K".into(),
                    value: "v".into(),
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let sensitive = [
            dyn_of(
                "hivekeep.api.v1.MintActorJwtResponse",
                &api::MintActorJwtResponse {
                    actor_jwt: "t".into(),
                    ..Default::default()
                },
            ),
            dyn_of("hivekeep.api.v1.ActorTemplate", &env_tpl),
            dyn_of(
                "hivekeep.api.v1.ListActorTemplatesResponse",
                &api::ListActorTemplatesResponse {
                    actor_templates: vec![api::ActorTemplate::default(), env_tpl.clone()],
                    ..Default::default()
                },
            ),
            dyn_of("hivekeep.keeper.v1.RunRequest", &run_request("v")),
        ];
        for m in &sensitive {
            assert!(
                needs_redaction(m),
                "{}: NeedsRedaction should be true",
                m.descriptor().full_name()
            );
            assert!(
                matches!(redacted(m), Cow::Owned(_)),
                "{}: not copied",
                m.descriptor().full_name()
            );
        }
    }

    #[test]
    fn needs_redaction_is_safe_under_concurrent_first_use() {
        let msgs = std::sync::Arc::new(vec![
            (
                dyn_of(
                    "hivekeep.api.v1.MintActorJwtResponse",
                    &api::MintActorJwtResponse {
                        actor_jwt: "t".into(),
                        ..Default::default()
                    },
                ),
                true,
            ),
            (
                dyn_of(
                    "hivekeep.api.v1.ListActorsResponse",
                    &api::ListActorsResponse::default(),
                ),
                false,
            ),
            (
                dyn_of("hivekeep.api.v1.Worker", &api::Worker::default()),
                false,
            ),
            (
                dyn_of("hivekeep.keeper.v1.RunRequest", &run_request("v")),
                true,
            ),
        ]);
        let handles: Vec<_> = (0..32)
            .map(|_| {
                let msgs = msgs.clone();
                std::thread::spawn(move || {
                    for (m, want) in msgs.iter() {
                        assert_eq!(needs_redaction(m), *want, "{}", m.descriptor().full_name());
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }

    /// Every field across the Hivekeep protos.
    fn for_each_field(mut f: impl FnMut(&FieldDescriptor)) {
        let pool = DescriptorPool::decode(hivekeep_proto::FILE_DESCRIPTOR_SET).unwrap();
        for md in pool.all_messages() {
            for fd in md.fields() {
                f(&fd);
            }
        }
    }

    #[test]
    fn debug_redact_fields_are_pinned() {
        let want: HashSet<&str> = [
            "hivekeep.api.v1.EnvVar.value",
            "hivekeep.api.v1.MintActorJwtResponse.actor_jwt",
            "hivekeep.api.v1.MintWorkerActorJwtResponse.actor_jwt",
            "hivekeep.keeper.v1.EnvVar.value",
            "hivekeep.worker.v1.ContainerSpec.env",
        ]
        .into();
        let mut got = HashSet::new();
        for_each_field(|fd| {
            if field_has_debug_redact(fd) {
                got.insert(fd.full_name().to_string());
            }
        });
        for name in &want {
            assert!(
                got.contains(*name),
                "{name} lost its debug_redact label; logs would show it in clear"
            );
        }
        for name in &got {
            assert!(
                want.contains(name.as_str()),
                "{name} is newly marked debug_redact; add it to this list if intended"
            );
        }
    }

    #[test]
    fn secret_like_fields_are_labeled() {
        let secret_like = regex::Regex::new(
            r"(?i)jwt|token|secret|passw|credential|private_?key|api_?key|bearer|authorization|cookie",
        )
        .unwrap();
        let not_secret: HashMap<&str, &str> = [
            ("page_token", "opaque list cursor"),
            ("next_page_token", "opaque list cursor"),
            (
                "hivekeep.api.v1.CredentialHeader.credential_uri",
                "reference to a credential, not the value",
            ),
            (
                "hivekeep.api.v1.CredentialHeader.actor_jwt",
                "minting parameters, not a token",
            ),
        ]
        .into();
        let mut offenders = Vec::new();
        for_each_field(|fd| {
            if !secret_like.is_match(fd.name()) || field_has_debug_redact(fd) {
                return;
            }
            if not_secret.contains_key(fd.full_name()) || not_secret.contains_key(fd.name()) {
                return;
            }
            offenders.push(fd.full_name().to_string());
        });
        assert!(
            offenders.is_empty(),
            "these look like credentials but have no debug_redact; label them or list them in not_secret: {offenders:?}"
        );
    }

    #[test]
    fn our_proto_files_are_all_listed() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../hivekeep-proto/proto");
        let pool = descriptor_pool();
        let listed: HashSet<String> = pool.files().map(|f| f.name().to_string()).collect();
        let mut stack = vec![root.clone()];
        let mut seen = 0;
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let p = entry.unwrap().path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|e| e == "proto") {
                    seen += 1;
                    let rel = p
                        .strip_prefix(&root)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/");
                    assert!(listed.contains(&rel), "{rel} is not in the descriptor pool");
                }
            }
        }
        assert!(seen > 0, "no protos found under {}", root.display());
    }

    #[test]
    fn reaches_debug_redact_is_memoized_and_handles_recursive_schemas() {
        let fdp = FileDescriptorProto {
            name: Some("recursive.proto".into()),
            package: Some("recursivetest".into()),
            syntax: Some("proto3".into()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Node".into()),
                    field: vec![
                        field("secret", 1, Type::String, Label::Optional, "", true),
                        field(
                            "children",
                            2,
                            Type::Message,
                            Label::Repeated,
                            ".recursivetest.Node",
                            false,
                        ),
                    ],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Tree".into()),
                    field: vec![field(
                        "children",
                        1,
                        Type::Message,
                        Label::Repeated,
                        ".recursivetest.Tree",
                        false,
                    )],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let pool = pool(vec![fdp]);
        let node = pool.get_message_by_name("recursivetest.Node").unwrap();
        let tree = pool.get_message_by_name("recursivetest.Tree").unwrap();
        assert!(!reachability_cached(&node), "cached before first use");
        assert!(reaches_debug_redact(&node));
        assert!(
            !reaches_debug_redact(&tree),
            "Tree must not reach a labeled field, and must not loop"
        );
        assert!(reachability_cached(&node) && reachability_cached(&tree));

        let new_node = |secret: &str, children: Vec<DynamicMessage>| {
            let mut m = DynamicMessage::new(node.clone());
            if !secret.is_empty() {
                m.set_field_by_name("secret", Value::String(secret.into()));
            }
            if !children.is_empty() {
                m.set_field_by_name(
                    "children",
                    Value::List(children.into_iter().map(Value::Message).collect()),
                );
            }
            m
        };
        let deep = new_node("", vec![new_node("", vec![new_node("deep", vec![])])]);
        assert!(has_populated_debug_redact(&deep));
        let got = redacted(&deep).into_owned();
        let l1 = list(&got, "children")[0].as_message().unwrap().clone();
        let l2 = list(&l1, "children")[0].as_message().unwrap().clone();
        assert_eq!(get_str(&l2, "secret"), PLACEHOLDER);

        let unset = new_node("", vec![new_node("", vec![new_node("", vec![])])]);
        assert!(!has_populated_debug_redact(&unset));
        assert!(matches!(redacted(&unset), Cow::Borrowed(_)));
    }

    #[test]
    fn extension_fields_are_found_and_masked() {
        let mut token = field("token", 100, Type::String, Label::Optional, "", true);
        token.extendee = Some(".exttest.Open".into());
        let fdp = FileDescriptorProto {
            name: Some("ext.proto".into()),
            package: Some("exttest".into()),
            syntax: Some("proto2".into()),
            message_type: vec![DescriptorProto {
                name: Some("Open".into()),
                field: vec![field("name", 1, Type::String, Label::Optional, "", false)],
                extension_range: vec![prost_types::descriptor_proto::ExtensionRange {
                    start: Some(100),
                    end: Some(200),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            extension: vec![token],
            ..Default::default()
        };
        let pool = pool(vec![fdp]);
        let open = pool.get_message_by_name("exttest.Open").unwrap();
        let token = pool.get_extension_by_name("exttest.token").unwrap();
        assert!(
            reaches_debug_redact(&open),
            "extension ranges count as reaching a labeled field"
        );

        let mut plain = DynamicMessage::new(open.clone());
        plain.set_field_by_name("name", Value::String("n".into()));
        assert!(!needs_redaction(&plain));
        assert!(matches!(redacted(&plain), Cow::Borrowed(_)));

        let mut with_token = DynamicMessage::new(open);
        with_token.set_extension(&token, Value::String("secret".into()));
        assert!(needs_redaction(&with_token));
        let got = redacted(&with_token).into_owned();
        assert_eq!(got.get_extension(&token).as_str(), Some(PLACEHOLDER));
        assert_eq!(with_token.get_extension(&token).as_str(), Some("secret"));
    }

    #[test]
    fn method_descriptor_resolves_grpc_paths() {
        let m = method_descriptor("/hivekeep.api.v1.Control/MintActorJwt").expect("method");
        assert_eq!(
            m.output().full_name(),
            "hivekeep.api.v1.MintActorJwtResponse"
        );
        assert!(method_descriptor("/no.Such/Method").is_none());
        assert!(method_descriptor("garbage").is_none());
    }

    #[test]
    fn to_json_uses_proto_field_names_and_skips_defaults() {
        let m = dyn_of(
            "hivekeep.api.v1.MintActorJwtResponse",
            &api::MintActorJwtResponse {
                actor_jwt: "t".into(),
                ..Default::default()
            },
        );
        let json = to_json(&redacted(&m));
        assert_eq!(json, serde_json::json!({"actor_jwt": PLACEHOLDER}));
    }
}
