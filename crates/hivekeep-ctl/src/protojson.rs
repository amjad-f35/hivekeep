//! Canonical protobuf JSON for the API messages.
//!
//! The generated types carry no JSON support of their own, so messages are
//! converted through reflection over the embedded descriptor set: lowerCamel
//! field names, enum value names, 64-bit integers as strings, RFC 3339
//! timestamps, and default-valued fields omitted. Parsing accepts both the
//! JSON and the original field names and rejects unknown fields.

use std::sync::LazyLock;

use hivekeep_proto::api::v1 as pb;
use prost_reflect::{DescriptorPool, DeserializeOptions, DynamicMessage, SerializeOptions};
use serde_json::Value;

static POOL: LazyLock<DescriptorPool> = LazyLock::new(|| {
    DescriptorPool::decode(hivekeep_proto::FILE_DESCRIPTOR_SET)
        .expect("the embedded descriptor set is valid")
});

/// A generated message with a known fully-qualified protobuf name.
pub trait ProtoMessage: prost::Message + Default + Sized {
    /// The message's fully-qualified name, e.g. `hivekeep.api.v1.Actor`.
    const FULL_NAME: &'static str;
}

macro_rules! proto_messages {
    ($($ty:ident),* $(,)?) => {
        $(impl ProtoMessage for pb::$ty {
            const FULL_NAME: &'static str = concat!("hivekeep.api.v1.", stringify!($ty));
        })*
    };
}

proto_messages!(
    AccessPolicy,
    Actor,
    ActorAssignment,
    ActorTemplate,
    EgressPolicy,
    Hive,
    Tag,
    Worker,
    ListActorsResponse,
    ListActorTemplatesResponse,
    ListHivesResponse,
    ListTagsResponse,
    ListWorkersResponse,
    ListWorkerActorAssignmentsResponse,
    ObjectRef,
    GetActorRequest,
    CreateActorRequest,
    UpdateActorRequest,
    DeleteActorRequest,
    SuspendActorRequest,
    SuspendActorResponse,
    ResumeActorRequest,
    ResumeActorResponse,
    RevertActorRequest,
    RevertActorResponse,
    ListActorsRequest,
    GetActorTemplateRequest,
    CreateActorTemplateRequest,
    DeleteActorTemplateRequest,
    ListActorTemplatesRequest,
    GetHiveRequest,
    CreateHiveRequest,
    DeleteHiveRequest,
    ListHivesRequest,
    GetTagRequest,
    CreateTagRequest,
    UpdateTagRequest,
    DeleteTagRequest,
    ListTagsRequest,
    GetWorkerRequest,
    ListWorkersRequest,
    DrainWorkerRequest,
    DrainWorkerResponse,
    MarkWorkerAvailableRequest,
    MarkWorkerAvailableResponse,
    MarkWorkerUnavailableRequest,
    MarkWorkerUnavailableResponse,
    ListWorkerActorAssignmentsRequest,
    GetActorEgressPolicyRequest,
    CreateActorEgressPolicyRequest,
    UpdateActorEgressPolicyRequest,
    DeleteActorEgressPolicyRequest,
    GetGlobalAccessPolicyRequest,
    CreateGlobalAccessPolicyRequest,
    UpdateGlobalAccessPolicyRequest,
    GetHiveAccessPolicyRequest,
    CreateHiveAccessPolicyRequest,
    UpdateHiveAccessPolicyRequest,
    DeleteHiveAccessPolicyRequest,
);

/// An error converting between a message and JSON.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct JsonError(String);

fn descriptor(name: &str) -> Result<prost_reflect::MessageDescriptor, JsonError> {
    POOL.get_message_by_name(name)
        .ok_or_else(|| JsonError(format!("unknown message type {name}")))
}

/// Converts `msg` to its canonical JSON value.
pub fn to_value<M: ProtoMessage>(msg: &M) -> Result<Value, JsonError> {
    let desc = descriptor(M::FULL_NAME)?;
    let dynamic = DynamicMessage::decode(desc, msg.encode_to_vec().as_slice())
        .map_err(|e| JsonError(e.to_string()))?;
    let value = dynamic
        .serialize_with_options(serde_json::value::Serializer, &SerializeOptions::new())
        .map_err(|e| JsonError(e.to_string()))?;
    Ok(sort_keys(value))
}

/// Renders `msg` as single-line JSON.
pub fn to_compact_string<M: ProtoMessage>(msg: &M) -> String {
    match to_value(msg) {
        Ok(v) => v.to_string(),
        Err(e) => format!("<marshal error: {e}>"),
    }
}

/// Parses a message from its JSON value, rejecting unknown fields.
pub fn from_value<M: ProtoMessage>(value: Value) -> Result<M, JsonError> {
    let desc = descriptor(M::FULL_NAME)?;
    let dynamic = DynamicMessage::deserialize_with_options(desc, value, &DeserializeOptions::new())
        .map_err(|e| JsonError(e.to_string()))?;
    dynamic
        .transcode_to::<M>()
        .map_err(|e| JsonError(e.to_string()))
}

/// Orders every object's keys, so output does not depend on how the JSON
/// map type is configured.
pub fn sort_keys(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(k, v)| (k, sort_keys(v)))
                    .collect(),
            )
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sort_keys).collect()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_json() {
        let actor = pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: "team-a".into(),
                name: "id-1".into(),
                version: 2,
                create_time: Some(prost_types::Timestamp {
                    seconds: 1_767_268_800,
                    nanos: 0,
                }),
                ..Default::default()
            }),
            status: Some(pb::ActorStatus {
                state: pb::ActorState::Running as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            to_value(&actor).unwrap(),
            json!({
                "metadata": {
                    "createTime": "2026-01-01T12:00:00Z",
                    "hive": "team-a",
                    "name": "id-1",
                    "version": "2"
                },
                "status": {"state": "ACTOR_STATE_RUNNING"}
            })
        );
    }

    #[test]
    fn round_trip_and_field_name_forms() {
        let parsed: pb::ObjectRef = from_value(json!({"hive": "h", "name": "n"})).unwrap();
        assert_eq!(parsed.hive, "h");
        let tpl: pb::ActorTemplate =
            from_value(json!({"worker_selector": {"match_labels": {"a": "b"}}})).unwrap();
        assert_eq!(tpl.worker_selector.unwrap().match_labels["a"], "b");
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let err = from_value::<pb::ObjectRef>(json!({"hive": "h", "nmae": "n"})).unwrap_err();
        assert!(err.to_string().contains("nmae"), "{err}");
    }

    #[test]
    fn compact_string() {
        let r = pb::GetActorRequest {
            actor: Some(pb::ObjectRef {
                hive: "demo".into(),
                name: "act-1".into(),
            }),
        };
        assert_eq!(
            to_compact_string(&r),
            r#"{"actor":{"hive":"demo","name":"act-1"}}"#
        );
    }
}
