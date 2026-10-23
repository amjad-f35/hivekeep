//! Strict protobuf JSON for the API messages the fixtures declare.

use std::sync::LazyLock;

use prost_reflect::{DescriptorPool, DynamicMessage};

use hivekeep_proto::api::v1 as pb;

static POOL: LazyLock<DescriptorPool> = LazyLock::new(|| {
    DescriptorPool::decode(hivekeep_proto::FILE_DESCRIPTOR_SET)
        .expect("embedded descriptor set decodes")
});

/// Decodes `json` (protojson: camelCase or original field names, enum
/// names) as the message `full_name`, rejecting unknown fields, and
/// transcodes it to the generated type `M`.
pub fn from_json_value<M: prost::Message + Default>(
    full_name: &str,
    json: &serde_json::Value,
) -> Result<M, String> {
    let desc = POOL
        .get_message_by_name(full_name)
        .ok_or_else(|| format!("unknown message {full_name}"))?;
    let dynamic = DynamicMessage::deserialize(desc, json.clone()).map_err(|e| e.to_string())?;
    dynamic.transcode_to::<M>().map_err(|e| e.to_string())
}

/// Encodes `msg` (of type `full_name`) as protojson.
pub fn to_json_value<M: prost::Message>(
    full_name: &str,
    msg: &M,
) -> Result<serde_json::Value, String> {
    let desc = POOL
        .get_message_by_name(full_name)
        .ok_or_else(|| format!("unknown message {full_name}"))?;
    let bytes = msg.encode_to_vec();
    let dynamic = DynamicMessage::decode(desc, bytes.as_slice()).map_err(|e| e.to_string())?;
    serde_json::to_value(&dynamic).map_err(|e| e.to_string())
}

/// Full name of [`pb::ActorTemplate`].
pub const ACTOR_TEMPLATE: &str = "hivekeep.api.v1.ActorTemplate";

/// Strict-decodes a rendered manifest of one or more `---`-separated
/// protojson ActorTemplate documents. Strict, so a block placeholder at the
/// wrong depth or a misspelled field fails here rather than applying
/// cleanly and doing nothing.
pub fn decode_actor_templates(rendered: &str) -> Result<Vec<pb::ActorTemplate>, String> {
    let mut templates = Vec::new();
    for doc in crate::manifest::split_documents(rendered) {
        let value: serde_json::Value = serde_yaml_ng::from_str(doc)
            .map_err(|e| format!("invalid YAML in ActorTemplate manifest: {e}"))?;
        if value.is_null() {
            continue;
        }
        let tmpl = from_json_value::<pb::ActorTemplate>(ACTOR_TEMPLATE, &value)
            .map_err(|e| format!("decoding protojson ActorTemplate: {e}"))?;
        templates.push(tmpl);
    }
    if templates.is_empty() {
        return Err("ActorTemplate manifest holds no documents".into());
    }
    Ok(templates)
}

// The descriptor-pool lookup above must agree with the generated type.
#[allow(dead_code)]
fn assert_names() -> bool {
    POOL.get_message_by_name(ACTOR_TEMPLATE)
        .map(|d| d.full_name() == ACTOR_TEMPLATE)
        .unwrap_or(false)
}

/// The dynamic descriptor of a generated message, for callers that want to
/// reflect on one.
pub fn descriptor_of(full_name: &str) -> Option<prost_reflect::MessageDescriptor> {
    POOL.get_message_by_name(full_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_and_reject_unknown() {
        let docs =
            "metadata:\n  hive: h\n  name: n\n---\n# comment only\n---\nmetadata:\n  name: m\n";
        let got = decode_actor_templates(docs).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].metadata.as_ref().unwrap().hive, "h");
        assert!(decode_actor_templates("metadata:\n  nmae: x\n").is_err());
        assert!(decode_actor_templates("# nothing\n").is_err());
        assert!(assert_names());
        let back = to_json_value(ACTOR_TEMPLATE, &got[0]).unwrap();
        assert_eq!(back["metadata"]["hive"], "h");
        let _ = prost_reflect::ReflectMessage::descriptor(&DynamicMessage::new(
            descriptor_of(ACTOR_TEMPLATE).unwrap(),
        ));
    }
}
