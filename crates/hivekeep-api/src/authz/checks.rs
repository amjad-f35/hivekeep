//! The checks each Control RPC requires, derived from its request.
//!
//! Every identifier a check needs must be present and a valid resource
//! name; otherwise the request fails INVALID_ARGUMENT before any check runs,
//! so a check is never skipped.

use hivekeep_core::names::is_valid_resource_name;
use hivekeep_proto::api::v1 as pb;
use tonic::Status;

use super::model::{rel, Object};

/// One required relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    /// The relation.
    pub relation: &'static str,
    /// On what.
    pub object: Object,
}

/// The checks of one request, or why its identifiers are unusable.
pub type Checks = Result<Vec<Check>, Status>;

fn name(value: &str, path: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument(format!("{path}: Required value")));
    }
    if !is_valid_resource_name(value) {
        return Err(Status::invalid_argument(format!(
            "{path}: Invalid value: {value:?}: must be a valid resource name"
        )));
    }
    Ok(())
}

fn one(relation: &'static str, object: Object) -> Checks {
    Ok(vec![Check { relation, object }])
}

/// `relation` on the deployment.
pub fn on_global(relation: &'static str) -> Checks {
    one(relation, Object::Global)
}

/// `relation` on Hive `hive`.
pub fn on_hive(relation: &'static str, hive: &str, path: &str) -> Checks {
    name(hive, path)?;
    one(relation, Object::Hive(hive.into()))
}

/// `relation` on Hive `hive`, or the deployment when listing every Hive.
pub fn on_hive_or_global(relation: &'static str, hive: &str, path: &str) -> Checks {
    if hive.is_empty() {
        on_global(relation)
    } else {
        on_hive(relation, hive, path)
    }
}

/// `relation` on Worker `w`.
pub fn on_worker(relation: &'static str, w: &str, path: &str) -> Checks {
    name(w, path)?;
    one(relation, Object::Worker(w.into()))
}

fn hived(hive: &str, n: &str, path: &str) -> Result<(String, String), Status> {
    name(hive, &format!("{path}.hive"))?;
    name(n, &format!("{path}.name"))?;
    Ok((hive.into(), n.into()))
}

fn parts(r: Option<&pb::ObjectRef>) -> (&str, &str) {
    r.map(|r| (r.hive.as_str(), r.name.as_str()))
        .unwrap_or(("", ""))
}

fn md_parts(m: Option<&pb::ResourceMetadata>) -> (&str, &str) {
    m.map(|m| (m.hive.as_str(), m.name.as_str()))
        .unwrap_or(("", ""))
}

/// `relation` on an Actor.
pub fn on_actor(relation: &'static str, (h, n): (&str, &str), path: &str) -> Checks {
    let (hive, name) = hived(h, n, path)?;
    one(relation, Object::Actor { hive, name })
}

/// `relation` on an ActorTemplate.
pub fn on_template(relation: &'static str, (h, n): (&str, &str), path: &str) -> Checks {
    let (hive, name) = hived(h, n, path)?;
    one(relation, Object::ActorTemplate { hive, name })
}

/// `relation` on a Tag.
pub fn on_tag(relation: &'static str, (h, n): (&str, &str), path: &str) -> Checks {
    let (hive, name) = hived(h, n, path)?;
    one(relation, Object::Tag { hive, name })
}

/// All of `parts`, reporting the first unusable identifier.
pub fn all(parts: impl IntoIterator<Item = Checks>) -> Checks {
    let mut out = Vec::new();
    for p in parts {
        out.extend(p?);
    }
    Ok(out)
}

/// Checks of CreateActor: create in the Hive, use the template, and use the
/// source Tag when one is named.
pub fn create_actor(r: &pb::CreateActorRequest) -> Checks {
    let a = r.actor.as_ref();
    let hive = a
        .and_then(|a| a.metadata.as_ref())
        .map(|m| m.hive.as_str())
        .unwrap_or("");
    let mut v = vec![
        on_hive(rel::CAN_CREATE_ACTOR, hive, "actor.metadata.hive"),
        on_template(
            rel::CAN_USE_TEMPLATE,
            parts(a.and_then(|a| a.actor_template.as_ref())),
            "actor.actor_template",
        ),
    ];
    if let Some(tag) = a.and_then(|a| a.source_tag.as_ref()) {
        v.push(on_tag(
            rel::CAN_USE_TAG,
            parts(Some(tag)),
            "actor.source_tag",
        ));
    }
    all(v)
}

/// Checks of UpdateActor.
pub fn update_actor(r: &pb::UpdateActorRequest) -> Checks {
    let a = r.actor.as_ref();
    all([
        on_actor(
            rel::CAN_UPDATE,
            md_parts(a.and_then(|a| a.metadata.as_ref())),
            "actor.metadata",
        ),
        on_template(
            rel::CAN_USE_TEMPLATE,
            parts(a.and_then(|a| a.actor_template.as_ref())),
            "actor.actor_template",
        ),
    ])
}

/// `relation` on the Actor a request names in its `actor` field.
pub fn actor_ref(relation: &'static str, r: Option<&pb::ObjectRef>) -> Checks {
    on_actor(relation, parts(r), "actor")
}

/// `relation` on the template a request names.
pub fn template_ref(relation: &'static str, r: Option<&pb::ObjectRef>) -> Checks {
    on_template(relation, parts(r), "actor_template")
}

/// `relation` on the Tag a request names.
pub fn tag_ref(relation: &'static str, r: Option<&pb::ObjectRef>) -> Checks {
    on_tag(relation, parts(r), "tag")
}

/// `relation` on the Tag a request carries in its metadata.
pub fn tag_md(relation: &'static str, m: Option<&pb::ResourceMetadata>) -> Checks {
    on_tag(relation, md_parts(m), "tag.metadata")
}

/// `relation` on the Hive a request names.
pub fn hive_ref(relation: &'static str, r: Option<&pb::ObjectRef>) -> Checks {
    on_hive(
        relation,
        r.map(|r| r.name.as_str()).unwrap_or(""),
        "hive.name",
    )
}

/// `relation` on the Worker a request names.
pub fn worker_ref(relation: &'static str, r: Option<&pb::ObjectRef>) -> Checks {
    on_worker(
        relation,
        r.map(|r| r.name.as_str()).unwrap_or(""),
        "worker.name",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oref(h: &str, n: &str) -> Option<pb::ObjectRef> {
        Some(pb::ObjectRef {
            hive: h.into(),
            name: n.into(),
        })
    }

    #[test]
    fn create_actor_checks() {
        let req = pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(pb::ResourceMetadata {
                    hive: "h".into(),
                    name: "a".into(),
                    ..Default::default()
                }),
                actor_template: oref("t", "tmpl"),
                source_tag: oref("h", "tag"),
                ..Default::default()
            }),
        };
        let c = create_actor(&req).unwrap();
        assert_eq!(c.len(), 3);
        assert_eq!(
            c[1].object,
            Object::ActorTemplate {
                hive: "t".into(),
                name: "tmpl".into()
            }
        );
        assert_eq!(c[2].relation, rel::CAN_USE_TAG);
    }

    #[test]
    fn rejects_unusable_identifiers() {
        assert_eq!(
            actor_ref(rel::CAN_GET, None).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert!(actor_ref(rel::CAN_GET, oref("h", "Bad_Name").as_ref()).is_err());
        assert!(
            on_hive_or_global(rel::CAN_LIST_ACTORS, "", "hive").unwrap()[0].object
                == Object::Global
        );
        assert!(worker_ref(rel::CAN_GET, oref("", "w").as_ref()).is_ok());
    }
}
