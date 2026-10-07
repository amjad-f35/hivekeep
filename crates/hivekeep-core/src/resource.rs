//! Typed references to hive-scoped resources.
//!
//! A [`ResourceRef<K>`] names one resource of kind `K` by `(hive, name)`.
//! The kind is a zero-sized marker, so an [`ActorRef`] cannot be passed where
//! a [`TagRef`] is expected even though both carry the same two strings.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use hivekeep_proto::api::v1 as pb;

use crate::names::is_valid_resource_name;

/// Name of every singleton sub-resource, such as an Actor's EgressPolicy or a
/// Hive's AccessPolicy.
pub const SINGLETON_NAME: &str = "default";

/// A kind of hive-scoped resource.
pub trait ResourceKind {
    /// Kind name used in messages and logs, e.g. `"Actor"`.
    const KIND: &'static str;
    /// The wire message for this kind.
    type Message;
    /// The resource's metadata, if set.
    fn metadata(msg: &Self::Message) -> Option<&pb::ResourceMetadata>;
}

/// Marker for [`pb::Actor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActorKind {}
/// Marker for [`pb::ActorTemplate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActorTemplateKind {}
/// Marker for [`pb::Tag`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TagKind {}

impl ResourceKind for ActorKind {
    const KIND: &'static str = "Actor";
    type Message = pb::Actor;
    fn metadata(msg: &pb::Actor) -> Option<&pb::ResourceMetadata> {
        msg.metadata.as_ref()
    }
}

impl ResourceKind for ActorTemplateKind {
    const KIND: &'static str = "ActorTemplate";
    type Message = pb::ActorTemplate;
    fn metadata(msg: &pb::ActorTemplate) -> Option<&pb::ResourceMetadata> {
        msg.metadata.as_ref()
    }
}

impl ResourceKind for TagKind {
    const KIND: &'static str = "Tag";
    type Message = pb::Tag;
    fn metadata(msg: &pb::Tag) -> Option<&pb::ResourceMetadata> {
        msg.metadata.as_ref()
    }
}

/// Identifies a hive-scoped resource of kind `K` by `(hive, name)`.
pub struct ResourceRef<K> {
    /// The Hive the resource lives in.
    pub hive: String,
    /// The resource's name, unique within `hive`.
    pub name: String,
    _kind: PhantomData<fn() -> K>,
}

/// Reference to an Actor.
pub type ActorRef = ResourceRef<ActorKind>;
/// Reference to an ActorTemplate.
pub type ActorTemplateRef = ResourceRef<ActorTemplateKind>;
/// Reference to a Tag.
pub type TagRef = ResourceRef<TagKind>;

impl<K> ResourceRef<K> {
    /// A reference to `name` in `hive`.
    pub fn new(hive: impl Into<String>, name: impl Into<String>) -> Self {
        ResourceRef {
            hive: hive.into(),
            name: name.into(),
            _kind: PhantomData,
        }
    }

    /// Converts a wire reference. Missing fields become empty strings.
    pub fn from_object_ref(r: &pb::ObjectRef) -> Self {
        Self::new(r.hive.clone(), r.name.clone())
    }

    /// The wire form of the reference.
    pub fn to_object_ref(&self) -> pb::ObjectRef {
        pb::ObjectRef {
            hive: self.hive.clone(),
            name: self.name.clone(),
        }
    }

    /// Whether both parts are valid resource names.
    pub fn is_valid(&self) -> bool {
        is_valid_resource_name(&self.hive) && is_valid_resource_name(&self.name)
    }
}

impl<K: ResourceKind> ResourceRef<K> {
    /// The reference addressing `msg`, from its metadata. A message without
    /// metadata yields empty parts.
    pub fn of(msg: &K::Message) -> Self {
        match K::metadata(msg) {
            Some(m) => Self::new(m.hive.clone(), m.name.clone()),
            None => Self::new("", ""),
        }
    }

    /// The kind name, e.g. `"Actor"`.
    pub fn kind(&self) -> &'static str {
        K::KIND
    }
}

impl<K> Clone for ResourceRef<K> {
    fn clone(&self) -> Self {
        Self::new(self.hive.clone(), self.name.clone())
    }
}

impl<K> PartialEq for ResourceRef<K> {
    fn eq(&self, other: &Self) -> bool {
        self.hive == other.hive && self.name == other.name
    }
}

impl<K> Eq for ResourceRef<K> {}

impl<K> Hash for ResourceRef<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hive.hash(state);
        self.name.hash(state);
    }
}

impl<K> PartialOrd for ResourceRef<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K> Ord for ResourceRef<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.hive, &self.name).cmp(&(&other.hive, &other.name))
    }
}

impl<K> fmt::Display for ResourceRef<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.hive, self.name)
    }
}

impl<K: ResourceKind> fmt::Debug for ResourceRef<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(K::KIND)
            .field("hive", &self.hive)
            .field("name", &self.name)
            .finish()
    }
}

impl<K> From<&pb::ObjectRef> for ResourceRef<K> {
    fn from(r: &pb::ObjectRef) -> Self {
        Self::from_object_ref(r)
    }
}

impl<K> From<ResourceRef<K>> for pb::ObjectRef {
    fn from(r: ResourceRef<K>) -> Self {
        pb::ObjectRef {
            hive: r.hive,
            name: r.name,
        }
    }
}

/// Error from [`parse_hived_name`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("malformed reference {value:?} (expected <name> or <hive>/<name>)")]
pub struct ParseRefError {
    /// The rejected input.
    pub value: String,
}

/// Parses a reference written as `<hive>/<name>`, or as a bare `<name>` that
/// lives in `default_hive`. Both parts must be valid resource names.
pub fn parse_hived_name(value: &str, default_hive: &str) -> Result<pb::ObjectRef, ParseRefError> {
    let (hive, name) = value.split_once('/').unwrap_or((default_hive, value));
    if !is_valid_resource_name(hive) || !is_valid_resource_name(name) {
        return Err(ParseRefError {
            value: value.to_string(),
        });
    }
    Ok(pb::ObjectRef {
        hive: hive.to_string(),
        name: name.to_string(),
    })
}

/// What telemetry about an Actor is attributed to: its reference plus the
/// server-assigned uid and the template it was built from. Carries nothing
/// secret.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActorAttribution {
    /// The Actor.
    pub actor: ActorRef,
    /// The Actor's uid.
    pub uid: String,
    /// Hive of the Actor's template.
    pub template_hive: String,
    /// Name of the Actor's template.
    pub template_name: String,
}

impl ActorAttribution {
    /// The attribution of a loaded Actor; missing fields become empty.
    pub fn of(actor: &pb::Actor) -> Self {
        let tmpl = actor.actor_template.as_ref();
        ActorAttribution {
            actor: ActorRef::of(actor),
            uid: actor
                .metadata
                .as_ref()
                .map(|m| m.uid.clone())
                .unwrap_or_default(),
            template_hive: tmpl.map(|t| t.hive.clone()).unwrap_or_default(),
            template_name: tmpl.map(|t| t.name.clone()).unwrap_or_default(),
        }
    }
}

/// Name of the Kubernetes NetworkPolicy guarding a worker pool's pods. Pool
/// names are label values, so the result never needs truncating.
pub fn network_policy_name(worker_pool: &str) -> String {
    format!("hivekeep-{worker_pool}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(hive: &str, name: &str) -> pb::Actor {
        pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: hive.into(),
                name: name.into(),
                uid: "u".into(),
                ..Default::default()
            }),
            actor_template: Some(pb::ObjectRef {
                hive: "th".into(),
                name: "tn".into(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn display_and_round_trip() {
        let r = ActorRef::new("team-a", "a1");
        assert_eq!(r.to_string(), "team-a/a1");
        assert_eq!(ActorRef::from_object_ref(&r.to_object_ref()), r);
        assert_eq!(format!("{r:?}"), "Actor { hive: \"team-a\", name: \"a1\" }");
        assert_eq!(TagRef::new("h", "t").kind(), "Tag");
        assert_eq!(ActorTemplateRef::new("h", "t").to_string(), "h/t");
    }

    #[test]
    fn of_message() {
        assert_eq!(ActorRef::of(&actor("h", "n")), ActorRef::new("h", "n"));
        assert_eq!(ActorRef::of(&pb::Actor::default()), ActorRef::new("", ""));
        let tag = pb::Tag {
            metadata: Some(pb::ResourceMetadata {
                hive: "h".into(),
                name: "t".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(TagRef::of(&tag), TagRef::new("h", "t"));
    }

    #[test]
    fn attribution() {
        let a = ActorAttribution::of(&actor("h", "n"));
        assert_eq!(a.actor, ActorRef::new("h", "n"));
        assert_eq!(
            (
                a.uid.as_str(),
                a.template_hive.as_str(),
                a.template_name.as_str()
            ),
            ("u", "th", "tn")
        );
        let empty = ActorAttribution::of(&pb::Actor::default());
        assert!(empty.uid.is_empty() && empty.template_name.is_empty());
    }

    #[test]
    fn parse_hived_names() {
        let ok = |v: &str, d: &str| parse_hived_name(v, d).map(|r| (r.hive, r.name));
        assert_eq!(
            ok("team-a/before-upgrade", "dflt"),
            Ok(("team-a".into(), "before-upgrade".into()))
        );
        assert_eq!(
            ok("before-upgrade", "dflt"),
            Ok(("dflt".into(), "before-upgrade".into()))
        );
        for bad in [
            "team-a/before/upgrade",
            "/before-upgrade",
            "team-a/",
            "Team_A/x",
            "team-a/Before_Upgrade",
        ] {
            assert!(parse_hived_name(bad, "dflt").is_err(), "{bad}");
        }
        assert!(parse_hived_name("before-upgrade", "").is_err());
    }

    #[test]
    fn network_policy() {
        assert_eq!(network_policy_name("pool-a"), "hivekeep-pool-a");
    }
}
