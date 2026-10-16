//! The authorization model: objects, roles and the relations each role
//! grants.
//!
//! Roles live on the deployment (global) and on each Hive. Global owners
//! own every Hive, global viewers view every Hive, and within a Hive owner
//! implies editor implies viewer. System components hold their own global
//! roles (controller, egress gateway, ingress router, keeper, API sidecar)
//! when they call over mTLS.

use std::collections::HashSet;
use std::fmt;

/// Relation names.
pub mod rel {
    #![allow(missing_docs)]
    pub const CAN_CREATE_HIVE: &str = "can_create_hive";
    pub const CAN_LIST_HIVES: &str = "can_list_hives";
    pub const CAN_CREATE_ACTOR: &str = "can_create_actor";
    pub const CAN_LIST_ACTORS: &str = "can_list_actors";
    pub const CAN_CREATE_ACTOR_TEMPLATE: &str = "can_create_actor_template";
    pub const CAN_LIST_ACTOR_TEMPLATES: &str = "can_list_actor_templates";
    pub const CAN_CREATE_WORKER: &str = "can_create_worker";
    pub const CAN_LIST_WORKERS: &str = "can_list_workers";
    pub const CAN_GET: &str = "can_get";
    pub const CAN_USE_TEMPLATE: &str = "can_use_template";
    pub const CAN_UPDATE: &str = "can_update";
    pub const CAN_DELETE: &str = "can_delete";
    pub const CAN_RESUME: &str = "can_resume";
    pub const CAN_SUSPEND: &str = "can_suspend";
    pub const CAN_REVERT: &str = "can_revert";
    pub const CAN_DRAIN: &str = "can_drain";
    pub const CAN_LIST_ACTOR_ASSIGNMENTS: &str = "can_list_actor_assignments";
    pub const CAN_CREATE_ACCESS_POLICY: &str = "can_create_access_policy";
    pub const CAN_GET_ACCESS_POLICY: &str = "can_get_access_policy";
    pub const CAN_UPDATE_ACCESS_POLICY: &str = "can_update_access_policy";
    pub const CAN_DELETE_ACCESS_POLICY: &str = "can_delete_access_policy";
    pub const CAN_CREATE_EGRESS_POLICY: &str = "can_create_egress_policy";
    pub const CAN_GET_EGRESS_POLICY: &str = "can_get_egress_policy";
    pub const CAN_UPDATE_EGRESS_POLICY: &str = "can_update_egress_policy";
    pub const CAN_DELETE_EGRESS_POLICY: &str = "can_delete_egress_policy";
    pub const CAN_CREATE_TAG: &str = "can_create_tag";
    pub const CAN_LIST_TAGS: &str = "can_list_tags";
    pub const CAN_USE_TAG: &str = "can_use_tag";
    pub const CAN_MINT_ACTOR_JWT: &str = "can_mint_actor_jwt";
    pub const CAN_MINT_ACTOR_CERTIFICATE: &str = "can_mint_actor_certificate";
}

/// An object relations are checked on.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Object {
    /// The deployment.
    Global,
    /// A Hive.
    Hive(String),
    /// A Worker.
    Worker(String),
    /// An Actor.
    Actor { hive: String, name: String },
    /// An ActorTemplate.
    ActorTemplate { hive: String, name: String },
    /// A Tag.
    Tag { hive: String, name: String },
}

impl Object {
    /// The Hive the object belongs to, if any.
    pub fn hive(&self) -> Option<&str> {
        match self {
            Object::Global | Object::Worker(_) => None,
            Object::Hive(h) => Some(h),
            Object::Actor { hive, .. }
            | Object::ActorTemplate { hive, .. }
            | Object::Tag { hive, .. } => Some(hive),
        }
    }
}

impl fmt::Display for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Object::Global => write!(f, "global:root"),
            Object::Hive(h) => write!(f, "hive:{h}"),
            Object::Worker(w) => write!(f, "worker:{w}"),
            Object::Actor { hive, name } => write!(f, "actor:{hive}/{name}"),
            Object::ActorTemplate { hive, name } => write!(f, "actor_template:{hive}/{name}"),
            Object::Tag { hive, name } => write!(f, "tag:{hive}/{name}"),
        }
    }
}

/// A role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Owner.
    Owner,
    /// Editor (Hive only).
    Editor,
    /// Viewer.
    Viewer,
    /// The worker controller.
    Controller,
    /// The egress gateway.
    EgressGateway,
    /// The ingress router.
    IngressRouter,
    /// Node keepers.
    Keeper,
    /// Sidecars of the API server pod.
    ApiServerSidecar,
}

impl Role {
    /// The role a binding names, if known.
    pub fn parse(s: &str) -> Option<Role> {
        Some(match s {
            "owner" => Role::Owner,
            "editor" => Role::Editor,
            "viewer" => Role::Viewer,
            _ => return None,
        })
    }
}

/// Roles a principal holds, globally and in the object's Hive.
#[derive(Debug, Clone, Default)]
pub struct Grants {
    /// Global roles, as bound or granted by configuration.
    pub global: HashSet<Role>,
    /// Roles bound in the object's Hive.
    pub hive: HashSet<Role>,
}

impl Grants {
    fn g(&self, r: Role) -> bool {
        self.global.contains(&r)
    }

    fn global_owner(&self) -> bool {
        self.g(Role::Owner)
    }

    fn global_viewer(&self) -> bool {
        self.g(Role::Viewer) || self.global_owner()
    }

    fn hive_owner(&self) -> bool {
        self.hive.contains(&Role::Owner) || self.global_owner()
    }

    fn hive_editor(&self) -> bool {
        self.hive.contains(&Role::Editor) || self.hive_owner()
    }

    fn hive_viewer(&self) -> bool {
        self.hive.contains(&Role::Viewer) || self.hive_editor() || self.global_viewer()
    }

    /// Whether the grants include `relation` on an object of `object`'s kind.
    pub fn allows(&self, relation: &str, object: &Object) -> bool {
        use rel::*;
        let owner_or_controller = self.global_owner() || self.g(Role::Controller);
        match object {
            Object::Global => match relation {
                CAN_CREATE_HIVE | CAN_CREATE_ACCESS_POLICY | CAN_UPDATE_ACCESS_POLICY => {
                    self.global_owner()
                }
                CAN_LIST_HIVES
                | CAN_LIST_ACTOR_TEMPLATES
                | CAN_LIST_TAGS
                | CAN_GET_ACCESS_POLICY => self.global_viewer(),
                CAN_CREATE_WORKER => owner_or_controller,
                CAN_LIST_WORKERS => self.global_viewer() || self.g(Role::Controller),
                CAN_LIST_ACTORS => {
                    self.global_viewer()
                        || self.g(Role::IngressRouter)
                        || self.g(Role::EgressGateway)
                }
                _ => false,
            },
            Object::Worker(_) => match relation {
                CAN_GET => self.global_viewer() || self.g(Role::Controller),
                CAN_LIST_ACTOR_ASSIGNMENTS => self.global_viewer(),
                CAN_UPDATE | CAN_DELETE | CAN_DRAIN => owner_or_controller,
                _ => false,
            },
            Object::Hive(_) => match relation {
                CAN_CREATE_ACCESS_POLICY
                | CAN_UPDATE_ACCESS_POLICY
                | CAN_DELETE_ACCESS_POLICY
                | CAN_DELETE => self.hive_owner(),
                CAN_GET_ACCESS_POLICY
                | CAN_LIST_ACTORS
                | CAN_LIST_ACTOR_TEMPLATES
                | CAN_LIST_TAGS
                | CAN_GET => self.hive_viewer(),
                CAN_CREATE_ACTOR | CAN_CREATE_ACTOR_TEMPLATE | CAN_CREATE_TAG => self.hive_editor(),
                _ => false,
            },
            Object::ActorTemplate { .. } => match relation {
                CAN_GET | CAN_USE_TEMPLATE => self.hive_viewer(),
                CAN_DELETE => self.hive_editor(),
                _ => false,
            },
            Object::Tag { .. } => match relation {
                CAN_GET => self.hive_viewer(),
                CAN_UPDATE | CAN_DELETE | CAN_USE_TAG => self.hive_editor(),
                _ => false,
            },
            Object::Actor { .. } => match relation {
                CAN_GET => {
                    self.hive_viewer()
                        || self.g(Role::EgressGateway)
                        || self.g(Role::ApiServerSidecar)
                }
                CAN_UPDATE
                | CAN_DELETE
                | CAN_CREATE_EGRESS_POLICY
                | CAN_UPDATE_EGRESS_POLICY
                | CAN_DELETE_EGRESS_POLICY
                | CAN_SUSPEND
                | CAN_REVERT => self.hive_editor(),
                CAN_GET_EGRESS_POLICY => {
                    self.hive_viewer() || self.g(Role::EgressGateway) || self.g(Role::Keeper)
                }
                CAN_MINT_ACTOR_JWT | CAN_MINT_ACTOR_CERTIFICATE => self.g(Role::EgressGateway),
                CAN_RESUME => self.hive_editor() || self.g(Role::IngressRouter),
                _ => false,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rel::*;

    fn grants(global: &[Role], hive: &[Role]) -> Grants {
        Grants {
            global: global.iter().copied().collect(),
            hive: hive.iter().copied().collect(),
        }
    }

    fn actor() -> Object {
        Object::Actor {
            hive: "h".into(),
            name: "a".into(),
        }
    }

    #[test]
    fn hive_role_hierarchy() {
        let viewer = grants(&[], &[Role::Viewer]);
        assert!(viewer.allows(CAN_GET, &actor()));
        assert!(!viewer.allows(CAN_RESUME, &actor()));
        let editor = grants(&[], &[Role::Editor]);
        assert!(editor.allows(CAN_RESUME, &actor()));
        assert!(editor.allows(CAN_CREATE_ACTOR, &Object::Hive("h".into())));
        assert!(!editor.allows(CAN_DELETE, &Object::Hive("h".into())));
        let owner = grants(&[], &[Role::Owner]);
        assert!(owner.allows(CAN_DELETE, &Object::Hive("h".into())));
        assert!(owner.allows(CAN_DELETE, &actor()));
        assert!(!owner.allows(CAN_CREATE_HIVE, &Object::Global));
    }

    #[test]
    fn global_roles_reach_hives() {
        let gowner = grants(&[Role::Owner], &[]);
        assert!(gowner.allows(CAN_CREATE_HIVE, &Object::Global));
        assert!(gowner.allows(CAN_SUSPEND, &actor()));
        assert!(gowner.allows(CAN_DRAIN, &Object::Worker("w".into())));
        let gviewer = grants(&[Role::Viewer], &[]);
        assert!(gviewer.allows(CAN_GET, &actor()));
        assert!(!gviewer.allows(CAN_UPDATE, &actor()));
        // Minting is reserved to the egress gateway.
        assert!(!gowner.allows(CAN_MINT_ACTOR_JWT, &actor()));
        let gw = grants(&[Role::EgressGateway], &[]);
        assert!(gw.allows(CAN_MINT_ACTOR_JWT, &actor()));
        assert!(gw.allows(CAN_LIST_ACTORS, &Object::Global));
        let router = grants(&[Role::IngressRouter], &[]);
        assert!(router.allows(CAN_RESUME, &actor()));
        assert!(!router.allows(CAN_SUSPEND, &actor()));
        let ctl = grants(&[Role::Controller], &[]);
        assert!(ctl.allows(CAN_CREATE_WORKER, &Object::Global));
        assert!(!ctl.allows(CAN_LIST_WORKERS, &Object::Hive("h".into())));
        assert_eq!(actor().to_string(), "actor:h/a");
    }
}
