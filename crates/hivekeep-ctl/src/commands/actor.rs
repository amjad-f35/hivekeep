//! Actor commands: get, create, update, delete, suspend, resume, revert.

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;

use crate::client::Api;
use crate::context::{hive_scope, rpc_error, Ctx};
use crate::error::{CliError, Result};
use crate::print;
use crate::refs::{parse_ref, parse_suspend_mode, parse_worker_selector};

/// `get actor [names...]`.
pub async fn get(ctx: &mut Ctx, names: &[String], hive: Option<&str>, all: bool) -> Result<()> {
    let hive = hive_scope("actors", names, hive, all)?;
    let api = ctx.api().await?;
    let mut out = ctx.out();
    if names.is_empty() {
        let actors = api
            .list_all_actors(&hive)
            .await
            .map_err(|s| rpc_error("failed to list actors", &s))?;
        return print::actor::print_list(&ctx.printer, &mut out, actors);
    }
    let mut actors = Vec::with_capacity(names.len());
    for name in names {
        let actor = api
            .get_actor(pb::GetActorRequest {
                actor: Some(ActorRef::new(&hive, name).to_object_ref()),
            })
            .await
            .map_err(|s| rpc_error(format!("failed to get actor {name:?}"), &s))?;
        actors.push(actor);
    }
    if let [one] = actors.as_slice() {
        return print::actor::print_one(&ctx.printer, &mut out, one);
    }
    print::actor::print_list(&ctx.printer, &mut out, actors)
}

/// Builds the request creating `name` in `hive` from `template`, and from
/// `tag` when given. Bare template and tag names resolve in `hive`.
pub fn build_create_request(
    name: &str,
    hive: &str,
    template: &str,
    tag: Option<&str>,
) -> Result<pb::CreateActorRequest> {
    let template_ref = parse_ref(template, hive).map_err(CliError::usage)?;
    let source_tag = tag
        .filter(|t| !t.is_empty())
        .map(|t| parse_ref(t, hive).map_err(CliError::usage))
        .transpose()?;
    Ok(pb::CreateActorRequest {
        actor: Some(pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: hive.to_string(),
                name: name.to_string(),
                ..Default::default()
            }),
            actor_template: Some(template_ref),
            source_tag,
            ..Default::default()
        }),
    })
}

/// `create actor <name>`.
pub async fn create(
    ctx: &mut Ctx,
    name: &str,
    hive: &str,
    template: &str,
    tag: Option<&str>,
) -> Result<()> {
    let req = build_create_request(name, hive, template, tag)?;
    let api = ctx.api().await?;
    let actor = api
        .create_actor(req)
        .await
        .map_err(|s| rpc_error("failed to create actor", &s))?;
    print::actor::print_one(&ctx.printer, &mut ctx.out(), &actor)
}

/// What `update actor` changes.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ActorUpdate {
    /// The new template, when changing it.
    pub template: Option<pb::ObjectRef>,
    /// The new worker selector, when changing it; `Some(None)` clears it.
    pub worker_selector: Option<Option<pb::Selector>>,
}

impl ActorUpdate {
    /// Parses the `--template` and `--worker-selector` flags; at least one
    /// is required.
    pub fn from_flags(hive: &str, template: Option<&str>, selector: Option<&str>) -> Result<Self> {
        if template.is_none() && selector.is_none() {
            return Err(CliError::usage(
                "at least one of --template or --worker-selector is required",
            ));
        }
        Ok(ActorUpdate {
            template: template
                .map(|t| parse_ref(t, hive).map_err(CliError::usage))
                .transpose()?,
            worker_selector: selector
                .map(|s| parse_worker_selector(s).map_err(CliError::usage))
                .transpose()?,
        })
    }

    /// Applies the change to `actor`.
    pub fn apply(&self, actor: &mut pb::Actor) {
        if let Some(t) = &self.template {
            actor.actor_template = Some(t.clone());
        }
        if let Some(s) = &self.worker_selector {
            actor.worker_selector = s.clone();
        }
    }
}

/// Reads the Actor, applies `update`, and writes it back; the read version
/// guards the write.
pub async fn update_actor(
    api: &Api,
    actor: pb::ObjectRef,
    update: &ActorUpdate,
) -> Result<pb::Actor> {
    let name = actor.name.clone();
    let mut current = api
        .get_actor(pb::GetActorRequest { actor: Some(actor) })
        .await
        .map_err(|s| rpc_error(format!("failed to get actor {name:?}"), &s))?;
    update.apply(&mut current);
    api.update_actor(pb::UpdateActorRequest {
        actor: Some(current),
    })
    .await
    .map_err(|s| rpc_error("failed to update actor", &s))
}

/// `update actor <name>`.
pub async fn update(
    ctx: &mut Ctx,
    name: &str,
    hive: &str,
    template: Option<&str>,
    selector: Option<&str>,
) -> Result<()> {
    let change = ActorUpdate::from_flags(hive, template, selector)?;
    let api = ctx.api().await?;
    let actor = update_actor(&api, ActorRef::new(hive, name).to_object_ref(), &change).await?;
    print::actor::print_one(&ctx.printer, &mut ctx.out(), &actor)
}

/// `delete actor <name>`.
pub async fn delete(ctx: &mut Ctx, name: &str, hive: &str) -> Result<()> {
    let api = ctx.api().await?;
    api.delete_actor(pb::DeleteActorRequest {
        actor: Some(ActorRef::new(hive, name).to_object_ref()),
        options: None,
    })
    .await
    .map_err(|s| rpc_error("failed to delete actor", &s))?;
    ctx.println(&format!("actor {name:?} deleted"))
}

/// Builds the SuspendActor request.
pub fn build_suspend_request(
    actor: &ActorRef,
    mode: pb::DurabilityMode,
) -> pb::SuspendActorRequest {
    pb::SuspendActorRequest {
        actor: Some(actor.to_object_ref()),
        options: Some(pb::SuspendOptions { mode: mode as i32 }),
    }
}

/// `suspend actor <name>`.
pub async fn suspend(ctx: &mut Ctx, name: &str, hive: &str, mode: &str) -> Result<()> {
    let mode = parse_suspend_mode(mode).map_err(CliError::usage)?;
    let api = ctx.api().await?;
    let resp = api
        .suspend_actor(build_suspend_request(&ActorRef::new(hive, name), mode))
        .await
        .map_err(|s| rpc_error("failed to suspend actor", &s))?;
    print::actor::print_one(
        &ctx.printer,
        &mut ctx.out(),
        &resp.actor.unwrap_or_default(),
    )
}

/// `resume actor <name>`.
pub async fn resume(ctx: &mut Ctx, name: &str, hive: &str) -> Result<()> {
    let api = ctx.api().await?;
    let resp = api
        .resume_actor(pb::ResumeActorRequest {
            actor: Some(ActorRef::new(hive, name).to_object_ref()),
        })
        .await
        .map_err(|s| rpc_error("failed to resume actor", &s))?;
    print::actor::print_one(
        &ctx.printer,
        &mut ctx.out(),
        &resp.actor.unwrap_or_default(),
    )
}

/// `revert actor <name>`.
pub async fn revert(ctx: &mut Ctx, name: &str, hive: &str) -> Result<()> {
    let api = ctx.api().await?;
    let resp = api
        .revert_actor(pb::RevertActorRequest {
            actor: Some(ActorRef::new(hive, name).to_object_ref()),
        })
        .await
        .map_err(|s| rpc_error("failed to revert actor", &s))?;
    print::actor::print_one(
        &ctx.printer,
        &mut ctx.out(),
        &resp.actor.unwrap_or_default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::refs::hived_ref;

    #[test]
    fn create_request() {
        let req = build_create_request("a1", "team", "tpl", None).unwrap();
        let actor = req.actor.unwrap();
        let m = actor.metadata.unwrap();
        assert_eq!((m.hive.as_str(), m.name.as_str()), ("team", "a1"));
        assert_eq!(actor.actor_template, Some(hived_ref("team", "tpl")));
        assert_eq!(actor.source_tag, None);

        let req = build_create_request("a1", "team", "shared/tpl", Some("other/v1")).unwrap();
        let actor = req.actor.unwrap();
        assert_eq!(actor.actor_template, Some(hived_ref("shared", "tpl")));
        assert_eq!(actor.source_tag, Some(hived_ref("other", "v1")));

        let req = build_create_request("a1", "team", "tpl", Some("v1")).unwrap();
        assert_eq!(req.actor.unwrap().source_tag, Some(hived_ref("team", "v1")));

        for (tpl, tag) in [("a/b/c", None), ("Bad", None), ("tpl", Some("x/y/z"))] {
            let err = build_create_request("a1", "team", tpl, tag).unwrap_err();
            assert!(
                err.is_usage() && err.message().contains("malformed reference"),
                "{err}"
            );
        }
    }

    #[test]
    fn suspend_request() {
        let req = build_suspend_request(&ActorRef::new("h", "a"), pb::DurabilityMode::Synchronous);
        assert_eq!(req.actor, Some(hived_ref("h", "a")));
        assert_eq!(
            req.options.unwrap().mode,
            pb::DurabilityMode::Synchronous as i32
        );
    }

    #[test]
    fn update_flags() {
        let err = ActorUpdate::from_flags("h", None, None).unwrap_err();
        assert!(err.is_usage() && err.message().contains("at least one of"));

        let u = ActorUpdate::from_flags("h", Some("t2"), None).unwrap();
        assert_eq!(u.template, Some(hived_ref("h", "t2")));
        assert_eq!(u.worker_selector, None);

        let u = ActorUpdate::from_flags("h", None, Some("")).unwrap();
        assert_eq!(u.worker_selector, Some(None));

        assert!(ActorUpdate::from_flags("h", None, Some("bad"))
            .unwrap_err()
            .is_usage());
        assert!(ActorUpdate::from_flags("h", Some("a/b/c"), None)
            .unwrap_err()
            .is_usage());
    }

    #[test]
    fn update_apply() {
        let mut actor = pb::Actor {
            actor_template: Some(hived_ref("h", "t1")),
            worker_selector: Some(pb::Selector {
                match_labels: [("a".to_string(), "b".to_string())].into(),
            }),
            ..Default::default()
        };
        // Changing only the template keeps the selector.
        ActorUpdate::from_flags("h", Some("t2"), None)
            .unwrap()
            .apply(&mut actor);
        assert_eq!(actor.actor_template, Some(hived_ref("h", "t2")));
        assert!(actor.worker_selector.is_some());
        // An empty selector clears it.
        ActorUpdate::from_flags("h", None, Some(""))
            .unwrap()
            .apply(&mut actor);
        assert_eq!(actor.worker_selector, None);
        assert_eq!(actor.actor_template, Some(hived_ref("h", "t2")));
    }
}
