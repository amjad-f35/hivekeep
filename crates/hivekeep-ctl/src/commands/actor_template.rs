//! Actor template commands: get, create (from a manifest), delete.

use hivekeep_core::resource::ActorTemplateRef;
use hivekeep_proto::api::v1 as pb;

use crate::context::{hive_scope, rpc_error, Ctx};
use crate::error::{CliError, Result};
use crate::{manifest, print};

/// `get actor-template [names...]`.
pub async fn get(ctx: &mut Ctx, names: &[String], hive: Option<&str>, all: bool) -> Result<()> {
    let hive = hive_scope("actor templates", names, hive, all)?;
    let api = ctx.api().await?;
    let mut out = ctx.out();
    if names.is_empty() {
        let templates = api
            .list_all_actor_templates(&hive)
            .await
            .map_err(|s| rpc_error("failed to list actor templates", &s))?;
        return print::actor_template::print_list(&ctx.printer, &mut out, templates);
    }
    let mut templates = Vec::with_capacity(names.len());
    for name in names {
        let t = api
            .get_actor_template(pb::GetActorTemplateRequest {
                actor_template: Some(ActorTemplateRef::new(&hive, name).to_object_ref()),
            })
            .await
            .map_err(|s| rpc_error(format!("failed to get actor template {name:?}"), &s))?;
        templates.push(t);
    }
    if let [one] = templates.as_slice() {
        return print::actor_template::print_one(&ctx.printer, &mut out, one);
    }
    print::actor_template::print_list(&ctx.printer, &mut out, templates)
}

/// `create actor-template -f <manifest>`. The template's hive and name come
/// from the manifest.
pub async fn create(ctx: &mut Ctx, filename: &str) -> Result<()> {
    let data = ctx.read_manifest(filename)?;
    let template: pb::ActorTemplate = manifest::parse(&data, "ActorTemplate").map_err(|e| {
        CliError::failed(format!(
            "failed to parse actor template manifest {filename:?}: {e}"
        ))
    })?;
    let api = ctx.api().await?;
    let created = api
        .create_actor_template(pb::CreateActorTemplateRequest {
            actor_template: Some(template),
        })
        .await
        .map_err(|s| rpc_error("failed to create actor template", &s))?;
    print::actor_template::print_one(&ctx.printer, &mut ctx.out(), &created)
}

/// `delete actor-template <name>`. The server also deletes the template's
/// golden Actor and golden snapshot.
pub async fn delete(ctx: &mut Ctx, name: &str, hive: &str) -> Result<()> {
    let api = ctx.api().await?;
    api.delete_actor_template(pb::DeleteActorTemplateRequest {
        actor_template: Some(ActorTemplateRef::new(hive, name).to_object_ref()),
        options: None,
    })
    .await
    .map_err(|s| rpc_error("failed to delete actor template", &s))?;
    ctx.println(&format!("actor template {name:?} deleted"))
}
