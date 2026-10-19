//! Tag commands: get, create, update (scope), delete.

use hivekeep_core::resource::TagRef;
use hivekeep_proto::api::v1 as pb;

use crate::client::Api;
use crate::context::{hive_scope, rpc_error, Ctx};
use crate::error::{CliError, Result};
use crate::print;
use crate::refs::{hived_ref, parse_tag_scope};

/// `get tag [names...]`.
pub async fn get(ctx: &mut Ctx, names: &[String], hive: Option<&str>, all: bool) -> Result<()> {
    let hive = hive_scope("tags", names, hive, all)?;
    let api = ctx.api().await?;
    let mut out = ctx.out();
    if names.is_empty() {
        let tags = api
            .list_all_tags(&hive)
            .await
            .map_err(|s| rpc_error("failed to list tags", &s))?;
        return print::tag::print_list(&ctx.printer, &mut out, tags);
    }
    let mut tags = Vec::with_capacity(names.len());
    for name in names {
        let t = api
            .get_tag(pb::GetTagRequest {
                tag: Some(TagRef::new(&hive, name).to_object_ref()),
            })
            .await
            .map_err(|s| rpc_error(format!("failed to get tag {name:?}"), &s))?;
        tags.push(t);
    }
    if let [one] = tags.as_slice() {
        return print::tag::print_one(&ctx.printer, &mut out, one);
    }
    print::tag::print_list(&ctx.printer, &mut out, tags)
}

/// Builds the request tagging the snapshot of `actor`, both in `hive`.
pub fn build_create_request(
    name: &str,
    hive: &str,
    actor: &str,
    scope: pb::TagScope,
) -> pb::CreateTagRequest {
    pb::CreateTagRequest {
        tag: Some(pb::Tag {
            metadata: Some(pb::ResourceMetadata {
                hive: hive.to_string(),
                name: name.to_string(),
                ..Default::default()
            }),
            scope: scope as i32,
            source_actor: Some(hived_ref(hive, actor)),
            status: None,
        }),
    }
}

/// `create tag <name>`.
pub async fn create(ctx: &mut Ctx, name: &str, hive: &str, actor: &str, scope: &str) -> Result<()> {
    let scope = parse_tag_scope(scope).map_err(CliError::usage)?;
    let api = ctx.api().await?;
    let tag = api
        .create_tag(build_create_request(name, hive, actor, scope))
        .await
        .map_err(|s| rpc_error("failed to create tag", &s))?;
    print::tag::print_one(&ctx.printer, &mut ctx.out(), &tag)
}

/// Reads the Tag, sets its scope, and writes it back.
pub async fn update_scope(api: &Api, tag: pb::ObjectRef, scope: pb::TagScope) -> Result<pb::Tag> {
    let name = tag.name.clone();
    let mut current = api
        .get_tag(pb::GetTagRequest { tag: Some(tag) })
        .await
        .map_err(|s| rpc_error(format!("failed to get tag {name:?}"), &s))?;
    current.scope = scope as i32;
    api.update_tag(pb::UpdateTagRequest { tag: Some(current) })
        .await
        .map_err(|s| rpc_error("failed to update tag", &s))
}

/// `update tag <name> --scope ...`.
pub async fn update(ctx: &mut Ctx, name: &str, hive: &str, scope: &str) -> Result<()> {
    let scope = parse_tag_scope(scope).map_err(CliError::usage)?;
    let api = ctx.api().await?;
    let tag = update_scope(&api, TagRef::new(hive, name).to_object_ref(), scope).await?;
    print::tag::print_one(&ctx.printer, &mut ctx.out(), &tag)
}

/// `delete tag <name>`.
pub async fn delete(ctx: &mut Ctx, name: &str, hive: &str) -> Result<()> {
    let api = ctx.api().await?;
    api.delete_tag(pb::DeleteTagRequest {
        tag: Some(TagRef::new(hive, name).to_object_ref()),
        options: None,
    })
    .await
    .map_err(|s| rpc_error("failed to delete tag", &s))?;
    ctx.println(&format!("tag {name:?} deleted"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_request_names_the_source_actor_in_the_same_hive() {
        let req = build_create_request("v1", "team", "a1", pb::TagScope::Published);
        let tag = req.tag.unwrap();
        assert_eq!(tag.metadata.unwrap().hive, "team");
        assert_eq!(tag.scope, pb::TagScope::Published as i32);
        assert_eq!(tag.source_actor, Some(hived_ref("team", "a1")));
    }
}
