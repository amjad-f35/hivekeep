//! Hive commands: get, create, delete.

use hivekeep_proto::api::v1 as pb;

use crate::context::{rpc_error, Ctx};
use crate::error::Result;
use crate::print;
use crate::refs::global_ref;

/// `get hive [names...]`.
pub async fn get(ctx: &mut Ctx, names: &[String]) -> Result<()> {
    let api = ctx.api().await?;
    let mut out = ctx.out();
    if names.is_empty() {
        let hives = api
            .list_all_hives()
            .await
            .map_err(|s| rpc_error("failed to list hives", &s))?;
        return print::hive::print_list(&ctx.printer, &mut out, hives);
    }
    let mut hives = Vec::with_capacity(names.len());
    for name in names {
        let h = api
            .get_hive(pb::GetHiveRequest {
                hive: Some(global_ref(name)),
            })
            .await
            .map_err(|s| rpc_error(format!("failed to get hive {name:?}"), &s))?;
        hives.push(h);
    }
    if let [one] = hives.as_slice() {
        return print::hive::print_one(&ctx.printer, &mut out, one);
    }
    print::hive::print_list(&ctx.printer, &mut out, hives)
}

/// `create hive <name>`.
pub async fn create(ctx: &mut Ctx, name: &str) -> Result<()> {
    let api = ctx.api().await?;
    let hive = api
        .create_hive(pb::CreateHiveRequest {
            hive: Some(pb::Hive {
                metadata: Some(pb::ResourceMetadata {
                    name: name.to_string(),
                    ..Default::default()
                }),
            }),
        })
        .await
        .map_err(|s| rpc_error("failed to create hive", &s))?;
    print::hive::print_one(&ctx.printer, &mut ctx.out(), &hive)
}

/// `delete hive <name>`: fails while the Hive still holds resources.
pub async fn delete(ctx: &mut Ctx, name: &str) -> Result<()> {
    let api = ctx.api().await?;
    api.delete_hive(pb::DeleteHiveRequest {
        hive: Some(global_ref(name)),
        options: None,
    })
    .await
    .map_err(|s| rpc_error("failed to delete hive", &s))?;
    ctx.println(&format!("hive {name:?} deleted"))
}
