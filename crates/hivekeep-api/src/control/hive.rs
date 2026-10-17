//! Hive RPCs.

use hivekeep_core::defaults;
use hivekeep_core::validation::api as v;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::StoreError;

use super::{delete_preconditions, invalid, list_options, scrub_for_create, ControlService};
use crate::authz::{checks, model::rel};
use crate::context::CallContext;
use crate::error::{status, Error, Result};

impl ControlService {
    pub(crate) async fn create_hive(
        &self,
        cc: &CallContext,
        mut req: pb::CreateHiveRequest,
    ) -> Result<pb::Hive> {
        self.authorize(cc, checks::on_global(rel::CAN_CREATE_HIVE))
            .await?;
        if let Some(h) = req.hive.as_mut() {
            scrub_for_create(h.metadata.as_mut());
            defaults::apply(h);
        }
        invalid(v::validate_create_hive_request(&self.op(cc), &req))?;
        let hive = req.hive.unwrap_or_default();
        let name = hive
            .metadata
            .as_ref()
            .map(|m| m.name.clone())
            .unwrap_or_default();
        self.store().create_hive(&hive).await.map_err(|e| match e {
            StoreError::AlreadyExists => {
                status::already_exists(format!("Hive {name} already exists"))
            }
            e => Error::from(e).context("while recording hive"),
        })
    }

    pub(crate) async fn get_hive(
        &self,
        cc: &CallContext,
        req: pb::GetHiveRequest,
    ) -> Result<pb::Hive> {
        self.authorize(cc, checks::hive_ref(rel::CAN_GET, req.hive.as_ref()))
            .await?;
        invalid(v::validate_get_hive_request(&self.op(cc), &req))?;
        let name = req.hive.map(|h| h.name).unwrap_or_default();
        self.store().get_hive(&name).await.map_err(|e| match e {
            StoreError::NotFound => status::not_found(format!("Hive {name} not found")),
            e => Error::from(e).context("while getting hive from DB"),
        })
    }

    pub(crate) async fn list_hives(
        &self,
        cc: &CallContext,
        req: pb::ListHivesRequest,
    ) -> Result<pb::ListHivesResponse> {
        self.authorize(cc, checks::on_global(rel::CAN_LIST_HIVES))
            .await?;
        invalid(v::validate_list_hives_request(&self.op(cc), &req))?;
        let page = self
            .store()
            .list_hives(&list_options(req.options.as_ref()))
            .await?;
        Ok(pb::ListHivesResponse {
            hives: page.items,
            next_page_token: page.next_page_token,
        })
    }

    pub(crate) async fn delete_hive(
        &self,
        cc: &CallContext,
        req: pb::DeleteHiveRequest,
    ) -> Result<pb::Hive> {
        self.authorize(cc, checks::hive_ref(rel::CAN_DELETE, req.hive.as_ref()))
            .await?;
        invalid(v::validate_delete_hive_request(&self.op(cc), &req))?;
        let name = req
            .hive
            .as_ref()
            .map(|h| h.name.clone())
            .unwrap_or_default();
        let pre = delete_preconditions(req.options.as_ref());
        self.store()
            .delete_hive(&name, pre.clone())
            .await
            .map_err(|e| match e {
                StoreError::NotFound => status::not_found(format!("Hive {name} not found")),
                StoreError::FailedPrecondition => {
                    status::failed_precondition(format!("Hive {name} is not empty"))
                }
                StoreError::UidConflict => {
                    status::aborted(format!("Hive {name} does not have uid {}", pre.uid))
                }
                e => Error::from(e).store_context("while deleting hive from DB"),
            })
    }
}
