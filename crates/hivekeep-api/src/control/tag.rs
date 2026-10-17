//! Tag RPCs.

use hivekeep_core::defaults;
use hivekeep_core::field::Path;
use hivekeep_core::resource::TagRef;
use hivekeep_core::validation::api as v;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{Precondition, StoreError};

use super::{
    delete_preconditions, invalid, list_options, scrub_for_create, scrub_for_update, ControlService,
};
use crate::authz::{checks, model::rel};
use crate::context::CallContext;
use crate::error::{reject, status, Error, Result};
use crate::fields;
use crate::snapshots;

fn tag_of(r: Option<&pb::ObjectRef>) -> TagRef {
    r.map(TagRef::from_object_ref)
        .unwrap_or_else(|| TagRef::new("", ""))
}

impl ControlService {
    pub(crate) async fn create_tag(
        &self,
        cc: &CallContext,
        mut req: pb::CreateTagRequest,
    ) -> Result<pb::Tag> {
        // The Tag lives in its source Actor's Hive.
        let hive = req
            .tag
            .as_ref()
            .and_then(|t| t.source_actor.as_ref())
            .map(|a| a.hive.clone())
            .unwrap_or_default();
        self.authorize(
            cc,
            checks::on_hive(rel::CAN_CREATE_TAG, &hive, "tag.source_actor.hive"),
        )
        .await?;
        if let Some(t) = req.tag.as_mut() {
            scrub_for_create(t.metadata.as_mut());
            t.status = None;
            defaults::apply(t);
        }
        invalid(v::validate_create_tag_request(&self.op(cc), &req))?;
        self.inner
            .workflow
            .tag_actor_snapshot(&req.tag.unwrap_or_default())
            .await
    }

    pub(crate) async fn get_tag(
        &self,
        cc: &CallContext,
        req: pb::GetTagRequest,
    ) -> Result<pb::Tag> {
        self.authorize(cc, checks::tag_ref(rel::CAN_GET, req.tag.as_ref()))
            .await?;
        invalid(v::validate_get_tag_request(&self.op(cc), &req))?;
        let r = tag_of(req.tag.as_ref());
        self.store().get_tag(&r).await.map_err(|e| match e {
            StoreError::NotFound => status::not_found(format!("Tag {r} not found")),
            e => Error::from(e).context("while getting tag"),
        })
    }

    pub(crate) async fn list_tags(
        &self,
        cc: &CallContext,
        req: pb::ListTagsRequest,
    ) -> Result<pb::ListTagsResponse> {
        self.authorize(
            cc,
            checks::on_hive_or_global(rel::CAN_LIST_TAGS, &req.hive, "hive"),
        )
        .await?;
        invalid(v::validate_list_tags_request(&self.op(cc), &req))?;
        let page = self
            .store()
            .list_tags(&req.hive, &list_options(req.options.as_ref()))
            .await?;
        Ok(pb::ListTagsResponse {
            tags: page.items,
            next_page_token: page.next_page_token,
        })
    }

    pub(crate) async fn update_tag(
        &self,
        cc: &CallContext,
        mut req: pb::UpdateTagRequest,
    ) -> Result<pb::Tag> {
        self.authorize(
            cc,
            checks::tag_md(
                rel::CAN_UPDATE,
                req.tag.as_ref().and_then(|t| t.metadata.as_ref()),
            ),
        )
        .await?;
        if let Some(t) = req.tag.as_mut() {
            scrub_for_update(t.metadata.as_mut());
            t.status = None;
        }
        let op = self.op(cc);
        invalid(v::validate_update_tag_request(&op, &req))?;
        let input = req.tag.unwrap_or_default();
        let r = fields::tag_ref(&input);
        let res = self
            .store()
            .update_tag(&r, Precondition::of(&input), &mut |to| {
                if to
                    .metadata
                    .as_ref()
                    .is_some_and(|m| m.delete_time.is_some())
                {
                    return Err(reject(status::failed_precondition(format!(
                        "Tag {r} is being deleted"
                    ))));
                }
                if snapshots::tag_durable_uri(to, Some(pb::SnapshotStorageState::Completed))
                    .is_empty()
                {
                    return Err(reject(status::failed_precondition(format!(
                        "Tag {r} is still being created"
                    ))));
                }
                let old = to.clone();
                let (md, st) = (to.metadata.take(), to.status.take());
                *to = input.clone();
                to.metadata = md;
                to.status = st;
                defaults::apply(to);
                v::validate_tag_update(&op, &Path::from("tag"), to, &old)
                    .into_result()
                    .map_err(|e| reject(Error::Status(e.to_invalid_argument())))
            })
            .await;
        res.map_err(|e| match Error::from(e) {
            Error::Store(StoreError::ImmutableField(m)) => {
                status::invalid_argument(format!("while updating tag {r}: {m}"))
            }
            Error::Store(StoreError::UidConflict) => status::aborted(format!(
                "Tag {r} not found with uid {}",
                fields::uid(input.metadata.as_ref())
            )),
            Error::Store(StoreError::NotFound) => status::not_found(format!("Tag {r} not found")),
            Error::Store(e @ StoreError::PreconditionRequired(_)) => {
                status::invalid_argument(format!("while updating tag {r}: {e}"))
            }
            e => e,
        })
    }

    pub(crate) async fn delete_tag(
        &self,
        cc: &CallContext,
        req: pb::DeleteTagRequest,
    ) -> Result<pb::Tag> {
        self.authorize(cc, checks::tag_ref(rel::CAN_DELETE, req.tag.as_ref()))
            .await?;
        invalid(v::validate_delete_tag_request(&self.op(cc), &req))?;
        self.inner
            .workflow
            .delete_tag(
                &tag_of(req.tag.as_ref()),
                delete_preconditions(req.options.as_ref()),
            )
            .await
    }
}
