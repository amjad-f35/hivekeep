//! ActorTemplate RPCs. Templates are immutable to clients; only the golden
//! snapshot reconciler writes their status.

use hivekeep_core::defaults;
use hivekeep_core::field::Path;
use hivekeep_core::resource::ActorTemplateRef;
use hivekeep_core::validation::api as v;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::StoreError;

use super::{
    delete_preconditions, internal, invalid, list_options, scrub_for_create, ControlService,
};
use crate::authz::{checks, model::rel};
use crate::context::CallContext;
use crate::error::{status, Error, Result};
use crate::fields;
use crate::snapshots;

impl ControlService {
    pub(crate) async fn create_actor_template(
        &self,
        cc: &CallContext,
        mut req: pb::CreateActorTemplateRequest,
    ) -> Result<pb::ActorTemplate> {
        let hive = req
            .actor_template
            .as_ref()
            .and_then(|t| t.metadata.as_ref())
            .map(|m| m.hive.clone())
            .unwrap_or_default();
        self.authorize(
            cc,
            checks::on_hive(
                rel::CAN_CREATE_ACTOR_TEMPLATE,
                &hive,
                "actor_template.metadata.hive",
            ),
        )
        .await?;
        if let Some(t) = req.actor_template.as_mut() {
            scrub_for_create(t.metadata.as_mut());
            t.status = None;
            defaults::apply(t);
        }
        let op = self.op(cc);
        invalid(v::validate_create_actor_template_request(&op, &req))?;
        let input = req.actor_template.unwrap_or_default();
        let r = fields::template_ref(&input);
        let mut out = input.clone();
        out.status = Some(pb::ActorTemplateStatus::default());
        internal(v::validate_actor_template_update(
            &op,
            &Path::from("actor_template"),
            &out,
            &input,
        ))?;
        // Refuse a template whose Actors could never be snapshotted.
        snapshots::resolve_location(
            &self.settings().snapshot_location,
            &self.settings().gates,
            &out,
        )?;
        self.store()
            .create_actor_template(&out)
            .await
            .map_err(|e| match e {
                StoreError::AlreadyExists => {
                    status::already_exists(format!("ActorTemplate {r} already exists"))
                }
                StoreError::FailedPrecondition => {
                    status::failed_precondition(format!("Hive {} not found", r.hive))
                }
                e => Error::from(e).context("while recording actor template"),
            })
    }

    pub(crate) async fn get_actor_template(
        &self,
        cc: &CallContext,
        req: pb::GetActorTemplateRequest,
    ) -> Result<pb::ActorTemplate> {
        self.authorize(
            cc,
            checks::template_ref(rel::CAN_GET, req.actor_template.as_ref()),
        )
        .await?;
        invalid(v::validate_get_actor_template_request(&self.op(cc), &req))?;
        let r = req
            .actor_template
            .as_ref()
            .map(ActorTemplateRef::from_object_ref)
            .unwrap_or_else(|| ActorTemplateRef::new("", ""));
        self.store()
            .get_actor_template(&r)
            .await
            .map_err(|e| match e {
                StoreError::NotFound => status::not_found(format!("ActorTemplate {r} not found")),
                e => Error::from(e).context("while getting actor template from DB"),
            })
    }

    pub(crate) async fn list_actor_templates(
        &self,
        cc: &CallContext,
        req: pb::ListActorTemplatesRequest,
    ) -> Result<pb::ListActorTemplatesResponse> {
        self.authorize(
            cc,
            checks::on_hive_or_global(rel::CAN_LIST_ACTOR_TEMPLATES, &req.hive, "hive"),
        )
        .await?;
        invalid(v::validate_list_actor_templates_request(&self.op(cc), &req))?;
        let page = self
            .store()
            .list_actor_templates(&req.hive, &list_options(req.options.as_ref()))
            .await?;
        Ok(pb::ListActorTemplatesResponse {
            actor_templates: page.items,
            next_page_token: page.next_page_token,
        })
    }

    pub(crate) async fn delete_actor_template(
        &self,
        cc: &CallContext,
        req: pb::DeleteActorTemplateRequest,
    ) -> Result<pb::ActorTemplate> {
        self.authorize(
            cc,
            checks::template_ref(rel::CAN_DELETE, req.actor_template.as_ref()),
        )
        .await?;
        invalid(v::validate_delete_actor_template_request(
            &self.op(cc),
            &req,
        ))?;
        let r = req
            .actor_template
            .as_ref()
            .map(ActorTemplateRef::from_object_ref)
            .unwrap_or_else(|| ActorTemplateRef::new("", ""));
        self.inner
            .workflow
            .delete_actor_template(&r, delete_preconditions(req.options.as_ref()))
            .await
    }
}
