//! DeleteActorTemplate: delete the golden Actor and golden Tag (both named
//! after the template's uid, in its hive), then the template row.

use hivekeep_core::resource::{ActorRef, ActorTemplateRef, TagRef};
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{DeletePreconditions, StoreError};
use tonic::Code;

use super::ActorWorkflow;
use crate::error::{status, Error, Result};
use crate::fields;
use crate::leases;

impl ActorWorkflow {
    /// Deletes a template with its golden Actor and Tag.
    pub async fn delete_actor_template(
        &self,
        r: &ActorTemplateRef,
        pre: DeletePreconditions,
    ) -> Result<pb::ActorTemplate> {
        let lease = leases::acquire(
            self.store.as_ref(),
            &leases::template_key(r),
            &format!("ActorTemplate {r}"),
        )
        .await?;
        let res = self.delete_template_leased(r, pre).await;
        lease.close().await;
        res
    }

    async fn delete_template_leased(
        &self,
        r: &ActorTemplateRef,
        pre: DeletePreconditions,
    ) -> Result<pb::ActorTemplate> {
        let mut already: Option<pb::ActorTemplate> = None;
        let marked = self
            .store
            .mark_actor_template_for_deletion(
                r,
                pre.clone(),
                Some(&mut |t: &mut pb::ActorTemplate| {
                    if t.metadata.as_ref().is_some_and(|m| m.delete_time.is_some()) {
                        already = Some(t.clone());
                        return Err(crate::error::reject(Error::AlreadyMarked));
                    }
                    Ok(())
                }),
            )
            .await;
        let tmpl = match marked {
            Ok(t) => t,
            Err(e) => match Error::from(e) {
                Error::AlreadyMarked => already.expect("set with the refusal"),
                Error::Store(StoreError::UidConflict) => {
                    return Err(status::aborted(format!(
                        "ActorTemplate {r} does not have uid {}",
                        pre.uid
                    )))
                }
                Error::Store(StoreError::VersionConflict) => return Err(crate::error::conflict()),
                Error::Store(StoreError::NotFound) => {
                    return Err(status::not_found(format!("ActorTemplate {r} not found")))
                }
                e => {
                    return Err(e.context(format!("while marking actor template {r} for deletion")))
                }
            },
        };
        let golden = fields::template_uid(&tmpl).to_string();
        match self
            .delete_actor(
                &ActorRef::new(r.hive.clone(), golden.clone()),
                DeletePreconditions::none(),
            )
            .await
        {
            Ok(_) => {}
            Err(e) if e.code() == Code::NotFound => {}
            Err(e) => return Err(e.context("while deleting golden actor")),
        }
        match self
            .delete_tag(
                &TagRef::new(r.hive.clone(), golden),
                DeletePreconditions::none(),
            )
            .await
        {
            Ok(_) => {}
            Err(e) if e.code() == Code::NotFound => {}
            Err(e) => return Err(e.context("while deleting golden tag")),
        }
        let uid = fields::template_uid(&tmpl).to_string();
        match self
            .store
            .delete_actor_template(r, DeletePreconditions::uid(uid.clone()))
            .await
        {
            Ok(t) => Ok(t),
            Err(StoreError::NotFound) => {
                Err(status::not_found(format!("ActorTemplate {r} not found")))
            }
            Err(StoreError::UidConflict) => Err(status::aborted(format!(
                "ActorTemplate {r} does not have uid {uid}"
            ))),
            Err(e) => Err(Error::from(e).store_context("while deleting actor template from DB")),
        }
    }
}
