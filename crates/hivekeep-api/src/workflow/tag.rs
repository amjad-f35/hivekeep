//! Tags: a stable name for the snapshot a SUSPENDED Actor holds, backed by
//! the Tag's own copy so the Actor's later suspends and delete cannot
//! collect it.
//!
//! Creating one: check the name and location, make the Actor's newest
//! snapshot durable if it is still uploading, reserve the name (the store
//! assigns the uid), record the destination under the Tag's uid as PENDING,
//! copy, and mark COMPLETED. Not idempotent once the name is reserved: a
//! create that dies leaves a pending Tag; delete it to retry.

use hivekeep_core::resource::{ActorRef, TagRef};
use hivekeep_core::snapshot::{validate_snapshot_location, SnapshotUri};
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::keeper::v1 as kp;
use hivekeep_store::{DeletePreconditions, Precondition, StoreError};
use tonic::Code;

use super::resume::dial_error;
use super::{resolve_template, ActorWorkflow};
use crate::convert;
use crate::error::{status, Error, Result};
use crate::fields;
use crate::leases::{self, Ctx};
use crate::snapshots;

fn promote_error(ctx: &Ctx, s: tonic::Status) -> Error {
    let m = format!("while calling keeper PromoteSnapshot: {}", s.message());
    if ctx.is_done() {
        return status::aborted(format!("{m}: lease lost"));
    }
    match s.code() {
        Code::Aborted => status::aborted(m),
        Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded => status::unavailable(m),
        Code::FailedPrecondition | Code::NotFound => status::failed_precondition(m),
        _ => status::internal(m),
    }
}

impl ActorWorkflow {
    /// Creates `tag` from the snapshot of its source Actor.
    pub async fn tag_actor_snapshot(&self, tag: &pb::Tag) -> Result<pb::Tag> {
        let actor_ref = tag
            .source_actor
            .as_ref()
            .map(ActorRef::from_object_ref)
            .unwrap_or_else(|| ActorRef::new("", ""));
        let tag_ref = TagRef::new(
            actor_ref.hive.clone(),
            tag.metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default(),
        );
        let actor_lease = self.lease_actor(&actor_ref).await?;
        let res = async {
            let tag_lease =
                leases::acquire(self.store.as_ref(), &leases::tag_key(&tag_ref), "Tag").await?;
            let ctx = Ctx::background()
                .with_lease(&actor_lease)
                .with_lease(&tag_lease);
            let r = self.tag_leased(&ctx, &actor_ref, &tag_ref, tag).await;
            tag_lease.close().await;
            r
        }
        .await;
        actor_lease.close().await;
        res
    }

    async fn tag_leased(
        &self,
        ctx: &Ctx,
        actor_ref: &ActorRef,
        tag_ref: &TagRef,
        tag: &pb::Tag,
    ) -> Result<pb::Tag> {
        let actor = match self.store.get_actor(actor_ref).await {
            Ok(a) => a,
            Err(StoreError::NotFound) => {
                return Err(status::failed_precondition(format!(
                    "Actor {actor_ref} not found"
                )))
            }
            Err(e) => return Err(e.into()),
        };
        let st = fields::state(&actor);
        if st != pb::ActorState::Suspended {
            return Err(status::failed_precondition(format!(
                "Actor {actor_ref} must be {} to be tagged (got: {})",
                fields::state_name(pb::ActorState::Suspended),
                fields::state_name(st)
            )));
        }
        let template = resolve_template(self.store.as_ref(), &actor).await?;
        // Fail fast, before any upload.
        match self.store.get_tag(tag_ref).await {
            Ok(_) => {
                return Err(status::already_exists(format!(
                    "Tag {tag_ref} already exists; delete it and create it again to retry"
                )))
            }
            Err(StoreError::NotFound) => {}
            Err(e) => return Err(Error::from(e).context(format!("while getting tag {tag_ref}"))),
        }
        let location = self.snapshot_location(&template)?;
        validate_snapshot_location(&location).map_err(|e| {
            status::failed_precondition(format!("invalid storage location for tag {tag_ref}: {e}"))
        })?;

        let actor = self
            .ensure_tag_source_durable(ctx, actor_ref, actor)
            .await?;
        let snapshot = snapshots::latest_completed(actor.status.as_ref())
            .cloned()
            .ok_or_else(|| {
                status::failed_precondition(format!(
                    "Actor {actor_ref} holds no external snapshot to tag"
                ))
            })?;
        if snapshot.actor_template_uid.is_empty() {
            return Err(status::internal(format!(
                "Actor {actor_ref} holds an external snapshot but records no template it was built under"
            )));
        }

        // Reserve the name.
        let to_create = pb::Tag {
            metadata: Some(pb::ResourceMetadata {
                hive: tag_ref.hive.clone(),
                name: tag_ref.name.clone(),
                ..Default::default()
            }),
            scope: tag.scope,
            source_actor: Some(fields::actor_ref(&actor).to_object_ref()),
            status: Some(pb::TagStatus {
                snapshot: None,
                actor_template_uid: snapshot.actor_template_uid.clone(),
            }),
        };
        let reserved = match self.store.create_tag(&to_create).await {
            Ok(t) => t,
            Err(StoreError::FailedPrecondition) => {
                return Err(status::failed_precondition(format!(
                    "Hive {} not found",
                    tag_ref.hive
                )))
            }
            Err(StoreError::AlreadyExists) => {
                return Err(status::already_exists(format!(
                    "Tag {tag_ref} already exists; delete it and create it again to retry"
                )))
            }
            Err(e) => return Err(Error::from(e).context(format!("while reserving tag {tag_ref}"))),
        };

        // Record where the copy goes, before writing anything there.
        let tag_uid = fields::uid(reserved.metadata.as_ref()).to_string();
        let dst = SnapshotUri::for_tag(&location, &tag_ref.hive, &tag_uid).map_err(|e| {
            status::internal(format!(
                "while building the snapshot URI for tag {tag_ref}: {e}"
            ))
        })?;
        let make = |state| {
            let mut s = snapshots::new_durable(
                1,
                pb::SnapshotOwner::Tag,
                snapshot.fidelity,
                "",
                dst.name(),
                dst.as_str(),
                state,
            );
            s.sandbox_runtime = snapshot.sandbox_runtime.clone();
            s
        };
        let pending = make(pb::SnapshotStorageState::Pending);
        let recorded = self
            .write_tag_snapshot(tag_ref, &reserved, pending, "recording the snapshot of")
            .await?;

        if let Some(os) = &self.object_store {
            let src_raw = snapshots::durable_uri(&snapshot);
            let src = SnapshotUri::parse(src_raw).map_err(|e| {
                status::internal(format!(
                    "while parsing the external snapshot {src_raw:?} of the source actor: {e}"
                ))
            })?;
            os.copy_snapshot(src.prefix().as_str(), dst.prefix().as_str())
                .await
                .map_err(|e| {
                    status::unavailable(format!(
                        "while copying the external snapshot for tag {tag_ref}: {e}"
                    ))
                })?;
        }
        let done = make(pb::SnapshotStorageState::Completed);
        self.write_tag_snapshot(tag_ref, &recorded, done, "finalizing")
            .await
    }

    async fn write_tag_snapshot(
        &self,
        tag_ref: &TagRef,
        observed: &pb::Tag,
        snap: pb::Snapshot,
        doing: &str,
    ) -> Result<pb::Tag> {
        self.store
            .update_tag(tag_ref, Precondition::of(observed), &mut |t| {
                t.status.get_or_insert_with(Default::default).snapshot = Some(snap.clone());
                Ok(())
            })
            .await
            .map_err(|e| match e {
                StoreError::VersionConflict => crate::error::conflict(),
                StoreError::NotFound | StoreError::UidConflict => status::aborted(format!(
                    "Tag {tag_ref} was deleted while it was being created, please retry"
                )),
                e => Error::from(e).context(format!("while {doing} tag {tag_ref}")),
            })
    }

    async fn ensure_tag_source_durable(
        &self,
        ctx: &Ctx,
        r: &ActorRef,
        actor: pb::Actor,
    ) -> Result<pb::Actor> {
        let Some(snap) = snapshots::latest(actor.status.as_ref(), |_| true).cloned() else {
            return Ok(actor);
        };
        if snapshots::is_durable(&snap) || snap.locality.is_empty() {
            return Ok(actor);
        }
        let uri = snapshots::durable_uri(&snap).to_string();
        if uri.is_empty() {
            return Err(status::internal(format!(
                "Actor {r} holds a node-local snapshot at generation {} but records no URI to upload it to",
                snap.generation
            )));
        }
        if snap.actor_template_uid.is_empty() {
            return Err(status::internal(format!(
                "Actor {r} holds a snapshot at generation {} but records no template it was built under",
                snap.generation
            )));
        }
        let mut keeper = self.dialer.dial(&snap.locality).await.map_err(|e| {
            dial_error(e).context(format!(
                "while getting keeper conn for node {:?}",
                snap.locality
            ))
        })?;
        let md = actor.metadata.clone().unwrap_or_default();
        let tmpl = actor.actor_template.clone().unwrap_or_default();
        let req = kp::PromoteSnapshotRequest {
            hive: md.hive,
            actor_name: md.name,
            actor_uid: md.uid,
            actor_template_hive: tmpl.hive,
            actor_template_name: tmpl.name,
            snapshot: Some(kp::Snapshot {
                snapshot_uuid: snap.uuid.clone(),
                generation: snap.generation,
                fidelity: convert::fidelity_to_keeper(snap.fidelity) as i32,
                object: Some(kp::ObjectStorage {
                    snapshot_uri: uri,
                    sharing: 0,
                }),
            }),
        };
        let state = ctx
            .run(async { keeper.promote_snapshot(req).await })
            .await
            .map_err(|s| promote_error(ctx, s))?
            .into_inner()
            .state
            .unwrap_or_default();
        let reported = state
            .latest_durable
            .as_ref()
            .and_then(|d| d.snapshot.as_ref());
        if reported.map(|s| (s.snapshot_uuid.as_str(), s.generation))
            != Some((snap.uuid.as_str(), snap.generation))
        {
            return Err(status::internal(format!(
                "keeper on node {:?} was asked to promote Actor {r}'s snapshot {} at generation {}, but reported another durable",
                snap.locality, snap.uuid, snap.generation
            )));
        }
        let mut applied = actor.status.clone().unwrap_or_default();
        let rep = convert::snapshot_report_from_keeper(state.latest_durable.as_ref());
        snapshots::apply_latest_durable(&mut applied, rep.as_ref()).map_err(|e| {
            status::internal(format!(
                "keeper on node {:?} reported a state for Actor {r} that conflicts with its record: {}",
                snap.locality,
                tonic::Status::from(e).message()
            ))
        })?;
        self.store
            .update_actor(r, Precondition::of(&actor), &mut |a| {
                fields::status_mut(a).snapshots = applied.snapshots.clone();
                Ok(())
            })
            .await
            .map_err(|e| match e {
                StoreError::VersionConflict => crate::error::conflict(),
                e => Error::from(e).context(format!(
                    "while recording the upload of Actor {r}'s latest snapshot"
                )),
            })
    }

    /// Deletes a Tag after collecting the snapshot copy it owns. Idempotent.
    pub async fn delete_tag(&self, r: &TagRef, pre: DeletePreconditions) -> Result<pb::Tag> {
        let lease = leases::acquire(self.store.as_ref(), &leases::tag_key(r), "Tag").await?;
        let res = self.delete_tag_leased(r, pre).await;
        lease.close().await;
        res
    }

    async fn delete_tag_leased(&self, r: &TagRef, pre: DeletePreconditions) -> Result<pb::Tag> {
        let mut already: Option<pb::Tag> = None;
        let marked = self
            .store
            .mark_tag_for_deletion(
                r,
                pre.clone(),
                Some(&mut |t: &mut pb::Tag| {
                    if t.metadata.as_ref().is_some_and(|m| m.delete_time.is_some()) {
                        already = Some(t.clone());
                        return Err(crate::error::reject(Error::AlreadyMarked));
                    }
                    Ok(())
                }),
            )
            .await;
        let tag = match marked {
            Ok(t) => t,
            Err(e) => match Error::from(e) {
                Error::AlreadyMarked => already.expect("set with the refusal"),
                Error::Store(StoreError::UidConflict) => {
                    return Err(status::aborted(format!(
                        "Tag {r} does not have uid {}",
                        pre.uid
                    )))
                }
                Error::Store(StoreError::VersionConflict) => return Err(crate::error::conflict()),
                Error::Store(StoreError::NotFound) => {
                    return Err(status::not_found(format!("Tag {r} not found")))
                }
                e => return Err(e.context(format!("while marking tag {r} for deletion"))),
            },
        };
        if self.object_store.is_some() {
            let raw = snapshots::tag_durable_uri(&tag, None);
            if !raw.is_empty() {
                let uri = SnapshotUri::parse(raw).map_err(|e| {
                    status::internal(format!(
                        "while resolving the external snapshot of tag {r}: {e}"
                    ))
                })?;
                self.cleanup_snapshot(uri.prefix().as_str())
                    .await
                    .map_err(|e| {
                        e.context(format!(
                            "while releasing the external snapshot {raw:?} of tag {r}"
                        ))
                    })?;
            }
        }
        let uid = fields::uid(tag.metadata.as_ref()).to_string();
        match self
            .store
            .delete_tag(r, DeletePreconditions::uid(uid.clone()))
            .await
        {
            Ok(t) => Ok(t),
            Err(StoreError::NotFound) => Err(status::not_found(format!("Tag {r} not found"))),
            Err(StoreError::UidConflict) => {
                Err(status::aborted(format!("Tag {r} does not have uid {uid}")))
            }
            Err(e) => Err(Error::from(e).store_context(format!("while deleting tag {r}"))),
        }
    }
}
