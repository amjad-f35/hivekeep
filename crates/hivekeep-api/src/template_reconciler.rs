//! Drives ActorTemplates through the golden snapshot flow: create a golden
//! Actor named after the template's uid, resume it, let it warm up, suspend
//! it synchronously, tag its snapshot as a PUBLISHED golden Tag, delete the
//! Actor and record the Tag on the template. New Actors of the template then
//! start from that snapshot.
//!
//! Each pass is level-triggered: it re-derives the next action from the
//! golden Actor it observes, so every action is re-entrant.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hivekeep_core::resource::{ActorRef, ActorTemplateRef, SINGLETON_NAME};
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{ListOptions, Precondition, Store, StoreError};
use tokio_util::sync::CancellationToken;
use tonic::Code;

use crate::context::CallContext;
use crate::control::ControlService;
use crate::error::{status, Error, Result};
use crate::fields;
use crate::leases;
use crate::snapshots;
use crate::workflow::crash::truncate_utf8;
use crate::workqueue::WorkQueue;

const LIST_PAGE_SIZE: i32 = 100;
const WORKERS: usize = 5;
/// Warmup before the golden snapshot for templates without a wakeup probe on
/// every container.
pub const GOLDEN_SNAPSHOT_WARMUP: Duration = Duration::from_secs(20);
const MAX_ERROR_MESSAGE_LEN: usize = 4096;

/// Failure reasons recorded in `error_message`.
pub mod reason {
    #![allow(missing_docs)]
    pub const TAG_CONFLICT: &str = "GoldenTagConflict";
    pub const EGRESS_POLICY_CONFLICT: &str = "GoldenEgressPolicyConflict";
    pub const ACTOR_INVALID: &str = "GoldenActorInvalid";
    pub const ACTOR_CRASHED: &str = "GoldenActorCrashed";
    pub const UNEXPECTED_STATE: &str = "GoldenActorUnexpectedState";
}

/// Whether the golden build reached a terminal state.
pub fn golden_done(s: Option<&pb::GoldenSnapshotStatus>) -> bool {
    s.is_some_and(|s| s.golden_tag.is_some() || !s.error_message.is_empty())
}

/// Zero when every container has a wakeup probe (resume already waited for
/// the workload), else the default warmup.
pub fn warmup_for(containers: &[pb::Container]) -> Duration {
    if !containers.is_empty() && containers.iter().all(|c| c.wakeup_probe.is_some()) {
        Duration::ZERO
    } else {
        GOLDEN_SNAPSHOT_WARMUP
    }
}

fn golden_status(t: &pb::ActorTemplate) -> Option<&pb::GoldenSnapshotStatus> {
    t.status
        .as_ref()
        .and_then(|s| s.golden_snapshot_status.as_ref())
}

fn disabled(t: &pb::ActorTemplate) -> bool {
    t.snapshot_config
        .as_ref()
        .and_then(|c| c.golden_snapshot_config.as_ref())
        .map(|g| g.mode)
        == Some(pb::GoldenSnapshotMode::Disabled as i32)
}

/// The reconciler.
pub struct TemplateReconciler {
    store: Arc<dyn Store>,
    control: ControlService,
    resync: Duration,
}

impl TemplateReconciler {
    /// A reconciler resyncing every `resync`.
    pub fn new(store: Arc<dyn Store>, control: ControlService, resync: Duration) -> Arc<Self> {
        Arc::new(TemplateReconciler {
            store,
            control,
            resync,
        })
    }

    /// Runs the resync producer and the workers until `stop`.
    pub fn start(self: &Arc<Self>, stop: CancellationToken) {
        let q: WorkQueue<ActorTemplateRef> = WorkQueue::new(stop.clone());
        for _ in 0..WORKERS {
            let (me, q) = (self.clone(), q.clone());
            tokio::spawn(async move {
                while let Some(r) = q.get().await {
                    match me.reconcile_one(&r).await {
                        Ok(after) => {
                            q.forget(&r);
                            if let Some(d) = after {
                                q.add_after(r, d);
                            }
                        }
                        Err(e) => {
                            tracing::error!(template = %r, error = %e, "failed to reconcile actor template, requeueing");
                            q.add_rate_limited(r);
                        }
                    }
                }
            });
        }
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                me.resync_once(&q).await;
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tokio::time::sleep(me.resync) => {}
                }
            }
        });
    }

    async fn resync_once(&self, q: &WorkQueue<ActorTemplateRef>) {
        match self.actionable_templates().await {
            Ok(refs) => refs.into_iter().for_each(|r| q.add(r)),
            Err(e) => tracing::error!(error = %e, "failed to list actor templates"),
        }
    }

    /// Every template whose golden snapshot still needs work, across all
    /// pages of the listing.
    pub async fn actionable_templates(&self) -> Result<Vec<ActorTemplateRef>> {
        let mut out = Vec::new();
        let mut token = String::new();
        loop {
            let page = self
                .store
                .list_actor_templates("", &ListOptions::new(LIST_PAGE_SIZE, token.clone()))
                .await?;
            out.extend(
                page.items
                    .iter()
                    .filter(|t| !disabled(t) && !golden_done(golden_status(t)))
                    .map(fields::template_ref),
            );
            if page.next_page_token.is_empty() {
                return Ok(out);
            }
            token = page.next_page_token;
        }
    }

    /// Advances one template as far as it goes in one pass, under its lease.
    /// `Some(d)` asks to be revisited after `d`.
    pub async fn reconcile_one(&self, r: &ActorTemplateRef) -> Result<Option<Duration>> {
        let lease = match self.store.acquire_lease(&leases::template_key(r)).await {
            Ok(l) => l,
            Err(StoreError::LeaseConflict) => return Ok(None),
            Err(e) => return Err(Error::from(e).context("while acquiring lease")),
        };
        let res = self.reconcile_leased(r).await;
        lease.close().await;
        res
    }

    async fn reconcile_leased(&self, r: &ActorTemplateRef) -> Result<Option<Duration>> {
        let mut tmpl = match self.store.get_actor_template(r).await {
            Ok(t) => t,
            Err(StoreError::NotFound) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if disabled(&tmpl) {
            return Ok(None);
        }
        let cc = CallContext::system();
        let golden = pb::ObjectRef {
            hive: r.hive.clone(),
            name: fields::template_uid(&tmpl).to_string(),
        };
        loop {
            let gs = golden_status(&tmpl).cloned().unwrap_or_default();
            if !gs.error_message.is_empty() || gs.golden_tag.is_some() {
                return Ok(None);
            }
            // A completed Tag survives a crash during delete or checkpoint.
            match self
                .control
                .get_tag(
                    &cc,
                    pb::GetTagRequest {
                        tag: Some(golden.clone()),
                    },
                )
                .await
            {
                Ok(tag) => {
                    let src = tag.source_actor.as_ref().map(ActorRef::from_object_ref);
                    if tag.status.as_ref().map(|s| s.actor_template_uid.as_str())
                        != Some(fields::template_uid(&tmpl))
                        || src != Some(ActorRef::from_object_ref(&golden))
                    {
                        return self
                            .fail(
                                &tmpl,
                                reason::TAG_CONFLICT,
                                "golden tag belongs to another actor or template",
                            )
                            .await;
                    }
                    if !snapshots::tag_durable_uri(&tag, Some(pb::SnapshotStorageState::Completed))
                        .is_empty()
                    {
                        return self.save_golden_tag(&tmpl, &golden).await.map(|_| None);
                    }
                    // An incomplete copy cannot be resumed; start over.
                    match self
                        .control
                        .delete_tag(
                            &cc,
                            pb::DeleteTagRequest {
                                tag: Some(golden.clone()),
                                options: None,
                            },
                        )
                        .await
                    {
                        Ok(_) => {}
                        Err(e) if e.code() == Code::NotFound => {}
                        Err(e) => return Err(e.context("while deleting incomplete golden tag")),
                    }
                }
                Err(e) if e.code() == Code::NotFound => {}
                Err(e) => return Err(e.context("while getting golden tag")),
            }
            let actor = match self.ensure_actor(&cc, &tmpl, &golden).await {
                Ok(a) => a,
                Err(e) if e.code() == Code::InvalidArgument => {
                    let m = tonic::Status::from(e).message().to_string();
                    return self.fail(&tmpl, reason::ACTOR_INVALID, &m).await;
                }
                Err(e) => return Err(e),
            };
            match fields::state(&actor) {
                pb::ActorState::Crashed => {
                    return self
                        .fail(
                            &tmpl,
                            reason::ACTOR_CRASHED,
                            "golden actor crashed before its snapshot was taken",
                        )
                        .await
                }
                pb::ActorState::Running => {
                    let Some(take_at) = gs.snapshot_time else {
                        tracing::warn!(template = %r, "golden actor running without a snapshot deadline; restarting warmup");
                        let deadline = SystemTime::now() + warmup_for(&tmpl.containers);
                        tmpl = self
                            .checkpoint(&tmpl, |s| s.snapshot_time = Some(deadline.into()))
                            .await?;
                        continue;
                    };
                    let take_at = SystemTime::try_from(take_at).unwrap_or(SystemTime::UNIX_EPOCH);
                    if let Ok(rem) = take_at.duration_since(SystemTime::now()) {
                        if !rem.is_zero() {
                            return Ok(Some(rem));
                        }
                    }
                    self.suspend(&cc, &golden).await?;
                    return self.tag_golden(&cc, &tmpl, &golden).await.map(|_| None);
                }
                pb::ActorState::Suspending => {
                    self.suspend(&cc, &golden).await?;
                    return self.tag_golden(&cc, &tmpl, &golden).await.map(|_| None);
                }
                pb::ActorState::Resuming | pb::ActorState::Suspended => {
                    if snapshots::latest_completed(actor.status.as_ref()).is_some() {
                        // A suspend completed without being recorded.
                        return self.tag_golden(&cc, &tmpl, &golden).await.map(|_| None);
                    }
                    match self.ensure_golden_egress_policy(&cc, &tmpl, &golden).await {
                        Ok(()) => {}
                        Err(GoldenPolicy::Conflict) => {
                            return self
                                .fail(
                                    &tmpl,
                                    reason::EGRESS_POLICY_CONFLICT,
                                    "golden actor egress policy does not match the template",
                                )
                                .await
                        }
                        Err(GoldenPolicy::Other(e)) => {
                            return Err(e.context("while ensuring golden egress policy"))
                        }
                    }
                    self.control
                        .resume_actor(
                            &cc,
                            pb::ResumeActorRequest {
                                actor: Some(golden.clone()),
                            },
                        )
                        .await
                        .map_err(|e| e.context("while resuming golden actor"))?;
                    let deadline = SystemTime::now() + warmup_for(&tmpl.containers);
                    tmpl = self
                        .checkpoint(&tmpl, |s| s.snapshot_time = Some(deadline.into()))
                        .await?;
                }
                s @ (pb::ActorState::Deleting | pb::ActorState::Reverting) => {
                    return self
                        .fail(
                            &tmpl,
                            reason::UNEXPECTED_STATE,
                            &format!("golden actor in unexpected state {}", fields::state_name(s)),
                        )
                        .await
                }
                pb::ActorState::Unspecified => return Ok(Some(self.resync)),
            }
        }
    }

    async fn suspend(&self, cc: &CallContext, golden: &pb::ObjectRef) -> Result<()> {
        let resp = self
            .control
            .suspend_actor(
                cc,
                pb::SuspendActorRequest {
                    actor: Some(golden.clone()),
                    options: Some(pb::SuspendOptions {
                        mode: pb::DurabilityMode::Synchronous as i32,
                    }),
                },
            )
            .await
            .map_err(|e| e.context("while suspending golden actor"))?;
        if snapshots::latest_completed(resp.actor.as_ref().and_then(|a| a.status.as_ref()))
            .is_none()
        {
            return Err(status::internal(
                "suspending golden actor produced no external snapshot",
            ));
        }
        Ok(())
    }

    async fn tag_golden(
        &self,
        cc: &CallContext,
        tmpl: &pb::ActorTemplate,
        golden: &pb::ObjectRef,
    ) -> Result<()> {
        self.control
            .create_tag(
                cc,
                pb::CreateTagRequest {
                    tag: Some(pb::Tag {
                        metadata: Some(pb::ResourceMetadata {
                            hive: golden.hive.clone(),
                            name: golden.name.clone(),
                            ..Default::default()
                        }),
                        source_actor: Some(golden.clone()),
                        scope: pb::TagScope::Published as i32,
                        status: None,
                    }),
                },
            )
            .await
            .map_err(|e| e.context("while creating golden tag"))?;
        self.save_golden_tag(tmpl, golden).await
    }

    async fn save_golden_tag(
        &self,
        tmpl: &pb::ActorTemplate,
        golden: &pb::ObjectRef,
    ) -> Result<()> {
        match self
            .control
            .delete_actor(
                &CallContext::system(),
                pb::DeleteActorRequest {
                    actor: Some(golden.clone()),
                    options: None,
                },
            )
            .await
        {
            Ok(_) => {}
            Err(e) if e.code() == Code::NotFound => {}
            Err(e) => return Err(e.context("while deleting golden actor")),
        }
        let g = golden.clone();
        self.checkpoint(tmpl, move |s| s.golden_tag = Some(g.clone()))
            .await?;
        Ok(())
    }

    /// Commits a golden status change guarded by the observed template's uid
    /// and version, unless the build already reached a terminal state.
    pub async fn checkpoint(
        &self,
        observed: &pb::ActorTemplate,
        f: impl Fn(&mut pb::GoldenSnapshotStatus) + Send + Sync,
    ) -> Result<pb::ActorTemplate> {
        let r = fields::template_ref(observed);
        Ok(self
            .store
            .update_actor_template(&r, Precondition::of(observed), &mut |t| {
                let gs = t
                    .status
                    .get_or_insert_with(Default::default)
                    .golden_snapshot_status
                    .get_or_insert_with(Default::default);
                if golden_done(Some(gs)) {
                    return Err(crate::error::reject(status::aborted(
                        "actor template reached a terminal golden snapshot state concurrently",
                    )));
                }
                f(gs);
                Ok(())
            })
            .await?)
    }

    async fn fail(
        &self,
        tmpl: &pb::ActorTemplate,
        reason: &str,
        msg: &str,
    ) -> Result<Option<Duration>> {
        let m = truncate_utf8(&format!("{reason}: {msg}"), MAX_ERROR_MESSAGE_LEN);
        self.checkpoint(tmpl, move |s| s.error_message = m.clone())
            .await?;
        Ok(None)
    }

    async fn ensure_actor(
        &self,
        cc: &CallContext,
        tmpl: &pb::ActorTemplate,
        golden: &pb::ObjectRef,
    ) -> Result<pb::Actor> {
        match self
            .control
            .get_actor(
                cc,
                pb::GetActorRequest {
                    actor: Some(golden.clone()),
                },
            )
            .await
        {
            Ok(a) => return Ok(a),
            Err(e) if e.code() == Code::NotFound => {}
            Err(e) => return Err(e.context("while getting golden actor")),
        }
        self.control
            .create_actor(
                cc,
                pb::CreateActorRequest {
                    actor: Some(pb::Actor {
                        metadata: Some(pb::ResourceMetadata {
                            hive: golden.hive.clone(),
                            name: golden.name.clone(),
                            ..Default::default()
                        }),
                        actor_template: Some(fields::template_ref(tmpl).to_object_ref()),
                        ..Default::default()
                    }),
                },
            )
            .await
            .map_err(|e| match e.code() {
                Code::InvalidArgument => e,
                _ => e.context("while creating golden actor"),
            })
    }

    async fn ensure_golden_egress_policy(
        &self,
        cc: &CallContext,
        tmpl: &pb::ActorTemplate,
        golden: &pb::ObjectRef,
    ) -> std::result::Result<(), GoldenPolicy> {
        let Some(want) = &tmpl.golden_egress_policy else {
            return Ok(());
        };
        let created = self
            .control
            .create_actor_egress_policy(
                cc,
                pb::CreateActorEgressPolicyRequest {
                    actor: Some(golden.clone()),
                    egress_policy: Some(pb::EgressPolicy {
                        metadata: Some(pb::ResourceMetadata {
                            hive: golden.hive.clone(),
                            name: SINGLETON_NAME.into(),
                            ..Default::default()
                        }),
                        rules: want.rules.clone(),
                    }),
                },
            )
            .await;
        match created {
            Ok(_) => Ok(()),
            Err(e) if e.code() == Code::AlreadyExists => {
                let existing = self
                    .control
                    .get_actor_egress_policy(
                        cc,
                        pb::GetActorEgressPolicyRequest {
                            actor: Some(golden.clone()),
                            name: SINGLETON_NAME.into(),
                        },
                    )
                    .await
                    .map_err(GoldenPolicy::Other)?;
                if existing.rules != want.rules {
                    return Err(GoldenPolicy::Conflict);
                }
                Ok(())
            }
            Err(e) => Err(GoldenPolicy::Other(e)),
        }
    }
}

enum GoldenPolicy {
    Conflict,
    Other(Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers() {
        assert_eq!(warmup_for(&[]), GOLDEN_SNAPSHOT_WARMUP);
        let probed = pb::Container {
            wakeup_probe: Some(pb::WakeupProbe::default()),
            ..Default::default()
        };
        assert_eq!(warmup_for(std::slice::from_ref(&probed)), Duration::ZERO);
        assert_eq!(
            warmup_for(&[probed, pb::Container::default()]),
            GOLDEN_SNAPSHOT_WARMUP
        );
        assert!(!golden_done(None));
        assert!(golden_done(Some(&pb::GoldenSnapshotStatus {
            error_message: "x".into(),
            ..Default::default()
        })));
    }
}
