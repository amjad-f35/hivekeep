//! Actor lifecycle against the counter demo: create, pause (async suspend),
//! suspend, resume, revert and delete, snapshot stamps and tags, durable and
//! external volumes, and the worker or sandbox going away underneath.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use hivekeep_core::hardware::{cold_boot_runtime, matches, ATTR_ARCHITECTURE, SCHEMA_VERSION_V1};
use hivekeep_core::preview::Gate;
use hivekeep_core::resource::ActorRef;
use hivekeep_core::snapshot::SnapshotUri;
use hivekeep_e2e::clients::Clients;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::namespace::{create_namespace, Namespace};
use hivekeep_e2e::preview::require_preview;
use hivekeep_e2e::sandbox::{is_microvm, SandboxClass};
use hivekeep_e2e::sandboxkill::kill_actor_sandbox;
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_e2e::template::template_ref;
use hivekeep_proto::api::v1 as pb;
use k8s_openapi::api::core::v1::Pod;
use kube::api::Api;
use tonic::Code;

use crate::helpers::*;

use pb::ActorState::{Crashed, Running, Suspended};
use pb::DurabilityMode::{Asynchronous, Synchronous};

/// The table of TestActorLifecycle's subtests.
#[derive(Debug, Clone, Copy)]
enum LifecycleStep {
    Create,
    PauseResume,
    SuspendResume,
    DeleteRunning,
    DeletePaused,
    RevertResume,
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_lifecycle() {
    e2e_test("TestActorLifecycle", |t| async move {
        let ns = create_namespace(&t).await;
        let c = clients();
        let at = create_actor_template(
            &t,
            &c,
            &ns,
            TemplateKind::Plain,
            pb::SnapshotFidelity::Memory,
        )
        .await;
        let steps = [
            ("CreateActor", LifecycleStep::Create),
            ("PauseResumeActor", LifecycleStep::PauseResume),
            ("SuspendResumeActor", LifecycleStep::SuspendResume),
            ("DeleteRunningActor", LifecycleStep::DeleteRunning),
            ("DeletePausedActor", LifecycleStep::DeletePaused),
            ("RevertResumeActor", LifecycleStep::RevertResume),
        ];
        join_all(steps.iter().map(|(name, step)| {
            let (c, ns, at, step) = (c.clone(), ns.clone(), at.clone(), *step);
            t.run(name, move |t| async move {
                match step {
                    LifecycleStep::Create => create_actor_step(&t, &c, &ns, &at).await,
                    LifecycleStep::PauseResume => pause_actor_step(&t, &c, &ns, &at).await,
                    LifecycleStep::SuspendResume => suspend_actor_step(&t, &c, &ns, &at).await,
                    LifecycleStep::DeleteRunning => {
                        delete_running_actor_step(&t, &c, &ns, &at).await
                    }
                    LifecycleStep::DeletePaused => delete_paused_actor_step(&t, &c, &ns, &at).await,
                    LifecycleStep::RevertResume => revert_actor_step(&t, &c, &ns, &at).await,
                }
            })
        }))
        .await;
    });
}

async fn create_actor_step(t: &T, c: &Arc<Clients>, ns: &Namespace, at: &pb::ActorTemplate) {
    let name = format!("demo-actor-1-{}", ns.name);
    t.log(format!("Creating Actor {name:?} using the Hivekeep API..."));
    let created = create_actor(t, c, &name, at).await;
    t.log(format!(
        "Successfully created Actor: {}",
        created.metadata.clone().unwrap_or_default().name
    ));
    delete_on_cleanup(t, c, demo_ref(&name));

    let listed = c
        .api()
        .list_actors(pb::ListActorsRequest {
            hive: DEMO_HIVE.into(),
            ..Default::default()
        })
        .await
        .unwrap_or_else(|e| t.fatal(format!("ListActors RPC failed: {e}")))
        .into_inner();
    let tmpl_name = at.metadata.clone().unwrap_or_default().name;
    let mine: Vec<&pb::Actor> = listed
        .actors
        .iter()
        .filter(|a| {
            a.actor_template
                .as_ref()
                .is_some_and(|r| r.name == tmpl_name)
                && a.metadata.as_ref().is_some_and(|m| m.name == name)
        })
        .collect();
    if mine.len() != 1 {
        t.fatal(format!(
            "expected actor {name} from template {tmpl_name}, got {} actors: {mine:?}",
            mine.len()
        ));
    }
    let actor = mine[0];
    let got_name = actor.metadata.clone().unwrap_or_default().name;
    if got_name != name {
        t.error(format!("expected actor name {name}, got {got_name}"));
    }
    let got_tmpl = actor.actor_template.clone().unwrap_or_default().name;
    if got_tmpl != tmpl_name {
        t.error(format!(
            "expected actor template name {tmpl_name}, got {got_tmpl}"
        ));
    }
    if state(&status(actor)) != Suspended {
        t.error(format!(
            "expected actor state to be SUSPENDED, got {:?}",
            state(&status(actor))
        ));
    }
    t.log(format!(
        "Successfully queried the Hivekeep API. Found {} actors total, {} from our template {tmpl_name}.",
        listed.actors.len(),
        mine.len()
    ));
}

async fn create_and_run(t: &T, c: &Clients, name: &str, at: &pb::ActorTemplate) {
    t.log(format!("Creating Actor {name:?}..."));
    create_actor(t, c, name, at).await;
    wait_for_actor_state(t, c, name, Suspended).await;
    t.log(format!("Resuming Actor {name:?}..."));
    if let Err(e) = resume(t, c, &demo_ref(name)).await {
        t.fatal(format!("failed to resume Actor: {e}"));
    }
    wait_for_actor_state(t, c, name, Running).await;
}

async fn call(t: &T, c: &Clients, name: &str, what: &str) -> String {
    call_actor(t, c, &demo_actor(name))
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to call actor {what}: {e}")))
}

async fn resume_running(t: &T, c: &Clients, name: &str, what: &str) {
    t.log(format!("Resuming Actor {name:?} {what}..."));
    if let Err(e) = resume(t, c, &demo_ref(name)).await {
        t.fatal(format!("failed to resume Actor {what}: {e}"));
    }
    wait_for_actor_state(t, c, name, Running).await;
}

async fn delete_and_verify_gone(t: &T, c: &Clients, name: &str, want_not_found: bool) {
    t.log(format!("Deleting Actor {name:?}..."));
    if let Err(e) = delete_actor(c, &demo_ref(name)).await {
        t.fatal(format!("failed to delete Actor: {e}"));
    }
    match get_actor(c, &demo_ref(name)).await {
        Ok(_) => t.fatal(format!(
            "expected actor {name:?} to be deleted, but it still exists"
        )),
        Err(e) if want_not_found && e.code() != Code::NotFound => t.fatal(format!(
            "expected actor {name:?} to be NotFound after delete, got err: {e}"
        )),
        Err(_) => {}
    }
}

async fn pause_actor_step(t: &T, c: &Arc<Clients>, ns: &Namespace, at: &pb::ActorTemplate) {
    let name = format!("pause-actor-{}", ns.name);
    create_and_run(t, c, &name, at).await;
    validate_counter_response(t, &call(t, c, &name, "").await, "after creation", 1, 1);

    t.log(format!("Pausing Actor {name:?}..."));
    if let Err(e) = suspend(c, &demo_ref(&name), Asynchronous).await {
        t.fatal(format!("failed to pause Actor: {e}"));
    }
    wait_for_actor_paused(t, c, &name).await;
    resume_running(t, c, &name, "again").await;
    validate_counter_response(t, &call(t, c, &name, "again").await, "after pause", 2, 2);

    t.log(format!("Suspending Actor {name:?} before deletion..."));
    if let Err(e) = suspend_retrying(t, c, &demo_ref(&name), Synchronous).await {
        t.fatal(format!("failed to suspend Actor: {e}"));
    }
    wait_for_actor_state(t, c, &name, Suspended).await;
    delete_and_verify_gone(t, c, &name, false).await;
}

async fn suspend_actor_step(t: &T, c: &Arc<Clients>, ns: &Namespace, at: &pb::ActorTemplate) {
    let name = format!("suspend-actor-{}", ns.name);
    create_and_run(t, c, &name, at).await;
    validate_counter_response(t, &call(t, c, &name, "").await, "after creation", 1, 1);

    t.log(format!("Suspending Actor {name:?}..."));
    if let Err(e) = suspend(c, &demo_ref(&name), Synchronous).await {
        t.fatal(format!("failed to suspend Actor: {e}"));
    }
    wait_for_actor_state(t, c, &name, Suspended).await;
    resume_running(t, c, &name, "again").await;
    validate_counter_response(t, &call(t, c, &name, "again").await, "after suspend", 2, 2);

    t.log(format!("Suspending Actor {name:?} before deletion..."));
    if let Err(e) = suspend(c, &demo_ref(&name), Synchronous).await {
        t.fatal(format!("failed to suspend Actor: {e}"));
    }
    wait_for_actor_state(t, c, &name, Suspended).await;
    delete_and_verify_gone(t, c, &name, false).await;
}

async fn revert_actor_step(t: &T, c: &Arc<Clients>, ns: &Namespace, at: &pb::ActorTemplate) {
    let name = format!("revert-actor-{}", ns.name);
    let r = demo_ref(&name);
    delete_on_cleanup(t, c, r.clone());
    create_and_run(t, c, &name, at).await;
    validate_counter_response(t, &call(t, c, &name, "").await, "before snapshot", 1, 1);

    let suspended = suspend(c, &r, Synchronous)
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to suspend Actor: {e}")))
        .actor
        .unwrap_or_default();
    let snap_uri = durable_snapshot_uri(&status(&suspended));
    if snap_uri.is_empty() {
        t.fatal("suspend wrote no external snapshot");
    }

    // Mutate guest state after the snapshot, then revert from RUNNING: the
    // increment to 2 must be discarded and the snapshot left untouched.
    resume_running(t, c, &name, "after suspend").await;
    validate_counter_response(
        t,
        &call(t, c, &name, "before revert").await,
        "before revert from RUNNING",
        2,
        2,
    );

    let reverted = revert(c, &r)
        .await
        .unwrap_or_else(|e| t.fatal(format!("RevertActor from RUNNING: {e}")))
        .actor
        .unwrap_or_default();
    let st = status(&reverted);
    if state(&st) != Suspended {
        t.fatal(format!(
            "state after revert = {:?}, want SUSPENDED",
            state(&st)
        ));
    }
    if durable_snapshot_uri(&st) != snap_uri {
        t.fatal(format!(
            "snapshot URI after revert = {:?}, want {snap_uri:?}",
            durable_snapshot_uri(&st)
        ));
    }
    if st.worker_assignment.is_some() {
        t.fatal("worker assignment survived revert");
    }

    // Resuming proves the guest restored the count=1 snapshot rather than
    // continuing from 2: this call takes it from 1 to 2, not 2 to 3.
    resume_running(t, c, &name, "after revert").await;
    validate_counter_response(
        t,
        &call(t, c, &name, "after revert").await,
        "after revert from RUNNING",
        2,
        2,
    );

    // Revert from a pause (local snapshot): the revert drops the local
    // snapshot and keeps the newest durable one, the count=1 snapshot or this
    // pause's count=2 snapshot if its upload has already been reported.
    let paused = suspend(c, &r, Asynchronous)
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to pause Actor: {e}")))
        .actor
        .unwrap_or_default();
    wait_for_actor_paused(t, c, &name).await;
    let paused_uri = snapshot_uri(local_snapshot(&status(&paused)));
    if paused_uri.is_empty() {
        t.fatal("pause wrote no durable snapshot URI on local snapshot");
    }

    let reverted = revert_retrying(t, c, &r)
        .await
        .unwrap_or_else(|e| t.fatal(format!("RevertActor from paused: {e}")))
        .actor
        .unwrap_or_default();
    let st = status(&reverted);
    if state(&st) != Suspended {
        t.fatal(format!(
            "state after revert from paused = {:?}, want SUSPENDED",
            state(&st)
        ));
    }
    if has_local_snapshot(&st) {
        t.fatal("local snapshot pointer survived revert from paused");
    }
    let kept = durable_snapshot_uri(&st);
    let want_count = if kept == snap_uri {
        2
    } else if kept == paused_uri {
        3
    } else {
        t.fatal(format!("snapshot URI after revert from paused = {kept:?}, want {snap_uri:?} or the pause's {paused_uri:?}"))
    };

    // SUSPENDED is rejected with FailedPrecondition rather than no-oping.
    match revert(c, &r).await {
        Err(e) if e.code() == Code::FailedPrecondition => {}
        other => t.fatal(format!(
            "RevertActor from SUSPENDED: got {:?}, want FailedPrecondition",
            other.err()
        )),
    }

    resume_running(t, c, &name, "after paused revert").await;
    validate_counter_response(
        t,
        &call(t, c, &name, "after paused revert").await,
        "after revert from paused",
        want_count,
        want_count,
    );

    if let Err(e) = delete_actor(c, &r).await {
        t.fatal(format!("failed to delete Actor: {e}"));
    }
}

async fn delete_running_actor_step(
    t: &T,
    c: &Arc<Clients>,
    ns: &Namespace,
    at: &pb::ActorTemplate,
) {
    let name = format!("delete-running-actor-{}", ns.name);
    create_and_run(t, c, &name, at).await;
    // The workload must be actively serving on its worker.
    validate_counter_response(t, &call(t, c, &name, "").await, "after creation", 1, 1);

    t.log(format!("Deleting running Actor {name:?}..."));
    delete_and_verify_gone(t, c, &name, true).await;

    // The name can be reused at once.
    t.log(format!(
        "Re-creating Actor {name:?} to verify name reuse..."
    ));
    create_actor(t, c, &name, at).await;
    delete_on_cleanup(t, c, demo_ref(&name));
    wait_for_actor_state(t, c, &name, Suspended).await;
}

async fn delete_paused_actor_step(t: &T, c: &Arc<Clients>, ns: &Namespace, at: &pb::ActorTemplate) {
    let name = format!("delete-paused-actor-{}", ns.name);
    create_and_run(t, c, &name, at).await;
    t.log(format!("Pausing Actor {name:?}..."));
    if let Err(e) = suspend(c, &demo_ref(&name), Asynchronous).await {
        t.fatal(format!("failed to pause Actor: {e}"));
    }
    wait_for_actor_paused(t, c, &name).await;
    t.log(format!("Deleting paused Actor {name:?}..."));
    delete_and_verify_gone(t, c, &name, true).await;
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_snapshot_lifecycle() {
    e2e_test("TestActorSnapshotLifecycle", |t| async move {
        let c = clients();
        let ns = create_namespace(&t).await;
        let at = create_actor_template(
            &t,
            &c,
            &ns,
            TemplateKind::Plain,
            pb::SnapshotFidelity::Memory,
        )
        .await;

        let golden = golden_snapshot_runtime(&t, &c, &at).await;
        let want_class = SandboxClass::current().name();
        if golden.sandbox_class != want_class {
            t.error(format!(
                "golden snapshot sandbox_class = {:?}, want {want_class:?}",
                golden.sandbox_class
            ));
        }
        let schema = golden.version.clone().unwrap_or_default().schema_version;
        if schema != SCHEMA_VERSION_V1 {
            t.error(format!(
                "golden snapshot schema_version = {schema:?}, want {SCHEMA_VERSION_V1:?}"
            ));
        }
        if runtime_attribute(&golden, ATTR_ARCHITECTURE).is_empty() {
            t.error(format!(
                "golden snapshot stamp {golden:?} has no {ATTR_ARCHITECTURE:?} attribute"
            ));
        }
        let golden = Some(golden);

        let source = format!("snapshot-source-{}", ns.name);
        let clone = format!("snapshot-clone-{}", ns.name);
        for name in [&source, &clone] {
            let (c, r) = (c.clone(), demo_ref(name));
            t.cleanup(move |_| async move {
                let _ = tokio::time::timeout(Duration::from_secs(120), async {
                    let _ = suspend(&c, &r, pb::DurabilityMode::Unspecified).await;
                    let _ = delete_actor(&c, &r).await;
                })
                .await;
            });
        }

        let created = create_actor(&t, &c, &source, &at).await;
        require_same_stamp(
            &t,
            "golden snapshot stamp (golden vs actor)",
            &golden,
            &stamp(durable_snapshot(&status(&created))),
        );
        let resumed = resume(&t, &c, &demo_ref(&source))
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to resume source Actor: {e}")));
        wait_for_actor_state(&t, &c, &source, Running).await;
        let assignment = status(&resumed.actor.unwrap_or_default())
            .worker_assignment
            .unwrap_or_default();
        let worker_ref = assignment.worker.clone().unwrap_or_default();
        let worker = c
            .api()
            .get_worker(pb::GetWorkerRequest {
                worker: Some(worker_ref.clone()),
            })
            .await
            .unwrap_or_else(|e| {
                t.fatal(format!(
                    "failed to get assigned Worker {}: {e}",
                    worker_ref.name
                ))
            })
            .into_inner();
        require_same_stamp(
            &t,
            "worker_assignment.sandbox_runtime (worker default runtime vs assignment)",
            &cold_boot_runtime(worker.status.as_ref()),
            &assignment.sandbox_runtime,
        );
        if !matches(assignment.sandbox_runtime.as_ref(), golden.as_ref()) {
            t.fatal(format!(
                "assigned runtime {:?} cannot restore the golden stamp {golden:?}",
                assignment.sandbox_runtime
            ));
        }
        let resp = call_actor(&t, &c, &demo_actor(&source))
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to call source Actor: {e}")));
        validate_counter_response(&t, &resp, "source", 1, 1);

        let tag_ref = demo_ref(&format!("e2e-{}", ns.name));
        {
            let (c, r) = (c.clone(), tag_ref.clone());
            t.cleanup(move |_| async move {
                let _ = c
                    .api()
                    .delete_tag(pb::DeleteTagRequest {
                        tag: Some(r),
                        ..Default::default()
                    })
                    .await;
            });
        }
        let suspended = suspend(&c, &demo_ref(&source), Synchronous)
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to suspend source Actor: {e}")))
            .actor
            .unwrap_or_default();
        let sst = status(&suspended);
        let snap_uri = durable_snapshot_uri(&sst);
        if snap_uri.is_empty() {
            t.fatal("suspended Actor has no external snapshot");
        }
        let snapshot_stamp = stamp(durable_snapshot(&sst));
        require_same_stamp(
            &t,
            "suspended snapshot stamp (assignment vs snapshot)",
            &assignment.sandbox_runtime,
            &snapshot_stamp,
        );
        assert_snapshot_under_location(&t, &snap_uri, &want_snapshot_location(&t, &at));

        // The tag gets its own copy of the snapshot the suspend just wrote,
        // so it survives the Actor being suspended again or deleted.
        let mut tag = c
            .api()
            .create_tag(pb::CreateTagRequest {
                tag: Some(pb::Tag {
                    metadata: Some(pb::ResourceMetadata {
                        hive: DEMO_HIVE.into(),
                        name: tag_ref.name.clone(),
                        ..Default::default()
                    }),
                    scope: pb::TagScope::Hive as i32,
                    source_actor: Some(demo_ref(&source)),
                    ..Default::default()
                }),
            })
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to create the Tag: {e}")))
            .into_inner();
        let tag_uri = tag_snapshot_uri(&tag);
        if tag_uri.is_empty() || tag_uri == snap_uri {
            t.fatal(format!(
                "Tag {} snapshot uri = {tag_uri:?}, want a copy of its own",
                tag_ref.name
            ));
        }
        if let Err(e) = SnapshotUri::parse(&tag_uri) {
            t.error(format!("parsing snapshot URI {tag_uri:?}: {e}"));
        }
        require_same_stamp(
            &t,
            "tag snapshot stamp (actor snapshot vs tag)",
            &snapshot_stamp,
            &stamp(tag.status.as_ref().and_then(|s| s.snapshot.as_ref())),
        );
        assert_snapshot_under_location(&t, &tag_uri, &want_snapshot_location(&t, &at));
        let listed = c
            .api()
            .list_tags(pb::ListTagsRequest {
                hive: DEMO_HIVE.into(),
                ..Default::default()
            })
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to list Tags: {e}")))
            .into_inner();
        if !listed
            .tags
            .iter()
            .any(|x| x.metadata.as_ref().is_some_and(|m| m.name == tag_ref.name))
        {
            t.fatal(format!("tag {:?} missing from ListTags", tag_ref.name));
        }

        tag.scope = pb::TagScope::Published as i32;
        if let Err(e) = c
            .api()
            .update_tag(pb::UpdateTagRequest { tag: Some(tag) })
            .await
        {
            t.fatal(format!("failed to publish Tag: {e}"));
        }

        let cloned = c
            .api()
            .create_actor(pb::CreateActorRequest {
                actor: Some(pb::Actor {
                    metadata: Some(pb::ResourceMetadata {
                        hive: DEMO_HIVE.into(),
                        name: clone.clone(),
                        ..Default::default()
                    }),
                    actor_template: Some(template_ref(&at)),
                    source_tag: Some(tag_ref.clone()),
                    ..Default::default()
                }),
            })
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to create Actor from Tag: {e}")))
            .into_inner();
        require_same_stamp(
            &t,
            "cloned actor snapshot stamp (tag vs clone)",
            &snapshot_stamp,
            &stamp(durable_snapshot(&status(&cloned))),
        );
        let clone_resumed = resume(&t, &c, &demo_ref(&clone))
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to resume cloned Actor: {e}")));
        check_same_stamp(
            &t,
            "cloned actor assignment stamp (tag vs assignment)",
            &snapshot_stamp,
            &assignment_runtime(&status(&clone_resumed.actor.unwrap_or_default())),
        );
        wait_for_actor_state(&t, &c, &clone, Running).await;
        let resp = call_actor(&t, &c, &demo_actor(&clone))
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to call cloned Actor: {e}")));
        validate_counter_response(&t, &resp, "clone", 2, 2);

        if let Err(e) = c
            .api()
            .delete_tag(pb::DeleteTagRequest {
                tag: Some(tag_ref),
                ..Default::default()
            })
            .await
        {
            t.fatal(format!("failed to delete Tag: {e}"));
        }
    });
}

/// What one run of [`run_actor_lifecycle_case`] expects. A zero rootfs
/// expectation skips that check.
#[derive(Debug, Clone, Copy, Default)]
pub struct LifecycleCase {
    pub fidelity: Option<pb::SnapshotFidelity>,
    pub want_memory_after_pause: i64,
    pub want_file_after_pause: i64,
    pub want_memory_after_suspend: i64,
    pub want_file_after_suspend: i64,
    /// The counter on the root filesystem: ROOTFS and MEMORY snapshots carry
    /// it, VOLUMES resets it.
    pub want_rootfs_after_pause: i64,
    pub want_rootfs_after_suspend: i64,
    /// Also check the counter in the second durable volume, which must track
    /// the first exactly: a dropped or misplaced volume would diverge.
    pub check_second_file_counter: bool,
    /// When set, the fidelity recorded on the suspend's snapshot.
    pub want_snapshot_fidelity: Option<pb::SnapshotFidelity>,
}

/// Creates an Actor, calls it, pauses and resumes it, suspends and resumes
/// it, checking the counters and the runtime stamps after each step.
async fn run_actor_lifecycle_case(t: &T, prefix: &str, kind: TemplateKind, tc: LifecycleCase) {
    let ns = create_namespace(t).await;
    let c = clients();
    let at = create_actor_template(
        t,
        &c,
        &ns,
        kind,
        tc.fidelity.unwrap_or(pb::SnapshotFidelity::Unspecified),
    )
    .await;

    let id = format!("{prefix}-{}", ns.name);
    let r = demo_ref(&id);
    t.log(format!("Creating Actor {id:?} using the Hivekeep API..."));
    let created = create_actor(t, &c, &id, &at).await;
    t.log(format!(
        "Successfully created Actor: {}",
        created.metadata.unwrap_or_default().name
    ));
    delete_on_cleanup(t, &c, r.clone());

    t.log(format!("Resuming Actor {id:?}..."));
    let resumed1 = resume(t, &c, &r)
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to resume Actor: {e}")));
    wait_for_actor_state(t, &c, &id, Running).await;
    let assigned = assignment_runtime(&status(&resumed1.actor.unwrap_or_default()));
    if assigned.is_none() {
        t.fatal(format!(
            "resumed Actor {id:?} has no WorkerAssignment.SandboxRuntime"
        ));
    }

    let check = |resp: &str, stage: &str, memory: i64, file: i64, rootfs: i64, first: bool| {
        validate_counter_response(t, resp, stage, memory, file);
        if tc.check_second_file_counter {
            validate_second_file_counter(t, resp, stage, file);
        }
        // The rootfs check runs whenever the case asserts rootfs after pause.
        let wanted = if first {
            tc.want_rootfs_after_pause != 0
        } else {
            rootfs != 0
        };
        if wanted {
            validate_rootfs_counter(t, resp, stage, rootfs);
        }
    };

    let resp = call(t, &c, &id, "").await;
    check(&resp, "after creation", 1, 1, 1, true);

    t.log(format!("Pausing Actor {id:?}..."));
    let paused = suspend(&c, &r, Asynchronous)
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to pause Actor: {e}")))
        .actor
        .unwrap_or_default();
    wait_for_actor_paused(t, &c, &id).await;
    check_same_stamp(
        t,
        "local pause snapshot stamp (assignment vs local)",
        &assigned,
        &stamp(local_snapshot(&status(&paused))),
    );

    t.log(format!("Resuming Actor {id:?} again..."));
    let resumed2 = resume(t, &c, &r)
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to resume Actor again: {e}")));
    wait_for_actor_state(t, &c, &id, Running).await;
    let assigned2 = assignment_runtime(&status(&resumed2.actor.unwrap_or_default()));
    check_same_stamp(
        t,
        "resume-from-paused assignment stamp (first vs second)",
        &assigned,
        &assigned2,
    );

    let resp = call(t, &c, &id, "again").await;
    check(
        &resp,
        "after pause",
        tc.want_memory_after_pause,
        tc.want_file_after_pause,
        tc.want_rootfs_after_pause,
        false,
    );

    t.log(format!("Suspending Actor {id:?}..."));
    let suspended = suspend_retrying(t, &c, &r, Synchronous)
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to suspend Actor: {e}")))
        .actor
        .unwrap_or_default();
    wait_for_actor_state(t, &c, &id, Suspended).await;
    check_same_stamp(
        t,
        "durable snapshot stamp after suspend (assignment vs snapshot)",
        &assigned2,
        &stamp(durable_snapshot(&status(&suspended))),
    );

    if let Some(want) = tc.want_snapshot_fidelity {
        validate_snapshot_fidelity(t, &c, &id, want).await;
    }

    resume_running(t, &c, &id, "again").await;
    let resp = call(t, &c, &id, "again").await;
    check(
        &resp,
        "after suspend",
        tc.want_memory_after_suspend,
        tc.want_file_after_suspend,
        tc.want_rootfs_after_suspend,
        false,
    );
}

/// Checks the fidelity recorded on the suspended Actor's durable snapshot.
async fn validate_snapshot_fidelity(t: &T, c: &Clients, id: &str, want: pb::SnapshotFidelity) {
    let a = get_actor(c, &demo_ref(id))
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to get suspended Actor: {e}")));
    let st = status(&a);
    if durable_snapshot_uri(&st).is_empty() {
        t.fatal("suspended Actor has no external snapshot");
    }
    let got = durable_snapshot(&st)
        .map(|s| s.fidelity)
        .unwrap_or_default();
    if got != want as i32 {
        t.error(format!(
            "snapshot {:?} content scope = {got}, want {want:?}",
            durable_snapshot_uri(&st)
        ));
    }
}

async fn run_cases(
    t: &T,
    prefix: &'static str,
    kind: TemplateKind,
    cases: Vec<(&'static str, LifecycleCase)>,
) {
    join_all(cases.into_iter().map(|(name, tc)| {
        t.run(name, move |t| async move {
            run_actor_lifecycle_case(&t, prefix, kind, tc).await
        })
    }))
    .await;
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn durable_dir_lifecycle() {
    e2e_test("TestDurableDirLifecycle", |t| async move {
        let cases = vec![
            (
                "preferredFidelity:MEMORY",
                LifecycleCase {
                    fidelity: Some(pb::SnapshotFidelity::Memory),
                    want_memory_after_pause: 2,
                    want_file_after_pause: 2,
                    want_rootfs_after_pause: 2,
                    want_memory_after_suspend: 3,
                    want_file_after_suspend: 3,
                    want_rootfs_after_suspend: 3,
                    ..Default::default()
                },
            ),
            (
                // An async suspend captures the same VOLUMES fidelity as a sync
                // one, so memory and rootfs writes cold-boot on every resume.
                "preferredFidelity:VOLUMES",
                LifecycleCase {
                    fidelity: Some(pb::SnapshotFidelity::Volumes),
                    want_memory_after_pause: 1,
                    want_file_after_pause: 2,
                    want_rootfs_after_pause: 1,
                    want_memory_after_suspend: 1,
                    want_file_after_suspend: 3,
                    want_rootfs_after_suspend: 1,
                    ..Default::default()
                },
            ),
            (
                // ROOTFS keeps rootfs writes across a cold boot: the memory
                // counter restarts while the rootfs one continues.
                "preferredFidelity:ROOTFS",
                LifecycleCase {
                    fidelity: Some(pb::SnapshotFidelity::Rootfs),
                    want_memory_after_pause: 1,
                    want_file_after_pause: 2,
                    want_rootfs_after_pause: 2,
                    want_memory_after_suspend: 1,
                    want_file_after_suspend: 3,
                    want_rootfs_after_suspend: 3,
                    ..Default::default()
                },
            ),
        ];
        run_cases(&t, "durabledir-lifecycle", TemplateKind::Plain, cases).await;
    });
}

/// An Actor with two durable dirs: both must survive async and sync
/// suspend/resume independently.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn multiple_durable_dir_lifecycle() {
    e2e_test("TestMultipleDurableDirLifecycle", |t| async move {
        let cases = vec![
            (
                "preferredFidelity:MEMORY",
                LifecycleCase {
                    fidelity: Some(pb::SnapshotFidelity::Memory),
                    want_memory_after_pause: 2,
                    want_file_after_pause: 2,
                    want_memory_after_suspend: 3,
                    want_file_after_suspend: 3,
                    check_second_file_counter: true,
                    ..Default::default()
                },
            ),
            (
                "preferredFidelity:VOLUMES",
                LifecycleCase {
                    fidelity: Some(pb::SnapshotFidelity::Volumes),
                    want_memory_after_pause: 1,
                    want_file_after_pause: 2,
                    want_memory_after_suspend: 1,
                    want_file_after_suspend: 3,
                    check_second_file_counter: true,
                    ..Default::default()
                },
            ),
            (
                "preferredFidelity:ROOTFS",
                LifecycleCase {
                    fidelity: Some(pb::SnapshotFidelity::Rootfs),
                    want_memory_after_pause: 1,
                    want_file_after_pause: 2,
                    want_rootfs_after_pause: 2,
                    want_memory_after_suspend: 1,
                    want_file_after_suspend: 3,
                    want_rootfs_after_suspend: 3,
                    check_second_file_counter: true,
                    ..Default::default()
                },
            ),
        ];
        run_cases(
            &t,
            "multi-durabledir-lifecycle",
            TemplateKind::TwoDurableDirs,
            cases,
        )
        .await;
    });
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn external_volume_lifecycle() {
    e2e_test("TestExternalVolumeLifecycle", |t| async move {
        require_preview(&t, Gate::ExternalVolumes);
        let cases = vec![(
            "preferredFidelity:VOLUMES",
            LifecycleCase {
                fidelity: Some(pb::SnapshotFidelity::Volumes),
                want_memory_after_pause: 1,
                want_file_after_pause: 2,
                want_memory_after_suspend: 1,
                want_file_after_suspend: 3,
                ..Default::default()
            },
        )];
        run_cases(&t, "extvol-lifecycle", TemplateKind::ExternalVolume, cases).await;
    });
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn delete_running_actor_with_external_volume() {
    e2e_test("TestDeleteRunningActorWithExternalVolume", |t| async move {
        require_preview(&t, Gate::ExternalVolumes);
        let c = clients();
        let ns = create_namespace(&t).await;
        let at = create_actor_template(
            &t,
            &c,
            &ns,
            TemplateKind::ExternalVolume,
            pb::SnapshotFidelity::Volumes,
        )
        .await;
        let name = format!("delete-running-extvol-{}", ns.name);
        create_and_run(&t, &c, &name, &at).await;

        // The volume is provisioned.
        let a = get_actor(&c, &demo_ref(&name))
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to get actor: {e}")));
        let volumes = status(&a).external_volumes.unwrap_or_default().volumes;
        if volumes.is_empty() {
            t.fatal("expected actor to have volumes, got 0");
        }
        for v in &volumes {
            if v.storage_volume_id.is_empty() {
                t.fatal(format!(
                    "expected volume {:?} to be provisioned, got no storage volume ID",
                    v.name
                ));
            }
        }
        t.log(format!("Deleting running Actor {name:?}..."));
        delete_and_verify_gone(&t, &c, &name, true).await;
    });
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn external_volume_node_migration() {
    e2e_test("TestExternalVolume_NodeMigration", |t| async move {
        require_preview(&t, Gate::ExternalVolumes);
        if is_microvm() {
            t.skip("Skipping TestExternalVolume_NodeMigration for microVM environment");
        }
        let c = clients();
        let ns = create_namespace(&t).await;
        let at = create_actor_template(
            &t,
            &c,
            &ns,
            TemplateKind::ExternalVolume,
            pb::SnapshotFidelity::Volumes,
        )
        .await;
        let name = format!("extvol-migration-{}", ns.name);
        create_and_run(&t, &c, &name, &at).await;
        let resp = call(&t, &c, &name, "on initial worker").await;
        validate_counter_response(&t, &resp, "initial run", 1, 1);

        let a = get_actor(&c, &demo_ref(&name))
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to get actor: {e}")));
        let wa = status(&a).worker_assignment.unwrap_or_default();

        t.log(format!("Suspending Actor {name:?}..."));
        if let Err(e) = suspend(&c, &demo_ref(&name), Synchronous).await {
            t.fatal(format!("failed to suspend Actor: {e}"));
        }
        wait_for_actor_state(&t, &c, &name, Suspended).await;

        // Deleting the worker forces the resume onto another one.
        if !wa.worker_pod.is_empty() && !wa.worker_namespace.is_empty() {
            t.log(format!(
                "Evicting initial worker pod {}/{} to force migration...",
                wa.worker_namespace, wa.worker_pod
            ));
            let _ = delete_pod(&c, &wa.worker_namespace, &wa.worker_pod).await;
        }
        // Let the pod be marked for deletion before resuming, so the resume
        // does not race it.
        tokio::time::sleep(Duration::from_secs(5)).await;

        resume_running(&t, &c, &name, "after worker pod eviction").await;
        let resp = call(&t, &c, &name, "after migration").await;
        validate_counter_response(&t, &resp, "after migration", 1, 2);

        t.log(format!("Deleting Actor {name:?}..."));
        if let Err(e) = delete_actor(&c, &demo_ref(&name)).await {
            t.fatal(format!("failed to delete Actor: {e}"));
        }
    });
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn worker_pod_deletion() {
    e2e_test("TestWorkerPodDeletion", |t| async move {
        let ns = create_namespace(&t).await;
        let c = clients();
        let at = create_actor_template(
            &t,
            &c,
            &ns,
            TemplateKind::Plain,
            pb::SnapshotFidelity::Memory,
        )
        .await;
        let name = format!("crash-actor-{}", ns.name);
        t.log(format!("Creating Actor {name:?}..."));
        create_actor(&t, &c, &name, &at).await;
        delete_on_cleanup(&t, &c, demo_ref(&name));
        wait_for_actor_state(&t, &c, &name, Suspended).await;
        resume_running(&t, &c, &name, "").await;

        let a = get_actor(&c, &demo_ref(&name))
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to get Actor: {e}")));
        let st = status(&a);
        let wa = st.worker_assignment.clone().unwrap_or_default();
        let (pod, pod_ns, node) = (
            wa.worker_pod.clone(),
            wa.worker_namespace.clone(),
            wa.node_name.clone(),
        );
        if pod.is_empty() || pod_ns.is_empty() || node.is_empty() {
            t.fatal(format!("actor is running but pod details are missing: podName={pod:?}, podNamespace={pod_ns:?}, nodeName={node:?}"));
        }
        if st.assigned_node != node {
            t.fatal(format!(
                "running actor AssignedNode = {:?}, want {node:?}",
                st.assigned_node
            ));
        }

        t.log(format!(
            "Verifying worker for pod {pod_ns}/{pod} is listed..."
        ));
        let workers = list_workers(&c)
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to list workers: {e}")));
        if !workers
            .iter()
            .any(|w| w.worker_namespace == pod_ns && w.worker_pod == pod)
        {
            t.fatal(format!(
                "worker for pod {pod_ns}/{pod} was not found in ListWorkers response: {workers:?}"
            ));
        }

        t.log(format!("Deleting worker pod {pod_ns}/{pod}..."));
        if let Err(e) = delete_pod(&c, &pod_ns, &pod).await {
            t.fatal(format!("failed to delete pod {pod_ns}/{pod}: {e}"));
        }
        t.log(format!(
            "Waiting for actor {name:?} to transition to CRASHED..."
        ));
        wait_for_actor_state(&t, &c, &name, Crashed).await;

        let crashed = get_actor(&c, &demo_ref(&name))
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to get crashed actor {name:?}: {e}")));
        let cst = status(&crashed);
        if cst.assigned_node != node {
            t.error(format!(
                "crashed actor AssignedNode = {:?}, want {node:?} preserved",
                cst.assigned_node
            ));
        }
        let crash = cst.crash.unwrap_or_default();
        // The Actor was RUNNING, not mid-operation, so the message carries no
        // "<op> failed: " prefix.
        let want = "worker pod went away while hosting the actor";
        if crash.message != want {
            t.error(format!(
                "Crash.Message = {:?}, want {want:?}",
                crash.message
            ));
        }
        if crash.crash_time.is_none() {
            t.error("Crash.CrashTime = nil, want set");
        }

        t.log(format!(
            "Verifying worker for pod {pod_ns}/{pod} is removed from store..."
        ));
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut last = Vec::new();
        while Instant::now() < deadline {
            last = list_workers(&c)
                .await
                .unwrap_or_else(|e| t.fatal(format!("failed to list workers: {e}")));
            if !last
                .iter()
                .any(|w| w.worker_namespace == pod_ns && w.worker_pod == pod)
            {
                t.log(format!(
                    "Worker for pod {pod_ns}/{pod} successfully removed."
                ));
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        t.error(format!("worker for pod {pod_ns}/{pod} was not cleaned up within 30s. Current workers: {last:?}"));
    });
}

/// A sandbox that dies on its own is reported by its worker: the Actor goes
/// CRASHED without any operation on it failing, its worker is freed, and it
/// can be reverted and resumed.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn sandbox_crash_is_reported() {
    e2e_test("TestSandboxCrashIsReported", |t| async move {
        let ns = create_namespace(&t).await;
        let c = clients();
        let at = create_actor_template(
            &t,
            &c,
            &ns,
            TemplateKind::Plain,
            pb::SnapshotFidelity::Memory,
        )
        .await;
        let name = format!("sandbox-crash-actor-{}", ns.name);
        let r = demo_ref(&name);
        delete_on_cleanup(&t, &c, r.clone());
        create_and_run(&t, &c, &name, &at).await;

        let a = get_actor(&c, &r)
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to get Actor: {e}")));
        let wa = status(&a).worker_assignment.unwrap_or_default();
        if wa.worker_pod.is_empty() || wa.worker_namespace.is_empty() {
            t.fatal(format!("running actor has no pod recorded: {wa:?}"));
        }
        let uid = a.metadata.clone().unwrap_or_default().uid;
        kill_actor_sandbox(&t, &c, &wa.worker_namespace, &wa.worker_pod, &uid).await;
        wait_for_actor_state_with_timeout(&t, &c, &name, Crashed, Duration::from_secs(120)).await;

        let crashed = get_actor(&c, &r)
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to get crashed Actor: {e}")));
        let cst = status(&crashed);
        let msg = cst.crash.clone().unwrap_or_default().message;
        if !msg.contains("exited") {
            t.error(format!(
                "crash message = {msg:?}, want the worker's report of the sandbox exiting"
            ));
        }
        if cst.worker_assignment.is_some() {
            t.error(format!(
                "worker assignment survived the crash: {:?}",
                cst.worker_assignment
            ));
        }
        // The worker pod survives, unlike in TestRevertCrashedActor.
        if let Err(e) = Api::<Pod>::namespaced(c.kube(), &wa.worker_namespace)
            .get(&wa.worker_pod)
            .await
        {
            t.error(format!(
                "worker pod {}/{} is gone: {e}",
                wa.worker_namespace, wa.worker_pod
            ));
        }

        let reverted = revert(&c, &r)
            .await
            .unwrap_or_else(|e| t.fatal(format!("RevertActor from CRASHED: {e}")))
            .actor
            .unwrap_or_default();
        if state(&status(&reverted)) != Suspended {
            t.fatal(format!(
                "state after revert = {:?}, want SUSPENDED",
                state(&status(&reverted))
            ));
        }
        if let Err(e) = resume(&t, &c, &r).await {
            t.fatal(format!("failed to resume the reverted Actor: {e}"));
        }
        wait_for_actor_state(&t, &c, &name, Running).await;
        if let Err(e) = call_actor(&t, &c, &demo_actor(&name)).await {
            t.fatal(format!("failed to call the reverted Actor: {e}"));
        }
    });
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn revert_crashed_actor() {
    e2e_test("TestRevertCrashedActor", |t| async move {
        let ns = create_namespace(&t).await;
        let c = clients();
        let at = create_actor_template(
            &t,
            &c,
            &ns,
            TemplateKind::Plain,
            pb::SnapshotFidelity::Memory,
        )
        .await;
        let name = format!("revert-crash-actor-{}", ns.name);
        let r = demo_ref(&name);
        delete_on_cleanup(&t, &c, r.clone());
        create_and_run(&t, &c, &name, &at).await;
        validate_counter_response(&t, &call(&t, &c, &name, "").await, "before snapshot", 1, 1);

        let s1 = suspend(&c, &r, Synchronous)
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to suspend Actor (generation 1): {e}")))
            .actor
            .unwrap_or_default();
        let gen1 = durable_snapshot_uri(&status(&s1));
        if gen1.is_empty() {
            t.fatal("suspend (generation 1) wrote no external snapshot");
        }
        resume_running(&t, &c, &name, "after generation 1 suspend").await;
        validate_counter_response(
            &t,
            &call(&t, &c, &name, "before generation 2 snapshot").await,
            "before generation 2 snapshot",
            2,
            2,
        );

        let s2 = suspend(&c, &r, Synchronous)
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to suspend Actor (generation 2): {e}")))
            .actor
            .unwrap_or_default();
        let uri = durable_snapshot_uri(&status(&s2));
        if uri.is_empty() || uri == gen1 {
            t.fatal(format!("generation 2 snapshot URI = {uri:?}, want non-empty and distinct from generation 1 {gen1:?}"));
        }
        resume_running(&t, &c, &name, "after suspend").await;
        validate_counter_response(
            &t,
            &call(&t, &c, &name, "before crash").await,
            "before crash",
            3,
            3,
        );

        let a = get_actor(&c, &r)
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to get Actor: {e}")));
        let wa = status(&a).worker_assignment.unwrap_or_default();
        if wa.worker_pod.is_empty() || wa.worker_namespace.is_empty() || wa.node_name.is_empty() {
            t.fatal(format!("running actor has no pod recorded: {wa:?}"));
        }

        // Deleting the pod is how a real crash happens: the Actor goes
        // CRASHED with its assignment cleared, keeping AssignedNode so a
        // revert can still clean the node up.
        if let Err(e) = delete_pod(&c, &wa.worker_namespace, &wa.worker_pod).await {
            t.fatal(format!(
                "failed to delete worker pod {}/{}: {e}",
                wa.worker_namespace, wa.worker_pod
            ));
        }
        wait_for_actor_state(&t, &c, &name, Crashed).await;
        let crashed = get_actor(&c, &r)
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to get crashed Actor: {e}")));
        if status(&crashed).assigned_node != wa.node_name {
            t.fatal(format!(
                "crashed actor AssignedNode = {:?}, want {:?}",
                status(&crashed).assigned_node,
                wa.node_name
            ));
        }

        let reverted = revert(&c, &r)
            .await
            .unwrap_or_else(|e| t.fatal(format!("RevertActor from CRASHED: {e}")))
            .actor
            .unwrap_or_default();
        let st = status(&reverted);
        if state(&st) != Suspended {
            t.fatal(format!(
                "state after revert = {:?}, want SUSPENDED",
                state(&st)
            ));
        }
        if durable_snapshot_uri(&st) != uri {
            t.fatal(format!(
                "snapshot URI after revert = {:?}, want {uri:?}",
                durable_snapshot_uri(&st)
            ));
        }
        if st.worker_assignment.is_some() {
            t.fatal("worker assignment survived revert");
        }
        if !st.assigned_node.is_empty() {
            t.fatal(format!(
                "assigned node after revert = {:?}, want empty",
                st.assigned_node
            ));
        }

        resume_running(&t, &c, &name, "after crash and revert").await;
        validate_counter_response(
            &t,
            &call(&t, &c, &name, "after recovery").await,
            "after recovery from CRASHED",
            3,
            3,
        );
    });
}

/// Drives one Actor through the revert scenarios.
struct RevertHarness {
    clients: Arc<Clients>,
    name: String,
    r: pb::ObjectRef,
    /// The durable snapshot the Actor was created from: the template's
    /// golden snapshot.
    created_uri: String,
}

impl RevertHarness {
    async fn new(
        t: &T,
        clients: Arc<Clients>,
        ns: &Namespace,
        at: &pb::ActorTemplate,
        prefix: &str,
    ) -> Self {
        let name = format!("{prefix}-{}", ns.name);
        let r = demo_ref(&name);
        delete_on_cleanup(t, &clients, r.clone());
        let created = clients
            .api()
            .create_actor(pb::CreateActorRequest {
                actor: Some(pb::Actor {
                    metadata: Some(pb::ResourceMetadata {
                        hive: DEMO_HIVE.into(),
                        name: name.clone(),
                        ..Default::default()
                    }),
                    actor_template: Some(template_ref(at)),
                    ..Default::default()
                }),
            })
            .await
            .unwrap_or_else(|e| t.fatal(format!("CreateActor {name:?}: {e}")))
            .into_inner();
        wait_for_actor_state(t, &clients, &name, Suspended).await;
        RevertHarness {
            created_uri: durable_snapshot_uri(&status(&created)),
            clients,
            name,
            r,
        }
    }

    async fn resume_and_increment(&self, t: &T, want: i64) {
        if let Err(e) = resume(t, &self.clients, &self.r).await {
            t.fatal(format!("ResumeActor {:?}: {e}", self.name));
        }
        wait_for_actor_state(t, &self.clients, &self.name, Running).await;
        let resp = call_actor(t, &self.clients, &ActorRef::from_object_ref(&self.r))
            .await
            .unwrap_or_else(|e| t.fatal(format!("callActor {:?}: {e}", self.name)));
        validate_counter_response(t, &resp, &self.name, want, want);
    }

    async fn suspend_sync(&self, t: &T) -> String {
        let a = suspend_retrying(t, &self.clients, &self.r, Synchronous)
            .await
            .unwrap_or_else(|e| t.fatal(format!("SuspendActor(SYNCHRONOUS) {:?}: {e}", self.name)))
            .actor
            .unwrap_or_default();
        let uri = durable_snapshot_uri(&status(&a));
        if uri.is_empty() {
            t.fatal(format!(
                "SuspendActor(SYNCHRONOUS) {:?} wrote no durable snapshot",
                self.name
            ));
        }
        uri
    }

    async fn suspend_async_and_await_upload(&self, t: &T) -> String {
        let paused = suspend_retrying(t, &self.clients, &self.r, Asynchronous)
            .await
            .unwrap_or_else(|e| t.fatal(format!("SuspendActor(ASYNCHRONOUS) {:?}: {e}", self.name)))
            .actor
            .unwrap_or_default();
        wait_for_actor_paused(t, &self.clients, &self.name).await;
        let pst = status(&paused);
        let local = local_snapshot(&pst).cloned();
        let want_gen = local.as_ref().map(|s| s.generation).unwrap_or_default();
        let want_uri = snapshot_uri(local.as_ref());
        if want_uri.is_empty() {
            t.fatal(format!(
                "async suspend {:?} recorded no durable snapshot URI on local snapshot {local:?}",
                self.name
            ));
        }
        // e2e installs run the keeper with a 2 s snapshot upload delay; with
        // the 5 minute default this wait times out.
        let upload_wait = Duration::from_secs(60);
        let deadline = Instant::now() + upload_wait;
        let (mut last_err, mut last_snapshots) = (None, Vec::new());
        while Instant::now() < deadline {
            match get_actor(&self.clients, &self.r).await {
                Ok(a) => {
                    let st = status(&a);
                    last_err = None;
                    if let Some(snap) = durable_snapshot(&st).filter(|s| s.generation == want_gen) {
                        let got = snapshot_uri(Some(snap));
                        if got != want_uri {
                            t.fatal(format!(
                                "completed durable URI for {:?} = {got:?}, want {want_uri:?}",
                                self.name
                            ));
                        }
                        return got;
                    }
                    last_snapshots = st.snapshots;
                }
                Err(e) => last_err = Some(e),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        t.fatal(format!(
            "actor {:?} generation {want_gen} not durable after {upload_wait:?}: last error {last_err:?}, snapshots {last_snapshots:?}",
            self.name
        ))
    }

    async fn revert_and_assert(&self, t: &T, want_uri: &str) -> pb::Actor {
        let a = revert_retrying(t, &self.clients, &self.r)
            .await
            .unwrap_or_else(|e| t.fatal(format!("RevertActor {:?}: {e}", self.name)))
            .actor
            .unwrap_or_default();
        let st = status(&a);
        let n = &self.name;
        if state(&st) != Suspended {
            t.fatal(format!(
                "state after RevertActor {n:?} = {:?}, want SUSPENDED",
                state(&st)
            ));
        }
        if durable_snapshot_uri(&st) != want_uri {
            t.fatal(format!(
                "durable snapshot URI after RevertActor {n:?} = {:?}, want {want_uri:?}",
                durable_snapshot_uri(&st)
            ));
        }
        if has_local_snapshot(&st) {
            t.fatal(format!(
                "local snapshot survived RevertActor {n:?}: {:?}",
                st.snapshots
            ));
        }
        if st.worker_assignment.is_some() {
            t.fatal(format!(
                "worker assignment survived RevertActor {n:?}: {:?}",
                st.worker_assignment
            ));
        }
        if !st.assigned_node.is_empty() {
            t.fatal(format!(
                "assigned node after RevertActor {n:?} = {:?}, want empty",
                st.assigned_node
            ));
        }
        if st.crash.is_some() {
            t.fatal(format!(
                "crash status survived RevertActor {n:?}: {:?}",
                st.crash
            ));
        }
        a
    }
}

#[derive(Debug, Clone, Copy)]
enum RevertScenario {
    RunningBeforeActorSnapshot,
    RunningAfterMultipleDurableSnapshots,
    PausedAfterBackgroundUpload,
}

/// Drives the harness into the state the revert starts from and returns
/// the durable snapshot URI and counter value the revert must keep.
async fn revert_setup(t: &T, h: &RevertHarness, s: RevertScenario) -> (String, i64) {
    match s {
        RevertScenario::RunningBeforeActorSnapshot => {
            if h.created_uri.is_empty() {
                t.fatal("new actor has no golden snapshot");
            }
            h.resume_and_increment(t, 1).await;
            (h.created_uri.clone(), 0)
        }
        RevertScenario::RunningAfterMultipleDurableSnapshots => {
            // Generation 1: a synchronous durable snapshot at count=1.
            h.resume_and_increment(t, 1).await;
            let gen1 = h.suspend_sync(t).await;
            // Generation 2: an asynchronous suspend at count=2 whose background
            // upload completes, making it the newest durable snapshot.
            h.resume_and_increment(t, 2).await;
            let gen2 = h.suspend_async_and_await_upload(t).await;
            if gen2 == gen1 {
                t.fatal(format!(
                    "generation 2 durable URI {gen2:?} did not advance from generation 1"
                ));
            }
            // A third sprint (count=3) the revert must discard.
            h.resume_and_increment(t, 3).await;
            (gen2, 2)
        }
        RevertScenario::PausedAfterBackgroundUpload => {
            h.resume_and_increment(t, 1).await;
            let _ = h.suspend_sync(t).await;
            // Pause at count=2 and wait for the upload: the local checkpoint
            // stays on the node, so the revert starts from a paused Actor whose
            // newest snapshot is already durable.
            h.resume_and_increment(t, 2).await;
            let gen2 = h.suspend_async_and_await_upload(t).await;
            wait_for_actor_paused(t, &h.clients, &h.name).await;
            (gen2, 2)
        }
    }
}

/// RevertActor returns the Actor to its newest completed durable snapshot
/// across origin states and snapshot generations.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn revert_actor_to_most_recent_durable_snapshot() {
    e2e_test(
        "TestRevertActorToMostRecentDurableSnapshot",
        |t| async move {
            let ns = create_namespace(&t).await;
            let c = clients();
            let at = create_actor_template(
                &t,
                &c,
                &ns,
                TemplateKind::Plain,
                pb::SnapshotFidelity::Memory,
            )
            .await;
            let cases = [
                (
                    "FromRunningBeforeActorSnapshotRevertsToGolden",
                    "revert-to-golden",
                    RevertScenario::RunningBeforeActorSnapshot,
                ),
                (
                    "FromRunningAfterMultipleDurableSnapshots",
                    "revert-multi-running",
                    RevertScenario::RunningAfterMultipleDurableSnapshots,
                ),
                (
                    "FromPausedAfterBackgroundUploadCompleted",
                    "revert-paused-uploaded",
                    RevertScenario::PausedAfterBackgroundUpload,
                ),
            ];
            join_all(cases.iter().map(|(name, prefix, scenario)| {
            let (c, ns, at, prefix, scenario) = (c.clone(), ns.clone(), at.clone(), *prefix, *scenario);
            t.run(name, move |t| async move {
                let h = RevertHarness::new(&t, c.clone(), &ns, &at, prefix).await;
                let (want_uri, want_counter) = revert_setup(&t, &h, scenario).await;
                let reverted = h.revert_and_assert(&t, &want_uri).await;
                // Reverting again, already at the durable snapshot, fails.
                match revert(&c, &h.r).await {
                    Err(e) if e.code() == Code::FailedPrecondition => {}
                    other => t.fatal(format!(
                        "RevertActor on already-reverted actor {:?}: got {:?}, want FailedPrecondition",
                        reverted.metadata.unwrap_or_default().name,
                        other.err()
                    )),
                }
                // The next call increments from want_counter, proving the newest
                // durable snapshot won.
                h.resume_and_increment(&t, want_counter + 1).await;
            })
        }))
        .await;
        },
    );
}
