//! Repointing a suspended Actor at another ActorTemplate. The Actor runs
//! and writes its durable dir under template A, suspends, is moved to
//! template B with UpdateActor, and resumes: the resume must notice that the
//! snapshot's recorded template no longer matches and restore data only, so
//! the durable dir survives while the guest cold-boots from B.

use std::collections::BTreeMap;
use std::time::Duration;

use futures::future::join_all;
use hivekeep_e2e::clients::Clients;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::namespace::{create_namespace, Namespace};
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_e2e::template::{create_counter_template, HiveTemplateOptions, TemplateEdit};
use hivekeep_proto::api::v1 as pb;

use crate::helpers::*;

use pb::ActorState::{Running, Suspended};
use pb::DurabilityMode::{Asynchronous, Synchronous};

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn update_template_lifecycle() {
    e2e_test("TestUpdateTemplateLifecycle", |t| async move {
        let cases = [
            ("preferredFidelity:VOLUMES", pb::SnapshotFidelity::Volumes),
            ("preferredFidelity:MEMORY", pb::SnapshotFidelity::Memory),
            ("preferredFidelity:ROOTFS", pb::SnapshotFidelity::Rootfs),
        ];
        join_all(cases.iter().map(|(name, fidelity)| {
            let fidelity = *fidelity;
            t.run(name, move |t| async move {
                run_update_template_case(&t, fidelity).await
            })
        }))
        .await;
    });
}

/// A per-test pool plus a counter template capturing at `fidelity`.
#[allow(clippy::too_many_arguments)]
async fn create_update_test_template(
    t: &T,
    c: &Clients,
    ns: &Namespace,
    name: &str,
    pool: &str,
    bucket: &str,
    fidelity: pb::SnapshotFidelity,
    modify: Option<TemplateEdit>,
) -> pb::ActorTemplate {
    create_counter_template(
        t,
        c,
        &ns.name,
        HiveTemplateOptions {
            hive: DEMO_HIVE.into(),
            name: name.into(),
            pool_name: pool.into(),
            pool_replicas: 2,
            labels: BTreeMap::from([("demo".to_string(), ns.name.clone())]),
            snapshot_config: Some(pb::SnapshotConfig {
                storage_location: format!("gs://{bucket}/hivekeep-demo-{}", ns.name),
                preferred_fidelity: fidelity as i32,
                ..Default::default()
            }),
            modify,
        },
    )
    .await
}

fn uid(at: &pb::ActorTemplate) -> String {
    at.metadata.clone().unwrap_or_default().uid
}

fn snapshot_template_uid(s: Option<&pb::Snapshot>) -> String {
    s.map(|s| s.actor_template_uid.clone()).unwrap_or_default()
}

async fn run_update_template_case(t: &T, fidelity: pb::SnapshotFidelity) {
    let ns = create_namespace(t).await;
    let c = clients();
    let bucket = require_env(t, "HK_E2E_BUCKET_NAME");

    // Template A is the plain counter. Template B is the same workload whose
    // command also reads back the file A's sprint left in the durable dir:
    // the response's "file content" proves B's spec took effect and re-reads
    // the preserved data.
    let (name_a, name_b) = (
        format!("update-a-{}", ns.name),
        format!("update-b-{}", ns.name),
    );
    let created_a =
        create_update_test_template(t, &c, &ns, &name_a, "update-a", &bucket, fidelity, None).await;
    let edit: TemplateEdit = Box::new(|tmpl: &mut pb::ActorTemplate| {
        if let Some(ctr) = tmpl.containers.first_mut() {
            ctr.command
                .push("--validate-existing-file-path=/home/counter/a.txt".into());
        }
    });
    let created_b = create_update_test_template(
        t,
        &c,
        &ns,
        &name_b,
        "update-b",
        &bucket,
        fidelity,
        Some(edit),
    )
    .await;

    // A fresh Actor from template A sits SUSPENDED.
    let id = format!("update-{}", ns.name);
    let r = demo_ref(&id);
    t.log(format!("Creating Actor {id:?} from template {name_a:?}..."));
    let created = c
        .api()
        .create_actor(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(pb::ResourceMetadata {
                    hive: DEMO_HIVE.into(),
                    name: id.clone(),
                    ..Default::default()
                }),
                actor_template: Some(demo_ref(&name_a)),
                ..Default::default()
            }),
        })
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to create Actor: {e}")))
        .into_inner();
    delete_on_cleanup(t, &c, r.clone());
    if state(&status(&created)) != Suspended {
        t.fatal(format!(
            "created Actor state = {:?}, want SUSPENDED",
            state(&status(&created))
        ));
    }

    // Run under A, bumping the durable-dir file counter on every call.
    t.log(format!("Resuming Actor {id:?} under template A..."));
    if let Err(e) = resume(t, &c, &r).await {
        t.fatal(format!("failed to resume Actor: {e}"));
    }
    // The first resume also absorbs the new pool's worker startup.
    wait_for_actor_state_with_timeout(t, &c, &id, Running, Duration::from_secs(120)).await;
    for i in 1..=2 {
        let resp = call_actor(t, &c, &demo_actor(&id))
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to call actor (call {i}): {e}")));
        validate_counter_response(t, &resp, "under template A", i, i);
        validate_rootfs_counter(t, &resp, "under template A", i);
    }

    // The record of the template the sprint booted under must survive the
    // suspend.
    t.log(format!("Suspending Actor {id:?}..."));
    if let Err(e) = suspend(&c, &r, Synchronous).await {
        t.fatal(format!("failed to suspend Actor: {e}"));
    }
    wait_for_actor_state(t, &c, &id, Suspended).await;
    let suspended = get_actor(&c, &r)
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to get suspended Actor: {e}")));
    let sst = status(&suspended);
    let (got, want) = (
        snapshot_template_uid(durable_snapshot(&sst)),
        uid(&created_a),
    );
    if got != want {
        t.error(format!("suspended Actor external_snapshot.actor_template_uid = {got:?}, want template A's {want:?}"));
    }
    if durable_snapshot_uri(&sst).is_empty() {
        t.error("suspended Actor has no external snapshot");
    }
    if let Some(wa) = &sst.worker_assignment {
        t.error(format!(
            "suspended Actor still carries a worker assignment: {wa:?}"
        ));
    }

    // Repoint at template B.
    t.log(format!("Repointing Actor {id:?} at template {name_b:?}..."));
    let updated = c
        .api()
        .update_actor(pb::UpdateActorRequest {
            actor: Some(pb::Actor {
                metadata: suspended.metadata.clone(),
                actor_template: Some(demo_ref(&name_b)),
                ..Default::default()
            }),
        })
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to update Actor's template: {e}")))
        .into_inner();
    let got = updated.actor_template.clone().unwrap_or_default().name;
    if got != name_b {
        t.error(format!(
            "updated Actor actor_template = {got:?}, want {name_b:?}"
        ));
    }
    if state(&status(&updated)) != Suspended {
        t.error(format!(
            "updated Actor state = {:?}, want SUSPENDED",
            state(&status(&updated))
        ));
    }

    // Under B the guest cold-boots (memory counter resets) while the durable
    // dir carries over (file counter continues, and B reads A's file back).
    t.log(format!("Resuming Actor {id:?} under template B..."));
    if let Err(e) = resume(t, &c, &r).await {
        t.fatal(format!("failed to resume Actor after template update: {e}"));
    }
    wait_for_actor_state(t, &c, &id, Running).await;
    let resp = call_actor(t, &c, &demo_actor(&id))
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to call actor after template update: {e}")));
    validate_counter_response(t, &resp, "after template update", 1, 3);
    // A repoint drops the restore to VOLUMES whatever the snapshot holds: a
    // rootfs upper is only valid over the image it was written on.
    validate_rootfs_counter(t, &resp, "after template update", 1);
    if !resp.contains("file content: 3") {
        t.error(format!("[after template update] expected \"file content: 3\" (template B validating the preserved file), got response: {resp}"));
    }

    // Revert while running under B: back to SUSPENDED at the external
    // snapshot it still holds, which A captured. Only the snapshot's own
    // record says the Actor is repointed, so the next resume must judge by
    // it and restore data only again.
    t.log(format!("Reverting Actor {id:?} under template B..."));
    let reverted = revert(&c, &r)
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to revert Actor under template B: {e}")))
        .actor
        .unwrap_or_default();
    let rst = status(&reverted);
    if state(&rst) != Suspended {
        t.fatal(format!(
            "reverted Actor state = {:?}, want SUSPENDED",
            state(&rst)
        ));
    }
    if has_local_snapshot(&rst) {
        t.error(format!(
            "reverted Actor local snapshot present in {:?}, want cleared",
            rst.snapshots
        ));
    }
    let got = reverted.actor_template.clone().unwrap_or_default().name;
    if got != name_b {
        t.error(format!(
            "reverted Actor actor_template = {got:?}, want {name_b:?}"
        ));
    }
    let (got, want) = (
        snapshot_template_uid(durable_snapshot(&rst)),
        uid(&created_a),
    );
    if got != want {
        t.error(format!("reverted Actor external_snapshot.actor_template_uid = {got:?}, want template A's {want:?}"));
    }

    t.log(format!("Resuming Actor {id:?} after the revert..."));
    if let Err(e) = resume(t, &c, &r).await {
        t.fatal(format!("failed to resume Actor after the revert: {e}"));
    }
    wait_for_actor_state(t, &c, &id, Running).await;
    let resp = call_actor(t, &c, &demo_actor(&id))
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to call actor after the revert: {e}")));
    // A data-only restore cold-boots, so memory starts at 1; the revert
    // rewound the file counter to A's 2, and this call takes it to 3 again.
    validate_counter_response(t, &resp, "after revert under template B", 1, 3);
    validate_rootfs_counter(t, &resp, "after revert under template B", 1);
    if !resp.contains("file content: 3") {
        t.error(format!("[after revert under template B] expected \"file content: 3\" (template B reading the rewound file), got response: {resp}"));
    }

    // Pause under B while the newest durable snapshot is still A's. The
    // local checkpoint was captured under B (templates only change while
    // suspended), so resuming from it restores at its own fidelity.
    t.log(format!("Pausing Actor {id:?} under template B..."));
    let paused = suspend(&c, &r, Asynchronous)
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to pause Actor under template B: {e}")))
        .actor
        .unwrap_or_default();
    let pst = status(&paused);
    let Some(local) = local_snapshot(&pst) else {
        t.fatal(format!("paused Actor holds no local snapshot: {pst:?}"))
    };
    if local.actor_template_uid != uid(&created_b) {
        t.error(format!(
            "paused Actor local snapshot actor_template_uid = {:?}, want template B's {:?}",
            local.actor_template_uid,
            uid(&created_b)
        ));
    }
    let got = snapshot_template_uid(durable_snapshot(&pst));
    if got != uid(&created_a) {
        t.error(format!(
            "paused Actor durable snapshot actor_template_uid = {got:?}, want template A's {:?}",
            uid(&created_a)
        ));
    }

    t.log(format!(
        "Resuming Actor {id:?} from pause under template B..."
    ));
    if let Err(e) = resume(t, &c, &r).await {
        t.fatal(format!(
            "failed to resume Actor from pause under template B: {e}"
        ));
    }
    wait_for_actor_state(t, &c, &id, Running).await;
    let resp = call_actor(t, &c, &demo_actor(&id))
        .await
        .unwrap_or_else(|e| {
            t.fatal(format!(
                "failed to call actor after pause/resume under template B: {e}"
            ))
        });
    // MEMORY keeps both counters, ROOTFS the rootfs one, VOLUMES neither.
    let (want_mem, want_rootfs) = match fidelity {
        pb::SnapshotFidelity::Volumes => (1, 1),
        pb::SnapshotFidelity::Rootfs => (1, 2),
        _ => (2, 2),
    };
    validate_counter_response(t, &resp, "after pause/resume under template B", want_mem, 4);
    validate_rootfs_counter(t, &resp, "after pause/resume under template B", want_rootfs);

    // A second suspend closes the loop: its snapshot is captured under B, so
    // the Actor is no longer repointed.
    t.log(format!("Suspending Actor {id:?} again..."));
    if let Err(e) = suspend_retrying(t, &c, &r, Synchronous).await {
        t.fatal(format!("failed to suspend Actor again: {e}"));
    }
    wait_for_actor_state(t, &c, &id, Suspended).await;
    let again = get_actor(&c, &r)
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to get re-suspended Actor: {e}")));
    let got = snapshot_template_uid(durable_snapshot(&status(&again)));
    if got != uid(&created_b) {
        t.error(format!("re-suspended Actor external_snapshot.actor_template_uid = {got:?}, want template B's {:?}", uid(&created_b)));
    }
}
