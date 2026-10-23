//! An Actor's whole lifecycle run by users granted access through an
//! AccessPolicy rather than the bootstrap owner the other suites run as.

use hivekeep_e2e::clients::Clients;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::retry::resume_actor_await_capacity;
use hivekeep_proto::api::v1 as pb;

use crate::helpers::*;

/// Alice, an editor, creates, resumes, suspends and deletes an Actor; Bob, a
/// viewer, can watch it but not change it. Each step needs the one before.
pub async fn test_basic_flow(t: &T, f: &Fixture) {
    let alice = new_principal(t, &f.clients, &f.hive, "flow-alice").await;
    let bob = new_principal(t, &f.clients, &f.hive, "flow-bob").await;
    set_hive_policy(
        t,
        &f.clients,
        &f.hive,
        vec![binding("editor", &[&alice]), binding("viewer", &[&bob])],
    )
    .await;
    cleanup_hive_policy(t, &f.clients, &f.hive);

    // Wait for the grants to reach every API replica.
    expect_all(
        t,
        vec![
            call("GetActor", &alice, get_actor(&f.actor), ALLOWED),
            call("GetActor", &bob, get_actor(&f.actor), ALLOWED),
        ],
    )
    .await;
    if t.failed() {
        return;
    }

    let actor = pb::ObjectRef {
        hive: f.hive.clone(),
        name: "flow-actor".into(),
    };
    let req = pb::CreateActorRequest {
        actor: Some(pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: actor.hive.clone(),
                name: actor.name.clone(),
                ..Default::default()
            }),
            actor_template: Some(f.template.clone()),
            ..Default::default()
        }),
    };
    if let Err(e) = alice.apis.control.clone().create_actor(req).await {
        t.fatal(format!("CreateActor as alice: {e}"));
    }
    let mut api = f.clients.api();
    let a = actor.clone();
    t.cleanup(move |_| async move {
        // Best effort, in case a step below failed partway.
        let _ = api
            .suspend_actor(pb::SuspendActorRequest {
                actor: Some(a.clone()),
                ..Default::default()
            })
            .await;
        let _ = api
            .delete_actor(pb::DeleteActorRequest {
                actor: Some(a),
                ..Default::default()
            })
            .await;
    });

    let as_alice = Clients::from_parts(f.clients.kube(), alice.apis.control.clone());
    if let Err(e) = resume_actor_await_capacity(
        t,
        &as_alice,
        pb::ResumeActorRequest {
            actor: Some(actor.clone()),
        },
    )
    .await
    {
        t.fatal(format!("ResumeActor as alice: {e}"));
    }

    // Bob sees the running Actor but cannot suspend it.
    expect_all(
        t,
        vec![
            call("GetActor", &bob, get_actor(&actor), ALLOWED),
            call("ListActors", &bob, list_actors(&f.hive), ALLOWED),
            call("SuspendActor", &bob, suspend_actor(&actor), DENIED),
        ],
    )
    .await;
    if t.failed() {
        return;
    }

    if let Err(e) = alice
        .apis
        .control
        .clone()
        .suspend_actor(pb::SuspendActorRequest {
            actor: Some(actor.clone()),
            ..Default::default()
        })
        .await
    {
        t.fatal(format!("SuspendActor as alice: {e}"));
    }
    wait_for_actor_state(t, &f.clients, &actor, pb::ActorState::Suspended).await;
    if let Err(e) = alice
        .apis
        .control
        .clone()
        .delete_actor(pb::DeleteActorRequest {
            actor: Some(actor),
            ..Default::default()
        })
        .await
    {
        t.fatal(format!("DeleteActor as alice: {e}"));
    }
}
