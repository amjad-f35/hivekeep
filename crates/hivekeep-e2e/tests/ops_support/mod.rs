//! Actor helpers shared by the metrics, multiactor, networkpolicy, parking,
//! sandboxconfig and sizing suites.

#![allow(dead_code)]

use std::time::{Duration, Instant};

use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::retry::resume_actor_await_capacity;
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::suite::clients;
use hivekeep_proto::api::v1 as pb;

/// Why every cluster test is ignored by default.
pub const NEEDS_CLUSTER: &str = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG";

/// Names the snapshot bucket the fixtures render into.
pub const BUCKET_ENV: &str = "HK_E2E_BUCKET_NAME";

/// The snapshot bucket, failing `t` when it is not configured.
pub fn bucket(t: &T) -> String {
    hivekeep_e2e::env::check_env(&[BUCKET_ENV])
        .unwrap_or_else(|e| t.fatal(format!("check_env failed: {e}")))
        .remove(BUCKET_ENV)
        .unwrap_or_default()
}

/// The wire reference of `hive/name`.
pub fn object_ref(hive: &str, name: &str) -> pb::ObjectRef {
    pb::ObjectRef {
        hive: hive.to_string(),
        name: name.to_string(),
    }
}

/// Creates the Actor `hive/name` from `template`, suspending and deleting
/// it (best effort, two minutes) when the test ends.
pub async fn create_actor(t: &T, hive: &str, name: &str, template: pb::ObjectRef) {
    let actor = pb::Actor {
        metadata: Some(pb::ResourceMetadata {
            hive: hive.to_string(),
            name: name.to_string(),
            ..Default::default()
        }),
        actor_template: Some(template),
        ..Default::default()
    };
    if let Err(e) = clients()
        .api()
        .create_actor(pb::CreateActorRequest { actor: Some(actor) })
        .await
    {
        t.fatal(format!("CreateActor {name:?}: {e}"));
    }
    let r = object_ref(hive, name);
    t.cleanup(move |_| async move {
        let mut api = clients().api();
        let work = async {
            // Deletion requires the Actor to be suspended first.
            let _ = api
                .suspend_actor(pb::SuspendActorRequest {
                    actor: Some(r.clone()),
                    ..Default::default()
                })
                .await;
            let _ = api
                .delete_actor(pb::DeleteActorRequest {
                    actor: Some(r.clone()),
                    ..Default::default()
                })
                .await;
        };
        let _ = tokio::time::timeout(Duration::from_secs(120), work).await;
    });
}

/// Resumes `hive/name`, waiting out a saturated pool.
pub async fn resume_actor(t: &T, hive: &str, name: &str) {
    let req = pb::ResumeActorRequest {
        actor: Some(object_ref(hive, name)),
    };
    if let Err(e) = resume_actor_await_capacity(t, &clients(), req).await {
        t.fatal(format!("ResumeActor {name:?}: {e}"));
    }
}

/// Suspends `hive/name` (the call returns before the suspend completes).
pub async fn suspend_actor(t: &T, hive: &str, name: &str) {
    let req = pb::SuspendActorRequest {
        actor: Some(object_ref(hive, name)),
        ..Default::default()
    };
    if let Err(e) = clients().api().suspend_actor(req).await {
        t.fatal(format!("SuspendActor {name:?}: {e}"));
    }
}

/// Reads `hive/name`.
pub async fn get_actor(hive: &str, name: &str) -> Result<pb::Actor, tonic::Status> {
    clients()
        .api()
        .get_actor(pb::GetActorRequest {
            actor: Some(object_ref(hive, name)),
        })
        .await
        .map(|r| r.into_inner())
}

/// The Actor's state.
pub fn state_of(actor: &pb::Actor) -> pb::ActorState {
    actor
        .status
        .as_ref()
        .map(|s| s.state())
        .unwrap_or(pb::ActorState::Unspecified)
}

/// Polls `hive/name` every `poll` until it is in `want`, failing `t` after
/// `timeout`.
pub async fn wait_for_state(
    t: &T,
    hive: &str,
    name: &str,
    want: pb::ActorState,
    timeout: Duration,
    poll: Duration,
) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(a) = get_actor(hive, name).await {
            if state_of(&a) == want {
                return;
            }
        }
        tokio::time::sleep(poll).await;
    }
    t.fatal(format!(
        "timed out waiting for actor {name:?} to reach {want:?}"
    ))
}

/// GETs `path` on `hive/name` through the router, requires a 200, and
/// decodes the JSON body.
pub async fn get_json<D: serde::de::DeserializeOwned>(
    t: &T,
    rc: &RouterClient,
    hive: &str,
    name: &str,
    path: &str,
) -> D {
    let resp = rc
        .get(&ActorRef::new(hive, name), path)
        .await
        .unwrap_or_else(|e| t.fatal(format!("GET {path} for {name:?}: {e}")));
    if resp.status != 200 {
        t.fatal(format!(
            "GET {path} for {name:?}: status {}, body {:?}",
            resp.status,
            resp.text()
        ));
    }
    serde_json::from_slice(&resp.body)
        .unwrap_or_else(|e| t.fatal(format!("decoding {path} for {name:?}: {e}")))
}

/// A router client, failing `t` if it cannot be set up.
pub async fn router(t: &T) -> RouterClient {
    RouterClient::new(t, &clients())
        .await
        .unwrap_or_else(|e| t.fatal(format!("NewRouterClient: {e}")))
}
