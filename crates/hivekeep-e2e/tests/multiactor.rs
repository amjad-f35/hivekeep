//! Two Actors share one worker: placement, routing to the right sandbox,
//! and independent suspension.

mod ops_support;

use hivekeep_e2e::fixture::{deploy_hive_fixture, FixtureManifests, FixtureOptions};
use hivekeep_e2e::sandbox::fixture_name;
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_proto::api::v1 as pb;
use ops_support::*;
use serde::Deserialize;

const MULTIACTOR_TEMPLATE: &str = "probe-multiactor";

fn multiactor_hive() -> String {
    format!("{}-multiactor", fixture_name("hivekeep-e2e"))
}

/// The probe's `/whoami`: which Actor served the request.
#[derive(Debug, Default, Deserialize)]
struct WhoamiResponse {
    #[serde(default)]
    hive: String,
    #[serde(default)]
    file: String,
    #[serde(default)]
    uid: String,
    #[serde(default)]
    error: String,
}

async fn whoami(
    t: &hivekeep_e2e::harness::T,
    rc: &hivekeep_e2e::router_client::RouterClient,
    name: &str,
) -> WhoamiResponse {
    let out: WhoamiResponse = get_json(t, rc, &multiactor_hive(), name, "/whoami").await;
    if !out.error.is_empty() {
        t.log(format!("/whoami for {name:?} reported: {}", out.error));
    }
    out
}

/// The worker pod a RUNNING Actor was placed on.
async fn worker_pod_of(t: &hivekeep_e2e::harness::T, name: &str) -> String {
    let actor = get_actor(&multiactor_hive(), name)
        .await
        .unwrap_or_else(|e| t.fatal(format!("GetActor {name:?}: {e}")));
    let state = state_of(&actor);
    if state != pb::ActorState::Running {
        t.fatal(format!("actor {name:?} is {state:?}, want RUNNING"));
    }
    let pod = actor
        .status
        .and_then(|s| s.worker_assignment)
        .map(|w| w.worker_pod)
        .unwrap_or_default();
    if pod.is_empty() {
        t.fatal(format!("actor {name:?} is RUNNING with no worker pod"));
    }
    pod
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn two_actors_share_one_worker() {
    e2e_test("TestTwoActorsShareOneWorker", |t| async move {
        let bucket = bucket(&t);
        let hive = multiactor_hive();
        let manifests = FixtureManifests {
            pool: "fixtures/probe/probe-multiactor.yaml.tmpl",
            template: "fixtures/probe/probe-multiactor-template.yaml.tmpl",
        };
        deploy_hive_fixture(
            &t,
            &clients(),
            manifests,
            &bucket,
            "multiactor",
            &[],
            FixtureOptions::default(),
        )
        .await;

        let names = ["ma-first", "ma-second"];
        let mut pods = Vec::new();
        for name in names {
            create_actor(&t, &hive, name, object_ref(&hive, MULTIACTOR_TEMPLATE)).await;
            resume_actor(&t, &hive, name).await;
            let pod = worker_pod_of(&t, name).await;
            t.log(format!("actor {name} is RUNNING on worker pod {pod}"));
            pods.push(pod);
        }
        if pods[0] != pods[1] {
            t.fatal(format!(
                "actors landed on different workers ({} and {}); the pool has one replica, so one of them was not placed where expected",
                pods[0], pods[1]
            ));
        }

        let rc = router(&t).await;
        // Alternate between the Actors to catch one's tunnel serving the other.
        for _ in 0..3 {
            for name in names {
                let got = whoami(&t, &rc, name).await;
                if got.hive != hive || got.file != name {
                    t.error(format!("request for {name} was served by hive={:?} actor={:?}; each actor must be reached in its own sandbox", got.hive, got.file));
                }
            }
        }

        // Suspending one Actor leaves the other serving.
        suspend_actor(&t, &hive, names[0]).await;
        let got = whoami(&t, &rc, names[1]).await;
        if got.file != names[1] {
            t.error(format!("after suspending {}, {} answered as {:?}; suspending one actor disturbed its neighbor", names[0], names[1], got.file));
        }
        let _ = got.uid;
    });
}
